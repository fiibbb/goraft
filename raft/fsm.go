package raft

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
	"github.com/fiibbb/goraft/clock"
)

func NewNode(
	id string,
	addr string,
	peers []peerArg,
	minElectionTimeout time.Duration,
	heartbeatPeriod time.Duration,
	rpcTimeout time.Duration,
	maxRPCBackOff time.Duration,
	clock clock.Clock,
) (*Node, error) {

	// Validate arguments
	if id == none {
		return nil, ErrInvalidID
	}
	for _, pArg := range peers {
		if pArg.id == none {
			return nil, ErrInvalidID
		}
	}

	// maxRPCBackOff can not be larger than heartbeatPeriod/2
	// If that happens, a failed fanout heartbeat may not get retried soon enough after the target server
	// comes back online, thus triggering an election timeout on the target server. And then the whole system
	// has a tendency to elect the servers that dropped offline for too long to become leader when they come
	// back online. That's bad because if a server drops offline for too long it may not be stable.
	if int64(maxRPCBackOff)/2 > int64(heartbeatPeriod) {
		maxRPCBackOff = time.Duration(int64(heartbeatPeriod) / 2)
	}

	// Initialize nextIndex and matchIndex.
	nextIndex := make(map[string]uint64)
	for _, p := range peers {
		nextIndex[p.id] = 0
	}
	matchIndex := make(map[string]uint64)
	for _, pArg := range peers {
		matchIndex[pArg.id] = 0
	}

	// Connect to peers
	peersMap := make(map[string]*peer)
	for _, pArg := range peers {
		peersMap[pArg.id] = &peer{
			id:     pArg.id,
			addr:   pArg.addr,
			fanout: &fanoutStatus{},
		}
	}

	// TODO: Reload state from disk.
	return &Node{

		Id:       id,
		Term:     0,
		State:    Follower,
		VotedFor: none,

		Log:          []*pb.LogEntry{{Term: 0, Index: 0, Data: "initialize"}},
		CommitIndex:  0,
		AppliedIndex: 0,

		NextIndex:  nextIndex,
		MatchIndex: matchIndex,

		peers: peersMap,

		addr:   addr,
		server: grpc.NewServer(grpc.UnaryInterceptor(debugInterceptor)),

		requestVoteChan:   make(chan *requestVoteArg),
		appendEntriesChan: make(chan *appendEntriesArg),
		clientOpChan:      make(chan *clientOpArg),

		minElectionTimeout: minElectionTimeout,
		electionTimeout:    randElectionTimeout(minElectionTimeout),
		heartbeatPeriod:    heartbeatPeriod,
		rpcTimeout:         rpcTimeout,
		maxRPCBackOff:      maxRPCBackOff,

		clock: clock,

		started:  false,
		stopChan: make(chan interface{}),
	}, nil
}

func (n *Node) Start() error {
	if n.started {
		return ErrAlreadyStarted
	}
	for _, p := range n.peers {
		conn, err := grpc.Dial(
			p.addr,
			grpc.WithInsecure(),
		)
		if err != nil {
			return err
		}
		p.client = pb.NewRaftClient(conn)
	}
	pb.RegisterRaftServer(n.server, n)
	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	go n.server.Serve(lis)
	go n.eventLoop()
	n.started = true
	return nil
}

func (n *Node) Stop() error {
	if !n.started {
		return ErrNotStartedYet
	}
	n.server.Stop()
	n.stopChan <- 1
	n.started = false
	return nil
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
///////////////////////////// Core layer RPC handler ///////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

func (n *Node) handleRequestVote(arg *requestVoteArg) {

	// Reject request with lower term.
	if arg.req.Term < n.Term {
		arg.respChan <- &pb.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: false,
		}
		return
	}

	// Fall back to follower and catch up term on requests with higher term.
	if arg.req.Term > n.Term {
		n.Term = arg.req.Term
		n.State = Follower
		n.VotedFor = none
	}

	var shouldVote = func(req *pb.RequestVoteRequest) bool {
		if n.VotedFor != none && n.VotedFor != req.CandidateId {
			return false
		}
		if req.LastLogTerm < lastLog(n).Term {
			return false
		}
		if req.LastLogTerm == lastLog(n).Term && req.LastLogIndex < lastLogGlobalIndex(n) {
			return false
		}
		return true
	}

	// Now consider voting for the candidate.
	if shouldVote(arg.req) {
		n.VotedFor = arg.req.CandidateId
		arg.respChan <- &pb.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: true,
		}
	} else {
		arg.respChan <- &pb.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: false,
		}
	}
}

func (n *Node) handleAppendEntries(arg *appendEntriesArg) bool {

	// Reject request with lower term.
	if arg.req.Term < n.Term {
		arg.respChan <- &pb.AppendEntriesResponse{
			Term:    n.Term,
			Success: false,
		}
		return false
	}

	// Fall back to follower and catch up term on requests with higher term.
	if arg.req.Term > n.Term {
		debug("%s (receiving higher term %d > %d)\n", stateChange(n.Id, n.State, Follower), arg.req.Term, n.Term)
		n.Term = arg.req.Term
		n.State = Follower
		n.VotedFor = none
	}

	// Handle heartbeat
	if len(arg.req.Entries) == 0 {
		arg.respChan <- &pb.AppendEntriesResponse{
			Term:    n.Term,
			Success: true,
		}
		return true
	}

	// Consistency check: Reject request with non-succeeding entry
	idx, err := findLog(n, arg.req.PrevLogTerm, arg.req.PrevLogIndex)
	if !(arg.req.Term > lastLog(n).Term) { // Request does not have a higher term
		if err != nil {
			arg.respChan <- &pb.AppendEntriesResponse{
				Term:    n.Term,
				Success: false,
			}
			return false
		}
	}

	// Trim off log after `PrevLogTerm/PrevLogIndex`.
	n.Log = n.Log[:idx+1]

	// Append new log entries.
	n.Log = append(n.Log, arg.req.Entries...)
	verifyLog(n.Log)

	// If leader has a higher commitIndex, catch up to that new commitIndex
	if arg.req.LeaderCommitIndex > n.CommitIndex {
		newCommitIndex := n.CommitIndex
		if uint64(len(n.Log)-1) < newCommitIndex {
			newCommitIndex = uint64(len(n.Log) - 1)
		}
		n.CommitIndex = newCommitIndex
	}

	// Write back response.
	arg.respChan <- &pb.AppendEntriesResponse{
		Term:    n.Term,
		Success: true,
	}
	return true
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// State runners //////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

func (n *Node) runAsFollower() bool {
	// Run event loop until state changes.
	var electionTimer clock.Timer
	for {
		// Previous loop iteration may have cleared timer (upon receiving valid heartbeat),
		// so re-initialize timer if timer is nil.
		if electionTimer == nil {
			electionTimer = n.clock.NewTimer(n.electionTimeout)
		}
		select {
		case <-electionTimer.C():
			debug("%s (hitting election timeout after %v)\n", stateChange(n.Id, n.State, Candidate), n.electionTimeout)
			n.State = Candidate
		case arg := <-n.requestVoteChan:
			n.handleRequestVote(arg)
		case arg := <-n.appendEntriesChan:
			if n.handleAppendEntries(arg) {
				// Clear timer so it can be reset in next loop iteration.
				electionTimer.Stop()
				electionTimer = nil
			}
		case arg := <-n.clientOpChan:
			arg.errChan <- ErrCanNotHandleClientOpFollower
		case arg := <-n.dumpStateChan:
			arg.respChan <- &pb.DumpStateResponse{State: dumpState(n)}
		case <-n.stopChan:
			return false
		}
		if n.State != Follower {
			break
		}
	}
	return true
}

func (n *Node) runAsCandidate() bool {

	// Increment term.
	// Note that by bumping term upon entering candidate state, we give the system tendency such that:
	// When a follower stops receiving heartbeats (eg when it's network goes offline), it has the
	// tendency to become the leader as soon as it goes back online, because it would be staying in candidate
	// state with a higher term after hitting election time out. This can potentially cause the undesired
	// behavior of an unstable server keeps trying to become leader. But this seem an intrinsic property
	// in Raft.
	n.Term++

	// Vote for self.
	n.VotedFor = n.Id

	// Reset electionTimeout.
	n.electionTimeout = randElectionTimeout(n.minElectionTimeout)

	// Prepare a few sync primitives for following operations.
	var workMu sync.Mutex
	done := 0
	respChan := make(chan *pb.RequestVoteResponse)
	// Only need half peers to respond. In addition, we know we already self-voted.
	respsNeeded := len(n.peers) / 2

	// Fan out RequestVote rpc to all peers.
	// For each peer, spin a loop to keep retrying until either one of the following happens:
	// - request succeeds
	// - have received enough responses (done >= respsNeeded)
	req := &pb.RequestVoteRequest{
		Term:         n.Term,
		CandidateId:  n.Id,
		LastLogTerm:  lastLog(n).Term,
		LastLogIndex: lastLog(n).Index,
	}
	for _, pArg := range n.peers {
		go func(p *peer) {
			backOff := initialBackOff
			for {
				// Check if we already have majority votes, and bail if so.
				workMu.Lock()
				if done >= respsNeeded {
					workMu.Unlock()
					return
				}
				workMu.Unlock()
				// Send request
				ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
				resp, err := p.client.RequestVote(withSrc(ctx, n.Id), req)
				// Again, check if we already have majority votes, and bail if so.
				workMu.Lock()
				if done >= respsNeeded { // Already got majority vote, bail.
					workMu.Unlock()
					cancel()
					return
				}
				// Don't have majority vote yet
				// If rpc failed, release lock and retry. Note that this has to happen after
				// we check `done >= respsNeeded`.
				if err != nil {
					workMu.Unlock()
					cancel()
					// In case of RPC failure, do exponential back-off on retry.
					expBackOff(&backOff, n.maxRPCBackOff)
					continue
				} else { // RPC succeeded, send back resp, increment `done` and return.
					respChan <- resp
					done++
					workMu.Unlock()
					cancel()
					return
				}
			}
		}(pArg)
	}

	// At this point we have fanned out `RequestVote` call to all peers, we need to handle three things:
	// - receive response from the fanned out `RequestVote` calls
	//   - if recieve majority vote, then turn to leader here.
	// - receive RequestVote call.
	// - receive AppendEntries call.
	// Note that we can not block on waiting for response of our own `RequestVote` calls and not handle
	// any RPC we receive, because then if every server in cluster gets in this state then we deadlock.
	var resps []*pb.RequestVoteResponse
	for {
		select {
		case resp := <-respChan:
			resps = append(resps, resp)
		case arg := <-n.requestVoteChan:
			n.handleRequestVote(arg)
		case arg := <-n.appendEntriesChan:
			n.handleAppendEntries(arg)
		case arg := <-n.clientOpChan:
			arg.errChan <- ErrCanNotHandleClientOpCandidate
		case arg := <-n.dumpStateChan:
			arg.respChan <- &pb.DumpStateResponse{State: dumpState(n)}
		case <-n.stopChan:
			return false
		}
		// Previous step may have changed state to something else,
		// so check and terminate here if state is no longer candidate.
		// Also need to perform some clean up to make sure there's no lingering go routines running
		// `RequestVote` rpc.
		if n.State != Candidate {
			workMu.Lock()
			// Set done to max so that fanned out go routines will stop sending things to `respChan`.
			done = len(n.peers)
			// Drain `respChan`
			for i := 0; i < len(respChan); i++ {
				<-respChan
			}
			workMu.Unlock()
			// TODO: We have already set rs.votedFor to rs.id, verify that it's ok to simply return here.
			return true
		}
		// If we have got response (including rejected votes) from majority of peers,
		// then we are ready to calculate the result.
		if len(resps) >= respsNeeded {
			break
		}
	}

	// Calculate results. Become leader if have enough votes, otherwise become follower.
	votesReceived := 1 // self-vote
	for _, resp := range resps {
		if resp.VoteGranted {
			votesReceived++
		}
	}
	if votesReceived >= (len(n.peers)+1)/2+1 {
		debug("%s (upon receiving %d votes)\n", stateChange(n.Id, n.State, Leader), votesReceived)
		n.State = Leader
	} else {
		debug("%s (upon receiving %d votes)\n", stateChange(n.Id, n.State, Follower), votesReceived)
		n.State = Follower
	}
	return true
}

func (n *Node) runAsLeader() bool {

	// Reinitialize leader states (nextIndex, matchIndex)
	for id := range n.NextIndex {
		n.NextIndex[id] = uint64(len(n.Log))
	}
	for id := range n.MatchIndex {
		n.MatchIndex[id] = 0
	}

	// Sync primitives for `broadcast` and `broadcastResp`
	appendEntriesRespChan := make(chan *appendEntriesRespBundle)
	var workMu sync.Mutex
	demoted := false

	// broadcast sends `AppendEntriesRequest`s to all peers, including both empty (heartbeat) and non-empty ones.
	var broadcast = func() {
		for id := range n.peers {
			go func(idCopy string, logLen uint64, reqCopy *pb.AppendEntriesRequest, p *peer) {
				// If there's no fanout routine running for this peer, set `isRunning` to true and `runningTerm` to
				// the current request's Term.
				// If there's already a fanout routine running for this peer, but for an older term, bump the term
				// so the older routine quits next time it checks the `runningTerm`.
				p.fanout.Lock()
				if !p.fanout.isRunning {
					p.fanout.isRunning = true
					p.fanout.runningTerm = reqCopy.Term
				} else if p.fanout.isRunning && reqCopy.Term > p.fanout.runningTerm {
					p.fanout.runningTerm = reqCopy.Term
				} else { // p.fanOut.isRunning && reqCopy.Term <= p.fanOut.runningTerm
					p.fanout.Unlock()
					return
				}
				p.fanout.Unlock()
				defer func() {
					p.fanout.Lock()
					if p.fanout.runningTerm == reqCopy.Term {
						// Only set `isRunning` to false if the Term remains the same. If the term has changed, it means
						// another routine with higher term has started.
						p.fanout.isRunning = false
					}
					p.fanout.Unlock()
				}()
				// Run retry loop to send request
				backOff := initialBackOff
				for {
					// Check if we are still running as leader, if not, don't bother doing anything.
					workMu.Lock()
					if demoted {
						workMu.Unlock()
						return
					}
					workMu.Unlock()
					// Check if there's a new routine running (which must have a higher term). If so, bail.
					p.fanout.Lock()
					if p.fanout.runningTerm > reqCopy.Term {
						p.fanout.Unlock()
						return
					}
					p.fanout.Unlock()
					// Send request.
					ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
					resp, err := p.client.AppendEntries(withSrc(ctx, n.Id), reqCopy)
					// Again, Check if we are still running as leader, if not, don't bother doing anything.
					workMu.Lock()
					if demoted {
						workMu.Unlock()
						cancel()
						return
					}
					// If rpc failed, release lock and retry. Note that this has to happen after
					// we check `demoted` so that we can bail out the loop if necessary.
					if err != nil {
						workMu.Unlock()
						cancel()
						// In case of RPC failure, do exponential back-off on retry.
						expBackOff(&backOff, n.maxRPCBackOff)
						continue
					}
					// Send back response.
					appendEntriesRespChan <- &appendEntriesRespBundle{
						resp:           resp,
						peerId:         idCopy,
						updateLogIndex: logLen,
					}
					workMu.Unlock()
					cancel()
					return
				}
			}(id, uint64(len(n.Log)), &pb.AppendEntriesRequest{
				Term:              n.Term,
				LeaderId:          n.Id,
				PrevLogTerm:       lastLog(n).Term,
				PrevLogIndex:      lastLog(n).Index,
				LeaderCommitIndex: n.CommitIndex,
				Entries:           n.Log[n.NextIndex[id]:],
			}, n.peers[id])
		}
	}

	// broadcastResp handles `AppendEntriesResponse`s sent back by peers.
	var broadcastResp = func(resp *appendEntriesRespBundle) {
		if resp.resp.Term < n.Term {
			// We may be receiving responses from a long time ago with older term. Ignore these responses.
			return
		}
		if resp.resp.Term > n.Term { // Rejected and demoted.
			workMu.Lock()
			demoted = true
			workMu.Unlock()
			debug("%s (received higher term (%d > %d) in AppendEntries [Response])\n", stateChange(n.Id, n.State, Follower), resp.resp.Term, n.Term)
			n.Term = resp.resp.Term
			n.State = Follower
			n.VotedFor = none
		} else { // resp.resp.Term == rs.Term
			if resp.resp.Success { // Update nextIndex and matchIndex for the peer
				n.NextIndex[resp.peerId] = resp.updateLogIndex
				n.MatchIndex[resp.peerId] = resp.updateLogIndex
				// Update commitIndex
				for i := n.CommitIndex; i < uint64(len(n.Log)); i++ {
					// If log[i].Term == rs.term AND majority matchIndex >= i
					// then update commitIndex to `i`.
					if n.Log[i].Term == n.Term {
						count := 0
						for id := range n.MatchIndex {
							if n.MatchIndex[id] >= i {
								count++
							}
						}
						if count > len(n.peers)/2 {
							n.CommitIndex = i
						}
					}
				}
			} else { // Decrement nextIndex and wait for next retry
				n.NextIndex[resp.peerId] = n.NextIndex[resp.peerId] - 1
			}
		}
	}

	// clientOp handles `ClientOp` sent from clients.
	var clientOp = func(arg *clientOpArg) {
		// Append to local log.
		var index uint64
		lastLogEntry := lastLog(n)
		if lastLogEntry.Term == n.Term {
			index = lastLogEntry.Index + 1
		} else {
			index = 0
		}
		newLogEntry := &pb.LogEntry{
			Term:  n.Term,
			Index: index,
			Data:  arg.req.Data,
		}
		n.Log = append(n.Log, newLogEntry)
		// TODO: Wait until log is applied to state machine to reply
		// Note that this is a bit tricky. I'm not sure I fully understand how long
		// I'm supposed to "wait" -- Consider the case where the current leader
		// gets demoted to a follower and has the log overwritten. Seems like in that
		// case we should get notified here and reply error to indicate that the
		// client operation failed.
	}

	// Run event loop until state changes.
	heartbeatTicker := n.clock.NewTicker(n.heartbeatPeriod)
	broadcast()
	for {
		select {
		case <-heartbeatTicker.C():
			broadcast()
		case resp := <-appendEntriesRespChan:
			broadcastResp(resp)
		case arg := <-n.requestVoteChan:
			n.handleRequestVote(arg)
		case arg := <-n.appendEntriesChan:
			n.handleAppendEntries(arg)
		case arg := <-n.clientOpChan:
			clientOp(arg)
		case arg := <-n.dumpStateChan:
			arg.respChan <- &pb.DumpStateResponse{State: dumpState(n)}
		case <-n.stopChan:
			return false
		}
		if n.State != Leader {
			heartbeatTicker.Stop()
			break
		}
	}
	return true
}

func (n *Node) eventLoop() {
	debug("%s (bootstrap)\n", stateChange(n.Id, Follower, Follower))
	for {
		keepRunning := true
		switch n.State {
		case Follower:
			keepRunning = n.runAsFollower()
		case Candidate:
			keepRunning = n.runAsCandidate()
		case Leader:
			keepRunning = n.runAsLeader()
		default:
			panic("unrecognized process state")
		}
		if !keepRunning {
			return
		}
	}
}