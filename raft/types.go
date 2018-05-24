package raft

import (
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
	"github.com/fiibbb/goraft/clock"
)

type requestVoteArg struct {
	req      *pb.RequestVoteRequest
	respChan chan *pb.RequestVoteResponse
	errChan  chan error
}

type appendEntriesArg struct {
	req      *pb.AppendEntriesRequest
	respChan chan *pb.AppendEntriesResponse
	errChan  chan error
}

type clientOpArg struct {
	req      *pb.ClientOpRequest
	respChan chan *pb.ClientOpResponse
	errChan  chan error
}

type dumpStateArg struct {
	req      *pb.DumpStateRequest
	respChan chan *pb.DumpStateResponse
	errChan  chan error
}

type fanoutStatus struct {
	sync.RWMutex
	isRunning   bool
	runningTerm uint64
}

type peer struct {
	id     string
	addr   string
	client pb.RaftClient
	fanout *fanoutStatus
}

type peerArg struct {
	id   string
	addr string
}

type appendEntriesRespBundle struct {
	resp           *pb.AppendEntriesResponse
	peerId         string
	updateLogIndex uint64
}

type Node struct {
	// raft states
	Id       string
	Term     uint64
	State    ProcessState
	VotedFor string

	// data states
	Log          []*pb.LogEntry
	CommitIndex  uint64
	AppliedIndex uint64

	// leader states, reinitialize these upon election
	NextIndex  map[string]uint64 // peer id -> next log entry to send to that peer (initialized to leader last log index + 1)
	MatchIndex map[string]uint64 // peer id -> next index of highest log entry known to be replicated to that peer (initialized to 0, increases monotonically)

	// peer state
	peers map[string]*peer

	// server state
	addr   string
	server *grpc.Server

	// channels for rpc
	requestVoteChan   chan *requestVoteArg
	appendEntriesChan chan *appendEntriesArg
	clientOpChan      chan *clientOpArg
	dumpStateChan     chan *dumpStateArg

	// timers
	minElectionTimeout time.Duration
	electionTimeout    time.Duration
	heartbeatPeriod    time.Duration
	rpcTimeout         time.Duration
	maxRPCBackOff      time.Duration

	// clock
	clock clock.Clock

	// life cycle
	started  bool
	stopChan chan interface{}
}
