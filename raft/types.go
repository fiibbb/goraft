package raft

import (
	"sync"
	"time"

	pb "github.com/fiibbb/goraft/raftpb"
	"google.golang.org/grpc"
)

type ProcessState int

const (
	Follower ProcessState = iota
	Candidate
	Leader
)

type Log struct {
	entries []*pb.LogEntry
}

type PendingLog struct {
	entries map[*pb.LogEntry]*pendingLogEntry
}

type Node struct {
	// raft states
	Id       string
	Term     uint64
	State    ProcessState
	VotedFor string

	// data states
	log          *Log
	pendingLog   *PendingLog
	commitIndex  uint64
	appliedIndex uint64

	// leader states, reinitialize these upon election
	nextIndex  map[string]uint64 // peer id -> next log entry to send to that peer (initialized to leader last log index + 1)
	matchIndex map[string]uint64 // peer id -> highest log entry index known to be replicated to that peer (initialized to 0, increases monotonically)

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

	// life cycle
	clock    clock
	started  bool
	stopChan chan interface{}
}

type fanoutStatus struct {
	sync.RWMutex
	isRunning   bool
	runningTerm uint64
}

type pendingLogEntry struct {
	entry   *pb.LogEntry
	success chan interface{}
	failure chan interface{}
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
	peerId string
	req    *pb.AppendEntriesRequest
	resp   *pb.AppendEntriesResponse
}

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
