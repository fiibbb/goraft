package raft

import (
	"context"
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
	// leader election states
	Id       string
	Term     uint64
	State    ProcessState
	VotedFor string

	// log replication states (all nodes)
	log          *Log
	pendingLog   *PendingLog
	commitIndex  uint64
	appliedIndex uint64
	// log replication states (leader only, reinitialize these upon election)
	nextIndex  map[string]uint64 // peer id -> next log entry to send to that peer (initialized to leader last log index + 1)
	matchIndex map[string]uint64 // peer id -> highest log entry index known to be replicated to that peer (initialized to 0, increases monotonically)

	// peer state
	peers map[string]*peer

	// grpc server state
	addr   string
	server *grpc.Server

	// channels for rpc
	requestVoteChan   chan *requestVoteArg
	appendEntriesChan chan *appendEntriesArg
	writeChan         chan *writeArg
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
	ctx      context.Context
	req      *pb.RequestVoteRequest
	respChan chan *pb.RequestVoteResponse
	errChan  chan error
}

type appendEntriesArg struct {
	ctx      context.Context
	req      *pb.AppendEntriesRequest
	respChan chan *pb.AppendEntriesResponse
	errChan  chan error
}

type writeArg struct {
	ctx      context.Context
	req      *pb.WriteRequest
	respChan chan *pb.WriteResponse
	errChan  chan error
}

type dumpStateArg struct {
	ctx      context.Context
	req      *pb.DumpStateRequest
	respChan chan *pb.DumpStateResponse
	errChan  chan error
}
