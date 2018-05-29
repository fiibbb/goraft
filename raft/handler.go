package raft

import (
	"context"

	pb "github.com/fiibbb/goraft/raftpb"
)

// RequestVote simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	arg := &requestVoteArg{
		req:      req,
		respChan: make(chan *pb.RequestVoteResponse),
		errChan:  make(chan error),
	}
	n.requestVoteChan <- arg
	select {
	case resp := <-arg.respChan:
		return resp, nil
	case err := <-arg.errChan:
		return nil, err
	}
}

// AppendEntries simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	arg := &appendEntriesArg{
		req:      req,
		respChan: make(chan *pb.AppendEntriesResponse),
		errChan:  make(chan error),
	}
	n.appendEntriesChan <- arg
	select {
	case resp := <-arg.respChan:
		return resp, nil
	case err := <-arg.errChan:
		return nil, err
	}
}

// Write simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	arg := &writeArg{
		req:      req,
		respChan: make(chan *pb.WriteResponse),
		errChan:  make(chan error),
	}
	n.writeChan <- arg
	select {
	case resp := <-arg.respChan:
		return resp, nil
	case err := <-arg.errChan:
		return nil, err
	}
}

// DumpState simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) DumpState(ctx context.Context, req *pb.DumpStateRequest) (*pb.DumpStateResponse, error) {
	arg := &dumpStateArg{
		req:      req,
		respChan: make(chan *pb.DumpStateResponse),
		errChan:  make(chan error),
	}
	n.dumpStateChan <- arg
	select {
	case resp := <-arg.respChan:
		return resp, nil
	case err := <-arg.errChan:
		return nil, err
	}
}
