package raft

import (
	"context"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
)

// RequestVote simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	respChan := make(chan *pb.RequestVoteResponse)
	errChan := make(chan error)
	n.requestVoteChan <- &requestVoteArg{
		req:      req,
		respChan: respChan,
		errChan:  errChan,
	}
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	}
}

// AppendEntries simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	respChan := make(chan *pb.AppendEntriesResponse)
	errChan := make(chan error)
	n.appendEntriesChan <- &appendEntriesArg{
		req:      req,
		respChan: respChan,
		errChan:  errChan,
	}
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	}
}

// ClientOp simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) ClientOp(ctx context.Context, req *pb.ClientOpRequest) (*pb.ClientOpResponse, error) {
	respChan := make(chan *pb.ClientOpResponse)
	errChan := make(chan error)
	n.clientOpChan <- &clientOpArg{
		req:      req,
		respChan: respChan,
		errChan:  errChan,
	}
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	}
}

// DumpState simply forwards the requests to the Raft process so they can be handled in serial.
func (n *Node) DumpState(ctx context.Context, req *pb.DumpStateRequest) (*pb.DumpStateResponse, error) {
	respChan := make(chan *pb.DumpStateResponse)
	errChan := make(chan error)
	n.dumpStateChan <- &dumpStateArg{
		req:      req,
		respChan: respChan,
		errChan:  errChan,
	}
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	}
}
