package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
	"github.com/fiibbb/goraft/clock"
)

func n(id, addr string, peers []peerArg, interceptor func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)) (*Node, *clock.ManualClock, error) {
	c := clock.NewManualClock()
	n, err := NewNode(
		id,
		addr,
		peers,
		defaultMinElectionTimeout,
		defaultHeartbeatPeriod,
		defaultRPCTimeout,
		defaultMaxRPCBackOff,
		c,
		[]grpc.ServerOption{grpc.UnaryInterceptor(interceptor)},
	)
	return n, c, err
}

func i(before func(req interface{}), after func(resp interface{}, err error)) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if before != nil {
			before(req)
		}
		resp, err := handler(ctx, req)
		if after != nil {
			after(resp, err)
		}
		return resp, nil
	}
}

func step(c *clock.ManualClock) {
	c.StepChan <- time.Now()
}

func stepN(c *clock.ManualClock, n int) {
	for i := 0; i < n; i++ {
		step(c)
	}
}

func TestStartStop(t *testing.T) {
	n, c, err := n("A", "localhost:9001", []peerArg{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, n)
	assert.NotNil(t, c)
	assert.NoError(t, n.Start())
	step(c)
	assert.NoError(t, n.Stop())
}

func TestInitialLeaderElection(t *testing.T) {

	ps := []peerArg{
		{"A", "localhost:9001"},
		{"B", "localhost:9002"},
		{"C", "localhost:9003"},
	}

	i0 := make(chan interface{})
	i1 := make(chan interface{})
	i2 := make(chan interface{})

	n0, c0, err0 := n(ps[0].id, ps[0].addr, []peerArg{ps[1], ps[2]}, i(func(req interface{}) {
		i0 <- req
	}, nil))
	n1, c1, err1 := n(ps[1].id, ps[1].addr, []peerArg{ps[0], ps[2]}, i(func(req interface{}) {
		i1 <- req
	}, nil))
	n2, c2, err2 := n(ps[2].id, ps[2].addr, []peerArg{ps[0], ps[1]}, i(func(req interface{}) {
		i2 <- req
	}, nil))

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	assert.NoError(t, n0.Start())
	assert.NoError(t, n1.Start())
	assert.NoError(t, n2.Start())

	assert.Equal(t, n0.State, Follower)
	assert.Equal(t, n1.State, Follower)
	assert.Equal(t, n2.State, Follower)

	// Step every node into follower event loop.
	step(c0)
	step(c1)
	step(c2)

	// Trigger election time out on n0.
	c0.Timers[0].T()

	// Let through two `RequestVote` calls sent to n1 and n2.
	req1 := <-i1
	_, ok1 := req1.(*pb.RequestVoteRequest)
	req2 := <-i2
	_, ok2 := req2.(*pb.RequestVoteRequest)

	// Verify that both requests are `RequestVote` requests.
	assert.True(t, ok1)
	assert.True(t, ok2)

	// Verify that n0 is in `Candidate` state.
	assert.Equal(t, n0.State, Candidate)

	// Step n0 once to handle one `RequestVoteResponse`.
	step(c0)

	// Step n0 once again to enter leader event loop.
	step(c0)

	// Verify that n0 is in `Leader` state.
	assert.Equal(t, n0.State, Leader)

	// Verify that n1, n2 are in `Follower state.
	assert.Equal(t, n1.State, Follower)
	assert.Equal(t, n2.State, Follower)
}
