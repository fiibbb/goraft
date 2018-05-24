package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/fiibbb/goraft/clock"
)

func n(id, addr string, peers []peerArg) (*Node, *clock.ManualClock, error) {
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
	)
	return n, c, err
}

func step(c *clock.ManualClock) {
	c.StepChan <- time.Now()
}

func TestStartStop(t *testing.T) {
	n, c, err := n("A", "localhost:9001", []peerArg{})
	assert.NoError(t, err)
	assert.NotNil(t, n)
	assert.NotNil(t, c)
	assert.NoError(t, n.Start())
	step(c)
	assert.NoError(t, n.Stop())
}

func TestBootstrapState(t *testing.T) {
	ps := []peerArg{
		{"A", "localhost:9001"},
		{"B", "localhost:9002"},
		{"C", "localhost:9003"},
	}
	n0, c0, err0 := n(ps[0].id, ps[0].addr, []peerArg{ps[1], ps[2]})
	n1, c1, err1 := n(ps[1].id, ps[1].addr, []peerArg{ps[0], ps[2]})
	n2, c2, err2 := n(ps[2].id, ps[2].addr, []peerArg{ps[0], ps[1]})
	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, n0.Start())
	assert.NoError(t, n1.Start())
	assert.NoError(t, n2.Start())

	assert.Equal(t, n0.State, Follower)
	assert.Equal(t, n1.State, Follower)
	assert.Equal(t, n2.State, Follower)

	step(c0)
	step(c1)
	step(c2)

	// TODO: intercept network calls
}
