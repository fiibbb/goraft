package raft

import (
	"testing"

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

func TestStartStop(t *testing.T) {
	n, c, err := n("A", "localhost:9001", []peerArg{})
	assert.NoError(t, err)
	assert.NotNil(t, n)
	assert.NotNil(t, c)
	assert.NoError(t, n.Start())
	assert.NoError(t, n.Stop())
}
