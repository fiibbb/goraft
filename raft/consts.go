package raft

import (
	"time"
)

type ProcessState int

const (
	Follower ProcessState = iota
	Candidate
	Leader
)

const source = "source"
const none = "none"
const initialBackOff = time.Millisecond * 100

const requestVote = "RequestVote"
const appendEntries = "AppendEntries"
const clientOp = "ClientOp"

const defaultMinElectionTimeout = time.Second * 5
const defaultHeartbeatPeriod = time.Second
const defaultRPCTimeout = time.Second
const defaultMaxRPCBackOff = time.Second * 30
