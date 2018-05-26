package raft

import (
	"time"
)

const source = "source"
const none = "none"
const initialBackOff = time.Millisecond * 100

const requestVote = "RequestVote"
const appendEntries = "AppendEntries"
const write = "Write"
const dumpState = "DumpState"

const defaultMinElectionTimeout = time.Second * 5
const defaultHeartbeatPeriod = time.Second * 3
const defaultRPCTimeout = time.Second
const defaultMaxRPCBackOff = time.Second * 30
