package raft

import (
	"fmt"
)

var ErrInvalidID = fmt.Errorf("invalid ID")
var ErrLogNotFound = fmt.Errorf("log not found")
var ErrInvalidRequestVoteRequest = fmt.Errorf("invalid RequestVote request")
var ErrInvalidAppendEntriesRequest = fmt.Errorf("invalid AppendEntries request")
var ErrCanNotHandleClientOpFollower = fmt.Errorf("server (follower) can not handle operation")
var ErrCanNotHandleClientOpCandidate = fmt.Errorf("server (candidate) can not handle operation")

var ErrNYI = fmt.Errorf("NYI")
