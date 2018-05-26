package raft

import (
	"fmt"
)

var ErrInvalidID = fmt.Errorf("invalid ID")

var ErrLogAppendConsistency = fmt.Errorf("inconsistent log append request")
var ErrLogCorruption = fmt.Errorf("log corruption")

var ErrCanNotHandleWriteFollower = fmt.Errorf("node (follower) can not handle operation")
var ErrCanNotHandleWriteCandidate = fmt.Errorf("node (candidate) can not handle operation")
var ErrWriteFailure = fmt.Errorf("write operation failed")

var ErrAlreadyStarted = fmt.Errorf("node already started")
var ErrNotStartedYet = fmt.Errorf("node has not started yet")

var ErrNYI = fmt.Errorf("NYI")
