package raft

import (
	"fmt"
)

var ErrInvalidID = fmt.Errorf("invalid ID")
var ErrLogNotFound = fmt.Errorf("log not found")
var ErrCanNotHandleClientOpFollower = fmt.Errorf("node (follower) can not handle operation")
var ErrCanNotHandleClientOpCandidate = fmt.Errorf("node (candidate) can not handle operation")

var ErrAlreadyStarted = fmt.Errorf("node already started")
var ErrNotStartedYet = fmt.Errorf("node has not started yet")

var ErrNYI = fmt.Errorf("NYI")
