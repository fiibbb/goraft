package raft

import (
	"fmt"
	"time"
)

func debug(s string, a ...interface{}) {
	fmt.Printf(s, a...)
}

func stateChange(id string, old, new ProcessState) string {
	return fmt.Sprintf("%s [--STATE-]: %s: [%v] => [%v]", ts(), id, fmtState(old), fmtState(new))
}

func ts() string {
	return time.Now().Format("03:04:05")
}

func fmtState(s ProcessState) string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	}
	panic("unrecognized state")
}
