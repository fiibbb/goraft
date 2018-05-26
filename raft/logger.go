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

func fmtNode(n *Node) string {
	return fmt.Sprintf(
		"{id:%s,term:%d,state:%s,votedFor:%s,commitIndex:%d,log:%s}",
		n.Id, n.Term, fmtState(n.State), n.VotedFor, n.commitIndex, n.log.string())
}

func fmtState(s ProcessState) string {
	switch s {
	case Follower:
		return "F"
	case Candidate:
		return "C"
	case Leader:
		return "L"
	}
	panic("unrecognized state")
}
