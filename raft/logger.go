package raft

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
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

func fmtLog(log []*pb.LogEntry) string {
	var es []string
	for _, l := range log {
		es = append(es, fmt.Sprintf("{t%d,i%d:%s}", l.Term, l.Index, l.Data))
	}
	return fmt.Sprintf("[ %s ]", strings.Join(es, " > "))
}
