package raft

import (
	"fmt"
	"strings"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
	"github.com/golang/protobuf/proto"
)

func newHeadLog(head *pb.LogEntry) *Log {
	if head == nil {
		head = &pb.LogEntry{Term: 0, Index: 0, Data: []byte("head")}
	}
	return &Log{
		entries: []*pb.LogEntry{head},
	}
}

func (log *Log) head() *pb.LogEntry {
	return log.entries[0]
}

func (log *Log) last() *pb.LogEntry {
	return log.entries[len(log.entries)-1]
}

func (log *Log) get(index uint64) *pb.LogEntry {
	for i := len(log.entries) - 1; i >= 0; i-- {
		if log.entries[i].Index == index {
			return log.entries[i]
		}
	}
	return nil
}

func (log *Log) appendAsFollower(req *pb.AppendEntriesRequest) error {
	if req.PrevLogIndex > log.last().Index {
		return ErrLogAppendConsistency
	}
	for i := len(log.entries) - 1; i >= 0; i-- {
		if log.entries[i].Index == req.PrevLogIndex && log.entries[i].Term == req.PrevLogTerm {
			log.entries = log.entries[:i+1]
			log.entries = append(log.entries, req.Entries...)
			return nil
		}
	}
	panic(ErrLogCorruption)
}

func (log *Log) appendAsLeader(term uint64, data []byte) {
	log.entries = append(log.entries, &pb.LogEntry{
		Term:  term,
		Index: log.last().Index + 1,
		Data:  data,
	})
}

func copyLogEntry(e *pb.LogEntry) *pb.LogEntry {
	bytes, err := proto.Marshal(e)
	if err != nil {
		panic(err)
	}
	var c pb.LogEntry
	if err = proto.Unmarshal(bytes, &c); err != nil {
		panic(err)
	}
	return &c
}

func (log *Log) tail(index uint64) []*pb.LogEntry {
	for i := len(log.entries) - 1; i >= 0; i-- {
		if log.entries[i].Index == index {
			var tail []*pb.LogEntry
			for j := i; j < len(log.entries); j++ {
				tail = append(tail, copyLogEntry(log.entries[j]))
			}
			return tail
		}
	}
	return nil
}

func (log *Log) string() string {
	var es []string
	for _, l := range log.entries {
		es = append(es, fmt.Sprintf("{t%d,i%d:%s}", l.Term, l.Index, string(l.Data)))
	}
	return fmt.Sprintf("[ %s ]", strings.Join(es, " > "))
}

func mustVerifyLog(n *Node) {
	for i, l := range n.Log.entries {
		if l.Index != uint64(i) {
			panic(fmt.Sprintf("%s: Log corruption: %dth entry has Index %d", n.Id, i, l.Index))
		}
		if i != 0 && l.Term < n.Log.entries[i-1].Term {
			panic(fmt.Sprintf("%s: Log corruption: %dth entry has Term %d, %d-1th entry has Term %d", n.Id, i, l.Term, i, n.Log.entries[i-1].Term))
		}
	}
}
