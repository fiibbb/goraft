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

func (log *Log) appendAsFollower(req *pb.AppendEntriesRequest) ([]*pb.LogEntry, error) {
	if req.PrevLogIndex > log.last().Index {
		return nil, ErrLogAppendConsistency
	}
	// Something's missing here -- a follower may receive request for entries that already
	// exist in its log. For example, in a cluster of 5 servers, server A writes log (1,1)
	// to local as a leader, and replicates to B. (1,1) is not committed yet because it's
	// now only on A and B. Now A crashes, and B becomes leader, B would try to replicate
	// log (1,1) back to A.
	// On a second thought, the above is not an issue: In this case, A would truncate the
	// entry (1,1) after checking `PrevLogIndex` and `PrevLogTerm`, then re-apply log (1,1).
	var overwritten []*pb.LogEntry
	for i := len(log.entries) - 1; i >= 0; i-- {
		if log.entries[i].Index == req.PrevLogIndex && log.entries[i].Term == req.PrevLogTerm {
			for j := i + 1; j < len(log.entries); j++ {
				overwritten = append(overwritten, log.entries[j])
			}
			log.entries = log.entries[:i+1]
			log.entries = append(log.entries, req.Entries...)
			return overwritten, nil
		}
	}
	panic(ErrLogCorruption)
}

func (log *Log) appendAsLeader(term uint64, data []byte) *pb.LogEntry {
	newEntry := &pb.LogEntry{
		Term:  term,
		Index: log.last().Index + 1,
		Data:  data,
	}
	log.entries = append(log.entries, newEntry)
	return newEntry
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

func newPendingLog() *PendingLog {
	return &PendingLog{entries: make(map[*pb.LogEntry]*pendingLogEntry)}
}

func newPendingLogEntry(entry *pb.LogEntry) *pendingLogEntry {
	return &pendingLogEntry{
		entry:   entry,
		success: make(chan interface{}),
		failure: make(chan interface{}),
	}
}

func (pl *PendingLog) add(entry *pendingLogEntry) {
	pl.entries[entry.entry] = entry
}

func (pl *PendingLog) get(entry *pb.LogEntry) *pendingLogEntry {
	return pl.entries[entry]
}

func (pl *PendingLog) accept(entries []*pb.LogEntry) {
	for _, e := range entries {
		if _, exists := pl.entries[e]; exists {
			pl.entries[e].success <- 1
			delete(pl.entries, e)
		}
	}
}

func (pl *PendingLog) reject(entries []*pb.LogEntry) {
	for _, e := range entries {
		if _, exists := pl.entries[e]; exists {
			pl.entries[e].failure <- 1
			delete(pl.entries, e)
		}
	}
}

func mustVerifyLog(n *Node) {
	for i, l := range n.log.entries {
		if l.Index != uint64(i) {
			panic(fmt.Sprintf("%s: Log corruption: %dth entry has Index %d", n.Id, i, l.Index))
		}
		if i != 0 && l.Term < n.log.entries[i-1].Term {
			panic(fmt.Sprintf("%s: Log corruption: %dth entry has Term %d, %d-1th entry has Term %d", n.Id, i, l.Term, i, n.log.entries[i-1].Term))
		}
	}
}
