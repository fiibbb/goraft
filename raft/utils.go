package raft

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	pb "github.com/fiibbb/goraft/.gen/raftpb"
	"google.golang.org/grpc/metadata"
)

func randElectionTimeout(minElectionTimeout time.Duration) time.Duration {
	return time.Duration(int64(minElectionTimeout) + rand.Int63n(int64(minElectionTimeout)))
}

func lastLog(n *Node) *pb.LogEntry {
	return n.Log[len(n.Log)-1]
}

func lastLogGlobalIndex(n *Node) uint64 {
	return uint64(len(n.Log) - 1)
}

func findLog(n *Node, term uint64, index uint64) (uint64, error) {
	for i := len(n.Log) - 1; i >= 0; i-- {
		if n.Log[i].Term == term && n.Log[i].Index == index {
			return uint64(i), nil
		}
	}
	return 0, ErrLogNotFound
}

func expBackOff(t *time.Duration, max time.Duration) {
	time.Sleep(*t)
	if *t < max {
		*t = 2 * (*t)
		if *t > max {
			*t = max
		}
	}
}

func getSrc(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	srcs, ok := md[source]
	if !ok || len(srcs) != 1 {
		return ""
	}
	return srcs[0]
}

func withSrc(ctx context.Context, src string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(source, src))
}

func dumpState(n *Node) string {
	s, err := json.Marshal(n)
	if err != nil {
		debug("failed to dump state")
	}
	return string(s)
}

func verifyLog([]*pb.LogEntry) {
	// TODO: NYI
}
