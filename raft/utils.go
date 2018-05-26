package raft

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"google.golang.org/grpc/metadata"
)

func randElectionTimeout(minElectionTimeout time.Duration) time.Duration {
	return time.Duration(int64(minElectionTimeout) + rand.Int63n(int64(minElectionTimeout)))
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

func init() {
	rand.Seed(time.Now().UnixNano())
}
