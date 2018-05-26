package raft

import (
	"context"
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

func init() {
	rand.Seed(time.Now().UnixNano())
}
