package main

import (
	"fmt"
	"github.com/fiibbb/goraft/raft"
)

func main() {
	fmt.Println("Hello Raft")
	raft.RunBasicEnsemble()
}
