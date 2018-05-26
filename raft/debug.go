package raft

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	pb "github.com/fiibbb/goraft/raftpb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////
////////////////////////////// Test runs / /////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

var errIntercepted = fmt.Errorf("rpc intercepted")

var debugMu sync.RWMutex
var callEnabled map[string]bool

func callKey(src, dst, method string) string {
	return fmt.Sprintf("%s => %s [%s]", src, dst, method)
}

func callInfo(src, dst, method string, req, resp interface{}) string {
	var m = jsonpb.Marshaler{EmitDefaults: true}
	var reqMsg, respMsg proto.Message
	var ok bool
	switch method {
	case requestVote:
		reqMsg, ok = req.(*pb.RequestVoteRequest)
	case appendEntries:
		reqMsg, ok = req.(*pb.AppendEntriesRequest)
	case write:
		reqMsg, ok = req.(*pb.WriteRequest)
	case dumpState:
		reqMsg, ok = req.(*pb.DumpStateRequest)
	default:
		panic("unrecognized method")
	}
	if !ok {
		panic(fmt.Sprintf("casting failed on request: %s", callKey(src, dst, method)))
	}
	reqStr, err := m.MarshalToString(reqMsg)
	if err != nil {
		panic(err)
	}
	if resp != nil {
		switch method {
		case requestVote:
			respMsg, ok = resp.(*pb.RequestVoteResponse)
		case appendEntries:
			respMsg, ok = resp.(*pb.AppendEntriesResponse)
		case write:
			respMsg, ok = resp.(*pb.WriteResponse)
		case dumpState:
			respMsg, ok = resp.(*pb.DumpStateResponse)
		default:
			panic("unrecognized method")
		}
		if !ok {
			panic(fmt.Sprintf("casting failed on request: %s", callKey(src, dst, method)))
		}
		respStr, err := m.MarshalToString(respMsg)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("%s: %s => %s", callKey(src, dst, method), reqStr, respStr)
	} else {
		return fmt.Sprintf("%s: %s => intercepted", callKey(src, dst, method), reqStr)
	}
}

func isWhitedMethod(method string) bool {
	return method == write || method == dumpState
}

func debugInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	node := info.Server.(*Node)
	method := strings.Split(info.FullMethod, "/")[2]
	src := getSrc(ctx)
	dst := node.Id
	key := callKey(src, dst, method)
	debugMu.RLock()
	enabled, ok := callEnabled[key]
	debugMu.RUnlock()
	if !ok && !isWhitedMethod(method) {
		panic("call key not found")
	}
	if enabled || isWhitedMethod(method) {
		resp, err := handler(ctx, req)
		if err == nil {
			debug("%s [--CALL--]: %s\n", ts(), callInfo(src, dst, method, req, resp))
		}
		return resp, err
	} else {
		debug("%s [--INT---]: %s\n", ts(), callInfo(src, dst, method, req, nil))
		return nil, errIntercepted
	}
}

func setCalls(keys []string, enable bool) error {
	debugMu.Lock()
	defer debugMu.Unlock()
	for _, key := range keys {
		if _, ok := callEnabled[key]; !ok {
			return fmt.Errorf("call-key not found: %s", key)
		}
	}
	for _, key := range keys {
		callEnabled[key] = enable
	}
	return nil
}

func runDebugger(peers []peerArg) {

	// Enable all calls
	debugMu.Lock()
	callEnabled = make(map[string]bool)
	for _, pSrc := range peers {
		for _, pDst := range peers {
			if pSrc.id != pDst.id {
				callEnabled[callKey(pSrc.id, pDst.id, requestVote)] = true
				callEnabled[callKey(pSrc.id, pDst.id, appendEntries)] = true
			}
		}
	}
	debugMu.Unlock()

	// Set up debug http server
	m := mux.NewRouter()
	var isEnable = func(r *http.Request) bool {
		enableStr := mux.Vars(r)["enable"]
		return enableStr == "e"
	}
	var setCallsAndWriteBack = func(w http.ResponseWriter, r *http.Request, keys []string) {
		if err := setCalls(keys, isEnable(r)); err != nil {
			w.Write([]byte(err.Error()))
		} else {
			w.Write([]byte(fmt.Sprintf("%v: %s", isEnable(r), keys)))
		}
	}
	m.HandleFunc("/{src}/{dst}/{method}/{enable}", func(w http.ResponseWriter, r *http.Request) {
		keys := []string{callKey(mux.Vars(r)["src"], mux.Vars(r)["dst"], mux.Vars(r)["method"])}
		setCallsAndWriteBack(w, r, keys)
	})
	m.HandleFunc("/full/{id}/{enable}", func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		var keys []string
		for _, p := range peers {
			if p.id != id {
				keys = append(keys, callKey(p.id, id, requestVote))
				keys = append(keys, callKey(p.id, id, appendEntries))
				keys = append(keys, callKey(id, p.id, requestVote))
				keys = append(keys, callKey(id, p.id, appendEntries))
			}
		}
		setCallsAndWriteBack(w, r, keys)
	})
	m.HandleFunc("/write/{id}/{data}", func(w http.ResponseWriter, r *http.Request) {
		for _, p := range peers {
			if p.id == mux.Vars(r)["id"] {
				data := mux.Vars(r)["data"]
				err := Write([]byte(data), p.addr)
				if err != nil {
					w.Write([]byte(err.Error()))
				} else {
					w.Write([]byte("OK"))
				}
				return
			}
		}
	})
	m.HandleFunc("/states", func(w http.ResponseWriter, r *http.Request) {
		var states []string
		for _, p := range peers {
			states = append(states, DumpState(p.addr))
		}
		w.Write([]byte(strings.Join(states, "\n")))
	})
	panic(http.ListenAndServe("localhost:8090", m))
}

func RunBasicEnsemble() {
	peers := []peerArg{
		{id: "A", addr: "localhost:8080"},
		{id: "B", addr: "localhost:8081"},
		{id: "C", addr: "localhost:8082"},
	}
	for i, p := range peers {
		var ps []peerArg
		for j := 0; j < len(peers); j++ {
			if j != i {
				ps = append(ps, peers[j])
			}
		}
		n, err := NewNode(
			p.id,
			p.addr,
			ps,
			defaultMinElectionTimeout,
			defaultHeartbeatPeriod,
			defaultRPCTimeout,
			defaultMaxRPCBackOff,
			newClock(),
			[]grpc.ServerOption{grpc.UnaryInterceptor(debugInterceptor)},
		)
		if err != nil {
			panic(err)
		}
		if err := n.Start(); err != nil {
			panic(err)
		}
	}

	runDebugger(peers)
}

func withClient(addr string, f func(pb.RaftClient)) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	f(pb.NewRaftClient(conn))
}

func DumpState(addr string) string {
	var resp *pb.DumpStateResponse
	var err error
	withClient(addr, func(client pb.RaftClient) {
		resp, err = client.DumpState(context.Background(), &pb.DumpStateRequest{})
		if err != nil {
			panic(err)
		}
	})
	return resp.State
}

func Write(data []byte, addr string) error {
	var err error
	withClient(addr, func(client pb.RaftClient) {
		_, err = client.Write(context.Background(), &pb.WriteRequest{Data: data})
	})
	return err
}
