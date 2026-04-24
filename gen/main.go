package main

import (
	"github.com/hashicorp/raft"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "raft",
		raft.AppendEntriesRequest{},
		raft.AppendEntriesResponse{},
		raft.RequestVoteRequest{},
		raft.RequestVoteResponse{},
		raft.RequestPreVoteRequest{},
		raft.RequestPreVoteResponse{},
		raft.InstallSnapshotRequest{},
		raft.InstallSnapshotResponse{},
		raft.TimeoutNowRequest{},
		raft.TimeoutNowResponse{},
		raft.RPCHeader{},
		raft.Log{},
		raft.String{},
	)
	if err != nil {
		panic(err)
	}
}
