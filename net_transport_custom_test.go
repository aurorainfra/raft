// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// newCustomPair returns a server transport whose custom RPCs are answered by
// handle, and a client transport to call it with.
func newCustomPair(t *testing.T, handle func(rpc RPC)) (server, client *NetworkTransport) {
	server, err := makeTransport(t, false, "")
	require.NoError(t, err)
	client, err = makeTransport(t, false, "")
	require.NoError(t, err)

	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		_ = client.Close()
		_ = server.Close()
	})

	go func() {
		for {
			select {
			case rpc := <-server.ConsumerCustom():
				handle(rpc)
			case <-done:
				return
			}
		}
	}()
	return server, client
}

// payload returns n deterministic bytes, larger than the bufio buffers when
// n is big, so flushing and partial reads are exercised.
func payload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i) + seed
	}
	return b
}

// requireSinglePooledConn checks the connection was returned to the pool and
// reused, which only works if the stream stayed aligned.
func requireSinglePooledConn(t *testing.T, client, server *NetworkTransport) {
	t.Helper()
	client.connPoolLock.Lock()
	defer client.connPoolLock.Unlock()
	require.Len(t, client.connPool[server.LocalAddr()], 1)
}

// No per-test timeouts needed: makeTransport sets a 1s deadline, so a
// misaligned stream fails the call instead of hanging.

func TestNetworkTransportCustom_TransferLeadership(t *testing.T) {
	args := TransferLeadershipRequest{
		RPCHeader: RPCHeader{Addr: []byte("leader")},
		Id:        "node-2",
		Address:   "10.0.0.2:8300",
	}
	want := TransferLeadershipResponse{Ok: true}

	server, client := newCustomPair(t, func(rpc RPC) {
		req, ok := rpc.Command.(*TransferLeadershipRequest)
		if !ok || req.Id != args.Id || req.Address != args.Address {
			rpc.Respond(nil, errors.New("bad request"))
			return
		}
		rpc.Respond(&want, nil)
	})

	for i := 0; i < 2; i++ {
		var out TransferLeadershipResponse
		require.NoError(t, client.TransferLeadership("id", server.LocalAddr(), &args, &out))
		require.Equal(t, want, out)
	}
	requireSinglePooledConn(t, client, server)

	// Custom RPCs must not leak onto the Raft consumer channel.
	select {
	case rpc := <-server.Consumer():
		t.Fatalf("custom RPC delivered to Consumer: %T", rpc.Command)
	default:
	}
}

func TestNetworkTransportCustom_SendBuffer(t *testing.T) {
	received := make(chan []byte, 2)
	server, client := newCustomPair(t, func(rpc RPC) {
		req := rpc.Command.(*SendBufferRequest)
		buf, err := io.ReadAll(rpc.Reader)
		if err != nil {
			rpc.Respond(nil, err)
			return
		}
		received <- buf
		rpc.Respond(&SendBufferResponse{Ok: int64(len(buf)) == req.Size}, nil)
	})

	for i, size := range []int{10, 1 << 20} {
		data := payload(size, byte(i))
		args := SendBufferRequest{Id: uint64(i), Shard: 3, Size: int64(size)}
		var out SendBufferResponse
		require.NoError(t, client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(data)))
		require.True(t, out.Ok)
		require.Equal(t, data, <-received)
	}
	requireSinglePooledConn(t, client, server)
}

func TestNetworkTransportCustom_SendBuffer_ErrorResponse(t *testing.T) {
	server, client := newCustomPair(t, func(rpc RPC) {
		// Contract: consume the body even when failing.
		_, _ = io.Copy(io.Discard, rpc.Reader)
		rpc.Respond(nil, errors.New("disk full"))
	})

	for i := 0; i < 2; i++ {
		args := SendBufferRequest{Size: 1024}
		var out SendBufferResponse
		err := client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(payload(1024, 0)))
		require.EqualError(t, err, "disk full")
	}
	requireSinglePooledConn(t, client, server)
}

func TestNetworkTransportCustom_RecBuffer(t *testing.T) {
	store := map[uint64][]byte{
		1: payload(10, 1),
		2: payload(1<<20, 2),
	}
	server, client := newCustomPair(t, func(rpc RPC) {
		req := rpc.Command.(*RecBufferRequest)
		buf, found := store[req.Id]
		if !found {
			// Contract: always write exactly Size bytes.
			buf = make([]byte, req.Size)
		}
		if _, err := rpc.Writer.Write(buf); err != nil {
			rpc.Respond(nil, err)
			return
		}
		rpc.Respond(&RecBufferResponse{Found: found}, nil)
	})

	for _, id := range []uint64{1, 2} {
		args := RecBufferRequest{Id: id, Shard: 3, Size: int64(len(store[id]))}
		var out RecBufferResponse
		var got bytes.Buffer
		require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &args, &out, &got))
		require.True(t, out.Found)
		require.Equal(t, store[id], got.Bytes())
	}

	// Missing buffer: padding arrives, Found reports the miss.
	args := RecBufferRequest{Id: 99, Size: 64}
	var out RecBufferResponse
	require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &args, &out, io.Discard))
	require.False(t, out.Found)

	requireSinglePooledConn(t, client, server)
}

func TestNetworkTransportCustom_RecBuffer_ErrorResponse(t *testing.T) {
	server, client := newCustomPair(t, func(rpc RPC) {
		req := rpc.Command.(*RecBufferRequest)
		_, _ = rpc.Writer.Write(make([]byte, req.Size))
		rpc.Respond(nil, errors.New("shard offline"))
	})

	for i := 0; i < 2; i++ {
		args := RecBufferRequest{Size: 128}
		var out RecBufferResponse
		err := client.RecBuffer("id", server.LocalAddr(), &args, &out, io.Discard)
		require.EqualError(t, err, "shard offline")
	}
	requireSinglePooledConn(t, client, server)
}

// TestNetworkTransportCustom_Interleaved runs all custom RPCs back to back
// over one pooled connection; any misalignment breaks a later call.
func TestNetworkTransportCustom_Interleaved(t *testing.T) {
	stored := payload(70_000, 7)
	server, client := newCustomPair(t, func(rpc RPC) {
		switch req := rpc.Command.(type) {
		case *SendBufferRequest:
			_, _ = io.Copy(io.Discard, rpc.Reader)
			rpc.Respond(&SendBufferResponse{Ok: true}, nil)
		case *RecBufferRequest:
			_, _ = rpc.Writer.Write(stored[:req.Size])
			rpc.Respond(&RecBufferResponse{Found: true}, nil)
		case *TransferLeadershipRequest:
			rpc.Respond(&TransferLeadershipResponse{Ok: true}, nil)
		}
	})
	addr := server.LocalAddr()

	for i := 0; i < 3; i++ {
		var sOut SendBufferResponse
		require.NoError(t, client.SendBuffer("id", addr, &SendBufferRequest{Size: 5000}, &sOut, bytes.NewReader(payload(5000, 0))))
		require.True(t, sOut.Ok)

		var rOut RecBufferResponse
		var got bytes.Buffer
		require.NoError(t, client.RecBuffer("id", addr, &RecBufferRequest{Size: int64(len(stored))}, &rOut, &got))
		require.Equal(t, stored, got.Bytes())

		var tOut TransferLeadershipResponse
		require.NoError(t, client.TransferLeadership("id", addr, &TransferLeadershipRequest{}, &tOut))
		require.True(t, tOut.Ok)
	}
	requireSinglePooledConn(t, client, server)
}
