// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// No per-test timeouts needed: makeTransport sets a 1s deadline, so a
// misaligned stream fails the call instead of hanging.

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

// pooledConns returns how many idle connections the client holds to server.
func pooledConns(client, server *NetworkTransport) int {
	client.connPoolLock.Lock()
	defer client.connPoolLock.Unlock()
	return len(client.connPool[server.LocalAddr()])
}

// requireSinglePooledConn checks the connection was returned to the pool and
// reused, which only works if the stream stayed aligned.
func requireSinglePooledConn(t *testing.T, client, server *NetworkTransport) {
	t.Helper()
	require.Equal(t, 1, pooledConns(client, server))
}

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

type sentBuffer struct {
	req  SendBufferRequest
	data []byte
}

// sendBufferRecorder answers SendBuffer by reading the whole body and
// reporting what arrived on the returned channel.
func sendBufferRecorder() (func(rpc RPC), chan sentBuffer) {
	received := make(chan sentBuffer, 8)
	return func(rpc RPC) {
		req := rpc.Command.(*SendBufferRequest)
		buf, err := io.ReadAll(rpc.Reader)
		if err != nil {
			rpc.Respond(nil, err)
			return
		}
		received <- sentBuffer{*req, buf}
		rpc.Respond(&SendBufferResponse{Ok: int64(len(buf)) == req.Size}, nil)
	}, received
}

func TestNetworkTransportCustom_SendBuffer(t *testing.T) {
	handle, received := sendBufferRecorder()
	server, client := newCustomPair(t, handle)

	for i, size := range []int{10, 1 << 20} {
		data := payload(size, byte(i))
		args := SendBufferRequest{Id: PieceId(i + 1), Shard: 3, Size: int64(size)}
		var out SendBufferResponse
		require.NoError(t, client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(data)))
		require.True(t, out.Ok)

		got := <-received
		require.Equal(t, args.Id, got.req.Id)
		require.Equal(t, args.Shard, got.req.Shard)
		require.Equal(t, args.Size, got.req.Size)
		require.Equal(t, data, got.data)
	}
	requireSinglePooledConn(t, client, server)
}

// Data longer than Size is truncated to Size; the stream stays aligned.
func TestNetworkTransportCustom_SendBuffer_LongData(t *testing.T) {
	handle, received := sendBufferRecorder()
	server, client := newCustomPair(t, handle)

	data := payload(100, 0)
	for i := 0; i < 2; i++ {
		var out SendBufferResponse
		args := SendBufferRequest{Size: 40}
		require.NoError(t, client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(data)))
		require.True(t, out.Ok)
		require.Equal(t, data[:40], (<-received).data)
	}
	requireSinglePooledConn(t, client, server)
}

// Data shorter than Size fails the call and drops the connection; the next
// call works on a fresh one.
func TestNetworkTransportCustom_SendBuffer_ShortData(t *testing.T) {
	handle, received := sendBufferRecorder()
	server, client := newCustomPair(t, handle)

	var out SendBufferResponse
	args := SendBufferRequest{Size: 100}
	err := client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(payload(40, 0)))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, pooledConns(client, server))

	// The consumer still sees the RPC, with a truncated body ending in a
	// plain EOF; comparing the length to Size is its only way to tell.
	partial := <-received
	require.Less(t, int64(len(partial.data)), partial.req.Size)

	data := payload(100, 1)
	require.NoError(t, client.SendBuffer("id", server.LocalAddr(), &args, &out, bytes.NewReader(data)))
	require.Equal(t, data, (<-received).data)
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

// recBufferStore answers RecBuffer from store, reporting the piece's shard as
// its id plus 10; a missing piece is reported as Size -1 with no body.
func recBufferStore(store map[PieceId][]byte) func(rpc RPC) {
	return func(rpc RPC) {
		req := rpc.Command.(*RecBufferRequest)
		buf, found := store[req.Id]
		if !found {
			rpc.Respond(&RecBufferResponse{Size: -1}, nil)
			return
		}
		resp := &RecBufferResponse{Shard: int16(req.Id) + 10, Size: int64(len(buf))}
		rpc.RespondWithReader(resp, bytes.NewReader(buf))
	}
}

func TestNetworkTransportCustom_RecBuffer(t *testing.T) {
	store := map[PieceId][]byte{
		1: payload(10, 1),
		2: payload(1<<20, 2),
		3: {},
	}
	server, client := newCustomPair(t, recBufferStore(store))

	for _, id := range []PieceId{1, 2, 3} {
		args := RecBufferRequest{Id: id}
		var out RecBufferResponse
		var got bytes.Buffer
		require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &args, &out, &got))
		require.Equal(t, int16(id)+10, out.Shard)
		require.Equal(t, int64(len(store[id])), out.Size)
		require.Equal(t, store[id], got.Bytes())
	}

	// Missing piece: Size -1, nothing written.
	args := RecBufferRequest{Id: 99}
	var out RecBufferResponse
	var got bytes.Buffer
	require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &args, &out, &got))
	require.Equal(t, int64(-1), out.Size)
	require.Zero(t, got.Len())

	requireSinglePooledConn(t, client, server)
}

// A reader longer than Size is truncated to Size; the stream stays aligned.
func TestNetworkTransportCustom_RecBuffer_LongReader(t *testing.T) {
	data := payload(100, 0)
	server, client := newCustomPair(t, func(rpc RPC) {
		rpc.RespondWithReader(&RecBufferResponse{Size: 40}, bytes.NewReader(data))
	})

	for i := 0; i < 2; i++ {
		var out RecBufferResponse
		var got bytes.Buffer
		require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &RecBufferRequest{}, &out, &got))
		require.Equal(t, data[:40], got.Bytes())
	}
	requireSinglePooledConn(t, client, server)
}

// A reader shorter than Size makes the server drop the connection; the caller
// gets an error and the next call works on a fresh connection.
func TestNetworkTransportCustom_RecBuffer_ShortReader(t *testing.T) {
	short := true
	server, client := newCustomPair(t, func(rpc RPC) {
		n := 100
		if short {
			n, short = 40, false
		}
		rpc.RespondWithReader(&RecBufferResponse{Size: 100}, bytes.NewReader(payload(n, 0)))
	})

	var out RecBufferResponse
	err := client.RecBuffer("id", server.LocalAddr(), &RecBufferRequest{}, &out, io.Discard)
	require.Error(t, err)
	require.Equal(t, 0, pooledConns(client, server))

	var got bytes.Buffer
	require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &RecBufferRequest{}, &out, &got))
	require.Equal(t, payload(100, 0), got.Bytes())
	requireSinglePooledConn(t, client, server)
}

// slowReader yields first, pauses, then yields rest.
type slowReader struct {
	first, rest []byte
	pause       time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(s.first) > 0 {
		n := copy(p, s.first)
		s.first = s.first[n:]
		return n, nil
	}
	if s.pause > 0 {
		time.Sleep(s.pause)
		s.pause = 0
	}
	if len(s.rest) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.rest)
	s.rest = s.rest[n:]
	return n, nil
}

// The body deadline must scale with resp.Size like SendBuffer's does: a
// body that takes longer than the base timeout but within the scaled one
// has to succeed.
func TestNetworkTransportCustom_RecBuffer_ScaledDeadline(t *testing.T) {
	// The first chunk exceeds the server's 4KB write buffer, so the response
	// and part of the body are flushed before the pause.
	data := payload(64<<10, 0)
	server, client := newCustomPair(t, func(rpc RPC) {
		body := &slowReader{first: data[:32<<10], rest: data[32<<10:], pause: 1500 * time.Millisecond}
		rpc.RespondWithReader(&RecBufferResponse{Size: int64(len(data))}, body)
	})
	// Base timeout is 1s; 64KB/16KB scales it to 4s.
	client.TimeoutScale = 16 << 10

	var out RecBufferResponse
	var got bytes.Buffer
	require.NoError(t, client.RecBuffer("id", server.LocalAddr(), &RecBufferRequest{}, &out, &got))
	require.Equal(t, data, got.Bytes())
}

func TestNetworkTransportCustom_RecBuffer_ErrorResponse(t *testing.T) {
	server, client := newCustomPair(t, func(rpc RPC) {
		rpc.Respond(nil, errors.New("shard offline"))
	})

	for i := 0; i < 2; i++ {
		var out RecBufferResponse
		err := client.RecBuffer("id", server.LocalAddr(), &RecBufferRequest{}, &out, io.Discard)
		require.EqualError(t, err, "shard offline")
	}
	requireSinglePooledConn(t, client, server)
}

// TestNetworkTransportCustom_Interleaved runs all custom RPCs back to back
// over one pooled connection; any misalignment breaks a later call.
func TestNetworkTransportCustom_Interleaved(t *testing.T) {
	stored := payload(70_000, 7)
	recv := recBufferStore(map[PieceId][]byte{1: stored})
	server, client := newCustomPair(t, func(rpc RPC) {
		switch rpc.Command.(type) {
		case *SendBufferRequest:
			_, _ = io.Copy(io.Discard, rpc.Reader)
			rpc.Respond(&SendBufferResponse{Ok: true}, nil)
		case *RecBufferRequest:
			recv(rpc)
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
		require.NoError(t, client.RecBuffer("id", addr, &RecBufferRequest{Id: 1}, &rOut, &got))
		require.Equal(t, stored, got.Bytes())

		var tOut TransferLeadershipResponse
		require.NoError(t, client.TransferLeadership("id", addr, &TransferLeadershipRequest{}, &tOut))
		require.True(t, tOut.Ok)
	}
	requireSinglePooledConn(t, client, server)
}
