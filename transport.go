// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"io"
	"time"
)

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Reader   io.Reader
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	Reader   io.Reader
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, nil, err}
}

// RespondWithReader is used to respond with a response, body or both
func (r *RPC) RespondWithReader(resp interface{}, body io.Reader) {
	r.RespChan <- RPCResponse{resp, body, nil}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	LocalAddr() ServerAddress

	// AppendEntriesPipeline returns an interface that can be used to pipeline
	// AppendEntries requests.
	AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error)

	// AppendEntries sends the appropriate RPC to the target node.
	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node.
	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer is used to serialize a peer's address.
	EncodePeer(id ServerID, addr ServerAddress) []byte

	// DecodePeer is used to deserialize a peer's address.
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	SetHeartbeatHandler(cb func(rpc RPC))

	// TimeoutNow is used to start a leadership transfer to the target node.
	TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error
}

// TransportCustom is implemented by transports that carry the custom
// (non-Raft) RPCs. Connections are pooled and reused between calls, so the
// stream must stay aligned: consumers MUST honor the contracts below.
// Violating them leaves stray bytes on the connection that the next RPC
// will misread.
type TransportCustom interface {
	// ConsumerCustom returns a channel delivering SendBuffer, RecBuffer and
	// TransferLeadership RPCs. Every RPC must be answered via RPC.Respond.
	ConsumerCustom() <-chan RPC

	// SendBuffer sends args followed by exactly args.Size bytes read from
	// data, then waits for the response.
	//
	// Caller: data must yield at least args.Size bytes; only args.Size are
	// sent, and fewer fails the call and drops the connection.
	// Consumer: read exactly args.Size bytes from RPC.Reader before calling
	// RPC.Respond, including when responding with an error. Read with
	// io.CopyN(dst, RPC.Reader, req.Size) or io.ReadFull, not io.ReadAll or
	// io.Copy: if the caller drops the connection mid-body, RPC.Reader ends
	// with a plain io.EOF, which only a length-checked read reports as an
	// error.
	SendBuffer(id ServerID, target ServerAddress, args *SendBufferRequest, resp *SendBufferResponse, data io.Reader) error

	// RecBuffer sends args and decodes the response. When the call succeeds
	// and resp.Size >= 0, a body of exactly resp.Size bytes follows and is
	// copied into data. resp.Size == -1 means the piece was not found; no
	// body follows and data is untouched.
	//
	// Consumer: when the piece is found, answer with RPC.RespondWithReader
	// and RecBufferResponse{Size: n}, using a reader that yields at least n
	// bytes (only n are sent; fewer drops the connection). When it is not
	// found, answer with RPC.Respond and Size -1; on error, RPC.Respond with
	// the error and no reader.
	// Caller: on error, discard whatever was written to data.
	RecBuffer(id ServerID, target ServerAddress, args *RecBufferRequest, resp *RecBufferResponse, data io.Writer) error

	// TransferLeadership is a plain request/response RPC with no body.
	TransferLeadership(id ServerID, target ServerAddress, args *TransferLeadershipRequest, resp *TransferLeadershipResponse) error
}

// WithPreVote is an interface that a transport may provide which
// allows a transport to support a PreVote request.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
type WithPreVote interface {
	// RequestPreVote sends the appropriate RPC to the target node.
	RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error
}

// WithClose is an interface that a transport may provide which
// allows a transport to be shut down cleanly when a Raft instance
// shuts down.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
type WithClose interface {
	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
}

// LoopbackTransport is an interface that provides a loopback transport suitable for testing
// e.g. InmemTransport. It's there so we don't have to rewrite tests.
type LoopbackTransport interface {
	Transport   // Embedded transport reference
	WithPeers   // Embedded peer management
	WithClose   // with a close routine
	WithPreVote // with a prevote
}

// WithPeers is an interface that a transport may provide which allows for connection and
// disconnection. Unless the transport is a loopback transport, the transport specified to
// "Connect" is likely to be nil.
type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

// AppendPipeline is used for pipelining AppendEntries requests. It is used
// to increase the replication throughput by masking latency and better
// utilizing bandwidth.
type AppendPipeline interface {
	// AppendEntries is used to add another request to the pipeline.
	// The send may block which is an effective form of back-pressure.
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	// Consumer returns a channel that can be used to consume
	// response futures when they are ready.
	Consumer() <-chan AppendFuture

	// Close closes the pipeline and cancels all inflight RPCs
	Close() error
}

// AppendFuture is used to return information about a pipelined AppendEntries request.
type AppendFuture interface {
	Future

	// Start returns the time that the append request was started.
	// It is always OK to call this method.
	Start() time.Time

	// Request holds the parameters of the AppendEntries call.
	// It is always OK to call this method.
	Request() *AppendEntriesRequest

	// Response holds the results of the AppendEntries call.
	// This method must only be called after the Error
	// method returns, and will only be valid on success.
	Response() *AppendEntriesResponse
}
