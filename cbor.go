package raft

import "io"

type Marshaler interface {
	MarshalCBOR(w io.Writer) error
}

type Unmarshaler interface {
	UnmarshalCBOR(r io.Reader) error
}

type Er interface {
	Marshaler
	Unmarshaler
}

type String struct {
	Value string
}
