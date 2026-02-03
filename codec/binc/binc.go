package binc

import (
	"bytes"

	"github.com/ugorji/go/codec"
)

// BincCodec implements the Codec interface using Binc serialization.
// Binc is a compact binary format designed for efficient serialization
// with support for schema evolution and fast encoding/decoding.
// This implementation is stateless and thread-safe.
type BincCodec struct{}

// Marshal serializes a Go value to a Binc-encoded byte slice.
// Creates a new encoder with a BincHandle for each operation to ensure
// clean state and thread safety. Uses bytes.Buffer for efficient byte
// accumulation without repeated allocations.
func (BincCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	h := new(codec.BincHandle)
	enc := codec.NewEncoder(&buf, h)
	err := enc.Encode(v)
	return buf.Bytes(), err
}

// Unmarshal deserializes a Binc-encoded byte slice into a Go value.
// Creates a new decoder with a BincHandle for each operation.
// The target v must be a pointer to a variable of the appropriate type.
func (BincCodec) Unmarshal(data []byte, v any) error {
	h := new(codec.BincHandle)
	dec := codec.NewDecoderBytes(data, h)
	return dec.Decode(v)
}
