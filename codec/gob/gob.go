package gob

import (
	"bytes"
	"encoding/gob"
)

// GobCodec implements the Codec interface using Go's built-in gob serialization.
// Gob is Go-specific binary format designed for efficient serialization
// of Go data structures with automatic handling of complex types and
// versioning. This implementation is stateless and thread-safe.
type GobCodec struct{}

// Marshal serializes a Go value to a gob-encoded byte slice.
// Uses gob.NewEncoder with a bytes.Buffer for efficient encoding.
// Note: gob requires types to be registered with gob.Register() for
// interface types or when decoding in a different process.
func (GobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

// Unmarshal deserializes a gob-encoded byte slice into a Go value.
// Uses gob.NewDecoder with a bytes.Reader for efficient decoding.
// The target v must be a pointer to a variable of the appropriate type.
// Both encoder and decoder must use the same type definitions.
func (GobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}