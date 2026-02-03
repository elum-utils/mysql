package cbor

import (
	"github.com/fxamacker/cbor/v2"
)

// CborCodec implements the Codec interface using CBOR (Concise Binary Object Representation) serialization.
// CBOR is a binary data format designed for small code size, small message size, and extensibility,
// standardized as RFC 8949. This implementation is stateless and thread-safe.
type CborCodec struct{}

// Marshal serializes a Go value to a CBOR-encoded byte slice.
// It delegates the actual serialization to the cbor.Marshal function.
// The input value v can be any Go type supported by CBOR, including custom structs with tags.
func (CborCodec) Marshal(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

// Unmarshal deserializes a CBOR-encoded byte slice into a Go value.
// The target v must be a pointer to a variable of the appropriate type.
// It delegates the actual deserialization to the cbor.Unmarshal function.
func (CborCodec) Unmarshal(data []byte, v any) error {
	return cbor.Unmarshal(data, v)
}
