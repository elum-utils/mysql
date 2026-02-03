package jsoniter

import (
	jsoniter "github.com/json-iterator/go"
)

// JsoniterCodec implements the Codec interface using the jsoniter serialization library.
// Jsoniter is a high-performance JSON library that is 100% compatible with
// the standard encoding/json package but significantly faster.
// This implementation is stateless and thread-safe.
type JsoniterCodec struct{}

// Marshal serializes a Go value to a JSON-encoded byte slice.
// It delegates to jsoniter.Marshal which provides drop-in compatibility
// with encoding/json but with optimized performance.
// The input value v can be any Go type supported by JSON serialization.
func (JsoniterCodec) Marshal(v any) ([]byte, error) {
	return jsoniter.Marshal(v)
}

// Unmarshal deserializes a JSON-encoded byte slice into a Go value.
// It delegates to jsoniter.Unmarshal which provides drop-in compatibility
// with encoding/json but with optimized performance.
// The target v must be a pointer to a variable of the appropriate type.
func (JsoniterCodec) Unmarshal(data []byte, v any) error {
	return jsoniter.Unmarshal(data, v)
}
