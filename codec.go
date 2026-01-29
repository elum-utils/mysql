package mysql

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Codec defines the interface for serialization and deserialization operations.
// Implementations should provide methods to convert data between Go values and byte slices.
type Codec interface {
	// Marshal converts a Go value to a byte slice.
	// Returns an error if serialization fails.
	Marshal(v any) ([]byte, error)

	// Unmarshal converts a byte slice back to a Go value.
	// The provided value must be a pointer to the target type.
	// Returns an error if deserialization fails or if the data is malformed.
	Unmarshal(data []byte, v any) error
}

// MsgpackCodec implements the Codec interface using MessagePack serialization.
// MessagePack is a binary serialization format that is compact and efficient.
// This implementation is stateless and thread-safe.
type MsgpackCodec struct{}

// Marshal serializes a Go value to a MessagePack-encoded byte slice.
// It delegates the actual serialization to the msgpack.Marshal function.
// The input value v can be any Go type supported by MessagePack.
func (MsgpackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Unmarshal deserializes a MessagePack-encoded byte slice into a Go value.
// The target v must be a pointer to a variable of the appropriate type.
// It delegates the actual deserialization to the msgpack.Unmarshal function.
func (MsgpackCodec) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}