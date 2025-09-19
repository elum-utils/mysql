package proto

import (
	"errors"
	"google.golang.org/protobuf/proto"
)

type ProtoCodec struct{}

func (ProtoCodec) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, ErrNotProtoMessage
	}
	return proto.Marshal(msg)
}

func (ProtoCodec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return ErrNotProtoMessage
	}
	return proto.Unmarshal(data, msg)
}

// Use a simple error value
var ErrNotProtoMessage = errors.New("value does not implement proto.Message")
