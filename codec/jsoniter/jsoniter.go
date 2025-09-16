package jsoniter

import (
	jsoniter "github.com/json-iterator/go"
)

type JsoniterCodec struct{}

func (JsoniterCodec) Marshal(v any) ([]byte, error) {
	return jsoniter.Marshal(v)
}

func (JsoniterCodec) Unmarshal(data []byte, v any) error {
	return jsoniter.Unmarshal(data, v)
}
