package binc

import (
	"bytes"

	"github.com/ugorji/go/codec"
)

type BincCodec struct{}

func (BincCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	h := new(codec.BincHandle)
	enc := codec.NewEncoder(&buf, h)
	err := enc.Encode(v)
	return buf.Bytes(), err
}

func (BincCodec) Unmarshal(data []byte, v any) error {
	h := new(codec.BincHandle)
	dec := codec.NewDecoderBytes(data, h)
	return dec.Decode(v)
}
