package cbor

import (
	"github.com/fxamacker/cbor/v2"
)

type CborCodec struct{}

func (CborCodec) Marshal(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

func (CborCodec) Unmarshal(data []byte, v any) error {
	return cbor.Unmarshal(data, v)
}
