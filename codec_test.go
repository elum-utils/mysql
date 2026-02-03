package mysql

import "testing"

func TestMsgpackCodec_RoundTrip(t *testing.T) {
	type payload struct {
		ID   int    `msgpack:"id"`
		Name string `msgpack:"name"`
	}

	original := payload{ID: 7, Name: "alice"}
	codec := MsgpackCodec{}

	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded payload
	if err := codec.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded != original {
		t.Fatalf("expected round-trip value %+v, got %+v", original, decoded)
	}
}

func TestMsgpackCodec_MarshalError(t *testing.T) {
	codec := MsgpackCodec{}
	_, err := codec.Marshal(make(chan int))
	if err == nil {
		t.Fatalf("expected marshal error for unsupported type")
	}
}
