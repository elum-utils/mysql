package proto

import (
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
)

func TestProtoCodec_MarshalUnmarshal(t *testing.T) {
	codec := ProtoCodec{}
	
	// Используем готовое protobuf сообщение из стандартной библиотеки
	original, err := anypb.New(&anypb.Any{
		TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
		Value:   []byte("test value"),
	})
	if err != nil {
		t.Fatalf("Failed to create Any message: %v", err)
	}

	// Marshal
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Unmarshal
	result := &anypb.Any{}
	err = codec.Unmarshal(data, result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Проверяем, что данные корректно сериализовались/десериализовались
	if original.TypeUrl != result.TypeUrl {
		t.Errorf("TypeUrl mismatch: got %v, want %v", result.TypeUrl, original.TypeUrl)
	}
	if string(original.Value) != string(result.Value) {
		t.Errorf("Value mismatch: got %v, want %v", string(result.Value), string(original.Value))
	}
}

func TestProtoCodec_NotProtoMessage(t *testing.T) {
	codec := ProtoCodec{}
	
	// Попытка сериализации не-proto объекта
	notProto := struct {
		Name string
		Age  int
	}{
		Name: "Test",
		Age:  25,
	}

	// Marshal должен вернуть ошибку
	_, err := codec.Marshal(notProto)
	if err == nil {
		t.Error("Expected error for non-proto message, got nil")
	} else if err.Error() != "value does not implement proto.Message" {
		t.Errorf("Expected 'value does not implement proto.Message', got %v", err)
	}

	// Unmarshal должен вернуть ошибку
	err = codec.Unmarshal([]byte{1, 2, 3}, &notProto)
	if err == nil {
		t.Error("Expected error for non-proto message, got nil")
	} else if err.Error() != "value does not implement proto.Message" {
		t.Errorf("Expected 'value does not implement proto.Message', got %v", err)
	}
}

func TestProtoCodec_InvalidData(t *testing.T) {
	codec := ProtoCodec{}
	
	result := &anypb.Any{}
	err := codec.Unmarshal([]byte{0xFF, 0xFE, 0xFD}, result)
	if err == nil {
		t.Error("Expected error for invalid protobuf data, got nil")
	}
}

func TestProtoCodec_NilPointer(t *testing.T) {
	codec := ProtoCodec{}
	
	err := codec.Unmarshal([]byte{1, 2, 3}, nil)
	if err == nil {
		t.Error("Expected error for nil pointer, got nil")
	}
}

func TestProtoCodec_EmptyData(t *testing.T) {
	codec := ProtoCodec{}
	
	result := &anypb.Any{}
	err := codec.Unmarshal([]byte{}, result)
	// Protobuf может успешно обрабатывать пустые данные, создавая пустое сообщение
	// Это нормальное поведение, поэтому не ожидаем ошибку
	if err != nil {
		t.Errorf("Unexpected error for empty data: %v", err)
	}
	
	// Проверим, что результат - пустое сообщение
	if result.TypeUrl != "" || len(result.Value) != 0 {
		t.Errorf("Expected empty message, got TypeUrl: %s, Value: %v", result.TypeUrl, result.Value)
	}
}

func TestProtoCodec_NilData(t *testing.T) {
	codec := ProtoCodec{}
	
	result := &anypb.Any{}
	err := codec.Unmarshal(nil, result)
	// nil данные также могут обрабатываться как пустое сообщение
	if err != nil {
		t.Errorf("Unexpected error for nil data: %v", err)
	}
}

func BenchmarkProtoCodec_Marshal(b *testing.B) {
	codec := ProtoCodec{}
	
	testData, err := anypb.New(&anypb.Any{
		TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
		Value:   []byte("benchmark value"),
	})
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := codec.Marshal(testData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtoCodec_Unmarshal(b *testing.B) {
	codec := ProtoCodec{}
	
	testData, err := anypb.New(&anypb.Any{
		TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
		Value:   []byte("benchmark value"),
	})
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	data, err := codec.Marshal(testData)
	if err != nil {
		b.Fatal(err)
	}

	result := &anypb.Any{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := codec.Unmarshal(data, result)
		if err != nil {
			b.Fatal(err)
		}
	}
}