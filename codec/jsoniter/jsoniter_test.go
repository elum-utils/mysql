package jsoniter

import (
	"testing"
	"time"
)

func TestJsoniterCodec_MarshalUnmarshal(t *testing.T) {
	codec := JsoniterCodec{}
	
	original := struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}{
		Name:      "Test",
		Age:       25,
		CreatedAt: time.Now(),
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
	var result struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}
	err = codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if original.Name != result.Name {
		t.Errorf("Name mismatch: got %v, want %v", result.Name, original.Name)
	}
	if original.Age != result.Age {
		t.Errorf("Age mismatch: got %v, want %v", result.Age, original.Age)
	}
	if original.CreatedAt.Unix() != result.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: got %v, want %v", result.CreatedAt.Unix(), original.CreatedAt.Unix())
	}
}

func TestJsoniterCodec_InvalidJSON(t *testing.T) {
	codec := JsoniterCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte("{invalid json}"), &result)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestJsoniterCodec_EmptyData(t *testing.T) {
	codec := JsoniterCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

func BenchmarkJsoniterCodec_Marshal(b *testing.B) {
	codec := JsoniterCodec{}
	testData := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{
		Name: "Benchmark",
		Age:  30,
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

func BenchmarkJsoniterCodec_Unmarshal(b *testing.B) {
	codec := JsoniterCodec{}
	testData := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{
		Name: "Benchmark",
		Age:  30,
	}

	data, err := codec.Marshal(testData)
	if err != nil {
		b.Fatal(err)
	}

	var result struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := codec.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}