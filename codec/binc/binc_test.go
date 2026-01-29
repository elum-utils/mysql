package binc

import (
	"testing"
	"time"
)

// TestBincCodec_MarshalUnmarshal tests the basic functionality of the BincCodec
// by verifying that data can be successfully serialized and deserialized
// while preserving all field values, including complex types like time.Time.
func TestBincCodec_MarshalUnmarshal(t *testing.T) {
	codec := BincCodec{}
	
	// Create a test struct with various field types
	original := struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}{
		Name:      "Test",
		Age:       25,
		CreatedAt: time.Now(),
	}

	// Test serialization
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Test deserialization into a new struct
	var result struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}
	err = codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify all fields were preserved correctly
	if original.Name != result.Name {
		t.Errorf("Name mismatch: got %v, want %v", result.Name, original.Name)
	}
	if original.Age != result.Age {
		t.Errorf("Age mismatch: got %v, want %v", result.Age, original.Age)
	}
	// Compare timestamps by Unix seconds to avoid nanosecond precision issues
	if original.CreatedAt.Unix() != result.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: got %v, want %v", result.CreatedAt.Unix(), original.CreatedAt.Unix())
	}
}

// TestBincCodec_EmptyData tests error handling when attempting to deserialize
// empty or invalid data. This ensures the codec properly validates input.
func TestBincCodec_EmptyData(t *testing.T) {
	codec := BincCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

// TestBincCodec_NilPointer tests error handling when attempting to deserialize
// into a nil pointer. This ensures the codec properly validates the destination.
func TestBincCodec_NilPointer(t *testing.T) {
	codec := BincCodec{}
	
	data := []byte{1, 2, 3}
	err := codec.Unmarshal(data, nil)
	if err == nil {
		t.Error("Expected error for nil pointer, got nil")
	}
}

// BenchmarkBincCodec_Marshal measures the performance of serialization operations.
// This benchmark helps identify performance characteristics and memory allocation
// patterns of the BincCodec.Marshal method.
func BenchmarkBincCodec_Marshal(b *testing.B) {
	codec := BincCodec{}
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

// BenchmarkBincCodec_Unmarshal measures the performance of deserialization operations.
// This benchmark helps identify performance characteristics and memory allocation
// patterns of the BincCodec.Unmarshal method.
func BenchmarkBincCodec_Unmarshal(b *testing.B) {
	codec := BincCodec{}
	testData := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{
		Name: "Benchmark",
		Age:  30,
	}

	// Pre-serialize data once to avoid including marshaling in benchmark
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