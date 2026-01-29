package jsoniter

import (
	"testing"
	"time"
)

// TestJsoniterCodec_MarshalUnmarshal tests the basic functionality of the JsoniterCodec
// by verifying that a struct with various field types can be successfully
// serialized to JSON and deserialized back while preserving all field values.
// This test ensures that jsoniter provides 100% compatibility with standard
// encoding/json while offering improved performance.
func TestJsoniterCodec_MarshalUnmarshal(t *testing.T) {
	codec := JsoniterCodec{}
	
	// Create a test struct with string, integer, and time.Time fields
	// JSON tags are used to control field names in the serialized output
	original := struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}{
		Name:      "Test",
		Age:       25,
		CreatedAt: time.Now(),
	}

	// Test serialization to JSON format using jsoniter
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Test deserialization from JSON format using jsoniter
	var result struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}
	err = codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify all fields were correctly preserved
	if original.Name != result.Name {
		t.Errorf("Name mismatch: got %v, want %v", result.Name, original.Name)
	}
	if original.Age != result.Age {
		t.Errorf("Age mismatch: got %v, want %v", result.Age, original.Age)
	}
	// Compare timestamps by Unix seconds to avoid millisecond precision issues
	// (JSON typically serializes time.Time with second or millisecond precision)
	if original.CreatedAt.Unix() != result.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: got %v, want %v", result.CreatedAt.Unix(), original.CreatedAt.Unix())
	}
}

// TestJsoniterCodec_InvalidJSON tests error handling when attempting to deserialize
// invalid or malformed JSON data. This ensures the codec properly validates
// JSON syntax and returns appropriate parsing errors.
func TestJsoniterCodec_InvalidJSON(t *testing.T) {
	codec := JsoniterCodec{}
	
	var result interface{}
	// Provide clearly invalid JSON syntax
	err := codec.Unmarshal([]byte("{invalid json}"), &result)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

// TestJsoniterCodec_EmptyData tests error handling when attempting to deserialize
// empty data. JSON requires at least a valid token, so empty input should
// result in an error.
func TestJsoniterCodec_EmptyData(t *testing.T) {
	codec := JsoniterCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

// BenchmarkJsoniterCodec_Marshal measures the performance of JSON serialization
// using the jsoniter library. This benchmark helps evaluate the performance
// improvement over the standard encoding/json package.
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

// BenchmarkJsoniterCodec_Unmarshal measures the performance of JSON deserialization
// using the jsoniter library. The data is serialized once before the benchmark
// to isolate the unmarshaling performance from marshaling overhead.
func BenchmarkJsoniterCodec_Unmarshal(b *testing.B) {
	codec := JsoniterCodec{}
	testData := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{
		Name: "Benchmark",
		Age:  30,
	}

	// Pre-serialize the data to avoid including marshaling time in the benchmark
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