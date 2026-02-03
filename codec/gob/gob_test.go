package gob

import (
	"testing"
	"time"
)

// TestGobCodec_MarshalUnmarshal tests the basic functionality of the GobCodec
// by verifying that a struct with various field types can be successfully
// serialized and deserialized while preserving all field values.
// This test ensures that Go's built-in gob encoding works correctly
// with our codec wrapper.
func TestGobCodec_MarshalUnmarshal(t *testing.T) {
	codec := GobCodec{}

	// Create a test struct with string, integer, and time.Time fields
	// Note: gob requires field names to be exported (start with capital letter)
	original := struct {
		Name      string
		Age       int
		CreatedAt time.Time
	}{
		Name:      "Test",
		Age:       25,
		CreatedAt: time.Now(),
	}

	// Test serialization to gob format
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Test deserialization from gob format
	var result struct {
		Name      string
		Age       int
		CreatedAt time.Time
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
	// Compare timestamps by Unix seconds to avoid nanosecond precision issues
	if original.CreatedAt.Unix() != result.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: got %v, want %v", result.CreatedAt.Unix(), original.CreatedAt.Unix())
	}
}

// TestGobCodec_NilData tests error handling when attempting to serialize
// nil values or deserialize nil data. This ensures the codec properly
// validates input according to gob's requirements.
func TestGobCodec_NilData(t *testing.T) {
	codec := GobCodec{}

	// Test marshaling nil value (should return specific gob error)
	_, err := codec.Marshal(nil)
	if err == nil {
		t.Error("Expected error for nil value, got nil")
	} else if err.Error() != "gob: cannot encode nil value" {
		t.Errorf("Expected 'gob: cannot encode nil value', got %v", err)
	}

	// Test unmarshaling nil data (should return EOF error)
	var result interface{}
	err = codec.Unmarshal(nil, &result)
	if err == nil {
		t.Error("Expected error for nil data, got nil")
	}
}

// TestGobCodec_EmptyData tests error handling when attempting to deserialize
// empty data. gob decoder expects a valid gob stream, so empty data should
// result in an error.
func TestGobCodec_EmptyData(t *testing.T) {
	codec := GobCodec{}

	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

// TestGobCodec_InvalidGobData tests error handling when attempting to deserialize
// invalid or corrupted gob data. This ensures the codec properly validates
// the gob stream format and returns appropriate errors.
func TestGobCodec_InvalidGobData(t *testing.T) {
	codec := GobCodec{}

	// Provide arbitrary bytes that don't constitute a valid gob stream
	invalidData := []byte{0xFF, 0xFE, 0xFD}

	var result interface{}
	err := codec.Unmarshal(invalidData, &result)
	if err == nil {
		t.Error("Expected error for invalid gob data, got nil")
	}
}

// BenchmarkGobCodec_Marshal measures the performance of gob serialization
// for a typical data structure. This benchmark helps evaluate the efficiency
// of Go's built-in gob encoding through our codec wrapper.
func BenchmarkGobCodec_Marshal(b *testing.B) {
	codec := GobCodec{}
	testData := struct {
		Name string
		Age  int
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

// BenchmarkGobCodec_Unmarshal measures the performance of gob deserialization
// for a typical data structure. The data is serialized once before the benchmark
// to isolate the unmarshaling performance from marshaling overhead.
func BenchmarkGobCodec_Unmarshal(b *testing.B) {
	codec := GobCodec{}
	testData := struct {
		Name string
		Age  int
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
		Name string
		Age  int
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
