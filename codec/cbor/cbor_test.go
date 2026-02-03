package cbor

import (
	"testing"
	"time"
)

// TestCborCodec_MarshalUnmarshal tests the basic functionality of the CborCodec
// by verifying that a struct with various field types can be successfully
// serialized and deserialized while preserving all field values.
func TestCborCodec_MarshalUnmarshal(t *testing.T) {
	codec := CborCodec{}

	// Create a test struct with string, integer, and time.Time fields
	original := struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		CreatedAt time.Time `json:"created_at"`
	}{
		Name:      "Test",
		Age:       25,
		CreatedAt: time.Now(),
	}

	// Test serialization to CBOR format
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Test deserialization from CBOR format
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
	// Compare timestamps by Unix seconds to avoid microsecond precision issues
	if original.CreatedAt.Unix() != result.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: got %v, want %v", result.CreatedAt.Unix(), original.CreatedAt.Unix())
	}
}

// TestCborCodec_ComplexStructure tests the codec's ability to handle
// complex nested data structures including maps, arrays, and various data types.
// This verifies that CBOR can properly encode and decode complex Go types.
func TestCborCodec_ComplexStructure(t *testing.T) {
	codec := CborCodec{}

	// Create a complex nested structure with various data types
	original := map[string]interface{}{
		"string":  "value",
		"number":  42.5,
		"boolean": true,
		"array":   []int{1, 2, 3},
		"nested": map[string]string{
			"key": "value",
		},
	}

	// Serialize the complex structure
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Deserialize back to a map
	var result map[string]interface{}
	err = codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify each field type was preserved correctly
	if original["string"] != result["string"] {
		t.Errorf("String mismatch: got %v, want %v", result["string"], original["string"])
	}
	if original["number"] != result["number"] {
		t.Errorf("Number mismatch: got %v, want %v", result["number"], original["number"])
	}
	if original["boolean"] != result["boolean"] {
		t.Errorf("Boolean mismatch: got %v, want %v", result["boolean"], original["boolean"])
	}
}

// TestCborCodec_InvalidData tests error handling when attempting to deserialize
// invalid or malformed CBOR data. This ensures the codec properly validates
// input and returns appropriate errors.
func TestCborCodec_InvalidData(t *testing.T) {
	codec := CborCodec{}

	var result interface{}
	// Provide clearly invalid CBOR data (not a valid CBOR sequence)
	err := codec.Unmarshal([]byte{0xFF, 0xFE, 0xFD}, &result)
	if err == nil {
		t.Error("Expected error for invalid CBOR data, got nil")
	}
}

// BenchmarkCborCodec_Marshal measures the performance of CBOR serialization
// for a typical data structure. This benchmark helps evaluate the efficiency
// of the CBOR encoding implementation.
func BenchmarkCborCodec_Marshal(b *testing.B) {
	codec := CborCodec{}
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

// BenchmarkCborCodec_Unmarshal measures the performance of CBOR deserialization
// for a typical data structure. The data is serialized once before the benchmark
// to isolate the unmarshaling performance.
func BenchmarkCborCodec_Unmarshal(b *testing.B) {
	codec := CborCodec{}
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
