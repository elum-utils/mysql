package cbor

import (
	"testing"
	"time"
)

func TestCborCodec_MarshalUnmarshal(t *testing.T) {
	codec := CborCodec{}
	
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

func TestCborCodec_ComplexStructure(t *testing.T) {
	codec := CborCodec{}
	
	original := map[string]interface{}{
		"string":  "value",
		"number":  42.5,
		"boolean": true,
		"array":   []int{1, 2, 3},
		"nested": map[string]string{
			"key": "value",
		},
	}

	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var result map[string]interface{}
	err = codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

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

func TestCborCodec_InvalidData(t *testing.T) {
	codec := CborCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{0xFF, 0xFE, 0xFD}, &result)
	if err == nil {
		t.Error("Expected error for invalid CBOR data, got nil")
	}
}

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

func BenchmarkCborCodec_Unmarshal(b *testing.B) {
	codec := CborCodec{}
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