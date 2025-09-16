package binc

import (
	"testing"
	"time"
)

func TestBincCodec_MarshalUnmarshal(t *testing.T) {
	codec := BincCodec{}
	
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

func TestBincCodec_EmptyData(t *testing.T) {
	codec := BincCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

func TestBincCodec_NilPointer(t *testing.T) {
	codec := BincCodec{}
	
	data := []byte{1, 2, 3}
	err := codec.Unmarshal(data, nil)
	if err == nil {
		t.Error("Expected error for nil pointer, got nil")
	}
}

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

func BenchmarkBincCodec_Unmarshal(b *testing.B) {
	codec := BincCodec{}
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