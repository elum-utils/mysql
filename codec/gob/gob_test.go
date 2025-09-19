package gob

import (
	"testing"
	"time"
)

func TestGobCodec_MarshalUnmarshal(t *testing.T) {
	codec := GobCodec{}
	
	original := struct {
		Name      string
		Age       int
		CreatedAt time.Time
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
		Name      string
		Age       int
		CreatedAt time.Time
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

func TestGobCodec_NilData(t *testing.T) {
	codec := GobCodec{}
	
	// Marshal nil должен вернуть ошибку
	_, err := codec.Marshal(nil)
	if err == nil {
		t.Error("Expected error for nil value, got nil")
	} else if err.Error() != "gob: cannot encode nil value" {
		t.Errorf("Expected 'gob: cannot encode nil value', got %v", err)
	}

	// Unmarshal nil данных должен вернуть ошибку
	var result interface{}
	err = codec.Unmarshal(nil, &result)
	if err == nil {
		t.Error("Expected error for nil data, got nil")
	}
}

func TestGobCodec_EmptyData(t *testing.T) {
	codec := GobCodec{}
	
	var result interface{}
	err := codec.Unmarshal([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

func TestGobCodec_InvalidGobData(t *testing.T) {
	codec := GobCodec{}
	
	// Невалидные данные gob
	invalidData := []byte{0xFF, 0xFE, 0xFD} // произвольные байты
	
	var result interface{}
	err := codec.Unmarshal(invalidData, &result)
	if err == nil {
		t.Error("Expected error for invalid gob data, got nil")
	}
}

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

func BenchmarkGobCodec_Unmarshal(b *testing.B) {
	codec := GobCodec{}
	testData := struct {
		Name string
		Age  int
	}{
		Name: "Benchmark",
		Age:  30,
	}

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