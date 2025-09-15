package mysql

import (
	"strconv"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := []byte("bar")

	err := store.Set(key, val, time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "bar" {
		t.Errorf("Expected %q, got %q", "bar", got)
	}
}

func TestGetExpired(t *testing.T) {
	store := NewInMemoryStorage(1024, 5*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := []byte("bar")

	_ = store.Set(key, val, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // ждём пока протухнет

	_, err := store.Get(key)
	if err == nil {
		t.Errorf("Expected ErrNotFound for expired key")
	}
}

func TestDelete(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := []byte("bar")

	_ = store.Set(key, val, time.Second)

	err := store.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(key)
	if err == nil {
		t.Errorf("Expected ErrNotFound after delete")
	}
}

func TestReset(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	_ = store.Set("a", []byte("1"), time.Second)
	_ = store.Set("b", []byte("2"), time.Second)

	err := store.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	if len(store.items) != 0 {
		t.Errorf("Expected empty store after Reset, got %d", len(store.items))
	}
}

func TestEviction(t *testing.T) {
	store := NewInMemoryStorage(10, 10*time.Millisecond) // ограничим размер
	defer store.Stop()

	// каждый ключ = 5 байт
	_ = store.Set("a", []byte("aaaaa"), time.Second)
	_ = store.Set("b", []byte("bbbbb"), time.Second)
	_ = store.Set("c", []byte("ccccc"), time.Second)

	// влезают только 2 ключа по 5 байт
	if len(store.items) != 2 {
		t.Errorf("Expected 2 items after eviction, got %d", len(store.items))
	}
}

func TestEvictionOrder(t *testing.T) {
	store := NewInMemoryStorage(10, 10*time.Millisecond) // ограничим размер
	defer store.Stop()

	_ = store.Set("a", []byte("aaaaa"), time.Second)
	_ = store.Set("b", []byte("bbbbb"), time.Second)
	_ = store.Set("c", []byte("ccccc"), time.Second)

	if len(store.items) != 2 {
		t.Errorf("Expected 2 items after eviction, got %d", len(store.items))
	}
}

// --------- Benchmarks ----------

func BenchmarkSet(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := []byte("some-random-value")
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(keys[i], val, time.Minute)
	}
}

func BenchmarkGet(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := []byte("some-random-value")
	keys := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		_ = store.Set(keys[i], val, time.Minute)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(keys[i%100000])
	}
}

func BenchmarkDelete(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := []byte("some-random-value")
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		_ = store.Set(keys[i], val, time.Minute)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Delete(keys[i])
	}
}
