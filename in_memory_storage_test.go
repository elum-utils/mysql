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
	val := "bar" // теперь используем string вместо []byte

	err := store.Set(key, val, time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "bar" { // сравниваем напрямую, так как возвращается any
		t.Errorf("Expected %q, got %v", "bar", got)
	}
}

func TestGetExpired(t *testing.T) {
	store := NewInMemoryStorage(1024, 5*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	_ = store.Set(key, val, 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond) // ждём пока протухнет

	_, err := store.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for expired key, got %v", err)
	}
}

func TestGetExpiredByCleanup(t *testing.T) {
	store := NewInMemoryStorage(1024, 5*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	_ = store.Set(key, val, 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond) // ждём пока cleanup удалит

	store.mu.Lock()
	_, exists := store.items[key]
	store.mu.Unlock()
	
	if exists {
		t.Errorf("Expected key to be removed by cleanup")
	}
}

func TestDelete(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	_ = store.Set(key, val, time.Second)

	err := store.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	err := store.Delete("non-existent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for non-existent key, got %v", err)
	}
}

func TestReset(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)

	store.Reset()

	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()
	
	if itemsCount != 0 {
		t.Errorf("Expected empty store after Reset, got %d", itemsCount)
	}
	
	// Проверяем, что LRU список тоже очищен
	if store.head != nil || store.tail != nil {
		t.Errorf("Expected LRU list to be empty after Reset")
	}
}

func TestEvictionByCount(t *testing.T) {
	store := NewInMemoryStorage(2, 10*time.Millisecond) // максимум 2 элемента
	defer store.Stop()

	_ = store.Set("a", "val1", time.Second)
	_ = store.Set("b", "val2", time.Second)
	_ = store.Set("c", "val3", time.Second) // должен вытеснить "a"

	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()
	
	if itemsCount != 2 {
		t.Errorf("Expected 2 items after eviction, got %d", itemsCount)
	}

	// Проверяем, что "a" вытеснен
	_, err := store.Get("a")
	if err != ErrNotFound {
		t.Errorf("Expected 'a' to be evicted, but it's still present")
	}

	// Проверяем, что "b" и "c" остались
	_, err = store.Get("b")
	if err != nil {
		t.Errorf("Expected 'b' to be present, got error: %v", err)
	}
	
	_, err = store.Get("c")
	if err != nil {
		t.Errorf("Expected 'c' to be present, got error: %v", err)
	}
}

func TestLRUOrder(t *testing.T) {
	store := NewInMemoryStorage(3, 10*time.Millisecond)
	defer store.Stop()

	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)
	_ = store.Set("c", "3", time.Second)

	// Доступ к "a" должен переместить его в начало
	_, _ = store.Get("a")
	
	// Добавляем "d" - должен вытеснить "b" (самый старый неиспользованный)
	_ = store.Set("d", "4", time.Second)

	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()
	
	if itemsCount != 3 {
		t.Errorf("Expected 3 items, got %d", itemsCount)
	}

	// Проверяем, что "b" вытеснен
	_, err := store.Get("b")
	if err != ErrNotFound {
		t.Errorf("Expected 'b' to be evicted (LRU), but it's still present")
	}
}

func TestUpdateExistingKey(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	_ = store.Set("key", "value1", time.Second)
	_ = store.Set("key", "value2", 2*time.Second)

	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "value2" {
		t.Errorf("Expected updated value %q, got %v", "value2", got)
	}
}

func TestPoolReuse(t *testing.T) {
	store := NewInMemoryStorage(2, 10*time.Millisecond)
	defer store.Stop()

	// Заполняем до предела
	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)
	
	// Вытесняем "a" добавлением "c"
	_ = store.Set("c", "3", time.Second)
	
	// "a" должен быть возвращён в пул
	// Добавляем снова - должен взять из пула
	_ = store.Set("a", "4", time.Second)
	
	// Проверяем, что всё работает
	val, err := store.Get("a")
	if err != nil || val != "4" {
		t.Errorf("Pool reuse failed: got %v, err %v", val, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store := NewInMemoryStorage(1000, time.Second)
	defer store.Stop()

	numWorkers := 10
	numOperations := 100
	done := make(chan bool)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for j := 0; j < numOperations; j++ {
				key := strconv.Itoa(workerID*1000 + j)
				_ = store.Set(key, "value", time.Second)
				_, _ = store.Get(key)
				_ = store.Delete(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
}

// --------- Benchmarks ----------

func BenchmarkSet(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := "some-random-value"
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = store.Set(keys[i], val, time.Minute)
	}
}

func BenchmarkGet(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := "some-random-value"
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

	val := "some-random-value"
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

func BenchmarkConcurrentSetGet(b *testing.B) {
	store := NewInMemoryStorage(100000, 100*time.Millisecond)
	defer store.Stop()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i)
			_ = store.Set(key, "value", time.Minute)
			_, _ = store.Get(key)
			i++
		}
	})
}