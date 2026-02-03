package mysql

import (
	"strconv"
	"testing"
	"time"
)

// TestSetGet verifies basic Set and Get operations on the in-memory storage.
// Tests that a value can be stored and retrieved correctly, and that the
// storage correctly handles string values (as opposed to the original []byte design).
func TestSetGet(t *testing.T) {
	// Create storage with capacity for 1024 items and cleanup every 10ms
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop() // Ensure cleanup goroutine stops

	key := "foo"
	val := "bar" // теперь используем string вместо []byte

	// Set a value with 1 second TTL
	err := store.Set(key, val, time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Retrieve the value
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	// Compare directly since Get returns any (interface{})
	// Type assertion is done in test assertion
	if got != "bar" { // сравниваем напрямую, так как возвращается any
		t.Errorf("Expected %q, got %v", "bar", got)
	}
}

// TestGetExpired verifies that expired items are not returned by Get.
// Tests TTL expiration logic when Get is called after item expiration.
func TestGetExpired(t *testing.T) {
	store := NewInMemoryStorage(1024, 5*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	// Set item with very short TTL (5ms)
	_ = store.Set(key, val, 5*time.Millisecond)

	// Wait longer than TTL to ensure expiration
	time.Sleep(15 * time.Millisecond) // ждём пока протухнет

	// Attempt to get expired item
	_, err := store.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for expired key, got %v", err)
	}
}

// TestGetExpiredByCleanup verifies that the background cleanup goroutine
// removes expired items from the storage map.
// This tests the periodic cleanup mechanism rather than on-access expiration.
func TestGetExpiredByCleanup(t *testing.T) {
	store := NewInMemoryStorage(1024, 5*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	// Set item with short TTL
	_ = store.Set(key, val, 5*time.Millisecond)

	// Wait for cleanup goroutine to run (TTL + some margin)
	time.Sleep(15 * time.Millisecond) // ждём пока cleanup удалит

	// Directly check storage map (requires locking)
	store.mu.Lock()
	_, exists := store.items[key]
	store.mu.Unlock()

	if exists {
		t.Errorf("Expected key to be removed by cleanup")
	}
}

// TestDelete verifies that Delete removes items from storage.
// Tests that after deletion, Get returns ErrNotFound.
func TestDelete(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	key := "foo"
	val := "bar"

	_ = store.Set(key, val, time.Second)

	// Delete the item
	err := store.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify item is gone
	_, err = store.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

// TestDeleteNonExistent verifies that Delete returns ErrNotFound
// when attempting to delete a non-existent key.
func TestDeleteNonExistent(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	// Attempt to delete key that was never set
	err := store.Delete("non-existent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for non-existent key, got %v", err)
	}
}

// TestReset verifies that Reset clears all items from storage
// and resets the LRU list structure.
func TestReset(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	// Populate storage with some items
	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)

	// Reset storage
	store.Reset()

	// Verify items map is empty
	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()

	if itemsCount != 0 {
		t.Errorf("Expected empty store after Reset, got %d", itemsCount)
	}

	// Verify LRU list pointers are reset
	if store.head != nil || store.tail != nil {
		t.Errorf("Expected LRU list to be empty after Reset")
	}
}

// TestEvictionByCount verifies LRU eviction when storage reaches capacity.
// Tests that adding items beyond maxSize causes the least recently used
// item to be evicted.
func TestEvictionByCount(t *testing.T) {
	// Create storage with very small capacity (2 items)
	store := NewInMemoryStorage(2, 10*time.Millisecond) // максимум 2 элемента
	defer store.Stop()

	// Fill storage to capacity
	_ = store.Set("a", "val1", time.Second)
	_ = store.Set("b", "val2", time.Second)

	// Add third item - should evict least recently used ("a")
	_ = store.Set("c", "val3", time.Second) // должен вытеснить "a"

	// Verify only 2 items remain
	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()

	if itemsCount != 2 {
		t.Errorf("Expected 2 items after eviction, got %d", itemsCount)
	}

	// Verify "a" was evicted (first item, never accessed after insertion)
	_, err := store.Get("a")
	if err != ErrNotFound {
		t.Errorf("Expected 'a' to be evicted, but it's still present")
	}

	// Verify "b" and "c" remain
	_, err = store.Get("b")
	if err != nil {
		t.Errorf("Expected 'b' to be present, got error: %v", err)
	}

	_, err = store.Get("c")
	if err != nil {
		t.Errorf("Expected 'c' to be present, got error: %v", err)
	}
}

// TestLRUOrder verifies that LRU order is maintained correctly.
// Tests that item access (Get) updates LRU position, and that
// eviction targets the least recently used item, not just the oldest.
func TestLRUOrder(t *testing.T) {
	store := NewInMemoryStorage(3, 10*time.Millisecond)
	defer store.Stop()

	// Populate storage
	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)
	_ = store.Set("c", "3", time.Second)

	// Access "a" - should move it to most recently used position
	_, _ = store.Get("a")

	// Add "d" - should evict least recently used item ("b" since "a" was just accessed)
	_ = store.Set("d", "4", time.Second)

	// Verify storage size
	store.mu.RLock()
	itemsCount := len(store.items)
	store.mu.RUnlock()

	if itemsCount != 3 {
		t.Errorf("Expected 3 items, got %d", itemsCount)
	}

	// Verify "b" was evicted (least recently used)
	_, err := store.Get("b")
	if err != ErrNotFound {
		t.Errorf("Expected 'b' to be evicted (LRU), but it's still present")
	}
}

// TestUpdateExistingKey verifies that updating an existing key
// replaces the value and updates the TTL.
func TestUpdateExistingKey(t *testing.T) {
	store := NewInMemoryStorage(1024, 10*time.Millisecond)
	defer store.Stop()

	// Set initial value
	_ = store.Set("key", "value1", time.Second)

	// Update with new value and TTL
	_ = store.Set("key", "value2", 2*time.Second)

	// Verify new value is returned
	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "value2" {
		t.Errorf("Expected updated value %q, got %v", "value2", got)
	}
}

// TestPoolReuse verifies that entryStorage objects are properly
// recycled through sync.Pool when items are evicted or deleted.
func TestPoolReuse(t *testing.T) {
	store := NewInMemoryStorage(2, 10*time.Millisecond)
	defer store.Stop()

	// Fill storage to capacity
	_ = store.Set("a", "1", time.Second)
	_ = store.Set("b", "2", time.Second)

	// Evict "a" by adding "c"
	_ = store.Set("c", "3", time.Second)

	// "a" should be returned to pool
	// Adding "a" again should reuse pooled entry
	_ = store.Set("a", "4", time.Second)

	// Verify reused entry works correctly
	val, err := store.Get("a")
	if err != nil || val != "4" {
		t.Errorf("Pool reuse failed: got %v, err %v", val, err)
	}
}

// TestConcurrentAccess tests thread safety under concurrent access.
// Multiple goroutines perform Set, Get, and Delete operations simultaneously.
// This test primarily verifies absence of race conditions and deadlocks.
func TestConcurrentAccess(t *testing.T) {
	store := NewInMemoryStorage(1000, time.Second)
	defer store.Stop()

	numWorkers := 10
	numOperations := 100
	done := make(chan bool)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for j := 0; j < numOperations; j++ {
				// Generate unique keys per worker to minimize collisions
				key := strconv.Itoa(workerID*1000 + j)
				_ = store.Set(key, "value", time.Second)
				_, _ = store.Get(key)
				_ = store.Delete(key)
			}
			done <- true
		}(i)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		<-done
	}
}

func TestInMemoryStorage_Close(t *testing.T) {
	store := NewInMemoryStorage(10, time.Second)
	store.Close()
}

func TestInMemoryStorage_PushFront(t *testing.T) {
	store := NewInMemoryStorage(10, time.Second)
	defer store.Stop()

	first := &entryStorage{key: "first"}
	store.pushFront(first)
	if store.head != first || store.tail != first {
		t.Fatalf("expected head and tail to be the first element")
	}

	second := &entryStorage{key: "second"}
	store.pushFront(second)
	if store.head != second || store.tail != first {
		t.Fatalf("expected second to be head and first to remain tail")
	}
	if first.prev != second || second.next != first {
		t.Fatalf("expected list pointers to be updated for pushFront")
	}
}

func TestInMemoryStorage_EvictEmpty(t *testing.T) {
	store := NewInMemoryStorage(10, time.Second)
	defer store.Stop()

	store.evict()
}

// --------- Benchmarks ----------

// BenchmarkSet measures performance of Set operations.
// Tests throughput and memory allocations for setting items.
func BenchmarkSet(b *testing.B) {
	// Large storage to avoid evictions during benchmark
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := "some-random-value"

	// Pre-generate keys to avoid benchmark overhead in timing loop
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	// Reset timer to exclude setup time
	b.ResetTimer()
	b.ReportAllocs() // Report memory allocations
	for i := 0; i < b.N; i++ {
		_ = store.Set(keys[i], val, time.Minute)
	}
}

// BenchmarkGet measures performance of Get operations.
// Tests cache hit performance with a pre-populated storage.
func BenchmarkGet(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := "some-random-value"

	// Pre-populate with 100,000 items
	keys := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		_ = store.Set(keys[i], val, time.Minute)
	}

	// Benchmark Get operations with modulo to stay within range
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(keys[i%100000])
	}
}

// BenchmarkDelete measures performance of Delete operations.
// Tests deletion throughput with pre-populated storage.
func BenchmarkDelete(b *testing.B) {
	store := NewInMemoryStorage(10*1024*1024, 100*time.Millisecond)
	defer store.Stop()

	val := "some-random-value"

	// Pre-populate all items to be deleted
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		_ = store.Set(keys[i], val, time.Minute)
	}

	// Benchmark Delete operations
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Delete(keys[i])
	}
}

// BenchmarkConcurrentSetGet measures performance under concurrent access.
// Uses testing.B.RunParallel to simulate multiple goroutines.
func BenchmarkConcurrentSetGet(b *testing.B) {
	store := NewInMemoryStorage(100000, 100*time.Millisecond)
	defer store.Stop()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Generate unique keys per goroutine to minimize collisions
			key := "key" + strconv.Itoa(i)
			_ = store.Set(key, "value", time.Minute)
			_, _ = store.Get(key)
			i++
		}
	})
}
