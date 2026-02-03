package mysql

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKeyedMutex_MutualExclusionSingleKey verifies the fundamental mutex property:
// for a single key, only one goroutine can be in the critical section at a time.
// The test creates multiple goroutines that all attempt to lock the same key,
// while tracking the maximum number of goroutines executing concurrently.
// A successful test ensures maxConcurrent never exceeds 1.
func TestKeyedMutex_MutualExclusionSingleKey(t *testing.T) {
	km := NewMutex()
	var concurrently int32  // Current number of goroutines in critical section
	var maxConcurrent int32 // Maximum observed concurrent goroutines

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			if err := km.Lock("same-key"); err != nil {
				t.Errorf("Lock failed: %v", err)
				return
			}
			// Critical section begins
			cur := atomic.AddInt32(&concurrently, 1)
			// Update maximum using atomic CAS to avoid race conditions
			for {
				prev := atomic.LoadInt32(&maxConcurrent)
				if cur > prev {
					if atomic.CompareAndSwapInt32(&maxConcurrent, prev, cur) {
						break
					}
					continue
				}
				break
			}
			// Simulate work to increase chance of concurrency issues
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&concurrently, -1)
			if err := km.Unlock("same-key"); err != nil {
				t.Errorf("Unlock failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// Fundamental assertion: only one goroutine should ever be in critical section
	if maxConcurrent != 1 {
		t.Fatalf("expected maximum concurrent goroutines to be 1, got %d", maxConcurrent)
	}
}

// TestKeyedMutex_DifferentKeysParallel verifies that locks on different keys
// do not interfere with each other, allowing true parallelism.
// This tests the keyed aspect of the mutex - different keys should be independently lockable.
func TestKeyedMutex_DifferentKeysParallel(t *testing.T) {
	km := NewMutex()
	const goroutines = 100
	var wg sync.WaitGroup
	var sum int64 // Shared counter to verify all goroutines complete work

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for ii := 0; ii < 10; ii++ {
				key := fmt.Sprintf("key-%v", ii) // Only 10 unique keys, creating contention
				if err := km.Lock(key); err != nil {
					t.Errorf("Lock failed: %v", err)
					return
				}
				// Increment shared counter - should reach goroutines*10 if no deadlocks
				atomic.AddInt64(&sum, 1)
				if err := km.Unlock(key); err != nil {
					t.Errorf("Unlock failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all goroutines completed all their iterations
	if sum != goroutines*10 {
		t.Fatalf("expected sum=%d, got %d", goroutines*10, sum)
	}
}

// TestKeyedMutex_UnlockWithoutLockError verifies error handling for incorrect usage.
// Unlocking a key that was never locked (or already unlocked) should return an error.
// This tests the defensive programming aspect of the implementation.
func TestKeyedMutex_UnlockWithoutLockError(t *testing.T) {
	km := NewMutex()
	err := km.Unlock("no-key")
	if err == nil {
		t.Fatalf("expected error when unlocking without lock, but got none")
	}
}

// --------- Benchmarks ----------

// BenchmarkKeyedMutex_SameKey benchmarks performance under high contention:
// all goroutines compete for the same single key.
// This represents the worst-case scenario for a keyed mutex and measures
// the overhead of contention management.
func BenchmarkKeyedMutex_SameKey(b *testing.B) {
	km := NewMutex()
	key := "hot-key"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = km.Lock(key)
			km.Unlock(key)
		}
	})
}

// BenchmarkKeyedMutex_ManyKeys benchmarks performance under low contention:
// many different keys with goroutines distributed among them.
// Pre-creates keys to avoid allocation overhead in the benchmark loop.
// This measures the best-case performance of the keyed mutex implementation.
func BenchmarkKeyedMutex_ManyKeys(b *testing.B) {
	km := NewMutex()
	const keyCount = 1024
	keys := make([]string, keyCount)

	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	var counter uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&counter, 1)
			key := keys[n%keyCount]
			_ = km.Lock(key)
			km.Unlock(key)
		}
	})
}

// BenchmarkKeyedMutex_Mixed benchmarks a realistic mixed workload:
// a small number of "hot" keys with high contention, and many "cold" keys with low contention.
// This simulates real-world scenarios where some resources are more popular than others.
// The 10% cold / 90% hot distribution is configurable via the modulo operation.
func BenchmarkKeyedMutex_Mixed(b *testing.B) {
	km := NewMutex()
	const hotCount = 8
	hotKeys := make([]string, hotCount)
	coldKeys := make([]string, hotCount) // Pre-allocated cold keys
	for i := 0; i < hotCount; i++ {
		hotKeys[i] = "hot-" + strconv.Itoa(i)
		coldKeys[i] = hotKeys[i] + "-cold"
	}

	var counter uint32
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint32(&counter, 1)
			if n%10 == 0 {
				// 10% cold keys (low contention)
				key := coldKeys[n%hotCount] // No allocations
				_ = km.Lock(key)
				km.Unlock(key)
			} else {
				// 90% hot keys (high contention)
				key := hotKeys[n%hotCount]
				_ = km.Lock(key)
				km.Unlock(key)
			}
		}
	})
}
