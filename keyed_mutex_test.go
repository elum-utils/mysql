package mysql

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Тест: проверяем, что для одного ключа работает взаимное исключение.
func TestKeyedMutex_MutualExclusionSingleKey(t *testing.T) {
	km := NewMutex()
	var concurrently int32
	var maxConcurrent int32

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
			// Критическая секция
			cur := atomic.AddInt32(&concurrently, 1)
			// обновляем максимум
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
			// имитируем работу
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&concurrently, -1)
			if err := km.Unlock("same-key"); err != nil {
				t.Errorf("Unlock failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if maxConcurrent != 1 {
		t.Fatalf("ожидалось, что максимум одновременно выполняющихся будет 1, получили %d", maxConcurrent)
	}
}

// Тест: разные ключи не мешают друг другу.
func TestKeyedMutex_DifferentKeysParallel(t *testing.T) {
	km := NewMutex()
	const goroutines = 100
	var wg sync.WaitGroup
	var sum int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for ii := 0; ii < 10; ii++ {
				key := fmt.Sprintf("key-%v", ii) // только 10 уникальных ключей
				if err := km.Lock(key); err != nil {
					t.Errorf("Lock failed: %v", err)
					return
				}
				atomic.AddInt64(&sum, 1)
				if err := km.Unlock(key); err != nil {
					t.Errorf("Unlock failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	if sum != goroutines*10 {
		t.Fatalf("ожидалось sum=%d, получили %d", goroutines*10, sum)
	}
}

// Тест: некорректный Unlock должен вернуть ошибку.
func TestKeyedMutex_UnlockWithoutLockError(t *testing.T) {
	km := NewMutex()
	err := km.Unlock("no-key")
	if err == nil {
		t.Fatalf("ожидалась ошибка при Unlock без Lock, но её не было")
	}
}

// --------- Benchmarks ----------

// Бенчмарк: очень высокая конкуренция — все горутины используют один ключ.
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

// Бенчмарк: много ключей => низкая конкуренция.
// Заранее создаем все ключи чтобы избежать аллокаций в цикле.
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

// Бенчмарк: смешанный сценарий — небольшое количество "горячих" ключей.
func BenchmarkKeyedMutex_Mixed(b *testing.B) {
	km := NewMutex()
	const hotCount = 8
	hotKeys := make([]string, hotCount)
	coldKeys := make([]string, hotCount) // предварительно созданные cold keys
	for i := 0; i < hotCount; i++ {
		hotKeys[i] = "hot-" + strconv.Itoa(i)
		coldKeys[i] = hotKeys[i] + "-cold"
	}

	var counter uint32
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint32(&counter, 1)
			if n%10 == 0 {
				key := coldKeys[n%hotCount] // без аллокаций
				_ = km.Lock(key)
				km.Unlock(key)
			} else {
				key := hotKeys[n%hotCount]
				_ = km.Lock(key)
				km.Unlock(key)
			}
		}
	})
}
