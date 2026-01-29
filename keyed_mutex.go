package mysql

import (
	"errors"
	"sync"
)

// entry represents a reference-counted mutex for a specific key.
// It tracks how many goroutines are currently waiting on or holding this mutex.
// When refs reaches zero, the entry can be returned to the pool for reuse.
type entry struct {
	m    sync.Mutex // Underlying mutex for the key
	refs int32      // Reference counter for concurrent lock holders
}

// KeyedMutex provides per-key mutual exclusion with automatic entry management.
// It allows different goroutines to synchronize on specific keys independently,
// while reusing entry objects via sync.Pool to reduce allocation overhead.
// This is useful for synchronizing operations on database records, cache entries,
// or any resource identified by string keys.
type KeyedMutex struct {
	mu   sync.Mutex          // Protects access to the map
	m    map[string]*entry   // Maps keys to their corresponding mutex entries
	pool sync.Pool           // Pool of reusable entry objects
}

// NewMutex creates and initializes a new KeyedMutex instance.
// The returned mutex is ready for use and manages its own internal resources.
func NewMutex() *KeyedMutex {
	return &KeyedMutex{
		m: make(map[string]*entry),
		pool: sync.Pool{
			New: func() any {
				return &entry{}
			},
		},
	}
}

// Lock acquires the mutex for the specified key.
// If the mutex for this key is already locked by another goroutine,
// Lock blocks until the mutex is available.
//
// The function uses reference counting to handle multiple concurrent
// lock requests for the same key. Each call increments the reference count,
// and the underlying entry is only removed when all references are released.
//
// Returns nil on successful lock acquisition. Errors are not expected
// during normal operation but the signature allows for future extensions.
func (k *KeyedMutex) Lock(key string) error {
	k.mu.Lock()
	e, exists := k.m[key]
	if !exists {
		// First lock for this key - get entry from pool or create new one
		e = k.pool.Get().(*entry)
		e.refs = 1
		k.m[key] = e
	} else {
		// Additional lock request for existing key - increment reference count
		e.refs++
	}
	k.mu.Unlock()

	// Acquire the actual mutex (may block here)
	e.m.Lock()
	return nil
}

// Unlock releases the mutex for the specified key.
// It must be called the same number of times as Lock for each key
// to properly release all references.
//
// Returns an error if attempting to unlock a key that is not currently locked
// or if unlock is called more times than lock for a given key.
//
// When the reference count reaches zero, the entry is removed from the map
// and returned to the pool for reuse.
func (k *KeyedMutex) Unlock(key string) error {
	k.mu.Lock()
	e, exists := k.m[key]
	if !exists {
		k.mu.Unlock()
		return errors.New("keyedmutex: unlock of unlocked key")
	}

	// Release the underlying mutex first
	e.m.Unlock()
	e.refs--

	if e.refs <= 0 {
		// No more references to this key - clean up
		delete(k.m, key)
		e.refs = 0 // Reset for pool reuse
		k.pool.Put(e)
	}
	k.mu.Unlock()
	return nil
}