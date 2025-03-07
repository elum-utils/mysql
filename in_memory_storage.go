package mysql

import (
	"errors"
	"sync"
	"time"
)

// InMemoryStorage provides a thread-safe in-memory cache implementation.
// It stores key-value pairs with an expiration timestamp.
type InMemoryStorage struct {
	cache map[string]CacheEntry // The in-memory cache where data is stored.
	mu    sync.RWMutex          // A read-write mutex to ensure thread-safe access to the cache.
}

// CacheEntry represents a single entry in the cache.
// It holds the value and its expiration timestamp.
type CacheEntry struct {
	Value     []byte    // The cached value.
	Timestamp time.Time // The expiration time of the entry.
}

// NewInMemoryStorage creates and initializes a new InMemoryStorage instance.
// It also starts a cleanup process to remove expired entries after one minute.
func NewInMemoryStorage() *InMemoryStorage {
	st := &InMemoryStorage{
		cache: make(map[string]CacheEntry), // Initialize the cache map.
	}

	// Launch a cleanup goroutine to remove expired entries periodically.
	go func() {
		time.Sleep(1 * time.Minute) // Wait for one minute before starting the cleanup.
		st.cleanUp()                // Perform cleanup of expired entries.
	}()

	return st
}

// Get retrieves the value associated with the given key from the cache.
// It returns an error if the key does not exist or has expired.
func (i *InMemoryStorage) Get(key string) ([]byte, error) {
	i.mu.RLock() // Acquire a read lock to safely read from the cache.
	defer i.mu.RUnlock()

	entry, ok := i.cache[key]
	if !ok {
		// Key not found in the cache.
		return nil, errors.New("key not found")
	}

	if time.Now().After(entry.Timestamp) {
		// Key has expired. Remove it from the cache and return an error.
		delete(i.cache, key)
		return nil, errors.New("key is expired")
	}

	// Return the cached value if it is still valid.
	return entry.Value, nil
}

// Set stores a key-value pair in the cache with a specified expiration duration.
func (i *InMemoryStorage) Set(key string, val []byte, exp time.Duration) error {
	i.mu.Lock() // Acquire a write lock to safely modify the cache.
	defer i.mu.Unlock()

	// Create or update the cache entry.
	i.cache[key] = CacheEntry{
		Value:     val,
		Timestamp: time.Now().Add(exp), // Set the expiration time.
	}

	return nil
}

// Delete removes a key-value pair from the cache by its key.
func (i *InMemoryStorage) Delete(key string) error {
	i.mu.Lock() // Acquire a write lock to safely modify the cache.
	defer i.mu.Unlock()

	// Remove the key from the cache.
	delete(i.cache, key)

	return nil
}

// Reset clears all entries from the cache, effectively resetting it to an empty state.
func (i *InMemoryStorage) Reset() error {
	i.mu.Lock() // Acquire a write lock to safely modify the cache.
	defer i.mu.Unlock()

	// Replace the existing cache map with a new empty map.
	i.cache = make(map[string]CacheEntry)

	return nil
}

// Close is a placeholder for any cleanup operations when the storage is closed.
// Currently, it does nothing but exists for extensibility.
func (i *InMemoryStorage) Close() error {
	return nil
}

// cleanUp removes all expired entries from the cache.
// It is typically run periodically to free up space in the cache.
func (i *InMemoryStorage) cleanUp() {
	i.mu.Lock() // Acquire a write lock to safely modify the cache.
	defer i.mu.Unlock()

	now := time.Now()
	for key, entry := range i.cache {
		// If the entry has expired, remove it from the cache.
		if now.After(entry.Timestamp) {
			delete(i.cache, key)
		}
	}
}
