package mysql

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrNotFound is returned when a requested key does not exist in the cache.
	ErrNotFound = errors.New("key not found")
)

// entryStorage represents a single cache entry stored in a doubly-linked list.
// It's designed for efficient LRU (Least Recently Used) eviction policy.
// The struct is pooled to reduce memory allocations and GC pressure.
type entryStorage struct {
	key       string        // Cache key identifier
	value     any           // Stored value (interface{} for type flexibility)
	expiresIn time.Duration // TTL (Time To Live) from cache creation
	prev      *entryStorage // Previous node in LRU list (nil for head)
	next      *entryStorage // Next node in LRU list (nil for tail)
}

// entryPool is a sync.Pool for recycling entryStorage instances.
// This reduces garbage collection overhead by reusing allocated memory.
var entryPool = sync.Pool{
	New: func() any { return &entryStorage{} },
}

// InMemoryStorage implements an LRU (Least Recently Used) cache with TTL support.
// It maintains items in a doubly-linked list for O(1) access and eviction,
// with a map for O(1) lookups. Thread-safe with fine-grained locking.
type InMemoryStorage struct {
	mu           sync.RWMutex             // Protects concurrent access to the cache
	items        map[string]*entryStorage // Hash table for key lookups
	head         *entryStorage            // Most recently used item (front of LRU list)
	tail         *entryStorage            // Least recently used item (back of LRU list)
	maxSize      int                      // Maximum number of items cache can hold
	curSize      int                      // Current number of items in cache
	ttlCheck     time.Duration            // Interval for periodic TTL cleanup
	stopCh       chan struct{}            // Channel to signal background cleanup stop
	creationTime time.Time                // Cache creation time for TTL calculations
}

// NewInMemoryStorage creates and initializes a new LRU cache with TTL.
// The cache starts a background goroutine for periodic expiration checks.
// maxSize determines cache capacity; ttlCheck controls TTL cleanup frequency.
func NewInMemoryStorage(maxSize int, ttlCheck time.Duration) *InMemoryStorage {
	st := &InMemoryStorage{
		items:        make(map[string]*entryStorage),
		maxSize:      maxSize,
		ttlCheck:     ttlCheck,
		stopCh:       make(chan struct{}),
		creationTime: time.Now(),
	}
	go st.cleanupLoop() // Start background cleanup goroutine
	return st
}

// Get retrieves a value from the cache by key.
// If the key exists and hasn't expired, it's moved to the front (most recently used).
// Returns ErrNotFound if key doesn't exist or has expired.
func (s *InMemoryStorage) Get(key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, ErrNotFound
	}

	// Check if entry has expired based on TTL
	if e.expiresIn > 0 && time.Since(s.creationTime) > e.expiresIn {
		s.removeElement(e) // Remove expired entry
		return nil, ErrNotFound
	}

	s.moveToFront(e) // Update LRU position
	return e.value, nil
}

// Set adds or updates a key-value pair in the cache.
// If key already exists, updates its value and TTL, moving it to front.
// If cache is at capacity, evicts the least recently used item.
// exp is TTL duration from cache creation time; 0 means no expiration.
func (s *InMemoryStorage) Set(key string, val any, exp time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update existing entry
	if old, ok := s.items[key]; ok {
		old.value = val
		old.expiresIn = exp
		s.moveToFront(old) // Update LRU position
		return nil
	}

	// Create new entry (reuse from pool if available)
	ent := entryPool.Get().(*entryStorage)
	ent.key = key
	ent.value = val
	ent.expiresIn = exp
	ent.prev = nil
	ent.next = nil

	// Add to front of LRU list
	ent.next = s.head
	if s.head != nil {
		s.head.prev = ent
	}
	s.head = ent
	if s.tail == nil {
		s.tail = ent
	}

	s.items[key] = ent
	s.curSize++

	// Evict LRU item if capacity exceeded
	if s.curSize > s.maxSize {
		s.evict()
	}

	return nil
}

// Delete removes a key-value pair from the cache.
// Returns ErrNotFound if the key doesn't exist.
func (s *InMemoryStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return ErrNotFound
	}
	s.removeElement(e)
	return nil
}

// Reset clears all entries from the cache and resets its state.
// Resets creation time for TTL calculations.
func (s *InMemoryStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[string]*entryStorage)
	s.head, s.tail = nil, nil
	s.curSize = 0
	s.creationTime = time.Now()
}

// Close stops background cleanup and releases resources.
// Implements io.Closer interface for use with defer and resource management.
func (s *InMemoryStorage) Close() {
	s.Stop()
}

// -------- Internal Methods (not exported) --------

// pushFront inserts an entry at the front of the LRU list.
// Updates head and tail pointers accordingly.
func (s *InMemoryStorage) pushFront(e *entryStorage) {
	e.prev = nil
	e.next = s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

// moveToFront moves an existing entry to the front of LRU list.
// If entry is already at front, does nothing.
func (s *InMemoryStorage) moveToFront(e *entryStorage) {
	if e == s.head {
		return
	}
	s.remove(e)
	s.pushFront(e)
}

// remove extracts an entry from the LRU list without deleting from map.
// Maintains list integrity by updating neighboring nodes' pointers.
func (s *InMemoryStorage) remove(e *entryStorage) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

// removeElement completely removes an entry from cache.
// Removes from LRU list, deletes from map, returns entry to pool.
func (s *InMemoryStorage) removeElement(e *entryStorage) {
	s.remove(e)
	delete(s.items, e.key)
	s.curSize--
	entryPool.Put(e) // Recycle for future use
}

// evict removes the least recently used item (tail) from cache.
// Called when cache exceeds its maximum capacity.
func (s *InMemoryStorage) evict() {
	if s.tail == nil {
		return
	}
	s.removeElement(s.tail)
}

// cleanupLoop runs in a background goroutine, periodically removing expired entries.
// Uses a ticker to check TTL at configured intervals.
func (s *InMemoryStorage) cleanupLoop() {
	ticker := time.NewTicker(s.ttlCheck)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			elapsed := now.Sub(s.creationTime)
			for _, e := range s.items {
				if e.expiresIn > 0 && elapsed > e.expiresIn {
					s.removeElement(e)
				}
			}
			s.mu.Unlock()
		case <-s.stopCh:
			return
		}
	}
}

// Stop signals the background cleanup loop to terminate.
// Should be called before discarding the cache to prevent goroutine leaks.
func (s *InMemoryStorage) Stop() {
	close(s.stopCh)
}
