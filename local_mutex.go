package mysql

import (
	"errors"
	"sync"
	"time"
)

// MutexData holds a read-write mutex, the time of the last lock operation,
// and a timer to manage automatic unlocking after a set duration.
type MutexData struct {
	mu       *sync.RWMutex // The read-write mutex for a specific key.
	lastLock time.Time     // Timestamp of the last lock operation.
	timer    *time.Timer   // Timer to automatically unlock and remove the mutex.
}

// LocalMutex manages multiple MutexData instances mapped by keys.
// It provides thread-safe operations for acquiring and releasing locks.
type LocalMutex struct {
	mx   map[string]*MutexData // Map to hold MutexData keyed by string.
	lock sync.Mutex            // Mutex to synchronize access to the map.
}

// NewLocalMutex initializes and returns a new instance of LocalMutex,
// setting up the internal map to manage mutex entries.
func NewLocalMutex() *LocalMutex {
	return &LocalMutex{
		mx: make(map[string]*MutexData),
	}
}

// getMutexForKey retrieves the MutexData for a given key,
// creating a new MutexData entry if it doesn't already exist.
func (m *LocalMutex) getMutexForKey(key string) *MutexData {
	m.lock.Lock() // Lock to protect map access.
	defer m.lock.Unlock()

	// Create a new MutexData if it does not exist.
	if _, exists := m.mx[key]; !exists {
		m.mx[key] = &MutexData{
			mu:       &sync.RWMutex{},
			lastLock: time.Now(),
		}
	}
	return m.mx[key]
}

// startTimeoutTimer initiates a timer to remove a mutex for a given key
// after 10 seconds if it's not unlocked.
// It restarts the timer if it already exists.
func (m *LocalMutex) startTimeoutTimer(key string) {
	m.lock.Lock() // Lock to protect map access.
	defer m.lock.Unlock()

	// Stop any existing timer.
	if data, exists := m.mx[key]; exists && data.timer != nil {
		data.timer.Stop()
	}

	// Set a timer to delete the mutex after 10 seconds.
	m.mx[key].timer = time.AfterFunc(10*time.Second, func() {
		m.lock.Lock() // Lock for safe map modification.
		defer m.lock.Unlock()

		// Remove the mutex if it's still required.
		if data, exists := m.mx[key]; exists && time.Since(data.lastLock) >= 10*time.Second {
			delete(m.mx, key)
			println("Мьютекс для ключа", key, "удален из-за тайм-аута.")
		}
	})
}

// Lock acquires a write lock for the mutex associated with the given key.
// It refreshes the last lock timestamp and restarts the timeout timer.
func (m *LocalMutex) Lock(key string) error {
	data := m.getMutexForKey(key)
	data.mu.Lock()

	// Update last lock time and restart the timer.
	data.lastLock = time.Now()
	m.startTimeoutTimer(key)

	return nil
}

// Unlock releases the write lock for the mutex associated with the given key
// and stops the removal timer, removing the mutex from the map.
func (m *LocalMutex) Unlock(key string) error {
	data := m.getMutexForKey(key)
	data.mu.Unlock()

	m.lock.Lock() // Lock for safe map modification.
	defer m.lock.Unlock()

	if data.timer != nil {
		data.timer.Stop()
		delete(m.mx, key)
	}
	return nil
}

// RLock acquires a read lock for the mutex associated with the given key.
// It updates the last lock timestamp and restarts the timer.
func (m *LocalMutex) RLock(key string) error {
	data := m.getMutexForKey(key)
	data.mu.RLock()

	// Update last lock time and restart the timer.
	data.lastLock = time.Now()
	m.startTimeoutTimer(key)

	return nil
}

// RUnlock releases the read lock for the mutex associated with the given key,
// stops the removal timer, and removes the mutex from the map.
func (m *LocalMutex) RUnlock(key string) error {
	data := m.getMutexForKey(key)
	data.mu.RUnlock()

	m.lock.Lock() // Lock for safe map modification.
	defer m.lock.Unlock()

	if data.timer != nil {
		data.timer.Stop()
		delete(m.mx, key)
	}
	return nil
}

// DeleteKey removes the mutex associated with a given key from the map.
// It returns an error if the mutex does not exist.
func (m *LocalMutex) DeleteKey(key string) error {
	m.lock.Lock() // Lock for safe map modification.
	defer m.lock.Unlock()

	if _, exists := m.mx[key]; !exists {
		return errors.New("mutex for key does not exist")
	}

	delete(m.mx, key)
	return nil
}
