package mysql

import (
	"testing"
	"time"
)

func TestLocalMutex(t *testing.T) {
	// Create a new instance of LocalMutex for testing
	localMutex := NewLocalMutex()

	// Test the basic functionality of Lock and Unlock
	t.Run("Test Lock and Unlock", func(t *testing.T) {
		key := "testKeyLock"

		// Lock the mutex with a specific key
		if err := localMutex.Lock(key); err != nil {
			t.Fatalf("Failed to lock key: %v", err) // Fail the test if Lock returns an error
		}

		// Verify that the mutex has been created and stored in the map
		if _, exists := localMutex.mx[key]; !exists {
			t.Fatal("Mutex for key was not created correctly") // Fail if the mutex is missing
		}

		// Unlock the mutex with the same key
		if err := localMutex.Unlock(key); err != nil {
			t.Fatalf("Failed to unlock key: %v", err) // Fail the test if Unlock returns an error
		}

		// Verify that the mutex has been removed from the map after unlocking
		if _, exists := localMutex.mx[key]; exists {
			t.Fatal("Mutex for key was not removed after unlock") // Fail if the mutex still exists
		}
	})

	// Test the timeout feature of the mutex, ensuring it is removed automatically after a delay
	t.Run("Test Lock with Timeout", func(t *testing.T) {
		key := "testKeyTimeout"

		// Lock the mutex with a specific key
		if err := localMutex.Lock(key); err != nil {
			t.Fatalf("Failed to lock key: %v", err) // Fail the test if Lock returns an error
		}

		// Wait for 11 seconds to allow the mutex timeout to trigger (default is 10 seconds)
		time.Sleep(11 * time.Second)

		// Verify that the mutex has been removed from the map after the timeout
		if _, exists := localMutex.mx[key]; exists {
			t.Fatal("Mutex for key was not removed after timeout") // Fail if the mutex still exists
		}
	})

	// Test the functionality of RLock (read lock) and RUnlock (read unlock)
	t.Run("Test RLock and RUnlock", func(t *testing.T) {
		key := "testKeyRLock"

		// Acquire a read lock for the specified key
		if err := localMutex.RLock(key); err != nil {
			t.Fatalf("Failed to read lock key: %v", err) // Fail the test if RLock returns an error
		}

		// Verify that the mutex has been created and stored in the map
		if _, exists := localMutex.mx[key]; !exists {
			t.Fatal("Mutex for key was not created correctly") // Fail if the mutex is missing
		}

		// Release the read lock for the specified key
		if err := localMutex.RUnlock(key); err != nil {
			t.Fatalf("Failed to read unlock key: %v", err) // Fail the test if RUnlock returns an error
		}

		// Verify that the mutex has been removed from the map after unlocking
		if _, exists := localMutex.mx[key]; exists {
			t.Fatal("Mutex for key was not removed after read unlock") // Fail if the mutex still exists
		}
	})

	// Test manually deleting a mutex by key
	t.Run("Test Delete Key", func(t *testing.T) {
		key := "testKeyDelete"

		// Lock the mutex with a specific key
		if err := localMutex.Lock(key); err != nil {
			t.Fatalf("Failed to lock key: %v", err) // Fail the test if Lock returns an error
		}

		// Manually delete the mutex for the specified key
		if err := localMutex.DeleteKey(key); err != nil {
			t.Fatalf("Failed to delete key: %v", err) // Fail the test if DeleteKey returns an error
		}

		// Verify that the mutex has been removed from the map
		if _, exists := localMutex.mx[key]; exists {
			t.Fatal("Mutex for key was not removed manually") // Fail if the mutex still exists
		}
	})

	// Test trying to delete a mutex for a key that does not exist
	t.Run("Test Delete Non-Existent Key", func(t *testing.T) {
		key := "testKeyNonExistent"

		// Attempt to delete a mutex for a non-existent key
		err := localMutex.DeleteKey(key)

		// Verify that an appropriate error is returned
		if err == nil || err.Error() != "mutex for key does not exist" {
			t.Fatalf("Expected error 'mutex for key does not exist', got: %v", err) // Fail if the error is missing or incorrect
		}
	})
}
