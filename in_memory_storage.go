package mysql

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("key not found")
)

type entryStorage struct {
	key       string
	value     []byte
	expiresIn time.Duration // храним продолжительность вместо абсолютного времени
	size      int
	prev      *entryStorage
	next      *entryStorage
}

var entryPool = sync.Pool{
	New: func() any { return &entryStorage{} },
}

type InMemoryStorage struct {
	mu           sync.RWMutex
	items        map[string]*entryStorage
	head         *entryStorage
	tail         *entryStorage
	maxSize      int
	curSize      int
	ttlCheck     time.Duration
	stopCh       chan struct{}
	creationTime time.Time // время создания хранилища для reference point
}

// NewInMemoryStorage создает новое хранилище
func NewInMemoryStorage(maxSize int, ttlCheck time.Duration) *InMemoryStorage {
	st := &InMemoryStorage{
		items:        make(map[string]*entryStorage),
		maxSize:      maxSize,
		ttlCheck:     ttlCheck,
		stopCh:       make(chan struct{}),
		creationTime: time.Now(),
	}
	go st.cleanupLoop()
	return st
}

func (s *InMemoryStorage) Get(key string) ([]byte, error) {
	data := s.GetRaw(key)
	if data == nil {
		return nil, ErrNotFound
	}
	return data, nil
}

// GetRaw оставим внутренний метод для использования напрямую
func (s *InMemoryStorage) GetRaw(key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil
	}

	// Проверка TTL
	if e.expiresIn > 0 {
		elapsed := time.Since(s.creationTime)
		if elapsed > e.expiresIn {
			s.removeElement(e)
			return nil
		}
	}

	// LRU
	s.moveToFront(e)
	return e.value
}

// Set добавляет или обновляет значение
func (s *InMemoryStorage) Set(key string, val []byte, exp time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.items[key]; ok {
		s.curSize -= old.size
		s.remove(old)
		entryPool.Put(old)
	}

	ent := entryPool.Get().(*entryStorage)
	*ent = entryStorage{ // reset entry
		key:       key,
		value:     val,
		size:      len(val),
		expiresIn: exp, // ⚡ храним duration вместо вычисленного времени
	}

	s.pushFront(ent)
	s.items[key] = ent
	s.curSize += ent.size

	// освобождаем место если нужно
	for s.curSize > s.maxSize {
		s.evict()
	}
	return nil
}

// Delete удаляет ключ
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

// Reset очищает все хранилище
func (s *InMemoryStorage) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[string]*entryStorage)
	s.head, s.tail = nil, nil
	s.curSize = 0
	s.creationTime = time.Now() // сбрасываем reference point
	return nil
}

func (s *InMemoryStorage) Close() error {
	s.Stop()
	return nil
}

// ---- Внутренние методы ----

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

func (s *InMemoryStorage) moveToFront(e *entryStorage) {
	if e == s.head {
		return
	}
	s.remove(e)
	s.pushFront(e)
}

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

func (s *InMemoryStorage) removeElement(e *entryStorage) {
	s.remove(e)
	delete(s.items, e.key)
	s.curSize -= e.size
	entryPool.Put(e)
}

func (s *InMemoryStorage) evict() {
	if s.tail == nil {
		return
	}
	s.removeElement(s.tail)
}

// cleanupLoop удаляет устаревшие записи в фоне
func (s *InMemoryStorage) cleanupLoop() {
	ticker := time.NewTicker(s.ttlCheck)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			elapsedSinceCreation := now.Sub(s.creationTime)

			for _, e := range s.items {
				if e.expiresIn > 0 && elapsedSinceCreation > e.expiresIn {
					s.removeElement(e)
					continue
				}
			}
			s.mu.Unlock()
		case <-s.stopCh:
			return
		}
	}
}

// Stop останавливает фоновую очистку
func (s *InMemoryStorage) Stop() {
	close(s.stopCh)
}
