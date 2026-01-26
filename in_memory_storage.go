package mysql

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("key not found")
)

// entryStorage хранит элемент кеша
type entryStorage struct {
	key       string
	value     any
	expiresIn time.Duration
	prev      *entryStorage
	next      *entryStorage
}

var entryPool = sync.Pool{
	New: func() any { return &entryStorage{} },
}

// InMemoryStorage — LRU кеш с TTL
type InMemoryStorage struct {
	mu           sync.RWMutex
	items        map[string]*entryStorage
	head         *entryStorage
	tail         *entryStorage
	maxSize      int
	curSize      int
	ttlCheck     time.Duration
	stopCh       chan struct{}
	creationTime time.Time
}

// NewInMemoryStorage создает кеш
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

// Get возвращает значение по ключу
func (s *InMemoryStorage) Get(key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, ErrNotFound
	}

	if e.expiresIn > 0 && time.Since(s.creationTime) > e.expiresIn {
		s.removeElement(e)
		return nil, ErrNotFound
	}

	s.moveToFront(e)
	return e.value, nil
}

// Set добавляет или обновляет значение
func (s *InMemoryStorage) Set(key string, val any, exp time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Если ключ уже существует, обновляем его
	if old, ok := s.items[key]; ok {
		old.value = val
		old.expiresIn = exp
		s.moveToFront(old)
		return nil
	}

	// Создаем новый элемент
	ent := entryPool.Get().(*entryStorage)
	ent.key = key
	ent.value = val
	ent.expiresIn = exp
	ent.prev = nil
	ent.next = nil

	// Добавляем в начало списка
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

	// Эвикт если превышен размер
	if s.curSize > s.maxSize {
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

// Reset очищает кеш
func (s *InMemoryStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[string]*entryStorage)
	s.head, s.tail = nil, nil
	s.curSize = 0
	s.creationTime = time.Now()
}

// Close останавливает фоновую очистку
func (s *InMemoryStorage) Close() {
	s.Stop()
}

// ---- внутренние методы ----
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
	s.curSize--
	entryPool.Put(e)
}

func (s *InMemoryStorage) evict() {
	if s.tail == nil {
		return
	}
	s.removeElement(s.tail)
}

// cleanupLoop удаляет устаревшие элементы
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

// Stop останавливает фоновую очистку
func (s *InMemoryStorage) Stop() {
	close(s.stopCh)
}
