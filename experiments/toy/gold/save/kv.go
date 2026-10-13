// Package kv is a small in-memory key-value store, safe for concurrent use.
package kv

import (
	"encoding/json"
	"io"
	"sort"
	"sync"
)

// Store maps string keys to string values.
type Store struct {
	mu   sync.Mutex
	data map[string]string
}

// New returns an empty store.
func New() *Store {
	return &Store{data: map[string]string{}}
}

// Set stores value under key, replacing any previous value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value under key, and whether there is one.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// Delete removes key, if present.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// Keys returns the keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Save writes the store as a JSON object of keys to values.
func (s *Store) Save(w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(w).Encode(s.data)
}

// Load reads what Save wrote into a new store.
func Load(r io.Reader) (*Store, error) {
	s := New()
	if err := json.NewDecoder(r).Decode(&s.data); err != nil {
		return nil, err
	}
	return s, nil
}
