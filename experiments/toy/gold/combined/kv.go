// Package kv is a small in-memory key-value store, safe for concurrent use.
package kv

import (
	"encoding/json"
	"io"
	"sort"
	"sync"
	"time"
)

// Store maps string keys to string values.
type Store struct {
	mu   sync.Mutex
	data map[string]entry
}

type entry struct {
	value   string
	expires time.Time // zero: never
}

func (e entry) live(now time.Time) bool { return e.expires.IsZero() || now.Before(e.expires) }

// New returns an empty store.
func New() *Store {
	return &Store{data: map[string]entry{}}
}

// Set stores value under key, replacing any previous value. It never expires.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value}
}

// SetWithTTL stores value under key until ttl has passed.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value, expires: time.Now().Add(ttl)}
}

// Get returns the value under key, and whether there is one.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok || !e.live(time.Now()) {
		return "", false
	}
	return e.value, true
}

// Delete removes key, if present.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Len returns the number of keys.
func (s *Store) Len() int {
	return len(s.Keys())
}

// Keys returns the keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	keys := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if e.live(now) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

type savedEntry struct {
	Key     string     `json:"key"`
	Value   string     `json:"value"`
	Expires *time.Time `json:"expires,omitempty"`
}

// Save writes the live entries as JSON, with their expiry.
func (s *Store) Save(w io.Writer) error {
	s.mu.Lock()
	now := time.Now()
	out := []savedEntry{}
	for k, e := range s.data {
		if !e.live(now) {
			continue
		}
		se := savedEntry{Key: k, Value: e.value}
		if !e.expires.IsZero() {
			exp := e.expires
			se.Expires = &exp
		}
		out = append(out, se)
	}
	s.mu.Unlock()
	return json.NewEncoder(w).Encode(out)
}

// Load reads what Save wrote into a new store.
func Load(r io.Reader) (*Store, error) {
	var in []savedEntry
	if err := json.NewDecoder(r).Decode(&in); err != nil {
		return nil, err
	}
	s := New()
	for _, se := range in {
		e := entry{value: se.Value}
		if se.Expires != nil {
			e.expires = *se.Expires
		}
		s.data[se.Key] = e
	}
	return s, nil
}
