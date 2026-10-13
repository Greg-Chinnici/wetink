package kv

import (
	"slices"
	"testing"
	"time"
)

func TestHiddenTTLExpires(t *testing.T) {
	s := New()
	s.SetWithTTL("a", "1", 50*time.Millisecond)
	s.Set("b", "2")
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("before expiry: Get(a) = %q, %v", v, ok)
	}
	time.Sleep(100 * time.Millisecond)
	if _, ok := s.Get("a"); ok {
		t.Fatal("expired entry still returned")
	}
	if s.Len() != 1 || !slices.Equal(s.Keys(), []string{"b"}) {
		t.Fatalf("after expiry: Len %d, Keys %v", s.Len(), s.Keys())
	}
}

func TestHiddenSetClearsTTL(t *testing.T) {
	s := New()
	s.SetWithTTL("a", "1", 50*time.Millisecond)
	s.Set("a", "2")
	time.Sleep(100 * time.Millisecond)
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("Set should replace the expiring value with one that stays: Get(a) = %q, %v", v, ok)
	}
}
