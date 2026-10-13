package kv

import (
	"slices"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("a", "3")
	if v, ok := s.Get("a"); !ok || v != "3" {
		t.Fatalf("Get(a) = %q, %v", v, ok)
	}
	s.Delete("b")
	if _, ok := s.Get("b"); ok {
		t.Fatal("deleted key still there")
	}
	if s.Len() != 1 || !slices.Equal(s.Keys(), []string{"a"}) {
		t.Fatalf("Len %d, Keys %v", s.Len(), s.Keys())
	}
}
