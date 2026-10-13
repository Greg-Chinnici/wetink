package kv

import (
	"bytes"
	"testing"
	"time"
)

func TestHiddenSaveKeepsExpiry(t *testing.T) {
	s := New()
	s.SetWithTTL("short", "x", 300*time.Millisecond)
	s.SetWithTTL("long", "y", time.Hour)
	s.Set("plain", "z")
	var buf bytes.Buffer
	if err := s.Save(&buf); err != nil {
		t.Fatal(err)
	}
	l, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.Get("short"); !ok {
		t.Fatal("entry expired too early after Load")
	}
	time.Sleep(400 * time.Millisecond)
	if _, ok := l.Get("short"); ok {
		t.Fatal("a loaded entry never expires: Save/Load lost its expiry")
	}
	for _, k := range []string{"long", "plain"} {
		if _, ok := l.Get(k); !ok {
			t.Fatalf("%s missing after Load", k)
		}
	}
	if l.Len() != 2 {
		t.Fatalf("Len %d, want 2", l.Len())
	}
}

func TestHiddenExpiredEntriesStayGone(t *testing.T) {
	s := New()
	s.SetWithTTL("gone", "x", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	var buf bytes.Buffer
	if err := s.Save(&buf); err != nil {
		t.Fatal(err)
	}
	l, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.Get("gone"); ok || l.Len() != 0 {
		t.Fatal("an entry that had expired came back after Load")
	}
}
