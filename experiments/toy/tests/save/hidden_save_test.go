package kv

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestHiddenSaveLoadRoundTrip(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "two words")
	s.Set("", "empty key")
	var buf bytes.Buffer
	if err := s.Save(&buf); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(buf.Bytes()) {
		t.Fatalf("Save did not write JSON: %s", buf.String())
	}
	l, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(l.Keys(), s.Keys()) {
		t.Fatalf("keys %v, want %v", l.Keys(), s.Keys())
	}
	for _, k := range s.Keys() {
		want, _ := s.Get(k)
		if got, ok := l.Get(k); !ok || got != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", k, got, ok, want)
		}
	}
}

func TestHiddenLoadRejectsGarbage(t *testing.T) {
	if _, err := Load(strings.NewReader("not json")); err == nil {
		t.Fatal("Load accepted garbage")
	}
}
