// Package pystr holds string helpers with the semantics the rest of the code relies on:
// lengths and slices count characters (runes), not bytes, and whitespace is what Python's
// str.isspace accepts. The golden tests (testdata/golden) were produced with those rules.
package pystr

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// IsSpace reports whether r is whitespace in the sense of Python's str.isspace.
func IsSpace(r rune) bool {
	return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
}

// SpaceClass is a regexp character class body matching IsSpace runes.
const SpaceClass = `\t\n\v\f\r \x{1c}-\x{1f}\x{85}\p{Z}`

func Strip(s string) string  { return strings.TrimFunc(s, IsSpace) }
func LStrip(s string) string { return strings.TrimLeftFunc(s, IsSpace) }

// Len is the number of characters in s.
func Len(s string) int { return utf8.RuneCountInString(s) }

// Prefix returns the first n characters of s (all of s if it is shorter).
func Prefix(s string, n int) string {
	if n <= 0 {
		return ""
	}
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos]
		}
		i++
	}
	return s
}
