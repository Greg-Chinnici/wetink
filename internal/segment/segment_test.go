package segment

import (
	"slices"
	"testing"

	"wetink/internal/golden"
)

type goldenCase struct {
	Text     string   `json:"text"`
	Segment  []string `json:"segment"`
	Prefixes []struct {
		Prefix   string   `json:"prefix"`
		Streamed []string `json:"streamed"`
	} `json:"prefixes"`
}

func TestGolden(t *testing.T) {
	var cases []goldenCase
	golden.Load(t, "segment", &cases)
	for _, c := range cases {
		if got := Segment(c.Text); !slices.Equal(got, c.Segment) {
			t.Errorf("Segment(%q)\n got %q\nwant %q", c.Text, got, c.Segment)
		}
		// Every distinct stable prefix, in order of first appearance, and its final parts.
		var prefixes []string
		seen := map[string]bool{}
		ends := []int{0}
		for i := range c.Text {
			if i > 0 {
				ends = append(ends, i)
			}
		}
		ends = append(ends, len(c.Text))
		for _, end := range ends {
			p := c.Text[:StableCut(c.Text[:end])]
			if !seen[p] {
				seen[p] = true
				prefixes = append(prefixes, p)
			}
		}
		if len(prefixes) != len(c.Prefixes) {
			t.Fatalf("%q: %d prefixes, want %d", c.Text, len(prefixes), len(c.Prefixes))
		}
		for i, want := range c.Prefixes {
			if prefixes[i] != want.Prefix {
				t.Errorf("%q prefix %d: got %q want %q", c.Text, i, prefixes[i], want.Prefix)
			}
			if got := StreamedParts(want.Prefix); !slices.Equal(got, want.Streamed) {
				t.Errorf("StreamedParts(%q)\n got %q\nwant %q", want.Prefix, got, want.Streamed)
			}
		}
	}
}

// Streamed parts never disagree with the finished text, at any byte position.
func TestStreamedPrefixesAgree(t *testing.T) {
	var cases []goldenCase
	golden.Load(t, "segment", &cases)
	for _, c := range cases {
		checkPrefixes(t, c.Text)
	}
	checkPrefixes(t, headings)
}

func checkPrefixes(t *testing.T, text string) {
	t.Helper()
	full := Segment(text)
	seen := 0
	for end := 0; end <= len(text); end++ {
		parts := StreamedParts(text[:StableCut(text[:end])])
		if len(parts) > len(full) || !slices.Equal(parts, full[:len(parts)]) {
			t.Fatalf("%q at %d: %q vs %q", text, end, parts, full)
		}
		if len(parts) < seen {
			t.Fatalf("%q at %d: parts went from %d to %d", text, end, seen, len(parts))
		}
		seen = len(parts)
	}
}

// Headings as real masters wrote them.
const headings = "**URL Shortener Design**\n\nI'll use base-62 codes.\n\n## API\n\n- POST /shorten\n- GET /{code}\n\n" +
	"**My Key Decisions:**\n\n1. Postgres\n2. 302 redirects\n\n**Short Code Generation**: SHA-256 hash, base62.\n\n" +
	"__Storage__\n\n### Next steps\n\nIndex short_code."

func TestHeadingsDropped(t *testing.T) {
	want := []string{"I'll use base-62 codes.", "POST /shorten", "GET /{code}", "Postgres", "302 redirects",
		"**Short Code Generation**: SHA-256 hash, base62.", "Index short_code."}
	if got := Segment(headings); !slices.Equal(got, want) {
		t.Errorf("got %q", got)
	}
}

func TestStableCut(t *testing.T) {
	for text, want := range map[string]int{
		"P1\n\nP2":              4,
		"P1\n\nP2\n":            4,
		"```\na\n\nb":           0,
		"```\na\n\nb\n```\n\nc": len("```\na\n\nb\n```\n\n"),
	} {
		if got := StableCut(text); got != want {
			t.Errorf("StableCut(%q) = %d, want %d", text, got, want)
		}
	}
}
