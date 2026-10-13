// Package segment splits an item's text into the parts that become shared-context entries.
//
// A master's message often carries several claims of different kinds (a decision, a fact,
// an open question). Each part is classified on its own. Splitting is deterministic and
// structural:
//   - blank lines separate parts (paragraphs);
//   - each list item is a part, without its marker;
//   - a fenced code block stays whole, together with a short intro line ending in ":";
//   - a short lead-in ending in ":" before a list ("Progress update:") is dropped;
//   - a markdown heading on its own ("## Storage", "**API design**") is dropped: it is
//     structure, and as an entry it showed up on the board as content.
//
// Streaming (the tap publishes a message while it is written): a prefix cut at a blank line
// outside code fences has the same blocks as the finished text, and a block's part depends
// only on itself and the next block's kind. So every part of such a prefix is final except a
// trailing lead-in (which the next block may attach or drop) and parts past the merge cap.
// See StreamedParts.
package segment

import (
	"regexp"
	"strings"

	"wetink/internal/pystr"
)

const (
	MaxParts    = 12
	LeadInChars = 80
)

var heading = regexp.MustCompile(`^(#{1,6}[` + pystr.SpaceClass + `]+\S.*|\*\*[^*\n]+\*\*:?|__[^_\n]+__:?)$`)

// isHeading: a one-line markdown heading, or a line that is only bold text.
func isHeading(text string) bool {
	return pystr.Len(text) <= LeadInChars && heading.MatchString(text)
}

var listItem = regexp.MustCompile(`^[` + pystr.SpaceClass + `]*(?:[-*•]|\p{Nd}+[.)])[` + pystr.SpaceClass + `]+`)

type block struct{ kind, text string } // kind: "code", "list" or "para"

func isFence(line string) bool { return strings.HasPrefix(pystr.LStrip(line), "```") }

func blocks(text string) []block {
	var out []block
	var para []string
	var item []string
	inItem := false
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")

	flush := func() {
		if len(para) > 0 {
			out = append(out, block{"para", pystr.Strip(strings.Join(para, "\n"))})
			para = para[:0]
		}
		if inItem {
			out = append(out, block{"list", pystr.Strip(strings.Join(item, "\n"))})
			item, inItem = nil, false
		}
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		switch {
		case isFence(line):
			flush()
			fence := []string{line}
			i++
			for i < len(lines) {
				fence = append(fence, lines[i])
				if isFence(lines[i]) {
					break
				}
				i++
			}
			out = append(out, block{"code", strings.Join(fence, "\n")})
		case pystr.Strip(line) == "":
			flush()
		case listItem.MatchString(line):
			if inItem || len(para) > 0 {
				flush()
			}
			loc := listItem.FindStringIndex(line)
			item, inItem = []string{line[loc[1]:]}, true
		case inItem:
			item = append(item, pystr.Strip(line)) // continuation of a list item
		default:
			para = append(para, line)
		}
	}
	flush()
	kept := out[:0]
	for _, b := range out {
		if b.text != "" {
			kept = append(kept, b)
		}
	}
	return kept
}

func isLeadIn(text string) bool {
	return strings.HasSuffix(text, ":") && pystr.Len(text) <= LeadInChars && !strings.Contains(text, "\n")
}

// Segment splits text into at most maxParts parts (the tail is merged).
func Segment(text string) []string { return segmentN(text, MaxParts) }

func segmentN(text string, maxParts int) []string {
	bs := blocks(text)
	parts := []string{}
	for i, b := range bs {
		if b.kind == "para" && isHeading(b.text) {
			continue
		}
		next := ""
		if i+1 < len(bs) {
			next = bs[i+1].kind
		}
		if b.kind == "para" && isLeadIn(b.text) && (next == "code" || next == "list") {
			if next == "code" {
				bs[i+1].text = b.text + "\n" + bs[i+1].text
			}
			continue
		}
		parts = append(parts, b.text)
	}
	if len(parts) > maxParts {
		tail := strings.Join(parts[maxParts-1:], "\n\n")
		parts = append(parts[:maxParts-1:maxParts-1], tail)
	}
	return parts
}

// StableCut is the length in bytes of the longest prefix of text that ends with a blank line
// outside code fences (0 if none). Only complete lines count: the last line may still grow.
func StableCut(text string) int {
	cut, pos := 0, 0
	inFence := false
	lines := strings.Split(text, "\n")
	for _, line := range lines[:len(lines)-1] {
		pos += len(line) + 1
		if isFence(line) {
			inFence = !inFence
		} else if !inFence && pystr.Strip(line) == "" {
			cut = pos
		}
	}
	return cut
}

// StreamedParts returns the parts of a prefix (cut by StableCut) that the rest of the text
// cannot change; they equal the first parts of the finished text's Segment.
func StreamedParts(prefix string) []string {
	parts := Segment(prefix)
	bs := blocks(prefix)
	if last := len(bs) - 1; len(parts) > 0 && last >= 0 && bs[last].kind == "para" && isLeadIn(bs[last].text) && !isHeading(bs[last].text) {
		parts = parts[:len(parts)-1] // may still become a list lead-in or a code intro
	}
	if len(parts) > MaxParts-1 {
		parts = parts[:MaxParts-1]
	}
	return parts
}
