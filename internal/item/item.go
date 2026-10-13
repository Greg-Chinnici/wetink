// Package item reads the Open Responses items masters publish (kept as decoded JSON maps,
// since they are stored untouched).
package item

import (
	"encoding/json"
	"fmt"
	"strings"

	"wetink/internal/pystr"
)

// Str returns item[key] as text: a string as is, a missing or null value as "None", anything
// else as JSON. ("None" matches how the original implementation rendered a missing value.)
func Str(it map[string]any, key string) string {
	if it == nil {
		return "None"
	}
	switch v := it[key].(type) {
	case string:
		return v
	case nil:
		return "None"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// String returns item[key] if it is a string, else "".
func String(it map[string]any, key string) string {
	s, _ := it[key].(string)
	return s
}

// TextOf joins the text of message content: a string, or a list of parts with "text" or
// "refusal".
func TextOf(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		parts := make([]string, 0, len(c))
		for _, p := range c {
			m, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if s, _ := m["text"].(string); s != "" {
				parts = append(parts, s)
			} else if s, _ := m["refusal"].(string); s != "" {
				parts = append(parts, s)
			} else {
				parts = append(parts, "")
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// Render is the text the classifier sees for an item, and false if the item is not
// classified. A function_call is classified only through its output (rendered with the
// call), so a tool exchange always enters shared context whole.
func Render(it, call map[string]any) (string, bool) {
	var text string
	switch String(it, "type") {
	case "message":
		text = TextOf(it["content"])
	case "function_call_output":
		output := TextOf(it["output"])
		if call == nil {
			text = fmt.Sprintf("Tool call %s returned:\n%s", Str(it, "call_id"), output)
		} else {
			text = fmt.Sprintf("Tool call %s(%s) returned:\n%s", Str(call, "name"), Str(call, "arguments"), output)
		}
	default:
		return "", false
	}
	text = pystr.Strip(text)
	return text, text != ""
}
