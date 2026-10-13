// Package ojson is a JSON object that keeps its key order, both when encoding (the decision
// model sees choices in the order they are sent) and when decoding (ties between equal
// probabilities are broken by the order the server returned them in).
package ojson

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Object is an ordered JSON object. The zero value is empty and ready to use.
type Object struct {
	keys []string
	vals map[string]any
}

func New() *Object { return &Object{} }

// Set adds or replaces a key; a new key goes last.
func (o *Object) Set(key string, value any) *Object {
	if o.vals == nil {
		o.vals = map[string]any{}
	}
	if _, ok := o.vals[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = value
	return o
}

func (o *Object) Get(key string) (any, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.vals[key]
	return v, ok
}

func (o *Object) Keys() []string {
	if o == nil {
		return nil
	}
	return o.keys
}

func (o *Object) Len() int { return len(o.Keys()) }

func (o *Object) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.Keys() {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := Marshal(o.vals[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON decodes an object, keeping key order; nested objects become *Object,
// numbers float64.
func (o *Object) UnmarshalJSON(data []byte) error {
	v, err := Decode(data)
	if err != nil {
		return err
	}
	obj, ok := v.(*Object)
	if !ok {
		return fmt.Errorf("ojson: not an object")
	}
	*o = *obj
	return nil
}

// Marshal encodes like Python's json.dumps(separators=(",", ":"), ensure_ascii=False): no
// spaces, no HTML escaping.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Decode parses JSON with objects as *Object (ordered) and numbers as float64.
func Decode(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("ojson: trailing data")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := New()
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				o.Set(kt.(string), v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return o, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return arr, nil
		}
		return nil, fmt.Errorf("ojson: unexpected %v", t)
	default:
		return t, nil
	}
}

// Path walks nested objects: Path(v, "answers", "kind", "choice").
func Path(v any, keys ...string) (any, bool) {
	for _, k := range keys {
		o, ok := v.(*Object)
		if !ok {
			return nil, false
		}
		if v, ok = o.Get(k); !ok {
			return nil, false
		}
	}
	return v, true
}

// Plain converts *Object values (recursively) into map[string]any, e.g. for storing as jsonb.
func Plain(v any) any {
	switch t := v.(type) {
	case *Object:
		m := make(map[string]any, t.Len())
		for _, k := range t.Keys() {
			m[k] = Plain(t.vals[k])
		}
		return m
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = Plain(x)
		}
		return out
	}
	return v
}
