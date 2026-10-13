package proxy

import (
	"bytes"
	"strings"
)

// sseParser splits a server-sent event stream, written in arbitrary chunks, into the data
// payloads of its events.
type sseParser struct {
	on      func(payload string)
	partial []byte
	data    strings.Builder
}

func (s *sseParser) write(b []byte) {
	s.partial = append(s.partial, b...)
	for {
		i := bytes.IndexByte(s.partial, '\n')
		if i < 0 {
			return
		}
		s.line(strings.TrimRight(string(s.partial[:i]), "\r"))
		s.partial = s.partial[i+1:]
	}
}

func (s *sseParser) line(line string) {
	switch {
	case strings.HasPrefix(line, "data:"):
		if s.data.Len() > 0 {
			s.data.WriteByte('\n')
		}
		s.data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
	case line == "" && s.data.Len() > 0:
		payload := s.data.String()
		s.data.Reset()
		s.on(payload)
	}
}

// end handles an event the stream ended without a blank line after.
func (s *sseParser) end() {
	if len(s.partial) > 0 {
		s.line(strings.TrimRight(string(s.partial), "\r"))
		s.partial = nil
	}
	s.line("")
}
