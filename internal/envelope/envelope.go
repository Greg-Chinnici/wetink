// Package envelope is the wire format every published item travels in, plus NATS naming.
//
// On NATS (subject wetink.<session>.items) an envelope is the JSON object
//
//	{"session": "s-1", "master": "master-a", "seq": 3, "item": {...}, "via": "mcp", "rev": 2}
//
// where item is an Open Responses item stored untouched, via is "tap" (default, omitted) or
// "mcp" (published by the MCP server for share_context), and rev numbers the snapshots of a
// message still being written (omitted for a finished item). The Nats-Msg-Id header is
// "<master>:<item id>" plus "~<rev>" for snapshots; JetStream drops repeats within its
// duplicate window. Any language can publish this format; the Go tap is in internal/tap.
package envelope

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName      = "WETINK"
	StreamSubjects  = "wetink.*.items"
	DuplicateWindow = 120 * time.Second
)

// ItemTypes are the item types masters publish; reasoning and provider-specific items are
// dropped at the tap.
var ItemTypes = map[string]bool{"message": true, "function_call": true, "function_call_output": true}

// Via values: "tap", the master's own output; "mcp", an entry the master shared deliberately.
const (
	ViaTap = "tap"
	ViaMCP = "mcp"
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// CheckName requires a session or master name to be a single NATS subject token.
func CheckName(value, what string) error {
	if !nameRe.MatchString(value) {
		return fmt.Errorf("invalid %s %q: use letters, digits, '-' or '_'", what, value)
	}
	return nil
}

func Subject(session string) string { return "wetink." + session + ".items" }

func MsgID(master, itemID string, rev int) string {
	id := master + ":" + itemID
	if rev > 0 {
		id += "~" + strconv.Itoa(rev)
	}
	return id
}

type Envelope struct {
	Session string
	Master  string
	Seq     int64
	Item    map[string]any
	Via     string // "tap" if empty
	Rev     int    // 0 for a finished item
}

func (e Envelope) ItemID() string   { s, _ := e.Item["id"].(string); return s }
func (e Envelope) ItemType() string { s, _ := e.Item["type"].(string); return s }
func (e Envelope) Subject() string  { return Subject(e.Session) }
func (e Envelope) MsgID() string    { return MsgID(e.Master, e.ItemID(), e.Rev) }

func (e Envelope) ViaOrTap() string {
	if e.Via == "" {
		return ViaTap
	}
	return e.Via
}

func (e Envelope) Validate() error {
	if v := e.ViaOrTap(); v != ViaTap && v != ViaMCP {
		return fmt.Errorf("via must be tap or mcp, got %q", v)
	}
	if e.Rev < 0 {
		return fmt.Errorf("rev must be >= 1, got %d", e.Rev)
	}
	if err := CheckName(e.Session, "session"); err != nil {
		return err
	}
	if err := CheckName(e.Master, "master"); err != nil {
		return err
	}
	if e.Seq < 1 {
		return fmt.Errorf("seq must be >= 1, got %d", e.Seq)
	}
	if e.ItemID() == "" {
		return errors.New("item must have a non-empty string id")
	}
	if _, ok := e.Item["type"].(string); !ok {
		return errors.New("item must have a string type")
	}
	return nil
}

type wire struct {
	Session string         `json:"session"`
	Master  string         `json:"master"`
	Seq     int64          `json:"seq"`
	Item    map[string]any `json:"item"`
	Via     string         `json:"via,omitempty"`
	Rev     int            `json:"rev,omitempty"`
}

func (e Envelope) Marshal() ([]byte, error) {
	via := e.Via
	if via == ViaTap {
		via = ""
	}
	return json.Marshal(wire{e.Session, e.Master, e.Seq, e.Item, via, e.Rev})
}

// Unmarshal parses and validates an envelope.
func Unmarshal(data []byte) (Envelope, error) {
	var raw struct {
		Session *string        `json:"session"`
		Master  *string        `json:"master"`
		Seq     *json.Number   `json:"seq"`
		Item    map[string]any `json:"item"`
		Via     string         `json:"via"`
		Rev     *int           `json:"rev"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Envelope{}, fmt.Errorf("malformed envelope: %w", err)
	}
	if raw.Session == nil || raw.Master == nil || raw.Seq == nil || raw.Item == nil {
		return Envelope{}, errors.New("malformed envelope: missing field")
	}
	seq, err := raw.Seq.Int64()
	if err != nil {
		return Envelope{}, fmt.Errorf("malformed envelope: seq: %w", err)
	}
	env := Envelope{Session: *raw.Session, Master: *raw.Master, Seq: seq, Item: raw.Item, Via: raw.Via}
	if raw.Rev != nil {
		if *raw.Rev < 1 {
			return Envelope{}, fmt.Errorf("rev must be >= 1, got %d", *raw.Rev)
		}
		env.Rev = *raw.Rev
	}
	if env.Via == "" {
		env.Via = ViaTap
	}
	return env, env.Validate()
}

// StreamConfig is the WETINK stream: every session's items, kept on disk.
func StreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{StreamSubjects},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: DuplicateWindow,
	}
}

// EnsureStream creates the WETINK stream if it does not exist (an existing one is left as is).
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.Stream(ctx, StreamName); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	_, err := js.CreateStream(ctx, StreamConfig())
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return err
}
