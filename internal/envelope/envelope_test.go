package envelope

import (
	"reflect"
	"strings"
	"testing"
)

func item() map[string]any {
	return map[string]any{"type": "message", "id": "msg_1", "content": []any{}}
}

func TestRoundtrip(t *testing.T) {
	for _, env := range []Envelope{
		{Session: "s-1", Master: "master-a", Seq: 3, Item: item(), Via: ViaTap},
		{Session: "s-1", Master: "master-a", Seq: 1, Item: item(), Via: ViaMCP},
		{Session: "s-1", Master: "master-a", Seq: 2, Item: item(), Via: ViaTap, Rev: 3},
	} {
		data, err := env.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		got, err := Unmarshal(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, env) {
			t.Errorf("roundtrip: got %+v want %+v", got, env)
		}
		if env.Via == ViaTap && strings.Contains(string(data), `"via"`) {
			t.Errorf("tap is the default and not written: %s", data)
		}
	}
	env := Envelope{Session: "s-1", Master: "master-a", Seq: 3, Item: item()}
	if env.Subject() != "wetink.s-1.items" || env.MsgID() != "master-a:msg_1" {
		t.Errorf("subject %s msg id %s", env.Subject(), env.MsgID())
	}
	env.Rev = 3
	if env.MsgID() != "master-a:msg_1~3" {
		t.Errorf("snapshot msg id %s", env.MsgID())
	}
}

func TestInvalid(t *testing.T) {
	for _, s := range []string{"a.b", "a*", "a>", "a b", ""} {
		if CheckName(s, "session") == nil {
			t.Errorf("%q accepted", s)
		}
	}
	for _, data := range []string{
		`not json`, `[]`, `{"session": "s"}`,
		`{"session": "s", "master": "m", "seq": 0, "item": {"id": "x", "type": "message"}}`,
		`{"session": "s", "master": "m", "seq": 1, "item": {"type": "message"}}`,
		`{"session": "s", "master": "m", "seq": 1, "item": {"id": "x"}}`,
		`{"session": "s", "master": "m", "seq": 1, "item": {"id": "x", "type": "message"}, "via": "other"}`,
		`{"session": "s", "master": "m", "seq": 1, "item": {"id": "x", "type": "message"}, "rev": 0}`,
	} {
		if _, err := Unmarshal([]byte(data)); err == nil {
			t.Errorf("accepted %s", data)
		}
	}
}
