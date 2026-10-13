package tap_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/master"
	"wetink/internal/mockgw"
	"wetink/internal/oai"
	"wetink/internal/tap"
)

type published struct {
	subject, msgID string
	env            envelope.Envelope
}

type fakeJS struct {
	mu        sync.Mutex
	failFirst int
	got       []published
}

func (f *fakeJS) Publish(_ context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return nil, errors.New("nats down")
	}
	env, err := envelope.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	// The message id is the only option the tap sets; read it back from the envelope.
	f.got = append(f.got, published{subject, env.MsgID(), env})
	return &jetstream.PubAck{Sequence: uint64(len(f.got))}, nil
}

func (f *fakeJS) envs() []envelope.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []envelope.Envelope
	for _, p := range f.got {
		out = append(out, p.env)
	}
	return out
}

func newTap(t *testing.T, js *fakeJS, opts tap.Options) *tap.Tap {
	opts.JetStream = func(context.Context) (tap.Publisher, error) { return js, nil }
	tp, err := tap.New("s-test", "master-a", "nats://unused", opts)
	if err != nil {
		t.Fatal(err)
	}
	return tp
}

func gateway(t *testing.T) *oai.Client {
	srv := httptest.NewServer(mockgw.Handler())
	t.Cleanup(srv.Close)
	return oai.New(srv.URL+"/v1", "mock", 10*time.Second)
}

func seqs(envs []envelope.Envelope) []int64 {
	var out []int64
	for _, e := range envs {
		out = append(out, e.Seq)
	}
	return out
}

func TestMasterLoopPublishesEachCompletedItem(t *testing.T) {
	js := &fakeJS{}
	tp := newTap(t, js, tap.Options{})
	tp.Start()
	answer, err := master.Run(context.Background(), tp, gateway(t), "hello", master.Options{Model: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	if !tp.Flush(2 * time.Second) {
		t.Fatal("flush timed out")
	}
	tp.Close(time.Second)
	if !strings.HasPrefix(answer, "Mock reply to: hello") {
		t.Errorf("answer %q", answer)
	}
	envs := js.envs()
	var types []string
	for _, e := range envs {
		types = append(types, e.ItemType())
	}
	if !slices.Equal(types, []string{"function_call", "function_call_output", "message"}) || !slices.Equal(seqs(envs), []int64{1, 2, 3}) {
		t.Fatalf("published %v %v", types, seqs(envs))
	}
	for _, p := range js.got {
		if p.subject != "wetink.s-test.items" || p.msgID != "master-a:"+p.env.ItemID() {
			t.Errorf("subject %s msg id %s", p.subject, p.msgID)
		}
	}
	if envs[1].Item["call_id"] != envs[0].Item["call_id"] {
		t.Error("output does not match its call")
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(envs[0].Item["arguments"].(string)), &args); err != nil {
		t.Fatal(err)
	}
	if args["query"] != "hello" {
		t.Errorf("arguments %v", args)
	}
}

func TestPublishFiltersTypesAndDoesNotMutate(t *testing.T) {
	tp := newTap(t, &fakeJS{}, tap.Options{})
	if _, ok := tp.Publish(map[string]any{"type": "reasoning", "id": "rs_1"}, 0); ok {
		t.Error("reasoning published")
	}
	it := map[string]any{"type": "function_call_output", "call_id": "c1", "output": "x"}
	env, _ := tp.Publish(it, 0)
	if _, has := it["id"]; has {
		t.Error("caller's item was changed")
	}
	if !strings.HasPrefix(env.ItemID(), "fco_") || env.Seq != 1 {
		t.Errorf("env %+v", env)
	}
}

func TestPublishNeverBlocksAndRetriesInOrder(t *testing.T) {
	js := &fakeJS{failFirst: 3}
	tp := newTap(t, js, tap.Options{})
	for i := range 5 {
		tp.Publish(map[string]any{"type": "message", "id": "msg_" + string(rune('0'+i))}, 0)
	}
	tp.Start() // publishing before start is fine: items wait in the buffer
	if !tp.Flush(5 * time.Second) {
		t.Fatal("flush timed out")
	}
	tp.Close(time.Second)
	if got := seqs(js.envs()); !slices.Equal(got, []int64{1, 2, 3, 4, 5}) {
		t.Errorf("published %v", got)
	}
}

func TestBufferOverflowDropsOldest(t *testing.T) {
	js := &fakeJS{}
	tp := newTap(t, js, tap.Options{MaxBuffer: 2})
	for i := range 4 {
		tp.Publish(map[string]any{"type": "message", "id": "msg_" + string(rune('0'+i))}, 0)
	}
	tp.Start()
	tp.Flush(2 * time.Second)
	tp.Close(time.Second)
	if got := seqs(js.envs()); !slices.Equal(got, []int64{3, 4}) {
		t.Errorf("published %v", got)
	}
}

func TestCloseReturnsWhenNATSUnreachable(t *testing.T) {
	tp, _ := tap.New("s-test", "master-a", "nats://127.0.0.1:1", tap.Options{})
	tp.Start()
	tp.Publish(map[string]any{"type": "message", "id": "msg_1"}, 0)
	done := make(chan struct{})
	go func() { tp.Close(500 * time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("close hung")
	}
}

func TestMessagesArePublishedWhileWritten(t *testing.T) {
	js := &fakeJS{}
	tp := newTap(t, js, tap.Options{})
	tp.Start()
	prompt := "We use SQLite.\n\nIDs are base62.\n\nRedirects return 302."
	answer, err := master.Run(context.Background(), tp, gateway(t), "[tool: none]"+prompt, master.Options{Model: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	tp.Close(2 * time.Second)
	var msgs []envelope.Envelope
	for _, e := range js.envs() {
		if e.ItemType() == "message" {
			msgs = append(msgs, e)
		}
	}
	// The mock streams word by word: a snapshot once each of the first two paragraphs is
	// final (at its blank line), then the finished item.
	var revs []int
	for _, e := range msgs {
		revs = append(revs, e.Rev)
	}
	if !slices.Equal(revs, []int{1, 2, 0}) {
		t.Fatalf("revs %v", revs)
	}
	text := func(e envelope.Envelope) string {
		return e.Item["content"].([]any)[0].(map[string]any)["text"].(string)
	}
	if !strings.HasSuffix(text(msgs[0]), "We use SQLite.\n\n") || !strings.HasSuffix(text(msgs[1]), "IDs are base62.\n\n") || text(msgs[2]) != answer {
		t.Errorf("texts %q %q %q", text(msgs[0]), text(msgs[1]), text(msgs[2]))
	}
	if msgs[0].ItemID() != msgs[2].ItemID() || msgs[0].MsgID() != "master-a:"+msgs[0].ItemID()+"~1" || msgs[0].Item["status"] != "in_progress" {
		t.Errorf("snapshot identity: %+v", msgs[0])
	}
}

func TestStreamingCanBeTurnedOff(t *testing.T) {
	js := &fakeJS{}
	tp := newTap(t, js, tap.Options{NoStreamMessages: true})
	tp.Start()
	if _, err := master.Run(context.Background(), tp, gateway(t), "[tool: none]A.\n\nB.\n\nC.", master.Options{Model: "mock"}); err != nil {
		t.Fatal(err)
	}
	tp.Close(2 * time.Second)
	n := 0
	for _, e := range js.envs() {
		if e.ItemType() == "message" {
			n++
			if e.Rev != 0 {
				t.Errorf("snapshot published: %+v", e)
			}
		}
	}
	if n != 1 {
		t.Errorf("%d messages", n)
	}
}
