// Package tap publishes a master's completed Open Responses items to NATS.
//
// Publishing never blocks the master. Items go into a local in-memory buffer and a background
// goroutine drains it to JetStream in order, retrying until each publish is acknowledged. The
// buffer survives NATS outages for the life of the process.
//
// While a message is being written, the tap also publishes snapshots of it (rev 1, 2, ...)
// whenever a paragraph is complete, so other masters see each part within moments of it being
// written rather than when the whole message ends (MessageStream; segment.StreamedParts).
package tap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/oai"
	"wetink/internal/ojson"
	"wetink/internal/segment"
)

var idPrefix = map[string]string{"message": "msg", "function_call": "fc", "function_call_output": "fco"}

// NewID returns a random item id with the given prefix ("msg_3f2a...").
func NewID(prefix string) string {
	b := make([]byte, 16)
	rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

type Options struct {
	ItemTypes      map[string]bool // default envelope.ItemTypes
	MaxBuffer      int             // default 10,000; the oldest item is dropped beyond it
	PublishTimeout time.Duration   // default 2 s
	// NoStreamMessages turns off publishing messages while they are written.
	NoStreamMessages bool
	// JetStream, if set, is used instead of connecting to natsURL (tests).
	JetStream func(ctx context.Context) (Publisher, error)
}

// Publisher is the part of JetStream the tap uses.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type Tap struct {
	Session, Master string
	natsURL         string
	opts            Options

	mu      sync.Mutex
	seq     int64
	buf     []envelope.Envelope
	wake    chan struct{}
	drained chan struct{} // closed while the buffer is empty
	cancel  context.CancelFunc
	done    chan struct{}
	nc      *nats.Conn
}

func New(session, master, natsURL string, opts Options) (*Tap, error) {
	if err := envelope.CheckName(session, "session"); err != nil {
		return nil, err
	}
	if err := envelope.CheckName(master, "master"); err != nil {
		return nil, err
	}
	if opts.ItemTypes == nil {
		opts.ItemTypes = envelope.ItemTypes
	}
	if opts.MaxBuffer == 0 {
		opts.MaxBuffer = 10_000
	}
	if opts.PublishTimeout == 0 {
		opts.PublishTimeout = 2 * time.Second
	}
	drained := make(chan struct{})
	close(drained)
	return &Tap{Session: session, Master: master, natsURL: natsURL, opts: opts,
		wake: make(chan struct{}, 1), drained: drained}, nil
}

// Start starts the background publisher. It returns immediately, even if NATS is down.
func (t *Tap) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel, t.done = cancel, make(chan struct{})
	go t.run(ctx)
}

// Publish queues one completed item (or, with rev > 0, a snapshot of a message being written).
// It never blocks. It returns the envelope, or false if the item type is not shared. Items
// without an id get one, on a copy; the caller's map is untouched.
func (t *Tap) Publish(it map[string]any, rev int) (envelope.Envelope, bool) {
	typ, _ := it["type"].(string)
	if !t.opts.ItemTypes[typ] {
		return envelope.Envelope{}, false
	}
	if id, _ := it["id"].(string); id == "" {
		it = maps.Clone(it)
		prefix := idPrefix[typ]
		if prefix == "" {
			prefix = "item"
		}
		it["id"] = NewID(prefix)
	}
	t.mu.Lock()
	t.seq++
	env := envelope.Envelope{Session: t.Session, Master: t.Master, Seq: t.seq, Item: it, Via: envelope.ViaTap, Rev: rev}
	if len(t.buf) == t.opts.MaxBuffer {
		slog.Warn("tap buffer full, dropping the oldest item (consumer will see a gap)", "seq", t.buf[0].Seq)
		t.buf = t.buf[1:]
	}
	select {
	case <-t.drained: // was empty: Flush must now wait for this item
		t.drained = make(chan struct{})
	default: // still draining earlier items; drain closes it once all are out
	}
	t.buf = append(t.buf, env)
	t.mu.Unlock()
	select {
	case t.wake <- struct{}{}:
	default:
	}
	return env, true
}

// Flush waits until the buffer is empty; false on timeout.
func (t *Tap) Flush(timeout time.Duration) bool {
	t.mu.Lock()
	drained := t.drained
	t.mu.Unlock()
	select {
	case <-drained:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Close flushes (up to timeout) and stops the publisher.
func (t *Tap) Close(timeout time.Duration) {
	if !t.Flush(timeout) {
		t.mu.Lock()
		slog.Warn("tap closing with unpublished items", "n", len(t.buf))
		t.mu.Unlock()
	}
	t.mu.Lock()
	cancel, done := t.cancel, t.done
	t.cancel = nil
	t.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	if t.nc != nil {
		t.nc.Close()
	}
}

func (t *Tap) connect(ctx context.Context) (Publisher, error) {
	if t.opts.JetStream != nil {
		return t.opts.JetStream(ctx)
	}
	nc, err := nats.Connect(t.natsURL, nats.Name("tap-"+t.Master), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err == nil {
		err = envelope.EnsureStream(ctx, js)
	}
	if err != nil {
		nc.Close()
		return nil, err
	}
	t.nc = nc
	return js, nil
}

func (t *Tap) run(ctx context.Context) {
	defer close(t.done)
	var js Publisher
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := func() error {
			if js == nil {
				var err error
				if js, err = t.connect(ctx); err != nil {
					return err
				}
				slog.Info("tap connected to NATS", "master", t.Master)
			}
			return t.drain(ctx, js)
		}()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("tap publish failed, retrying", "master", t.Master, "err", err, "in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		select {
		case <-t.wake:
		case <-ctx.Done():
			return
		}
	}
}

func (t *Tap) drain(ctx context.Context, js Publisher) error {
	for {
		t.mu.Lock()
		if len(t.buf) == 0 {
			select {
			case <-t.drained:
			default:
				close(t.drained)
			}
			t.mu.Unlock()
			return nil
		}
		env := t.buf[0]
		t.mu.Unlock()
		data, err := env.Marshal()
		if err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(ctx, t.opts.PublishTimeout)
		_, err = js.Publish(pctx, env.Subject(), data, jetstream.WithMsgID(env.MsgID()))
		cancel()
		if err != nil {
			return err
		}
		// Only pop once acked; a retry after an unacked-but-stored publish is dropped by
		// JetStream's duplicate window.
		t.mu.Lock()
		if len(t.buf) > 0 && t.buf[0].Seq == env.Seq {
			t.buf = t.buf[1:]
		}
		t.mu.Unlock()
	}
}

// MessageStream publishes a message while it is written: a snapshot each time a part becomes
// final (segment.StreamedParts).
type MessageStream struct {
	tap    *Tap
	itemID string
	text   strings.Builder
	Revs   int
	cut    int
	parts  int
}

func (t *Tap) NewMessageStream(itemID string) *MessageStream {
	return &MessageStream{tap: t, itemID: itemID}
}

func (s *MessageStream) Feed(delta string) {
	s.text.WriteString(delta)
	if !strings.Contains(delta, "\n") {
		return // a part only becomes final at a blank line
	}
	text := s.text.String()
	cut := segment.StableCut(text)
	if cut <= s.cut {
		return
	}
	s.cut = cut
	prefix := text[:cut]
	n := len(segment.StreamedParts(prefix))
	if n <= s.parts {
		return
	}
	s.parts = n
	s.Revs++
	s.tap.Publish(map[string]any{
		"type":    "message",
		"id":      s.itemID,
		"role":    "assistant",
		"status":  "in_progress",
		"content": []any{map[string]any{"type": "output_text", "text": prefix, "annotations": []any{}}},
	}, s.Revs)
}

// Observer publishes what a stream of /responses events produces: each finished item, and
// messages while they are written (unless NoStreamMessages).
type Observer struct {
	tap     *Tap
	streams map[string]*MessageStream
}

func (t *Tap) NewObserver() *Observer {
	return &Observer{tap: t, streams: map[string]*MessageStream{}}
}

// Observe handles one event.
func (o *Observer) Observe(ev oai.Event) {
	switch ev.Type() {
	case "response.output_text.delta":
		itemID, _ := ev["item_id"].(string)
		ci, _ := ev["content_index"].(float64)
		if !o.tap.opts.NoStreamMessages && itemID != "" && ci == 0 {
			s := o.streams[itemID]
			if s == nil {
				s = o.tap.NewMessageStream(itemID)
				o.streams[itemID] = s
			}
			delta, _ := ev["delta"].(string)
			s.Feed(delta)
		}
	case "response.output_item.done":
		it := ev.Item()
		if id, _ := it["id"].(string); id != "" {
			delete(o.streams, id)
		}
		if it != nil {
			o.tap.Publish(it, 0)
		}
	}
}

// Stream runs a streaming /responses request and yields its events unchanged, publishing what
// they produce (Observer).
func (t *Tap) Stream(ctx context.Context, client *oai.Client, body *ojson.Object) iter.Seq2[oai.Event, error] {
	return func(yield func(oai.Event, error) bool) {
		o := t.NewObserver()
		for ev, err := range client.StreamResponse(ctx, body) {
			if err != nil {
				yield(nil, err)
				return
			}
			o.Observe(ev)
			if !yield(ev, nil) {
				return
			}
		}
	}
}

func (t *Tap) String() string { return fmt.Sprintf("tap %s/%s", t.Session, t.Master) }
