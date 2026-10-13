// Package bench measures the pipeline under load: several sessions, each with masters
// publishing multi-part messages, through NATS, the consumer and the classifier, until every
// item is processed. It reports throughput and publish -> processed latency.
//
// The classifier is the mock decision model with a fixed latency and a fixed number of slots
// (like llama-server's), so numbers are repeatable, or a real one.
package bench

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/consumer"
	"wetink/internal/envelope"
	"wetink/internal/eval"
	"wetink/internal/mockgw"
	"wetink/internal/tap"
)

type Config struct {
	NatsURL, DatabaseURL string
	Sessions             int           // sessions in parallel
	Events               int           // messages per session
	Parts                int           // paragraphs per message
	Masters              int           // masters per session
	Interval             time.Duration // between a session's messages (0: all at once)
	Classifier           classifier.Classifier
	MockDelay            time.Duration // per model call, when Classifier is nil
	MockSlots            int           // calls served at once per mock server
	MockServers          int           // mock model servers (endpoints)
	Keep                 bool          // keep the sessions (e.g. to look at them in the dashboard)
	Out                  io.Writer
}

type Result struct {
	Events, Entries    int
	Wall               time.Duration
	P50, P95, Max      time.Duration // publish -> processed, per event
	EventsPS, EntriesS float64
}

func message(session string, n, parts int) map[string]any {
	var paras []string
	for p := range parts {
		topic := []string{"storage", "api", "ids", "testing"}[(n+p)%4]
		paras = append(paras, fmt.Sprintf("[topic: %s] Finding %d.%d in %s: the measured value is %d. [score: %d]",
			topic, n, p, session, (n*7+p*13)%100, 3+(n+p)%3))
	}
	return map[string]any{
		"type": "message", "id": tap.NewID("msg"), "role": "assistant",
		"content": []any{map[string]any{"type": "output_text", "text": strings.Join(paras, "\n\n"), "annotations": []any{}}},
	}
}

// Run publishes the load and waits until every event is processed.
func Run(ctx context.Context, cfg Config, db *pgxpool.Pool) (Result, error) {
	var res Result
	c := cfg.Classifier
	if c == nil {
		var urls []string
		for range max(cfg.MockServers, 1) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return res, err
			}
			srv := &http.Server{Handler: mockgw.NewHandler(mockgw.Options{Delay: cfg.MockDelay, Slots: cfg.MockSlots})}
			go func() { _ = srv.Serve(ln) }()
			defer srv.Close()
			urls = append(urls, "http://"+ln.Addr().String()+"/v1")
		}
		c = classifier.NewSystemOne(strings.Join(urls, ","), "", "", nil, max(cfg.MockSlots, 1), time.Minute)
	}
	run := "bench-" + tap.NewID("x")[2:8]
	var sessions, subjects []string
	for i := range cfg.Sessions {
		s := fmt.Sprintf("%s-%02d", run, i)
		sessions = append(sessions, s)
		subjects = append(subjects, envelope.Subject(s))
	}
	cctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(cctx, consumer.Config{NatsURL: cfg.NatsURL, DatabaseURL: cfg.DatabaseURL,
			Durable: run, Subjects: subjects}, c)
	}()
	defer func() {
		stop()
		<-done
		if nc, err := nats.Connect(cfg.NatsURL); err == nil {
			if js, err := jetstream.New(nc); err == nil {
				_ = js.DeleteConsumer(context.Background(), envelope.StreamName, run)
			}
			nc.Close()
		}
		if !cfg.Keep {
			for _, s := range sessions {
				_ = eval.Forget(context.Background(), db, cfg.NatsURL, s)
			}
		}
	}()

	total := cfg.Sessions * cfg.Events
	start := time.Now()
	errs := make(chan error, cfg.Sessions)
	for _, s := range sessions {
		go func() {
			taps := make([]*tap.Tap, cfg.Masters)
			for m := range taps {
				t, err := tap.New(s, fmt.Sprintf("master-%c", 'a'+m), cfg.NatsURL, tap.Options{})
				if err != nil {
					errs <- err
					return
				}
				t.Start()
				defer t.Close(10 * time.Second)
				taps[m] = t
			}
			for n := range cfg.Events {
				taps[n%cfg.Masters].Publish(message(s, n, cfg.Parts), 0)
				if cfg.Interval > 0 {
					time.Sleep(cfg.Interval)
				}
			}
			errs <- nil
		}()
	}
	for range sessions {
		if err := <-errs; err != nil {
			return res, err
		}
	}
	deadline := time.Now().Add(20 * time.Minute)
	for {
		var processed int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM events WHERE session = ANY($1) AND classified_at IS NOT NULL`,
			sessions).Scan(&processed); err != nil {
			return res, err
		}
		if processed >= total {
			break
		}
		if time.Now().After(deadline) {
			return res, fmt.Errorf("timed out: %d of %d events processed", processed, total)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return res, ctx.Err()
		}
	}
	res.Wall = time.Since(start)
	var p50, p95, worst float64
	err := db.QueryRow(ctx, `
		SELECT count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), max(ms)
		FROM (SELECT extract(epoch FROM classified_at - published_at)::float8 * 1000 AS ms FROM events WHERE session = ANY($1)) t`,
		sessions).Scan(&res.Events, &p50, &p95, &worst)
	if err != nil {
		return res, err
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM entries WHERE session = ANY($1)`, sessions).Scan(&res.Entries); err != nil {
		return res, err
	}
	ms := func(x float64) time.Duration { return time.Duration(x * float64(time.Millisecond)) }
	res.P50, res.P95, res.Max = ms(p50), ms(p95), ms(worst)
	res.EventsPS = float64(res.Events) / res.Wall.Seconds()
	res.EntriesS = float64(res.Entries) / res.Wall.Seconds()
	return res, nil
}

func (r Result) String() string {
	round := func(d time.Duration) time.Duration { return d.Round(10 * time.Millisecond) }
	return fmt.Sprintf("  %d events, %d entries in %s: %.1f events/s, %.1f entries/s\n  publish -> processed: p50 %s, p95 %s, max %s",
		r.Events, r.Entries, round(r.Wall), r.EventsPS, r.EntriesS, round(r.P50), round(r.P95), round(r.Max))
}
