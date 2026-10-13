// Package mcpserver is the MCP server with the shared-context tools for masters (streamable
// HTTP, stateless, at /mcp).
//
//   - get_shared_context(topic?, since_seq?): what the other masters found and decided
//     (handoff.Pull).
//   - share_context(text, kind, topic?): publish an entry deliberately. The server publishes it
//     on the stream itself (via "mcp"), so it works for masters without a tap too; the
//     consumer turns it into a declared entry (pipeline).
//
// Each master connects with its identity in headers (X-Wetink-Session, X-Wetink-Master; or
// ?session=&master= on the URL) and, if a token is set, `Authorization: Bearer <token>`.
package mcpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/handoff"
	"wetink/internal/pipeline"
	"wetink/internal/pystr"
	"wetink/internal/store"
	"wetink/internal/tap"
)

const Instructions = "Shared context for agents working on one task in parallel. Call get_shared_context to " +
	"see what the other agents have found, decided and asked, and call it again with the " +
	"since_seq it returns to get only what changed. Call share_context when you make a " +
	"decision, verify a result, or have a question that the other agents need."

const pullDescription = "Get what the other agents working on this task have found, decided and asked. " +
	"With since_seq=0 it returns the whole current shared context; pass the since_seq from " +
	"the previous result to get only the changes after it (new entries, and entries that " +
	"were superseded or answered). Entries are attributed to the agent that reported them; " +
	"treat them as that agent's claims and check anything critical."

const shareDescription = "Tell the other agents something they need: a decision you have committed to, a result " +
	"you verified, or an open question. Share one self-contained point per call, written so " +
	"that it makes sense without your conversation; related details (the columns of one " +
	"table) belong in one call. The result gives the new entry's id. To change something you " +
	"shared earlier, share the new version and put the earlier entry's id in replaces."

var pullSchema = json.RawMessage(`{"type":"object","properties":{` +
	`"topic":{"type":"string","description":"only this topic (optional)"},` +
	`"since_seq":{"type":"integer","default":0,"description":"the since_seq from the previous result; 0 for everything"}}}`)

var shareSchema = json.RawMessage(`{"type":"object","properties":{` +
	`"text":{"type":"string"},` +
	`"kind":{"type":"string","enum":["decision","fact","open_question","artifact"]},` +
	`"topic":{"type":"string","description":"optional; one of the topics get_shared_context lists, if one fits"},` +
	`"replaces":{"type":"array","items":{"type":"integer"},"description":"ids of your earlier entries this one replaces (optional)"}},` +
	`"required":["text","kind"]}`)

// ShareWait is how long share_context waits for its entry to be processed, to report its id
// and outcome.
var ShareWait = 5 * time.Second

const selectShared = `
SELECT x.id, x.status, coalesce(x.relation, ''), x.related_id, coalesce(r.master, ''),
       coalesce((SELECT array_agg(o.id ORDER BY o.id) FROM entries o WHERE o.closed_by = x.id), '{}')
FROM entries x JOIN events e ON e.id = x.event_id
LEFT JOIN entries r ON r.id = x.related_id
WHERE e.session = $1 AND e.stream_seq = $2`

const selectMaxSeq = `
SELECT coalesce(max(seq), 0) FROM events WHERE session = $1 AND master = $2 AND via = 'mcp'`

// SharePublisher publishes share_context calls to JetStream with their own per-master seq.
type SharePublisher struct {
	natsURL string
	db      store.DB
	mu      sync.Mutex
	nc      *nats.Conn
	js      jetstream.JetStream
	seq     map[[2]string]int64
}

func (p *SharePublisher) Publish(ctx context.Context, session, master string, args map[string]any) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nc == nil || !p.nc.IsConnected() {
		nc, err := nats.Connect(p.natsURL, nats.Name("mcp-server"))
		if err != nil {
			return 0, err
		}
		js, err := jetstream.New(nc)
		if err == nil {
			err = envelope.EnsureStream(ctx, js)
		}
		if err != nil {
			nc.Close()
			return 0, err
		}
		p.nc, p.js = nc, js
	}
	key := [2]string{session, master}
	if _, ok := p.seq[key]; !ok {
		// Continue after the last share stored for this master (e.g. after a restart).
		var last int64
		if err := p.db.QueryRow(ctx, selectMaxSeq, session, master).Scan(&last); err != nil {
			return 0, err
		}
		p.seq[key] = last
	}
	p.seq[key]++
	argBytes, _ := json.Marshal(args)
	env := envelope.Envelope{
		Session: session, Master: master, Seq: p.seq[key], Via: envelope.ViaMCP,
		Item: map[string]any{
			"type":      "function_call",
			"id":        tap.NewID("fc"),
			"call_id":   tap.NewID("call"),
			"name":      pipeline.ShareTool,
			"arguments": string(argBytes),
			"status":    "completed",
		},
	}
	data, err := env.Marshal()
	if err != nil {
		return 0, err
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ack, err := p.js.Publish(pctx, env.Subject(), data, jetstream.WithMsgID(env.MsgID()))
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

func (p *SharePublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nc != nil {
		p.nc.Close()
	}
}

type Server struct {
	DB        store.TxDB
	Token     string
	Publisher *SharePublisher
	mcp       *mcp.Server
}

func New(db store.TxDB, natsURL, token string) *Server {
	s := &Server{DB: db, Token: token, Publisher: &SharePublisher{natsURL: natsURL, db: db, seq: map[[2]string]int64{}}}
	s.mcp = mcp.NewServer(&mcp.Implementation{Name: "wetink-shared-context", Version: "1.0.0"}, &mcp.ServerOptions{Instructions: Instructions})
	s.mcp.AddTool(&mcp.Tool{Name: pipeline.PullTool, Description: pullDescription, InputSchema: pullSchema}, s.pull)
	s.mcp.AddTool(&mcp.Tool{Name: pipeline.ShareTool, Description: shareDescription, InputSchema: shareSchema}, s.share)
	return s
}

// Handler serves MCP at /mcp. Query parameters session and master are accepted in place of
// the headers, and the bearer token (if any) is checked for every request.
func (s *Server) Handler() http.Handler {
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s.mcp },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	mux := http.NewServeMux()
	mux.Handle("/mcp", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.Token)) != 1 {
			http.Error(w, "missing or wrong bearer token", http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		for header, param := range map[string]string{"X-Wetink-Session": "session", "X-Wetink-Master": "master"} {
			if r.Header.Get(header) == "" && q.Get(param) != "" {
				r.Header.Set(header, q.Get(param))
			}
		}
		h.ServeHTTP(w, r)
	}))
	return mux
}

func (s *Server) Close() { s.Publisher.Close() }

func identity(req *mcp.CallToolRequest) (string, string, error) {
	if req.Extra == nil || req.Extra.Header == nil {
		return "", "", errors.New("these tools need the streamable HTTP transport")
	}
	session, master := req.Extra.Header.Get("X-Wetink-Session"), req.Extra.Header.Get("X-Wetink-Master")
	if session == "" || master == "" {
		return "", "", errors.New("connect with X-Wetink-Session and X-Wetink-Master headers")
	}
	if err := envelope.CheckName(session, "session"); err != nil {
		return "", "", err
	}
	if err := envelope.CheckName(master, "master"); err != nil {
		return "", "", err
	}
	return session, master, nil
}

func text(s string, isError bool) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}, IsError: isError}
}

func toolError(err error) (*mcp.CallToolResult, error) { return text("Error: "+err.Error(), true), nil }

func (s *Server) pull(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	session, master, err := identity(req)
	if err != nil {
		return toolError(err)
	}
	var args struct {
		Topic    *string  `json:"topic"`
		SinceSeq *float64 `json:"since_seq"`
	}
	if len(req.Params.Arguments) > 0 {
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			return toolError(fmt.Errorf("bad arguments: %w", err))
		}
	}
	o := handoff.PullOptions{ExcludeMaster: master}
	if args.Topic != nil {
		o.Topic = *args.Topic
	}
	if args.SinceSeq != nil && *args.SinceSeq > 0 {
		o.SinceSeq = int64(*args.SinceSeq)
	}
	result, err := handoff.Pull(ctx, s.DB, session, o)
	if err != nil {
		return toolError(err)
	}
	slog.Info("pulled", "session", session, "master", master, "topic", o.Topic, "since", o.SinceSeq,
		"entries", len(result.EntryIDs), "closed", len(result.Closed), "upto_seq", result.UptoSeq)
	return text(result.Text, false), nil
}

func (s *Server) share(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	session, master, err := identity(req)
	if err != nil {
		return toolError(err)
	}
	var args struct {
		Text     string  `json:"text"`
		Kind     string  `json:"kind"`
		Topic    *string `json:"topic"`
		Replaces []any   `json:"replaces"`
	}
	if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
		return toolError(fmt.Errorf("bad arguments: %w", err))
	}
	body := pystr.Strip(args.Text)
	if body == "" {
		return toolError(errors.New("text is empty"))
	}
	valid := false
	for _, k := range pipeline.ShareKinds {
		valid = valid || k == args.Kind
	}
	if !valid {
		return toolError(fmt.Errorf("kind must be one of %s", strings.Join(pipeline.ShareKinds, ", ")))
	}
	published := map[string]any{"text": body, "kind": args.Kind}
	if args.Topic != nil && *args.Topic != "" {
		published["topic"] = *args.Topic
	}
	if len(args.Replaces) > 0 {
		ids := pipeline.ReplaceIDs(map[string]any{"replaces": args.Replaces})
		if len(ids) != len(args.Replaces) {
			return toolError(errors.New("replaces must be a list of entry ids, e.g. [41]"))
		}
		published["replaces"] = ids
	}
	seq, err := s.Publisher.Publish(ctx, session, master, published)
	if err != nil {
		return toolError(err)
	}
	slog.Info("shared", "session", session, "master", master, "kind", args.Kind, "stream_seq", seq)
	return text(s.sharedText(ctx, session, int64(seq), strings.ReplaceAll(args.Kind, "_", " ")), false), nil
}

// sharedText waits (up to ShareWait) for the share's entry and says what became of it.
func (s *Server) sharedText(ctx context.Context, session string, seq int64, kind string) string {
	deadline := time.Now().Add(ShareWait)
	var id int64
	var status, relation, relatedMaster string
	var related *int64
	var closed []int64
	for {
		err := s.DB.QueryRow(ctx, selectShared, session, seq).Scan(&id, &status, &relation, &related, &relatedMaster, &closed)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("share lookup failed", "session", session, "stream_seq", seq, "err", err)
			break
		}
		if (err == nil && status != "pending") || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Sprintf("Shared as a %s (stream sequence %d).", kind, seq)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if id == 0 {
		return fmt.Sprintf("Shared as a %s (stream sequence %d). It is not processed yet; the other agents see "+
			"it once it is, and get_shared_context does not list your own entries.", kind, seq)
	}
	out := fmt.Sprintf("Shared as a %s: entry #%d.", kind, id)
	refs := func(ids []int64) string {
		parts := make([]string, len(ids))
		for i, x := range ids {
			parts[i] = fmt.Sprintf("#%d", x)
		}
		return strings.Join(parts, ", ")
	}
	switch {
	case status == "pending":
		out += " It is still being processed."
	case len(closed) > 0 && relation == "replaces":
		out += " It replaces your " + refs(closed) + "; they are no longer shown."
	case len(closed) > 0:
		out += " It resolves " + refs(closed) + "."
	case relation == "contradicts" && related != nil:
		out += fmt.Sprintf(" It conflicts with #%d from %s; both show, marked as a conflict.", *related, relatedMaster)
	case status == "duplicate" && related != nil:
		out += fmt.Sprintf(" It repeats #%d, so it is not shown again.", *related)
	}
	return out
}
