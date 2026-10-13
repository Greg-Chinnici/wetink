package handoff

import (
	"crypto/subtle"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"wetink/internal/envelope"
	"wetink/internal/ojson"
	"wetink/internal/store"
)

// API serves sessions, handoffs and pulls over HTTP (bearer token if Token is set):
//
//	PUT /v1/sessions/{session}           {"task": "...", "topics": {"name": "description", ...}}
//	GET /v1/sessions/{session}/handoff   [?per_topic=8&max_chars=8000&exclude_master=master-a]
//	GET /v1/sessions/{session}/context   [?topic=storage&since_seq=812&exclude_master=master-a]
//	GET /healthz
type API struct {
	DB    store.TxDB
	Token string
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{session}/handoff", a.handoff)
	mux.HandleFunc("GET /v1/sessions/{session}/context", a.context)
	mux.HandleFunc("PUT /v1/sessions/{session}", a.putSession)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	return mux
}

type httpError struct {
	status int
	msg    string
}

func badRequest(format string, args ...any) *httpError {
	return &httpError{http.StatusBadRequest, fmt.Sprintf(format, args...)}
}

func (a *API) authorize(r *http.Request) (string, *httpError) {
	if a.Token != "" {
		want := "Bearer " + a.Token
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(want)) != 1 {
			return "", &httpError{http.StatusUnauthorized, "missing or wrong bearer token"}
		}
	}
	session := r.PathValue("session")
	if err := envelope.CheckName(session, "session"); err != nil {
		return "", badRequest("%v", err)
	}
	return session, nil
}

func queryInt(r *http.Request, name string, def, lo, hi int64) (int64, *httpError) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, badRequest("%s must be an integer", name)
	}
	if v < lo || v > hi {
		return 0, badRequest("%s must be %d-%d", name, lo, hi)
	}
	return v, nil
}

func excludeMaster(r *http.Request) (string, *httpError) {
	m := r.URL.Query().Get("exclude_master")
	if m != "" {
		if err := envelope.CheckName(m, "master"); err != nil {
			return "", badRequest("%v", err)
		}
	}
	return m, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := ojson.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func fail(w http.ResponseWriter, e *httpError) { http.Error(w, e.msg, e.status) }

func (a *API) handoff(w http.ResponseWriter, r *http.Request) {
	session, herr := a.authorize(r)
	if herr != nil {
		fail(w, herr)
		return
	}
	exclude, herr := excludeMaster(r)
	if herr != nil {
		fail(w, herr)
		return
	}
	perTopic, herr := queryInt(r, "per_topic", DefaultPerTopic, 1, MaxPerTopic)
	if herr != nil {
		fail(w, herr)
		return
	}
	maxChars, herr := queryInt(r, "max_chars", DefaultMaxChars, 500, 200_000)
	if herr != nil {
		fail(w, herr)
		return
	}
	result, err := Snapshot(r.Context(), a.DB, session, Options{PerTopic: int(perTopic), MaxChars: int(maxChars), ExcludeMaster: exclude})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

func (a *API) context(w http.ResponseWriter, r *http.Request) {
	session, herr := a.authorize(r)
	if herr != nil {
		fail(w, herr)
		return
	}
	exclude, herr := excludeMaster(r)
	if herr != nil {
		fail(w, herr)
		return
	}
	since, herr := queryInt(r, "since_seq", 0, 0, 1<<62)
	if herr != nil {
		fail(w, herr)
		return
	}
	maxChars, herr := queryInt(r, "max_chars", DefaultMaxChars, 500, 200_000)
	if herr != nil {
		fail(w, herr)
		return
	}
	result, err := Pull(r.Context(), a.DB, session, PullOptions{
		Topic: r.URL.Query().Get("topic"), SinceSeq: since, ExcludeMaster: exclude, MaxChars: int(maxChars),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

func (a *API) putSession(w http.ResponseWriter, r *http.Request) {
	session, herr := a.authorize(r)
	if herr != nil {
		fail(w, herr)
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		fail(w, badRequest("%v", err))
		return
	}
	v, err := ojson.Decode(data)
	body, ok := v.(*ojson.Object)
	if err != nil || !ok {
		fail(w, badRequest("body must be a JSON object"))
		return
	}
	var task *string
	if t, _ := body.Get("task"); t != nil {
		s, ok := t.(string)
		if !ok {
			fail(w, badRequest("task must be a string"))
			return
		}
		task = &s
	}
	rawTopics, _ := body.Get("topics")
	topics, err := ParseTopics(rawTopics)
	if err != nil {
		fail(w, badRequest("%v", err))
		return
	}
	if err := PutSession(r.Context(), a.DB, session, task, topics); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, ojson.New().Set("session", session).Set("task", task).Set("topics", topics))
}
