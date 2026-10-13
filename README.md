# wetink: a streaming blackboard for LLM agents

A research project on the **blackboard architecture** (Hearsay-II and its successors) applied to
parallel LLM agents ("masters"), with two constraints most current systems do not take on:

1. **LLM-agnostic.** Masters can be any model from any provider, on any host. The board speaks
   [Open Responses](https://www.openresponses.org/specification) items in and out, and never
   depends on provider-side state.
2. **Written while the model runs.** A master's output reaches the board a paragraph at a time
   as it is generated, not when the full response is done. Other masters can read it within
   about a second of it being written, while the ink is still wet.

Plan: [idea.md](idea.md). Design, evidence and limits: [docs/shared-context.md](docs/shared-context.md).
Status: **milestone 5 (real masters)** in progress: two masters on OpenRouter models share a board
mid-run; first findings and fixes in [docs/shared-context.md](docs/shared-context.md#10-real-masters-milestone-5).

## The blackboard

| Blackboard concept | Here |
| --- | --- |
| Knowledge sources | Masters: independent agents on any model and provider |
| Board | `entries` in Postgres, per session and topic, each with kind, relevance, author and lifecycle (active, superseded, resolved, duplicate) |
| Writing | **Passive**: a tap on each master publishes every output item to NATS JetStream, streaming messages paragraph by paragraph. **Explicit**: the `share_context` MCP tool |
| Board upkeep | A consumer splits items into entries, classifies each with a small decision model (topic, kind, relevance) and relates it to what is on the board (replaces, answers, restates, contradicts) |
| Reading | A **handoff** (the board as a portable `input` array) when a master starts; `get_shared_context` (everything, or only changes since a stream sequence) while it runs |
| Control | Deliberately none: masters decide when to read. The board marks conflicts but leaves resolving them to the masters, since the small model cannot reason about them |

Every line a master reads names its author, sequence and entry id
(`- [decision from master-a, seq 3, #41] ...`), so agents do not mistake each other's claims for
their own or for confirmation.

## Research questions

| # | Question | Evidence so far |
| --- | --- | --- |
| 1 | Can a board filled **passively** from agents' output, maintained by a small model (3B, ~0.6 s per entry), keep the right things visible and hide the rest? | Scripted scenarios, 3 runs each: 17/17, 17/19, 15-16/17, 13/13 handoff checks ([Evals](#evals)) |
| 2 | Does **streaming writes** get information to other agents earlier without changing what the board holds? | Same 17/17 with streaming; 5 of 7 parts were on the board before their message finished: 0.6 s earlier at 40 words/s, 1.8 s at 12 words/s |
| 3 | Can the board stay **provider-neutral** end to end? | Handoffs accepted by the mock gateway and LM Studio's `/v1/responses`; tool exchanges are never split across a handoff |
| 4 | Do agents **do better** with the board: less duplicated work, fewer contradictions, more tasks solved? | Not measured yet. The harness is ready ([Experiments](#experiments)): two coding agents on related issues of one repo, isolated or sharing a board, scored by hidden tests and by merging their branches. Further baselines: full-transcript sharing, explicit-write-only memory, merging only at checkpoints |

Question 4 is the main open one. Questions 1-3 show the board works as designed; they do not yet
show it helps.

## Related work

| Work | Shared with this project | Different here |
| --- | --- | --- |
| Blackboard systems (Hearsay-II, 1970s-80s); LLM revivals such as [bMAS](https://arxiv.org/abs/2507.01701) and [arXiv 2510.01285](https://arxiv.org/abs/2510.01285) | A shared board that autonomous agents read and write | Those agents read and write whole turns, usually with one model family and in one process. Here writes stream mid-generation, from remote agents on mixed providers |
| MetaGPT's shared message pool | Publish/subscribe between agent roles | Messages are whole outputs, and nothing classifies them or retires stale ones |
| [Mem0](https://mem0.ai), [Zep / Graphiti](https://help.getzep.com/v2/concepts) | Extraction from conversations; new facts update or invalidate old ones | Built as memory for one assistant across sessions, not as a live board for peers working in parallel; extraction uses a large LLM |
| Shared-memory MCP servers (e.g. [mcp-agent-memory](https://github.com/justanotherspy/mcp-agent-memory), [shared-memory-mcp](https://glama.ai/mcp/servers/@haasonsaas/shared-memory-mcp)) | MCP tools for agents to share context | Explicit writes only: agents share what they choose to. Here capture is passive as well, through the tap |
| [Group Think](https://arxiv.org/abs/2505.11107), [Hogwild! Inference](https://arxiv.org/abs/2504.06261) | Parallel workers see each other's partial output | Token level, through a shared KV cache: one model, one machine. Here it is paragraph level, over a message log: any models, any hosts |

More, with links and ideas (including decision models to classify with): [research/](research/README.md).

## Components

A tap publishes items from a master to NATS JetStream, and messages a paragraph at a time while
they are being written. A consumer stores them in Postgres `events`, splits each into entries
(paragraphs, list items, code blocks, tool exchanges), classifies each entry (topic, kind,
relevance) and relates it to the current board (supersedes, answers, restates, contradicts). A
handoff service returns the active entries as a portable Open Responses `input` array. An MCP
server gives running masters two tools: `get_shared_context` (read what the others found, or
only what changed) and `share_context` (write a decision, result or question deliberately).

## Setup

Everything is one Go binary, `wetink`. Tooling comes from `flake.nix`; NATS and Postgres run in
Docker Compose.

```sh
nix develop            # or `direnv allow` (uses .envrc)
docker compose up -d --wait
go build -o bin/wetink ./cmd/wetink   # or: nix build (result/bin/wetink)
```

The dev shell sets defaults for `NATS_URL`, `DATABASE_URL`, `OPENAI_BASE_URL` (mock gateway)
and `OPENAI_API_KEY`. Override any of them in your environment.

## Run

The classifier is the LiquidAI d1-3B decision model served by `llama-server` (built from a
pinned llama.cpp in `flake.nix`). New topics are named by LFM2.5 1.2B in LM Studio.

```sh
hf download LiquidAI/d1-3B-GGUF d1-3B-Q4_K_M.gguf --local-dir models/d1-3B   # once, 1.7 GB
llama-server -m models/d1-3B/d1-3B-Q4_K_M.gguf --port 9931 -ngl 99 -c 8192 &
lms load lfm2.5-1.2b-instruct-mlx     # topic namer, in LM Studio
wetink mockgw &                       # Open Responses mock on :8080 (for masters)
wetink consumer &                     # NATS -> events -> entries, sessions in parallel
wetink api &                          # sessions, handoffs and pulls on :8090
wetink mcp &                          # shared-context tools on :8091/mcp
wetink dashboard &                    # live view on http://127.0.0.1:8095
curl -X PUT -H "Authorization: Bearer $WETINK_TOKEN" localhost:8090/v1/sessions/s-1 \
  -d '{"task": "...", "topics": {"storage": "database choice and schema"}}'   # optional
wetink master --session s-1 --master master-a "your prompt"
curl -H "Authorization: Bearer $WETINK_TOKEN" localhost:8090/v1/sessions/s-1/handoff
wetink master --session s-1 --master master-b \
  --handoff-url http://localhost:8090/v1/sessions/s-1/handoff "continue the task"   # fresh master
wetink master --session s-1 --master master-b \
  --mcp-url http://127.0.0.1:8091/mcp "continue the task"   # with the pull and share tools
psql "$DATABASE_URL" -c "select id, master, topic, kind, score, status, relation, related_id, left(text, 60) from entries where session = 's-1'"
wetink stats --session s-1            # publish -> classified latency
```

Masters use `OPENAI_BASE_URL`/`OPENAI_API_KEY` (the mock by default), `--model` and
`--max-turns` (default 12). For OpenRouter, put `OPENROUTER_API_KEY` in `.env` (git-ignored,
loaded by direnv) and run masters with
`OPENAI_BASE_URL=https://openrouter.ai/api/v1 OPENAI_API_KEY=$OPENROUTER_API_KEY`. Rate-limited
requests (429, 502, 503) are retried.

## Handoff

`PUT /v1/sessions/{session}` declares the task and its topics (recommended; see the design doc).
`GET /v1/sessions/{session}/handoff[?per_topic=8&max_chars=8000&exclude_master=X]` (bearer
`WETINK_TOKEN`) returns `{session, upto_seq, pending, items, entry_ids, input}`. `input` is empty or one `user`
message: active entries grouped by topic, one attributed line each
(`- [decision from master-a, seq 3, #41] ...`), conflicts marked, open questions in their own
section, fitted to `max_chars`. Prepend it to a master's own input; it was accepted by the mock
and by LM Studio's `/v1/responses` (streaming and not). Details:
[docs/shared-context.md](docs/shared-context.md).

The classifier is configured by `WETINK_CLASSIFIER_API`, `WETINK_CLASSIFIER_BASE_URL`,
`WETINK_CLASSIFIER_MODEL` and `WETINK_CLASSIFIER_API_KEY`:

| `WETINK_CLASSIFIER_API` | Backend |
| --- | --- |
| `systemone` (dev default) | A decision model on a TypeSafe System One endpoint (`llama-server`, `:9931`). Questions: kind (choice), relevance (5-level score), topic (choice of known topics or new); final is derived from kind. `WETINK_TOPIC_NAMER_*` names new topics with a chat model. |
| `chat` | A chat model with a JSON schema on `/v1/chat/completions` (e.g. LM Studio). |
| `responses` | A chat model on an Open Responses provider or gateway. |

Hosted decision models speak the same System One API, so `systemone` works with them too
(not yet run against our evals; see [research/](research/README.md#decision-models)):

| Model | `WETINK_CLASSIFIER_BASE_URL` | `WETINK_CLASSIFIER_MODEL` |
| --- | --- | --- |
| TypeSafe Jev | `https://api.typesafe.ai/v1` | `jev-latest` |
| Cloudflare Clef / Clef-flash | `https://api.cloudflare.com/client/v4/accounts/<ACCOUNT_ID>/ai/run/@cf/cloudflare/clef-flash` (or `.../clef`) | `clef-flash` (or `clef`) |

The key goes in `WETINK_CLASSIFIER_API_KEY`. Workers AI URLs are used as given and their
`result` envelope is unwrapped. A local server is compute-bound, so 4 calls in flight per
endpoint is the default; hosted models are not, so raise it with
`WETINK_CLASSIFIER_CONCURRENCY` (and `WETINK_RELATE_CONCURRENCY`).

Relation judgments can use a separate backend (`WETINK_RELATE_API`, `WETINK_RELATE_MODEL`,
`WETINK_RELATE_BASE_URL`, `WETINK_RELATE_API_KEY`), e.g. a chat model while d1 classifies; compare
models with `wetink eval-relations --api chat --model <model> --base-url <url>`.

For the mock: `--classifier-api responses --classifier-model mock --classifier-base-url
http://localhost:8080/v1` (the mock also serves `/v1/systemone` and `/v1/chat/completions`).

## Pull and share (MCP)

`wetink mcp` serves streamable HTTP at `/mcp` (stateless, JSON responses). A master connects with
`X-Wetink-Session` and `X-Wetink-Master` headers (or `?session=&master=`) and, when `WETINK_TOKEN` is
set, `Authorization: Bearer <token>`. Any MCP client works (an agent CLI, a provider's hosted
MCP tools); `wetink master --mcp-url` bridges the tools to plain function tools so they work with
any provider behind the gateway.

- `get_shared_context(topic?, since_seq=0)`: the current shared context from the other masters
  (like a handoff), or with `since_seq` (the `upto_seq` from the last call) only the changes:
  new entries and earlier ones that were superseded or answered. Also
  `GET /v1/sessions/{session}/context` on `wetink api`.
- `share_context(text, kind, topic?, replaces?)`: the MCP server publishes the entry on the
  stream, and it becomes a declared entry (the master's kind and topic, top relevance). The
  result names the new entry's id once it is processed; `replaces: [id, ...]` retracts the
  master's own earlier entries. It is still compared with existing entries, so it can answer
  a question or replace the master's earlier entry without `replaces`.

The outputs of these tools never become entries, so pulled context does not come back as the
pulling master's own work.

## Streaming writes

The tap publishes a message while the model writes it: each time a paragraph is complete (a
blank line outside a code block), it sends a snapshot (`rev 1, 2, ...`, item status
`in_progress`), and the consumer turns the paragraphs that can no longer change into entries
right away. The finished item adds only the rest. Other masters see each part within about a
second of it being written, not when the whole message ends; the slower the model, the bigger
the head start. Turn it off with `tap.Options{NoStreamMessages: true}`.

## Tapping any agent

`wetink proxy` is a model endpoint that taps whatever agent calls it, so coding agents write to the
board while their model runs, without changes to the agent. The agent's base URL names its
session and master:

```sh
wetink proxy &                        # :8092; Responses calls go to OPENAI_BASE_URL with its key
ANTHROPIC_BASE_URL=http://127.0.0.1:8092/s/s-1/master-a claude -p "..."    # Claude Code
# Codex and other Responses clients: base URL http://127.0.0.1:8092/s/s-1/master-a/v1
```

Anthropic Messages calls go on to the Anthropic API with the agent's own credentials (a
claude.ai login works); Open Responses calls go to `--upstream` with the proxy's key. Text and
tool calls are published as items, text a paragraph at a time while it streams. Claude Code's
side calls (titles, summaries: calls offering no tools) are not tapped, nor are the outputs of
the agents' own tools unless `--tool-outputs`. Every call is logged to
`runs/proxy/<session>/<master>.jsonl` (new input, output items, usage).

## Experiments

`wetink experiment` measures question 4 on coding work: agents fix related issues of one
repository, each in its own clone of the same base commit, and the result is judged by hidden
tests, by merging their branches, and by hidden tests that need both changes.

```sh
wetink experiment check experiments/toy         # hidden tests fail on the base, pass with the gold fixes
wetink experiment run experiments/toy --runs 3  # Claude Code (haiku by default; --model sonnet ...)
wetink experiment report experiments/toy        # compare the conditions over all runs so far
wetink experiment rescore experiments/toy       # judge finished runs again (scoring or tests changed)
```

| Condition | Agents get | Board |
| --- | --- | --- |
| `isolated` | their own issue | none (calls are logged, not tapped) |
| `shared` | their issue, the other agent's issue, and the shared-context tools | one session for the run, with its own consumer |

An experiment is a directory with `experiment.json`: the repository (git URL, repository or
plain directory) and base commit, setup commands, environment (`$CACHE` is each agent's
writable cache), the session's topics, and per agent an issue (Markdown), hidden tests (a patch
or a directory laid over the checkout), a test command and optionally a gold fix. Conditions
and prompts can be overridden there. With `test_glob` (e.g. `"*_test.go"`), the combined tests
run on a merge without the agents' own test files: two agents adding tests to one file conflict
for no reason that matters here. Both merges are reported.

Each run gets its own proxy and MCP server and, with a board, a consumer for its session; it
needs NATS, Postgres and a classifier (`--classifier-*`, as for `wetink consumer`). Agents run in
parallel under `runs/<experiment>/<run>/`: a checkout per agent (whose git holds only the base
history, so neither the answer nor the other agent's branch can be found), its prompt, its
output stream and, after scoring, `result.json`. Agents:

- **claude** (default): Claude Code headless (`claude -p`), with `Bash` in Claude Code's sandbox
  (writes only to its checkout and cache, no network), no web tools or sub-agents. With
  `CLAUDE_CODE_OAUTH_TOKEN` (from `claude setup-token`) or `ANTHROPIC_API_KEY` set, each agent
  gets a fresh config directory; otherwise the logged-in config is used without user settings.
- **codex**: Codex CLI against the proxy's Responses endpoint (`--model`, `--upstream`).
  Codex sends OpenAI-specific tools (namespaced MCP tools, hosted web search) that other
  providers may reject; untested beyond the mock.

`experiments/toy` is a small Go key-value store with two issues, entries that expire and
saving to JSON. Each has its own hidden tests; the combined ones check that a saved store keeps
its expiry, which only works if the two changes agree on it. The gold fixes made apart show
the failure the experiment is after: both pass their own tests and merge cleanly, and the
merged store loses expiry on save.

A smoke run on the toy (Claude Code, Haiku, one run each, mock classifier) went through end to
end: in both conditions both agents passed their own hidden tests, conflicted only in
`kv_test.go`, and failed the combined tests. With the board they shared 8 times and pulled 6,
and discussed the format (master-b made it "extensible", master-a asked for it after finishing),
but neither wrote the expiry into it. One run is not evidence; it shows the harness measures
the thing in question. Real runs should classify with d1 (the mock has no narration kind, so
"let me check..." lines show as facts).

## Dashboard

`wetink dashboard` (http://127.0.0.1:8095) shows a session live: each stage of the
pipeline, the timeline of items and the entries made from them, the shared context by topic,
and per entry what the models were asked and answered and what the pipeline decided. Other tabs
show the handoff, scenario checks, the raw NATS messages and run logs. **Run test** plays a
scenario with a pace so you can follow it.

`wetink seed --reset` deletes all data (Postgres tables and the NATS stream) and plays
every scenario into a session named after it.

## Evals

Shared context end to end: scripted sessions through the whole pipeline, checking what the
handoff includes and leaves out (needs NATS, Postgres and `llama-server`):

```sh
wetink eval evals/context/url-shortener.json --runs 3 --show
```

| Scenario | Checks per run |
| --- | --- |
| url-shortener | 17, 17, 17 of 17 |
| sales-pipeline | 17, 17, 17 of 19 |
| docs-migration | 15-16 of 17 (latest: 16, 16, 16; Python: 16, 15, 16) |
| pull-and-share (pull and share steps through the MCP server) | 13, 13, 13 of 13 |

After the milestone 5 changes (narration, headings, pooled change probability, explicit
replaces), 2 runs each: 17, 17 / 17, 17 / 15, 16 / 13, 13, the same as before.

Relation judgments alone: `evals/relations.jsonl` (18 cases), `wetink eval-relations --api
systemone --model default --base-url http://localhost:9931/v1`; d1-3B gets 14/18.

docs-migration varies because the model rates "all 214 pages now render" as replacing the
earlier "36 of the 38 pages" fact at p = 0.68-0.75, either side of the 0.7 bar for closing an
entry; the Go port and the Python original behave the same.

url-shortener streams its three multi-part messages (`"stream": true` steps) and gets the same
17/17 as without streaming; 5 of their 7 parts are published before their message finishes
(1.8 s earlier on average at 12 words/s, 0.6 s at 40).

Classifier alone: `evals/classifier.jsonl` holds labelled items. Compare models with:

```sh
wetink eval-classifier --api systemone --model default --base-url http://localhost:9931/v1   # d1-3B
wetink eval-classifier --api chat --base-url http://localhost:1234/v1 --model <lms model>
```

Results on the 21 items (2026-10-09, M4 Pro):

| Classifier | topic | kind | final | score | p50 / item |
| --- | --- | --- | --- | --- | --- |
| Mock keyword rules | n/a | 14/19 | 15/17 | 18/21 | 0 |
| LFM2.5 1.2B (chat, JSON schema) | 18/19 | 10/19 | 15/17 | 13/21 | 0.63 s |
| LiquidAI d1-3B (systemone) | 19/19 | 17/19 | 17/17 | 20/21 | 0.25 s |
| LiquidAI d1-omni-600M (systemone) | 17/19 | 14/19 | 14/17 | 16/21 | 0.04 s |

Where the time goes (d1-3B, M4 Pro): the decision model runs one pass per question, about
60 ms for a one-line question and 80-100 ms with our rubric descriptions, so about 270 ms for
three questions. Naming a new topic adds about 65 ms. The pipeline (NATS, Postgres) adds about
15 ms. Shortening the rubric to bare labels saved 25% but cost 5 points each on kind and score.
d1-omni-600M is 6x faster but tagged "leaning towards X, not decided" as a decision and scored
chit-chat above the shared-context threshold, so d1-3B stays the default.
End to end, with two masters publishing at the same moment every 2 s: p50 0.67 s, p95 0.77 s.

The mock classifier is a keyword heuristic: `[topic: X]` sets the topic, `[score: N]` the
relevance and `[rel: replaces: text]` a relation; "decided", "maybe", "?" and code fences pick
decision, hypothesis, open_question and artifact. See `internal/mockgw`.

## Throughput

`wetink bench` publishes many sessions at once and measures throughput and publish -> processed
latency, with a mock decision model (`--mock-delay 250ms --mock-slots 4 --mock-servers 1`) or
the configured one (`--real`). Sessions are processed in parallel and entries are classified
ahead within a session: under the mock, 4 sessions go from 1.9 to 7.2 entries/s, one session
from 1.9 to 3.5, and a second endpoint doubles it again (14.2). One local llama-server is
compute-bound at ~3.4 calls/s (about 1.5 entries/s) however many calls it gets at once, so more
throughput needs more endpoints: `WETINK_CLASSIFIER_BASE_URL=http://gpu-a:9931/v1,http://gpu-b:9931/v1`.
Details: [docs/shared-context.md](docs/shared-context.md#cost-and-throughput).

## Test

```sh
go test ./...          # integration tests skip if NATS/Postgres are not up; stop `wetink consumer` first
go vet ./... && golangci-lint run
```

`testdata/golden` holds outputs of the original Python implementation (commit `a44b120`) (segmentation including
every streaming prefix, the exact JSON sent to the decision model, parsing edge cases, rendered
handoffs and change lists, check results); the Go code reproduces them exactly, except where
behaviour changed on purpose since (noted in the file, e.g. the narration kind).

## Layout

| Path | What |
| --- | --- |
| `cmd/wetink` | The `wetink` binary: one subcommand per service and tool |
| `internal/envelope` | Envelope wire format, subject `wetink.<session>.items`, `Nats-Msg-Id`, stream setup |
| `internal/tap` | Non-blocking publisher with local buffer; streams messages a paragraph at a time |
| `internal/consumer` | Ingest (store, then ack) and one ordered worker per session; retries in Postgres |
| `internal/segment` | Splits an item's text into entry parts; stable streaming prefixes |
| `internal/pipeline` | Events -> entries: classify, relate, lifecycle (`Decide`) |
| `internal/classifier` | Decision-model and chat-model classifiers, relation questions, topic namer |
| `internal/store` | `schema.sql` (embedded): `events`, `sessions`, `entries`, views, notify triggers |
| `internal/handoff` | Handoffs, pulls (changes since a stream sequence) and their HTTP API |
| `internal/mcpserver` | MCP server with `get_shared_context` and `share_context` |
| `internal/master` | Minimal master tool loop that keeps its own input; MCP tool bridge |
| `internal/proxy` | Model endpoint that taps any agent (Anthropic Messages, Open Responses) |
| `internal/experiment` | Coding experiments: checkouts, agents (Claude Code, Codex), scoring, report |
| `experiments/` | Experiment specs (`toy`: two coupled issues on a small Go store) |
| `internal/mockgw` | Deterministic mock of Responses (SSE), Chat Completions and System One |
| `internal/dashboard` | Live view of sessions, models' answers and checks (page embedded) |
| `internal/eval` | Scenario runner, check evaluation, seed/reset, classifier eval |
| `internal/bench` | Load benchmark: throughput and latency, mock or real classifier |
| `internal/oai`, `ojson`, `item`, `pystr` | Raw-JSON model client, ordered JSON, item helpers, string rules |
