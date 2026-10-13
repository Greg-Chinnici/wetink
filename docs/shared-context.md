# Shared context: design

Shared context is what one master learns about the others' work: in a handoff when it starts,
and through the pull tool (`get_shared_context`) while it runs. Masters can also publish entries
deliberately (`share_context`). This document records how it is built,
why, the evidence behind each rule, and what it cannot do yet. Plan and goals: [idea.md](../idea.md).

## What a receiving master gets

One `user` message (portable across providers), for example:

```
Shared context from the other agents working on this task (session s-1, up to stream sequence 803).
The task: Build a URL shortener HTTP API. ...

## storage (decisions from master-a and master-b; check that they agree)
- [decision from master-a, seq 4, #95] For the MVP we'll use SQLite instead of Postgres. ...
- [decision from master-b, seq 7, #96] I decided to keep the link table in Redis for now ...

## ids
- [fact from master-a, seq 3, #91] Short codes are the bigint ID encoded in base62 ...

## Open questions
Unresolved; answer them if you can, and do not treat them as settled.
- [open question from master-a, seq 1, #87, topic analytics] Do we need per-link click analytics?
```

Every line names its author (so masters do not take each other's claims as their own), its
per-master sequence, and a stable entry id (`#95`) that later tools can refer to.

## Pipeline

```
item (Open Responses) ──► events (raw log, untouched)
                              │ segment
                              ▼
                          entries ──► classify: topic, kind, relevance
                              │
                              ▼ relate to current entries
                          lifecycle: active · superseded · resolved · duplicate
                              │
                              ▼
                   context_entries / context_ranked views ──► handoff
```

Within a session, events are processed in stream order (`internal/pipeline`): each entry is
related to the context as it stood before it. Sessions are independent and run in parallel
(`internal/consumer`, see Throughput).

### 1. Entries, not items

A real master message carries several claims of different kinds ("I'll take storage. My first
thought is Postgres. Open question: analytics?"). Classified as one unit it got one label: the
first scenario showed that message as a *decision* and lost a whole progress update (two facts
and a question).

Items are split deterministically (`internal/segment`): paragraphs; each list item; a fenced code
block whole, with its short intro line; short lead-ins before lists ("Progress update:") and
markdown headings on their own line (`## API`, `**URL Shortener Design**`) are dropped (real
masters' headings became visible entries). A tool exchange is one entry, rendered with its call ("Tool call lookup(...)
returned: ..."), so a handoff never contains half of a call/output pair. Each entry keeps its
event id and part number back to the raw item.

### 2. Session topics

Per-topic ranking and relations only work if related entries share a topic. Left to the
classifier and a namer, topics fragmented (`postgres`, `mvp_choice`, `redis`,
`short_codes_should_be_3_lowercase_letters_or_numbers`). Now:

- A session can declare its task and topics with descriptions: `PUT /v1/sessions/{session}`.
  Whoever splits the task between masters knows the split.
- The classifier picks from declared topics, then recently used ones (in order of first use, so
  the choices it sees stay stable from entry to entry). It creates a topic only
  if `(new)` has probability >= 0.6, and only for entries that will be shown: a hidden
  hypothesis once created `postgres` and pulled later storage entries into it.
- New topics are named by a small chat model (LFM2.5 1.2B), at most three words.

### 3. Classification

Decision model (LiquidAI d1-3B, `/v1/systemone`), one pass per question:

| Field | How | Why |
| --- | --- | --- |
| kind | choice of fact, decision, hypothesis, open question, narration | |
| narration | model choice at p >= 0.6, for a short single paragraph without markup; never shown | real masters open with "I'll start by sharing my design ..." (relevance 3-3.7); without the bar, short decisions ("We'll load everything into DuckDB") went to narration at 0.41-0.55 |
| artifact | structural: the entry contains a code block | the model labelled plain decisions ("we'll load everything into DuckDB") as artifacts whatever the wording |
| open question | model choice, or a `?` in the text plus p(open question) >= 0.25 | "What format should the short codes have?" came out fact 0.49, open question 0.36 |
| relevance | 5-level score, expected value 1-5, with confidence | ranking and the noise filter |
| topic | choice of known topics or `(new)` | see above |

Two wordings mattered and are commented in the code: "a question" in the open-question
description (questions went from p=0.08 to 0.41-0.75) and "suggestion" in the hypothesis one
("perhaps we should ..." was a decision).

### 4. Lifecycle

Each visible new entry is related to up to ~14 candidates: recent entries on its topic, the
session's open questions, and the same master's recent entries on any topic (topic assignment
is imperfect; answers and revisions must still connect). Two questions: the relation (new,
restates, replaces, answers, contradicts) and which entry. `Decide` turns that into an
outcome, conservatively: hiding a current decision is worse than showing a stale one.

| Relation | Effect | Conditions |
| --- | --- | --- |
| replaces, own entry | older entry **superseded** | p >= 0.7 and same kind (a load-test fact "replacing" a decision at p=0.51 once hid the session's storage decision). For the master's own entry, replaces and contradicts mean the same change, so their probabilities are added: d1 split "switch to MongoDB" 0.52 contradicts / 0.4 replaces |
| share with `replaces: [ids]` | the master's own named entries **superseded**; another master's entry is a **conflict** | explicit, no model |
| replaces / contradicts, another master's entry | **conflict**: both shown, marked `conflicts with #id` | p >= 0.5; nothing is hidden |
| answers | open question **resolved** | p >= 0.7 and the target is an open question |
| restates | new entry is a **duplicate** (hidden) | p >= 0.7; a decision adopting a fact is kept (it is a commitment) |
| new | nothing | default, also below the thresholds |

### 5. Visibility and ranking

Visible (`context_entries`): active, not a hypothesis or narration, relevance >= 3 ("somewhat useful"), or
>= 2.5 for open questions, which score lower by nature. Ranked per topic by relevance, then
recency, with open questions ranked separately (`context_ranked`).

### 6. Conflicts

d1-3B judges "use SQLite for the MVP" and "keep the link table in Redis" compatible (p=0.93)
however it is asked: seeing that two choices exclude each other needs reasoning. So the work is
split: the classifier puts both into the same topic, and the handoff flags any topic with
active decisions from more than one master ("check that they agree"); the receiving master, a
full model, does the reasoning. Detected contradictions get explicit `conflicts with #id`
links and are always shown, past the per-topic cap.

### 7. Handoff

`GET /v1/sessions/{session}/handoff?per_topic=8&max_chars=8000&exclude_master=X`

- Fitted to `max_chars` in priority order: conflicts, decisions, facts and artifacts by rank,
  open questions. What is left out is counted in the intro.
- Long artifacts are previews (600 chars) and other entries are cut at 1,500 chars, both with
  the entry id for the full text.
- Topics with conflicts or several deciders first, then by their best entry.
- `upto_seq`: everything of the session at or below it is processed; `pending` counts what is
  not. Events that keep failing are given up after 5 attempts and marked with their error, so
  the watermark does not stall.
- All values come from one repeatable-read snapshot.

### 8. Continuous sharing

Masters write long messages, and a message used to reach the others only once it was finished.
Now the tap also publishes a message while it is being written (`tap.MessageStream`):

- **When.** Each time a part becomes final, i.e. at a blank line outside a code fence. The
  segmenter decides a block's part from the block and the next block's kind, so a prefix cut
  there has the same parts as the finished text, except a trailing short line ending in ":"
  (it may become a list lead-in, which is dropped, or a code intro, which is attached) and parts
  past the merge cap (`segment.StreamedParts`). A test checks this at every character of every
  scenario message and of edge cases (code with blank lines, lists, lead-ins, 20 paragraphs).
- **What.** A snapshot of the message so far: the same item id, status `in_progress`, and a
  `rev` (1, 2, ...) in the envelope; `Nats-Msg-Id` is `master:item~rev`. The finished item
  follows as usual. Events are unique per (master, item, rev).
- **Entries** are keyed by (master, item, part): the first event in which a part is final
  creates it, and later snapshots and the finished item only add new parts. If a part ever
  differed between a snapshot and the finished item, the consumer logs it.

Evidence: url-shortener with its three multi-part messages streamed gets the same 17/17 over 3
runs as unstreamed; 5 of their 7 parts are published before their message ends, 0.6 s earlier
at 40 words/s and 1.8 s at 12 words/s (a local 27B model). The classifier (~0.5 s per entry)
then becomes the limit for how soon a part is visible.

### 9. Pull and share (milestone 4)

Both tools are served by an MCP server (`internal/mcpserver`); a master identifies itself with
headers.

- **Pull.** `since_seq=0` returns the current context like a handoff. A later call passes the
  returned `upto_seq` and gets only the changes in (since_seq, upto_seq]: new visible entries,
  plus earlier entries that those items superseded or answered ("#209 was answered by #211 from
  master-b"). `upto_seq` is the processing watermark, so across a chain of calls each entry is
  reported once. The caller's own entries are left out; changes others made to them are not.
- **Share.** The MCP server publishes the call on the stream itself, marked `via: mcp` with its
  own per-master seq, so masters without a tap can share too. The consumer turns it into a
  *declared* entry: the master's kind and topic (the classifier fills in a missing topic),
  relevance 5. It still goes through the relation step, so a share can answer an open question
  or replace the master's earlier entry. Handoff lines say `declared` instead of a seq.
- **No echo.** The tap publishes a master's tool calls and outputs as usual, but outputs of the
  shared-context tools (and the tap's copy of a share) never become entries. Otherwise every
  pull would come back as the pulling master's own entries.

Shares make the most important entries independent of classifier errors, and they answer the
"who decides final" question in idea.md for the entries that matter most: the master does.

### 10. Real masters (milestone 5)

Two masters on OpenRouter's free tier (Nemotron 3 Super, Cohere North Mini), streaming through
the tap and using both tools, on the URL shortener task with declared topics. OpenRouter's
`/api/v1/responses` is stateless and streams the standard events, so nothing changed for it.

What the runs showed, and what changed:

- **The board changes behaviour.** master-a pulled a paragraph master-b was still streaming
  ("a simple document database") and switched its decision to MongoDB "matching the other
  agent", then back to a relational table after a later pull.
- **Revisions were not retired.** master-a's switch to MongoDB and back left its Postgres
  columns, the MongoDB decision and the final table all active. Two fixes:
  - **Explicit replaces.** `share_context` waits (up to 5 s) for its entry and returns the id
    ("Shared as a decision: entry #3702. It replaces your #3699."); a later share passes
    `replaces: [3702]`. Only the master's own entries are retracted; naming another master's
    entry marks a conflict. A topic-wide "newer replaces older" rule was rejected: masters
    share one column per call, and each would have retired the previous one.
  - **Pooled change probability** (lifecycle table).
- **Invented topics.** A share with topic `storage_layer` split the board next to the declared
  `storage`. Pulls now list the session's topics, and a share topic the session does not use
  goes to the known topic the classifier picks (the master's name is kept only for a topic
  the classifier also finds new).
- **Process talk and headings** were visible entries: the narration kind and heading removal.

Relation judgments have their own eval (`evals/relations.jsonl`, 18 cases from these runs and
the scenarios; `wetink eval-relations`):

| Relation model | Correct outcome | Latency |
| --- | --- | --- |
| d1-3B, before | 11/18 (replaces 3/5, restates 1/3, contradicts 1/3) | 0.24 s |
| d1-3B, pooled change probability | 14/18 (replaces 5/5, new 5/5, restates 1/3, contradicts 2/3, answers 1/2) | 0.24 s |
| LFM2.5 1.2B (chat, JSON schema) | 7/18: calls most things "replaces" | 0.36 s |
| Qwen3.6 27B Q6 (LM Studio) | right on the probe, but 65-100 s per judgment | |

A separate relation backend is supported (`WETINK_RELATE_API`, `WETINK_RELATE_MODEL`,
`WETINK_RELATE_BASE_URL`, `WETINK_RELATE_API_KEY`: classify with d1, relate with a chat model), but
no local model is both better and fast enough, and OpenRouter's free tier allows 50 requests a
day, which one session's relations would use up. The eval cases were written from the runs
they fix, so they overstate; the scenarios guard against regressions.

### 11. Coding experiments (question 4)

The scenario evals show the board keeps the right things; they cannot show agents do better
with it. `wetink experiment` gives two coding agents related issues of one repository and compares
agents that know nothing of each other with agents that know each other's issue and share a
board. Design choices:

- **Off-the-shelf agents, tapped by a proxy.** Writing a coding agent would measure the agent.
  `wetink proxy` sits between an agent and its provider: Anthropic Messages (Claude Code) and Open
  Responses (Codex and others). Text and tool calls become the same items the master's tap
  publishes, text streamed a paragraph at a time. Calls without tools (Claude Code's titles and
  summaries) are not tapped. Tool results (file reads, test logs) are not tapped by default:
  they are long and mostly noise for the board, and the agent shares what it learns from them.
- **A clone per agent, not worktrees of one repository.** Worktrees share branches, so an
  isolated agent could see the other's branch. Each clone comes from a repository holding only
  the history up to the base commit, so a task mined from history cannot be answered from git.
- **Scoring by tests and by merging.** Each agent's branch is run with its hidden tests (laid
  over, replacing files of the same name). Then the branches are merged: a textual conflict
  is recorded as such; a clean merge is run with every hidden test, plus combined tests that
  need both changes. Agents add tests to the same test file and conflict there for no reason
  that matters, so with a test glob the combined tests run on a merge without the agents' own
  test files (both merges are reported). The interesting failure is the one git does not catch: two changes that
  merge cleanly and pass their own tests but disagree.
- **Spend from the proxy's log**: model calls, tokens (cached input counted apart), tool calls,
  pulls and shares, per agent.
- **Sandboxed agents.** Claude Code runs with its sandbox (writes to the checkout and a cache
  only, no network) in `dontAsk` mode with an allow list; Codex with its workspace-write
  sandbox. Agents get a scrubbed environment (no provider, database or NATS settings).

Tasks: `experiments/toy` is a check of the whole path, not evidence (a dozen lines each). The
real test needs a mature repository and two related issues from its history, with the tests of
their pull requests as hidden tests (`wetink experiment check` verifies they fail before and pass
after), and several runs per condition.

## Evidence

`wetink eval evals/context/<scenario>.json --runs 3` plays a scripted two-master
session through the real pipeline (tap, NATS, consumer, d1-3B, Postgres, handoff) and checks
what the handoff includes and leaves out. Runs vary slightly (parallel llama-server slots), so
results are over 3 runs.

| Scenario | Before redesign | Now (3 runs) | Remaining failures |
| --- | --- | --- | --- |
| url-shortener (tuned on) | 9/17 | 17, 17, 17 | none |
| sales-pipeline (written untuned, then tuned on) | 14/19 untuned | 17, 17, 17 | the Airflow schedule is classified as a hypothesis, so it and the cron/Airflow disagreement are hidden |
| docs-migration (written untuned) | 15/17 untuned | 16, 16, 16; later 15, 16, 15 | "CI runs jekyll build" (a fact in topic ci) vs "we'll use Hugo" (a decision in topic generator) is not flagged; the later progress fact supersedes the earlier one at p = 0.68-0.75, either side of the 0.7 bar, so that check varies |
| pull-and-share (milestone 4) | | 13, 13, 13 | none: a joining master pulls the other's decision and question, a declared share answers the question, a revised share supersedes the first, and a change pull reports both |

The classifier eval (`evals/classifier.jsonl`, 21 items) with the current rules: topic 19/19,
kind 17/19, final 17/17, relevance 20/21.

## Cost and throughput

About 0.6 s of model time per entry on an M4 Pro: classification ~0.30 s and the relation check
~0.32 s (each is one pass per question; prompt length matters little), a topic name ~0.07 s when
needed. The pipeline itself adds ~15 ms.

The consumer (`internal/consumer`) stores and acks messages first; the events table is the work
queue. One worker per session (up to 16 at once, a Postgres advisory lock per session, so
several consumer processes can share the load) processes its events in order. Within a
session, the next entries are classified ahead (up to 4) while the current one is related; an
entry classified before an earlier one created a topic is classified again. A failing event
blocks only its session and is given up after 5 attempts.

`wetink bench` measures it. With a mock decision model (250 ms per call, 4 slots, like
llama-server):

| Load | One-at-a-time consumer | Now |
| --- | --- | --- |
| 4 sessions x 10 messages x 3 parts | 1.9 entries/s, last item after 61 s | 7.2 entries/s, 16.7 s |
| 1 session x 20 messages x 3 parts | 1.9 entries/s, 31 s | 3.5 entries/s, 17 s |
| 8 sessions, 2 model endpoints instead of 1 | | 14.2 entries/s (7.4 with one) |

With the real d1-3B on one llama-server, both consumers reach about 1.3-1.5 entries/s: the
server's slots share the GPU, so it serves ~3.4 calls/s whether calls come one or four at a
time (measured), and two calls per entry cap it near 1.7 entries/s. More throughput needs more
model capacity: `WETINK_CLASSIFIER_BASE_URL` takes several endpoints (comma-separated) and each call
goes to the least busy one. Within one session, relating stays sequential, which caps a single
session at about one relation call at a time.

## Known limits

- **Conflicts that need reasoning** are only caught when both sides are decisions in the same
  topic (flagged for the reader), or when the model detects them. Facts that contradict a
  decision, across topics, are missed (docs-migration).
- **Kind errors** still happen (the Airflow decision as a hypothesis, which hides it). Hiding is
  the costly direction; the relation thresholds avoid it, the kind question cannot.
- **Topics depend on the declaration.** Without declared topics the namer invents them, and
  they drift.
- **No ageing.** An old fact stays until something replaces it; nothing expires.
- **Model capacity bounds throughput.** On one local model server, about 1.5 entries/s, shared
  by all sessions; it scales with endpoints, not with concurrency.
- **Restatements and cross-master conflicts** are where d1 is weakest (relation eval: restates
  1/3, contradicts 2/3). A master's share of something it already wrote in prose usually
  stays as a second entry.
- **Narration is caught only when d1 is fairly sure** (3 of 4 eval cases); below the bar it
  stays, scored as content.
- **A proxy restart starts its taps' sequence numbers again at 1** (they live in the process,
  like a master's); a session should not span two proxy processes.
- **The scenarios are small** (4 sessions, 66 checks). They catch regressions; they do not
  measure accuracy on real master output.

## Decisions for the plan

1. **Open questions are shared.** idea.md says only final items enter shared context. Open
   questions are now shared in their own, clearly labelled section, because they are how
   masters coordinate (and answers resolve them). Hypotheses stay out.
2. **Who declares topics?** The design assumes whoever starts the session (the orchestrator
   that splits the task) calls `PUT /v1/sessions/{session}`.
3. **Who decides final?** (open in idea.md) The classifier, via kind, for everything a master
   does not share; the master itself for what it publishes with `share_context`.
4. **Escalating conflicts to a reasoning model** (System 2) for topics with several deciders
   would close the main gap, at a cost per conflict, not per entry.
