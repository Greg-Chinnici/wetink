# wetink: MVP & Design Goals

Oct 9, 2026 · @greg

## Problem

When several masters (agents) work on one task in parallel, each master's context stays local until the run hits a checkpoint, so parallel masters work blind to each other. This project adds a streaming layer that captures every master's completed output, classifies it, and keeps it in a shared context that can be handed off at any moment.

- **Today:** context merges only at checkpoints; a master can duplicate or contradict work another master already finished.
- **Target:** any master, or a new one, can pull the current shared state at any time, stamped with how fresh it is.

## Design goals

The system must work when masters run remotely on different machines and providers, and speak one open API format.

1. **Provider-neutral.** Target the [Open Responses](https://www.openresponses.org/specification) spec (based on the OpenAI Responses API). Providers without native support sit behind a gateway that translates.
2. **Remote-first.** Assume masters, consumer and store are on separate hosts. All communication goes over a durable, replayable message log.
3. **Handoff at any time.** A snapshot of shared context is always available, stamped with the log sequence it reflects. No waiting for checkpoints.
4. **Portable handoffs.** A handoff is a plain `input` array of items, rebuilt from our store. Never rely on provider-side state such as `previous_response_id`.
5. **Non-blocking.** Masters never wait on the classifier or the store. Publishing is fire-and-forget with local buffering.
6. **Traceable.** Every item in shared context keeps its source master, sequence number and raw original.
7. **Simple first.** Few moving parts, standard infrastructure, nothing clever until measurements demand it.

**Non-goals (for now):** token-level streaming between masters, automatic conflict resolution, a vector or semantic search layer, multi-tenant auth.

## Architecture

&#91;embedded content: MVP architecture · write path and read path\]

Masters publish each completed item to NATS; the classifier tags it, and Postgres holds the shared view that handoffs and the pull tool read from.

## MVP scope

The MVP proves the full remote path with two masters, one classifier and one handoff, all running in Docker Compose as separate services.

| Component | MVP version | Later |
| --- | --- | --- |
| Master tap | Python wrapper around streaming `responses.create`; publishes on `response.output_item.done` | Sidecar or SDK middleware for any language |
| Transport | One NATS JetStream server, one stream `wetink.<session>.items` | Clustered JetStream |
| Classifier | Small model with JSON-schema structured output: topic, kind, final | Rules fast-path, batching, re-tagging |
| Store | Postgres: `events` log + `context_view` table | Compaction of old topics |
| Handoff | HTTP endpoint returning an `input` array + `upto_seq` | Per-topic and token-budgeted snapshots |
| Pull tool | `get_shared_context(topic, since_seq)` function tool for masters | Exposed as an MCP server |

**In:** 2 masters, 1 session, item kinds `message` and `function_call` / `function_call_output`.

**Out:** reasoning items, provider-specific item types, UI, auth beyond a shared token.

## Data model

Every published item travels in one envelope; the raw Open Responses item is stored untouched so nothing is lost to classification.

**Envelope (NATS message body):**

```json
{
  "session": "s-123",
  "master": "master-a",
  "seq": 42,
  "item": { "type": "message", "id": "msg_abc", "...": "raw spec item" }
}
```

Header `Nats-Msg-Id: <master>:<item.id>` lets JetStream drop duplicate publishes.

**Classifier tags:** `topic` (string), `kind` (fact, decision, hypothesis, open\_question, artifact), `final` (tentative vs committed).

**Postgres tables:**

| Table | Key columns | Purpose |
| --- | --- | --- |
| `events` | `id`, `session`, `master`, `seq`, `item_id`, `item` (jsonb), `topic`, `kind`, `final`, `stream_seq` | Append-only log; unique on `(master, item_id)` |
| `context_view` | `session`, `topic`, `kind`, `event_id`, `updated_seq` | Current best item per topic and kind |

Only `final = true` events update `context_view`; tentative ones stay in the log for audit.

## Risks and mitigations

The biggest risks are invalid handoffs and masters treating each other's guesses as facts.

| Risk | Mitigation |
| --- | --- |
| Orphaned `function_call` without its `function_call_output` breaks the next request | Summarize tool exchanges into text in handoffs; never forward one half of a pair |
| Reasoning items or provider-specific items fail on another provider | Drop them from handoffs unless the receiver uses the same provider |
| Tentative thoughts read as facts | `final` tag; only final items enter `context_view` |
| Duplicate deliveries (at-least-once) | `Nats-Msg-Id` dedup + unique `(master, item_id)` in Postgres |
| Masters echo each other's claims as confirmation | Keep `master` provenance in every rendered line |
| Conflicting decisions across masters | MVP: latest final wins, conflicts logged; later: explicit `conflict` kind |
| Classifier lag makes view stale | `upto_seq` stamp on every handoff; pull tool fetches newer items |
| Master loses connection to NATS | Local buffer and replay; per-master `seq` gaps detected by consumer |
| Shared context outgrows a model's window | Cap items per topic; compaction after MVP |

## Milestones and success criteria

Each milestone builds on the last; the MVP is done when milestone 5 passes.

1. **Plumbing.** Compose file with NATS, Postgres, gateway. A tap publishes items from one master; they land in `events`.
2. **Classification.** Consumer tags each item and updates `context_view` for final items.
3. **Handoff.** Endpoint returns a valid `input` array that a fresh master accepts without errors.
4. **Pull tool.** A running master calls `get_shared_context` mid-loop and uses the result.
5. **Two-master demo.** Two masters split one task; master B uses a decision master A made before any checkpoint.

**Success criteria:**

- Item visible in shared context within a target latency of the master finishing it (measure in milestone 2, then set the number).
- Zero invalid handoffs across the demo runs (no orphaned tool calls, no provider rejections).
- No duplicate rows after killing and restarting the consumer mid-run.
- Works unchanged when one master's provider is switched behind the gateway.

## Open questions

- [ ] Which gateway: self-hosted or a managed one, and which providers must work on day one?
- [ ] Which small model runs the classifier, and is a rules fast-path enough for common items?
- [ ] Who decides `final`: the master itself (via an explicit marker) or the classifier?
- [ ] Target latency from item done to visible in shared context?
- [ ] Handoff budget: maximum tokens a snapshot may use?
- [ ] Should masters only pull context, or should it also be pushed into their next turn?

## Sources

- [Open Responses specification](https://www.openresponses.org/specification)
- [Open Responses](https://openresponses.org/)
- [Portkey: Responses API gateway](https://portkey.ai/docs/aigw/product/ai-gateway/responses-api)

