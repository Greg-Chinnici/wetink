# Research notes

Related projects, papers and models, with what each could mean for wetink. Collected
2026-10-13. "Read" means we read the paper or article (some only through their abstract or a
summary); "listed" means we only saw it named, so check it before citing.

## Decision models

Models that return typed answers (yes/no, choice, score) with probabilities instead of text.
The System One API they share is the one `WETINK_CLASSIFIER_API=systemone` already speaks
(today with LiquidAI d1-3B behind `llama-server`). Setup: [README](../README.md#run).

| Model | Who | Size, access | Reported speed and price | Status | Links |
| --- | --- | --- | --- | --- | --- |
| Jev (`jev-latest`, `jev-1.13.0`) | TypeSafe AI | Hosted API, `POST https://api.typesafe.ai/v1/systemone` | 70-500 ms end to end, ~100 ms typical; $0.042 / M input tokens, output free; 80 req/s | Read | [deep dive](https://flaviocopes.com/jev/), [Cisco: decision models on trial](https://blogs.cisco.com/ai/jev-decision-models-on-trial), [Jev in the Wild (arXiv 2609.30216)](https://arxiv.org/pdf/2609.30216), [Fortune](https://fortune.com/2026/10/08/jev-an-ai-for-making-quick-decisions-has-been-a-viral-hit-in-silicon-valley-but-openai-is-hot-on-its-heels/), [funding](https://www.techtimes.com/articles/328875/20261013/typesafe-ai-raises-870m-jev-decision-model-that-never-generates-text.htm), [model page](https://hokai.io/hub/models/jev) |
| Clef | Cloudflare | 27B (Qwen3.8-27B backbone), 65k context; Workers AI `@cf/cloudflare/clef`; Apache 2.0 weights on Hugging Face | Self-reported median 209 ms; measured 524-726 ms over REST from Italy; $0.24 / M input tokens | Read | [deep dive](https://flaviocopes.com/clef/), [Agentic Commons](https://agenticcommons.xyz/blog/cloudflare-clef-decision-models-rl-finetuning-en/), [InfoQ](https://infoq.com/news/2026/10/clef-decision-models), [Developers Digest](https://www.developersdigest.tech/blog/cloudflare-clef-decision-models-2026), [BenchLM](https://benchlm.ai/models/cloudflare-clef) |
| Clef-flash | Cloudflare | 9B (Qwen3.5-9B backbone); `@cf/cloudflare/clef-flash`; Apache 2.0 weights | Self-reported median 39 ms; measured 191-205 ms over REST from Italy, 394 ms in the comparison below; $0.09 / M input tokens | Read | as Clef |
| Microsoft Decision-1, OpenAI Decisions | Microsoft, OpenAI | Hosted | Decision-1 matched Jev's accuracy at a third of the price in the comparison below | Listed | [comparison](https://akitaonrails.com/en/2026/10/10/which-jev-like-ai-decision-classifier-model-is-best/) |
| Perplexity Decider 27B, Decision 3.0 | Perplexity, others | Open weights, local | Top accuracy (99.4%) but ~1.5 s per call on a LAN GPU | Listed | [comparison](https://akitaonrails.com/en/2026/10/10/which-jev-like-ai-decision-classifier-model-is-best/) |
| Kev (0.8B/4B/9B), Jebadiah 4B v2, Mapika Decider 4B, Eikos 4B | various | Open weights, local | 4B models: 87-90% on the compact cases | Listed | [comparison](https://akitaonrails.com/en/2026/10/10/which-jev-like-ai-decision-classifier-model-is-best/) |
| laya, Julia-1, GLiNER2.5-Decide | convaiinnovations, SupersonicLabs, fastino | Small open weights | Julia-1: 12 ms but 31.5%, and wrong on 48 of 71 answers it gave above 0.9 | Listed | [comparison](https://akitaonrails.com/en/2026/10/10/which-jev-like-ai-decision-classifier-model-is-best/), [HF trending 2026-10-05](https://github.com/THTHDGCS/agents-radar/issues/1080) |
| RSI-Jev | OrcaRouter | A self-improving loop around a decision model | | Listed | [article](https://www.orcarouter.ai/blog/what-is-rsi-jev) |

Overviews: [Decision models explained: Jev, Clef and the "System One" wave](https://www.superteams.ai/blog/decision-models-explained-jev-cloudflare-clef-and-the-system-one-wave/),
[Top 10 decision models ranked](https://explainx.ai/blog/top-10-decision-models-jev-perplexity-openai-open-source-ranked-2026),
[Which Jev-like model is best? (23 models, 2026-10-10)](https://akitaonrails.com/en/2026/10/10/which-jev-like-ai-decision-classifier-model-is-best/).

Our baseline for comparison (README, 21 labelled items, M4 Pro): d1-3B gets 19/19 topic, 17/19
kind, 20/21 score at 0.25 s per item, and 14/18 on relation judgments.

### Ideas

1. **Measure before switching.** Run `wetink eval-classifier`, `wetink eval-relations` and the
   four scenario evals with Jev, Clef and Clef-flash, and add them to the README's tables. None
   of the published accuracy numbers are on our task.
2. **Fast model to classify, strong model to relate.** Relation judgments are d1-3B's weakest
   step (14/18), and they decide what leaves the board. Try Clef-flash for `WETINK_CLASSIFIER_*`
   and Jev or Clef for `WETINK_RELATE_*`.
3. **Speed comes from parallelism more than from per-call latency.** Over the network, hosted
   calls (100-700 ms measured) are no faster than local d1-3B (0.25 s). The gain is that they
   are not capped at one GPU's ~3.4 calls/s: raise `WETINK_CLASSIFIER_CONCURRENCY` and rerun
   `wetink bench --real`.
4. **Retune the probability bars per model.** Closing an entry at p >= 0.7, `QuestionMinProb`
   and `NarrationMinProb` were set from d1-3B's probabilities. Calibration differs between models
   (Julia-1's confident wrong answers), and docs-migration already flips on 0.68-0.75.
5. **Fine-tune on our labels.** Cloudflare offers RL fine-tuning on top of Clef. Our labelled
   sets (21 classifier items, 18 relation cases) would need to grow first.
6. **Run Clef-flash locally.** The weights are Apache 2.0. Whether our pinned `llama-server`
   can serve them on `/v1/systemone` is untested.

## Shared context and blackboards for agents

| Work | What it is | Relevance to wetink | Status | Link |
| --- | --- | --- | --- | --- |
| DeLM: Decentralized Multi-Agent Systems with Shared Context | Peer agents on existing harnesses share a context and a task queue; up to +17.5 points over Codex and Claude Code on SWE-bench Verified, Terminal-Bench and others | Closest prior work. Writes are explicit, at step boundaries, append-only, uncurated. It is the natural "explicit-write-only" baseline for question 4 | Read | [arXiv 2606.10662](https://arxiv.org/pdf/2606.10662) |
| STALE: Passes Alone, Fails Together | Benchmark of parallel agents whose patches pass alone and fail when merged | Same design as `wetink experiment`. Real Django PR pairs almost never interfere (1 in 834 runs); constructed tasks do (97%); a message describing the other agent's finished change recovers 82% | Read (abstract) | [arXiv 2609.25396](https://arxiv.org/pdf/2609.25396) |
| StreamMA: Streaming Communication in Multi-Agent Reasoning | Streams each reasoning step to downstream agents in chain, tree and graph topologies; +7.3 points on average | Streaming between agents, but pipelined, not peers on a board | Read (abstract) | [arXiv 2606.05158](https://arxiv.org/pdf/2606.05158) |
| Staircase Streaming for Low-Latency Multi-Agent Inference | Overlaps agents' generation using partial results | Partial outputs as useful input for other models | Listed | [arXiv 2510.05059](https://arxiv.org/pdf/2510.05059) |
| PatchBoard | Schema-grounded state changes on a blackboard, auditable | Compare with our entry lifecycle and relations | Listed | [arXiv 2605.29313](https://arxiv.org/html/2605.29313v1) |
| Governed Shared Memory for Multi-Agent LLM Systems | Scoped access, temporal correctness, provenance, synchronization | Provenance overlaps with our attributed lines | Listed | [arXiv 2606.24535](https://arxiv.org/html/2606.24535v1) |
| Shared Organizational Memory for Enterprise Coding Agents | Deployed shared memory for coding agents | Long-lived memory, not a live board | Listed | [arXiv 2608.00122](https://arxiv.org/pdf/2608.00122) |
| Codified Context | Infrastructure for agents in a complex codebase | Static context files, not live sharing | Listed | [arXiv 2602.20478](https://arxiv.org/pdf/2602.20478) |
| LLM-based Multi-Agent Blackboard System | A central agent posts requests, agents volunteer | Blackboard revival, whole turns | Read (README) | [arXiv 2510.01285](https://arxiv.org/abs/2510.01285) |
| bMAS | Blackboard multi-agent system | Blackboard revival, whole turns | Read (README) | [arXiv 2507.01701](https://arxiv.org/abs/2507.01701) |
| Group Think, Hogwild! Inference | Parallel workers share a KV cache, see each other's tokens | Token level, one model on one machine | Read (README) | [2505.11107](https://arxiv.org/abs/2505.11107), [2504.06261](https://arxiv.org/abs/2504.06261) |
| Mem0, Zep / Graphiti | Memory for one assistant; new facts invalidate old ones with a large LLM | Similar lifecycle, different purpose | Read (README) | [Mem0](https://mem0.ai), [Zep](https://help.getzep.com/v2/concepts) |
| MCP Agent Mail | Inbox, outbox and file leases for coding agents over MCP | Explicit messages and reservations, no passive capture | Read (summary) | [GitHub](https://github.com/Dicklesworthstone/mcp_agent_mail) |
| junto-memory, mcp-agent-memory, shared-memory-mcp | Shared-memory MCP servers for coding agents | Explicit writes only | Listed | [junto-memory](https://glama.ai/mcp/servers/tlemmons/junto-memory), [mcp-agent-memory](https://github.com/justanotherspy/mcp-agent-memory), [shared-memory-mcp](https://glama.ai/mcp/servers/@haasonsaas/shared-memory-mcp) |
| Claude Code Agent Teams, Codex subagents | Product multi-agent modes: a shared task list and messages; parallel sandboxes | One vendor, explicit | Listed | [comparison](https://www.morphllm.com/comparisons/codex-vs-claude-code) |

### Ideas

1. **Use STALE's constructed tasks for question 4**, beside or instead of `experiments/toy`.
   Pairs taken from a mature repo's history may not interfere at all.
2. **Add two baselines**: DeLM-style explicit, append-only publishing (`share_context` only, no
   tap, no curation), and STALE's message about the other agent's finished change. The board has
   to beat the first and approach the second to show that passive, streamed, curated writes help.
3. **Differentiators to test, not assume**: passive capture, writes mid-generation, and
   small-model curation (supersede, answer, contradict). Each can be turned off on its own.
