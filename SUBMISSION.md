# Submission — Problem 1: Resumable Realtime Conversation

## Demo Video

> 📹 **[INSERT LOOM/YOUTUBE LINK HERE]**

---

## Setup & Running

### Prerequisites
- Node.js v22+ (uses built-in `node:sqlite`)

### Install
```bash
# From repo root
npm install --prefix server
npm install --prefix client
```

### Run (two terminals)
```bash
# Terminal 1 — API server (port 3001)
npm run dev --prefix server

# Terminal 2 — Frontend (port 5173)
npm run dev --prefix client
```

Open http://localhost:5173

### Tests (all 26 pass, ~360ms)
```bash
npm test --prefix server
```

### Verification Benchmark (single command)
```bash
npm run benchmark --prefix server
```

Expected output:
```
Events received       : 30
Duplicate events      : 0
Missing events        : 0
Final run state       : completed ✓  BENCHMARK PASSED
```

---

## Architecture

```
Browser (Vite)
  │  POST /api/conversations/:id/messages  → start run
  │  GET  /api/conversations/:id/stream    → SSE (cursor=<seq>)
  ▼
Fastify Server
  ├── MessageHandler    — creates run, spawns generator fire-and-forget
  ├── StreamHandler     — SSE: subscribe-first → replay → drain → live
  ├── GeneratorWorker   — writes chunks, sets terminal state
  ├── EventStore        — SQLite (node:sqlite, synchronous writes)
  └── RunBus            — EventEmitter, transient in-process pub/sub
```

### Component responsibilities

| Component | Owns | Does NOT own |
|---|---|---|
| `EventStore` | Durable ordering (seq autoincrement), state transitions, terminal state lock | Delivery |
| `RunBus` | Transient live delivery to open SSE connections | Persistence (dies on restart) |
| `StreamHandler` | Subscribe-first replay→live, dedup logic, cursor validation | Generation |
| `GeneratorFn` | Chunk production, terminal state setting | Transport |
| `ConnectionManager` (client) | Reconnect backoff, cursor tracking | Server state |

### Ordering invariant

> **Every event is persisted before it is published to the live bus.
> The event store is the source of truth; the bus is only a delivery optimisation.**

```
         EVENT
           │
           ▼
      appendEvent()
           │
           ▼
        COMMITTED
           │
   ┌───────┴───────┐
   ▼               ▼
 EventStore      RunBus
 (truth)       (delivery)
```

This invariant makes the replay→live protocol safe: anything on the bus is
guaranteed to be in the event store.

---

## Decisions

### Transport: SSE not WebSockets

The protocol is strictly server→client for events; the client submits one POST per message. SSE is the minimal correct fit:
- No upgrade handshake overhead
- Degrades gracefully through HTTP/1.1 proxies without a protocol upgrade
- Native browser `EventSource` API handles low-level framing, reconnect signaling, and `Last-Event-ID` propagation

### Cursor: explicit `?cursor=<seq>` over browser-native `Last-Event-ID`

The browser sends `Last-Event-ID` automatically on reconnect — but only if the connection was closed by the server (network drops do not guarantee the header is sent, and the value is controlled by the SSE `id:` field which requires the server to write it correctly). We use an **explicit `?cursor=<seq>` query parameter** for reconnection instead, because:

- **Reliability:** `?cursor` is always present and always reflects the last seq the client confirmed, regardless of how the connection closed. `Last-Event-ID` silently falls back to the empty string if the first SSE frame hasn't been received yet.
- **Testability:** `?cursor` can be asserted on by test code without needing to inspect request headers across an SSE boundary.
- **Transparency:** the cursor is visible in the URL — useful for debugging, logging, and reverse-proxy tracing.

The SSE `id:` field is still written per-frame (so `Last-Event-ID` stays in sync as a free belt-and-suspenders fallback), but our server reads `?cursor`, not the header.

### Cursor = global `seq` (SQLite AUTOINCREMENT)

`seq` is a monotonically increasing integer assigned by SQLite on each `INSERT`. Writes are synchronous via `node:sqlite`'s `DatabaseSync` API.

**Trade-off vs per-run sequence:** A `(run_id, local_seq)` cursor would map more naturally to the brief's language. A global `seq` was chosen because:
- This system has one run per conversation, so a global seq is unambiguous
- A global cursor allows replaying across multiple run events (user_message, run_started, chunks) in a single query, which simplifies the stream handler
- In a multi-run future, the global seq still provides a total order; per-run cursors would require merging

**Invariant:** no two events ever share a seq; order is server-defined and unambiguous. Clients carry the last `seq` they processed; the server replays everything `seq > cursor`.

### SSE stream: subscribe-first protocol (eliminates the replay→live race)

The critical protocol decision. The naive approach is:
```
  replay → query max(seq) → subscribe
```
This has a race: events persisted between the last replay query and the subscribe call are silently missed.

The correct protocol is:
```
  SUBSCRIBE (buffer on)        ← captures every live event from this moment
  REPLAY events > cursor       ← from the durable store
  DRAIN buffer (dedup)         ← emit buffered events not already in replay
  LIVE (direct delivery)       ← buffer off, deliver from bus directly
```

The invariant that makes this safe: **every event is persisted before it is published to the bus**. So any event that hits the buffer during replay is guaranteed to be in the store already. The dedup filter (`seq > lastSentSeq`) handles the overlap between replay and buffer.

### Ordering authority: EventStore (server-side, single writer)

The EventStore is the sole authority on ordering. SQLite's AUTOINCREMENT is an atomic sequence counter. `DatabaseSync` makes writes synchronous — no two Node.js async operations can interleave writes.

### Terminal state lock

Run state machine:

```
              ┌───────────┐
              │  RUNNING  │
              └─────┬─────┘
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
     COMPLETED    FAILED   INTERRUPTED
```

| Transition | Allowed |
|---|---|
| running → completed | ✓ |
| running → failed | ✓ |
| running → interrupted | ✓ |
| completed → anything | ✗ (terminal) |
| failed → anything | ✗ (terminal) |
| interrupted → anything | ✗ (terminal) |

`EventStore.setRunState()` enforces this and throws on illegal transitions.

### Startup recovery (`interruptStaleRuns`)

On startup, the server:
1. Selects all runs with `state = 'running'` (crashed mid-generation)
2. Sets their state to `interrupted`
3. **Appends a `run_interrupted` event** to each run's history

Step 3 is important: a reconnecting client replaying from a cursor will see an explicit terminal event rather than an ambiguous "history ends here" gap.

### What survives a service restart

SQLite file contains: all conversations, all runs, all events, final run states, `run_interrupted` tombstones for crashed runs.

The in-process `RunBus` is intentionally transient — rebuilt empty on restart. Clients reconnect with their last cursor and recover via replay.

### Cursor error semantics

Two distinct cases with different semantics:

| Case | Meaning | HTTP |
|---|---|---|
| `cursor > max_seq` | Cursor is ahead of known history — likely a client bug | 409 `stale_cursor` with `max_seq` |
| `cursor < min_available_seq` | History has been pruned — client cannot safely replay | 410 `cursor_expired` with `min_available_seq` |
| Non-numeric cursor | Invalid request | 400 `invalid_cursor` |

This implementation has no event expiry, so only the 409 case applies. The 410 path is documented for future retention windows (see Assumptions).

### Reconnect delay: exponential backoff

`ConnectionManager` implements: `delay = min(base * 2^retryCount, maxDelay)` with ±20% jitter.

Defaults: base=200ms, max=30s. Bounded — clients cannot hammer the server indefinitely.

---

## Acceptance Scenarios

| AC | Status | Evidence |
|---|---|---|
| AC1 — Ordered live stream | ✅ | `ordered_delivery.test.ts` — 30 chunks + run_completed, ascending seqs |
| AC2 — Missed-event recovery | ✅ | `cursor_replay.test.ts` — replay from cursor=15 returns exactly seqs 16-31 |
| AC3 — Replay/live overlap | ✅ | `deduplication.test.ts` + `replay_live_race.test.ts` — subscribe-first: zero missed, zero duplicates across 3 race scenarios |
| AC4 — Service restart | ✅ | `service_restart.test.ts` — close DB, reopen, all events intact; `run_interrupted` event appended; stale runs → interrupted |
| AC5 — Generation failure | ✅ | `generator_failure.test.ts` — failAfter=10; state=failed; no run_completed event |
| AC6 — Unknown/stale cursor | ✅ | `stale_cursor.test.ts` — 409 with max_seq; 400 for non-numeric |

---

## Benchmark Results

```
npm run benchmark --prefix server

━━━ Verification Benchmark: 30-Event Resumable Stream ━━━

[Phase 1] Starting generator — will interrupt after 15 chunks
  → chunk 1: word_01 … chunk 15: word_15

[Interrupt] Disconnected at cursor=15 (phase1 received 15 chunks)

[Phase 2] Reconnecting from cursor…
  ← replay seq 16: word_16 … seq 30: word_30

━━━ Benchmark Results ━━━

  Total chunks expected : 30
  Events received       : 30      ✓
  Duplicate events      : 0       ✓
  Missing events        : 0       ✓
  Interrupt at chunk    : 15
  Reconnect cursor      : 15
  Final run state       : completed

  ✓ BENCHMARK PASSED
```

---

## Test Suite

```
npm test --prefix server

▶ AC1: Ordered live event delivery           ✔ 2 tests
▶ AC2: Cursor replay                         ✔ 3 tests
▶ AC3: Deduplication of replay/live overlap  ✔ 2 tests
▶ AC3 (explicit): Subscribe-first race       ✔ 3 tests
▶ AC4: Service restart                       ✔ 3 tests
▶ AC5: Generation failure                    ✔ 3 tests
▶ AC6: Unknown/stale cursor                  ✔ 4 tests
▶ Terminal state machine                     ✔ 6 tests

tests 26 | pass 26 | fail 0 | duration ~360ms
```

Tests use `node:test` + `node:assert` directly — no external test framework, no Vite bundler, no arbitrary sleeps, no paid API calls.

---

## Stack Choices

| Layer | Choice | Why |
|---|---|---|
| Runtime | Node.js 26 | Ships with `node:sqlite` built-in |
| SQLite | `node:sqlite` (built-in) | Zero native compilation; synchronous writes eliminate ordering races |
| HTTP | Fastify 4 | Fast, schema-first, built-in inject for test |
| Validation | Zod | Runtime-safe schema for request bodies |
| Frontend | Vanilla TS + Vite | No framework overhead; full control over SSE reconnect loop |
| Test runner | `node --test` | Native, zero bundling, works with `node:sqlite` without workarounds |

---

## Assumptions & Limitations

1. **Single server process** — `RunBus` is in-process. Horizontal scaling would require replacing it with Redis pub/sub or similar.
2. **In-progress generators on restart** become `interrupted`. The generator itself does not resume; clients replay persisted history. This is explicitly allowed by AC4.
3. **No event expiry** — in production, events older than N days would be pruned. If a client reconnects with a cursor older than the retention window, the server returns a documented error.
4. **`node:sqlite` is experimental** in Node.js 22-24, stable in Node.js 26. The `--experimental-sqlite` flag is required for Node 22-25.
5. **Single run per conversation** — per the spec, concurrent assistant runs are out of scope.

---

## Follow-up: 50-event retention window

If the server retained only the last 50 events and a client reconnected with an older cursor:

- The server detects `cursor < min(seq)` in the conversation's retained events
- Returns `410 Gone` with `{"error":"cursor_expired","min_available_seq":N,"cursor":C}`
- The client must either discard local state and replay from `cursor=min_available_seq` (showing partial history), or surface a "conversation history unavailable" state to the user
- Critically: the client must **never silently display partial content** as if it were complete — the error must be surfaced or handled explicitly

---

## AI Usage

Used Antigravity (Google DeepMind) for:
- Initial architecture planning and trade-off analysis
- Code scaffolding (types, route structure)
- Test case generation

All code reviewed and debugged manually. The `node:sqlite` switch from `better-sqlite3`, the vitest→`node --test` runner switch, and the benchmark interrupt-timing fix were diagnosed and implemented directly.

---

## Credibility Note

I focus on protocol-level correctness before UI polish. This submission reflects that: every design decision has a concrete reason — the subscribe-first buffer, the persist-before-publish invariant, the `?cursor` over `Last-Event-ID`, the `DatabaseSync` synchronous write model. These aren't cargo-culted patterns; they came from working through the exact failure modes the spec is testing (silent event loss, phantom duplicates, stale-cursor ambiguity, crashed-process recovery).

I'm comfortable in the full stack — Fastify route design, SQLite schema (including understanding AUTOINCREMENT vs ROWID reuse), TypeScript type safety, and browser-side state management — but I weight backend protocol correctness and testability over frontend aesthetics for infrastructure-level challenges like this.

The test suite uses only `node:test` and `node:assert` — no Vitest, no Jest, no arbitrary `setTimeout` sleeps. Every scenario is deterministic. The benchmark uses a Promise gate (not a timer) to pause the generator at exactly chunk 15, release it, and confirm 0 missed + 0 duplicates after reconnection. That's the kind of discipline I bring to production systems where flaky tests mask real race conditions.

GitHub: [github.com/gnshx](https://github.com/gnshx)
