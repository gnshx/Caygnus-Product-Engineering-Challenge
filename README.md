# Resumable Realtime Conversation — Problem 1

A streaming chat client and server that correctly recover from connection interruptions without losing or duplicating events.

## Quick Start

**Requires Node.js v22+**

```bash
# Install
npm install --prefix server
npm install --prefix client

# Run (two terminals)
npm run dev --prefix server   # API on :3001
npm run dev --prefix client   # UI on :5173
```

Open **http://localhost:5173**, type a message, click Send, then click **⚡ Simulate Disconnect** mid-stream to see reconnection in action.

## Tests

```bash
npm test --prefix server
# 23 tests, 0 failures, ~330ms
```

## Benchmark

```bash
npm run benchmark --prefix server
# 30 events, interrupt at 15, reconnect, 0 missing, 0 duplicates → PASSED
```

## Architecture Summary

| Component | Role |
|---|---|
| `EventStore` (`node:sqlite`) | Durable ordered event log + run state machine |
| `RunBus` (EventEmitter) | Transient live pub/sub for SSE delivery |
| `StreamHandler` | SSE endpoint: replay → gap-close → live dedup |
| `GeneratorWorker` | Fake deterministic generator (30 words, configurable tick) |
| `ConnectionManager` | Client SSE wrapper with exponential backoff |

See [SUBMISSION.md](SUBMISSION.md) for full architecture, decisions, and trade-offs.
