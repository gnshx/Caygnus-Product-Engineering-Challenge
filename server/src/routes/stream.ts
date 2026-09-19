import type { FastifyInstance } from 'fastify';
import type { EventStore, StoredEvent } from '../store.js';
import type { RunBus } from '../bus.js';

// ─── SSE helpers ──────────────────────────────────────────────────────────────

function sseFrame(event: StoredEvent): string {
  return (
    `id: ${event.seq}\n` +
    `event: ${event.type}\n` +
    `data: ${JSON.stringify({
      seq: event.seq,
      run_id: event.run_id,
      type: event.type,
      ...event.data,
      created_at: event.created_at,
    })}\n\n`
  );
}

const TERMINAL_EVENT_TYPES = new Set(['run_completed', 'run_failed', 'run_interrupted']);

// ─── Stream route ─────────────────────────────────────────────────────────────

export async function streamRoutes(
  app: FastifyInstance,
  { store, bus }: { store: EventStore; bus: RunBus },
) {
  /**
   * GET /conversations/:convId/stream?cursor=<seq>
   *
   * Opens an SSE stream for a conversation.
   *
   * PROTOCOL (subscribe-first to close the replay→live race):
   *
   *  Step 1. Validate cursor (AC6).
   *  Step 2. SUBSCRIBE to RunBus — buffer all live events during replay.
   *          Events arrive in bus only AFTER being persisted (store invariant).
   *          Buffering here means zero events can slip through the gap.
   *  Step 3. REPLAY persisted events with seq > cursor from EventStore.
   *  Step 4. DRAIN the live buffer, skipping anything already replayed (dedup
   *          by seq > lastSentSeq). This closes the replay→live overlap window.
   *  Step 5. Switch to LIVE delivery. The bus subscriber now writes directly,
   *          still filtered by seq > lastSentSeq.
   *  Step 6. On terminal event (run_completed / run_failed / run_interrupted):
   *          close the stream.
   *  Step 7. On client disconnect: unsubscribe and clean up.
   *
   * WHY subscribe-first?
   * --------------------
   * If we replayed first and then subscribed, an event persisted between the
   * last replay query and the subscribe call would be silently missed. By
   * subscribing first and buffering, we guarantee every persisted event is
   * either in the replay result set OR in the live buffer — never neither.
   *
   * DEDUPLICATION is handled by the single invariant:
   *   emit event iff event.seq > lastSentSeq
   */
  app.get<{
    Params: { convId: string };
    Querystring: { cursor?: string };
  }>('/conversations/:convId/stream', async (req, reply) => {
    const conv = store.getConversation(req.params.convId);
    if (!conv) {
      return reply.status(404).send({ error: 'conversation_not_found' });
    }

    // ── Cursor validation (AC6) ───────────────────────────────────────────────
    const rawCursor = req.query.cursor ?? '0';
    const cursor = parseInt(rawCursor, 10);

    if (isNaN(cursor) || cursor < 0) {
      return reply.status(400).send({
        error: 'invalid_cursor',
        detail: 'cursor must be a non-negative integer',
      });
    }

    const maxSeq = store.getMaxSeq(req.params.convId);

    // A cursor strictly greater than maxSeq is stale (AC6).
    // Note: cursor == maxSeq is fine — means "give me everything after the last event I saw".
    if (cursor > maxSeq) {
      return reply.status(409).send({
        error: 'stale_cursor',
        detail: 'Cursor is ahead of known event history. Use cursor=0 or a valid seq.',
        cursor,
        max_seq: maxSeq,
        // In a system with retention, also expose min_available_seq here.
      });
    }

    // ── Set SSE headers ───────────────────────────────────────────────────────
    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });

    let lastSentSeq = cursor;
    let closed = false;
    let replayComplete = false;

    // Live events that arrive during replay are buffered here.
    const liveBuffer: StoredEvent[] = [];

    // ── Step 2: SUBSCRIBE FIRST (before any replay) ───────────────────────────
    // From this moment on, every event published to the bus is captured.
    // The generator always persists → then publishes, so nothing can be missed.
    const unsubscribe = bus.subscribe(req.params.convId, (event: StoredEvent) => {
      if (closed) return;

      if (!replayComplete) {
        // Buffer events that arrive while we're replaying
        liveBuffer.push(event);
        return;
      }

      // Replay is done — deliver live (dedup: only if not already sent)
      if (event.seq <= lastSentSeq) return;

      reply.raw.write(sseFrame(event));
      lastSentSeq = event.seq;

      if (TERMINAL_EVENT_TYPES.has(event.type)) {
        closed = true;
        reply.raw.end();
      }
    });

    // ── Step 3: REPLAY persisted events ──────────────────────────────────────
    const replayEvents = store.getEvents(req.params.convId, cursor);
    for (const event of replayEvents) {
      if (closed) break;
      reply.raw.write(sseFrame(event));
      lastSentSeq = event.seq;

      if (TERMINAL_EVENT_TYPES.has(event.type)) {
        closed = true;
        reply.raw.end();
        break;
      }
    }

    // ── Step 4: DRAIN live buffer (dedup) ────────────────────────────────────
    // Events in liveBuffer may overlap with replay. Only emit seq > lastSentSeq.
    for (const event of liveBuffer) {
      if (closed) break;
      if (event.seq <= lastSentSeq) continue; // already sent in replay
      reply.raw.write(sseFrame(event));
      lastSentSeq = event.seq;

      if (TERMINAL_EVENT_TYPES.has(event.type)) {
        closed = true;
        reply.raw.end();
        break;
      }
    }
    liveBuffer.length = 0; // release buffer memory

    // ── Step 5: Mark replay complete — live events now delivered directly ─────
    replayComplete = true;

    // If already closed (terminal event seen in replay or buffer), clean up now
    if (closed) {
      unsubscribe();
      return reply;
    }

    // ── Keepalive (prevents proxy timeouts) ───────────────────────────────────
    const keepalive = setInterval(() => {
      if (!closed) reply.raw.write(': keepalive\n\n');
    }, 15_000);

    // ── Cleanup on client disconnect ──────────────────────────────────────────
    req.raw.on('close', () => {
      closed = true;
      clearInterval(keepalive);
      unsubscribe();
    });

    return reply;
  });
}
