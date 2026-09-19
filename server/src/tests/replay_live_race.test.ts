/**
 * Test: Replay→Live race condition (AC3 explicit)
 *
 * This test specifically verifies the subscribe-first guarantee:
 *
 *   1. Client subscribes to RunBus BEFORE replaying
 *   2. Events that arrive DURING replay are buffered
 *   3. After replay, buffer is drained with dedup (seq > lastSentSeq)
 *   4. Result: every event appears exactly once, in order
 *
 * The "race" that would occur with replay-first:
 *
 *   cursor = 10
 *   Replay:  11, 12, 13
 *   max(seq) = 13
 *              ← event 14 generated HERE (between replay end and subscribe)
 *   subscribe
 *   event 14 was already published → MISSED ❌
 *
 * With subscribe-first:
 *   subscribe (buffer on)
 *   Replay: 11, 12, 13
 *   event 14 generated → buffered ✓
 *   drain buffer: 14 (seq > lastSentSeq=13) → emitted ✓
 *   live: 15, 16... → emitted ✓
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import type { StoredEvent } from '../store.js';

describe('AC3 (explicit): Subscribe-first replay→live race', () => {
  /**
   * Core scenario:
   * - Replay returns events 11-15 (already persisted)
   * - During replay, events 16-20 are appended AND published
   * - After replay, buffer drain should emit 16-20 (not repeat 11-15)
   * - Net result: 11-20 each exactly once
   */
  it('emits each event exactly once when live events arrive during replay', () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    // Pre-populate 15 events (these will be replayed)
    for (let i = 1; i <= 15; i++) {
      store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `w${i}` } });
    }

    const cursor = 10;
    let lastSentSeq = cursor;
    const emitted: number[] = [];
    const liveBuffer: StoredEvent[] = [];
    let replayComplete = false;

    // ── Step 1: SUBSCRIBE FIRST ───────────────────────────────────────────────
    const unsubscribe = bus.subscribe(conv.id, (event: StoredEvent) => {
      if (!replayComplete) {
        liveBuffer.push(event);
        return;
      }
      if (event.seq <= lastSentSeq) return;
      emitted.push(event.seq);
      lastSentSeq = event.seq;
    });

    // ── Step 2: REPLAY (events 11-15) ────────────────────────────────────────
    const replayed = store.getEvents(conv.id, cursor);
    for (const event of replayed) {
      emitted.push(event.seq);
      lastSentSeq = event.seq;
    }

    // ── RACE WINDOW: generate events 16-20 while "still in replay" ───────────
    // In the real server this happens between Step 2 and Step 4.
    // Without subscribe-first these would be MISSED.
    const racingEvents: StoredEvent[] = [];
    for (let i = 16; i <= 20; i++) {
      const e = store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `w${i}` } });
      racingEvents.push(e);
      bus.publish(e); // publish — hits the buffer (since replayComplete=false)
    }

    // ── Step 3: DRAIN BUFFER (dedup) ─────────────────────────────────────────
    for (const event of liveBuffer) {
      if (event.seq <= lastSentSeq) continue; // dedup: skip if already replayed
      emitted.push(event.seq);
      lastSentSeq = event.seq;
    }
    liveBuffer.length = 0;
    replayComplete = true;

    // ── Step 4: LIVE — publish events 21-25 ──────────────────────────────────
    for (let i = 21; i <= 25; i++) {
      const e = store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `w${i}` } });
      bus.publish(e);
    }

    unsubscribe();

    // ── Assertions ───────────────────────────────────────────────────────────
    assert.equal(emitted.length, 15, `Expected 15 events (11-25), got ${emitted.length}`);
    assert.equal(emitted[0], 11, 'First emitted event must be seq 11');
    assert.equal(emitted[emitted.length - 1], 25, 'Last emitted event must be seq 25');

    // Strict ascending order
    for (let i = 1; i < emitted.length; i++) {
      assert.ok(emitted[i]! > emitted[i - 1]!, `seq[${i}]=${emitted[i]} must > seq[${i-1}]=${emitted[i-1]}`);
    }

    // No duplicates
    const unique = new Set(emitted);
    assert.equal(unique.size, emitted.length, 'No duplicate seqs');
  });

  /**
   * Verify the dedup: even if the same event arrives in BOTH replay AND live buffer,
   * it must appear exactly once in the output.
   */
  it('deduplicates an event that appears in both replay result and live buffer', () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    // Write 10 events
    const written: StoredEvent[] = [];
    for (let i = 1; i <= 10; i++) {
      written.push(store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `c${i}` } }));
    }

    const cursor = 0;
    let lastSentSeq = cursor;
    const emitted: number[] = [];
    const liveBuffer: StoredEvent[] = [];
    let replayComplete = false;

    const unsubscribe = bus.subscribe(conv.id, (e: StoredEvent) => {
      if (!replayComplete) { liveBuffer.push(e); return; }
      if (e.seq <= lastSentSeq) return;
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    });

    // Replay events 1-10
    const replayed = store.getEvents(conv.id, cursor);
    for (const e of replayed) {
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    }

    // Simulate: events 5-10 also arrive via the live bus during replay
    for (const e of written.slice(4)) { // events 5-10
      bus.publish(e);
    }

    // Drain: events 5-10 in buffer must be skipped (seq <= lastSentSeq=10)
    for (const e of liveBuffer) {
      if (e.seq <= lastSentSeq) continue;
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    }
    liveBuffer.length = 0;
    replayComplete = true;

    unsubscribe();

    // Must have exactly 10, each once
    assert.equal(emitted.length, 10);
    const unique = new Set(emitted);
    assert.equal(unique.size, 10, 'Each event exactly once');
  });

  /**
   * Subscribe-first + gap-drain proves: no event can ever be missed.
   *
   * Timeline:
   *   subscribe
   *   replay 1..N (already persisted)
   *   event N+1 published → buffered
   *   event N+2 published → buffered
   *   drain: N+1 emitted, N+2 emitted
   *   live: N+3 emitted
   */
  it('cannot miss an event regardless of when it is published relative to replay', () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    for (let i = 1; i <= 5; i++) {
      store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `c${i}` } });
    }

    const cursor = 0;
    let lastSentSeq = cursor;
    const emitted: number[] = [];
    const liveBuffer: StoredEvent[] = [];
    let replayComplete = false;

    const unsubscribe = bus.subscribe(conv.id, (e: StoredEvent) => {
      if (!replayComplete) { liveBuffer.push(e); return; }
      if (e.seq <= lastSentSeq) return;
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    });

    // Replay events 1-5
    for (const e of store.getEvents(conv.id, cursor)) {
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    }

    // Publish events 6-8 (race window — arrive between replay end and live switch)
    for (let i = 6; i <= 8; i++) {
      const e = store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `c${i}` } });
      bus.publish(e);
    }

    // Drain
    for (const e of liveBuffer) {
      if (e.seq <= lastSentSeq) continue;
      emitted.push(e.seq);
      lastSentSeq = e.seq;
    }
    liveBuffer.length = 0;
    replayComplete = true;

    // Events 9-10 come in live
    for (let i = 9; i <= 10; i++) {
      const e = store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `c${i}` } });
      bus.publish(e);
    }

    unsubscribe();

    assert.equal(emitted.length, 10, 'All 10 events received');
    const unique = new Set(emitted);
    assert.equal(unique.size, 10, 'No duplicates');
    assert.equal(emitted[0], 1);
    assert.equal(emitted[9], 10);
  });
});
