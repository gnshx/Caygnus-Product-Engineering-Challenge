/**
 * Test: Deduplication when replay and live delivery overlap
 * AC3 — replayed events + live events produce exactly one copy each
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import type { StoredEvent } from '../store.js';

describe('AC3: Deduplication of replay/live overlap', () => {
  it('produces zero duplicates when replay and live overlap', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    // Write 20 events synchronously
    const allChunks = Array.from({ length: 30 }, (_, i) => `chunk${i}`);
    for (const chunk of allChunks.slice(0, 20)) {
      store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: chunk } });
    }

    // Client had cursor=0, received first 10 events
    const cursor = 10;
    let lastSentSeq = cursor;
    const displayed: number[] = [];

    // Phase 1: Replay events 11..20
    const replayEvents = store.getEvents(conv.id, cursor);
    for (const event of replayEvents) {
      displayed.push(event.seq);
      lastSentSeq = event.seq;
    }

    // Phase 2: Gap close
    const gapEvents = store.getEvents(conv.id, lastSentSeq);
    for (const event of gapEvents) {
      if (event.seq > lastSentSeq) {
        displayed.push(event.seq);
        lastSentSeq = event.seq;
      }
    }

    // Subscribe live
    bus.subscribe(conv.id, (event: StoredEvent) => {
      if (event.seq <= lastSentSeq) return; // dedup
      displayed.push(event.seq);
      lastSentSeq = event.seq;
    });

    // Write remaining 10 events (21..30) live
    for (const chunk of allChunks.slice(20)) {
      const event = store.appendEvent({
        runId: run.id,
        conversationId: conv.id,
        type: 'chunk',
        data: { text: chunk },
      });
      bus.publish(event);
    }

    // Should have exactly 20 events (seqs 11..30)
    assert.equal(displayed.length, 20, 'Should display exactly 20 events');

    const unique = new Set(displayed);
    assert.equal(unique.size, displayed.length, 'No duplicates');

    for (let i = 1; i < displayed.length; i++) {
      assert.ok(displayed[i]! > displayed[i - 1]!, 'Must be in ascending order');
    }
  });

  it('handles race: live events published during replay gap are deduplicated', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    // Write 15 events
    for (let i = 0; i < 15; i++) {
      store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `c${i}` } });
    }

    const cursor = 0;
    let lastSentSeq = cursor;
    const displayed: number[] = [];

    // Phase 1 replay
    const replayed = store.getEvents(conv.id, cursor);
    for (const e of replayed) {
      displayed.push(e.seq);
      lastSentSeq = e.seq;
    }

    // Write 5 more events before gap-close (simulating race window)
    const liveEvents: StoredEvent[] = [];
    for (let i = 15; i < 20; i++) {
      const e = store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `live${i}` } });
      liveEvents.push(e);
    }

    // Phase 2: gap close
    const gapEvents = store.getEvents(conv.id, lastSentSeq);
    for (const e of gapEvents) {
      displayed.push(e.seq);
      lastSentSeq = e.seq;
    }

    // Subscribe live — all events <= lastSentSeq must be skipped
    bus.subscribe(conv.id, (e: StoredEvent) => {
      if (e.seq <= lastSentSeq) return;
      displayed.push(e.seq);
      lastSentSeq = e.seq;
    });

    // Publish live events (some already seen via gap-close)
    for (const e of liveEvents) {
      bus.publish(e);
    }

    const unique = new Set(displayed);
    assert.equal(unique.size, displayed.length, 'No duplicates after race');
    assert.equal(displayed.length, 20, 'All 20 events present');
  });
});
