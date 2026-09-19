/**
 * Test: Cursor-based replay
 * AC2 — reconnect with cursor returns exactly missed events, no gaps, no repeats
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import { makeControllableGenerator } from '../generator.js';

describe('AC2: Cursor replay after connection drop', () => {
  it('returns only events after the cursor, in order', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 30 }, (_, i) => `w${i}`);
    const gen = makeControllableGenerator({ chunks });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus });

    const allEvents = store.getEvents(conv.id, 0);
    assert.equal(allEvents.length, 31);

    const cursorSeq = allEvents[14]!.seq;
    const replayed = store.getEvents(conv.id, cursorSeq);

    assert.equal(replayed.length, 16);
    assert.equal(replayed[0]!.seq, cursorSeq + 1);
    for (const e of replayed) {
      assert.ok(e.seq > cursorSeq);
    }
    for (let i = 1; i < replayed.length; i++) {
      assert.ok(replayed[i]!.seq > replayed[i - 1]!.seq);
    }
  });

  it('returns all events when cursor is 0', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const gen = makeControllableGenerator({ chunks: ['a', 'b', 'c'] });
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus });
    const events = store.getEvents(conv.id, 0);
    assert.equal(events.length, 4); // 3 chunks + run_completed
  });

  it('returns empty when cursor is at the end', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const gen = makeControllableGenerator({ chunks: ['a', 'b', 'c'] });
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus });
    const maxSeq = store.getMaxSeq(conv.id);
    const events = store.getEvents(conv.id, maxSeq);
    assert.equal(events.length, 0);
  });
});
