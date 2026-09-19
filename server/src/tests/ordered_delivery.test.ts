/**
 * Test: Ordered live event delivery
 * AC1 — events arrive in seq order; run_completed is last
 */
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import { makeControllableGenerator } from '../generator.js';

describe('AC1: Ordered live event delivery', () => {
  it('delivers 30 chunks in ascending seq order and terminates with run_completed', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 30 }, (_, i) => `word${i}`);
    const gen = makeControllableGenerator({ chunks });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    const received: { seq: number; type: string }[] = [];
    bus.subscribe(conv.id, (e) => received.push({ seq: e.seq, type: e.type }));

    await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus });

    // 30 chunks + 1 run_completed
    assert.equal(received.length, 31);

    // Ascending seq order
    for (let i = 1; i < received.length; i++) {
      assert.ok(received[i]!.seq > received[i - 1]!.seq, `seq[${i}] must > seq[${i - 1}]`);
    }

    // Last event is run_completed
    assert.equal(received[received.length - 1]!.type, 'run_completed');

    // Run state must be completed
    assert.equal(store.getRun(run.id)?.state, 'completed');
  });

  it('stores all events in the EventStore with strictly monotonic seqs', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 30 }, (_, i) => `chunk${i}`);
    const gen = makeControllableGenerator({ chunks });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus });

    const events = store.getEvents(conv.id, 0);
    assert.equal(events.length, 31);

    const seqs = events.map((e) => e.seq);
    for (let i = 1; i < seqs.length; i++) {
      assert.equal(seqs[i], seqs[i - 1]! + 1, `seq should be consecutive`);
    }
  });
});
