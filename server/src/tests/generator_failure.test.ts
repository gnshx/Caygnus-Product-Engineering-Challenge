/**
 * Test: Generator failure after partial output
 * AC5 — run becomes failed; history remains; cannot later become completed
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import { makeControllableGenerator } from '../generator.js';

describe('AC5: Generation failure after partial output', () => {
  it('sets run state to failed and preserves partial event history', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 30 }, (_, i) => `w${i}`);
    const gen = makeControllableGenerator({ chunks, failAfter: 10 });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    await assert.rejects(
      () => gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus }),
      /Simulated generator failure at chunk 10/,
    );

    assert.equal(store.getRun(run.id)?.state, 'failed');

    const events = store.getEvents(conv.id, 0);
    assert.equal(events.length, 11); // 10 chunks + run_failed

    const chunkEvents = events.filter((e) => e.type === 'chunk');
    assert.equal(chunkEvents.length, 10);

    const failEvent = events.find((e) => e.type === 'run_failed');
    assert.ok(failEvent, 'run_failed event must exist');
    assert.ok(
      (failEvent.data as { error?: string }).error?.includes('Simulated generator failure'),
    );
  });

  it('emits run_failed on the bus', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 5 }, (_, i) => `w${i}`);
    const gen = makeControllableGenerator({ chunks, failAfter: 3 });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    const busEventTypes: string[] = [];
    bus.subscribe(conv.id, (e) => busEventTypes.push(e.type));

    await assert.rejects(() =>
      gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus }),
    );

    assert.ok(busEventTypes.includes('run_failed'));
    assert.ok(!busEventTypes.includes('run_completed'));
  });

  it('does not contain run_completed in event history after failure', async () => {
    const store = new EventStore(':memory:');
    const bus = new RunBus();
    const chunks = Array.from({ length: 20 }, (_, i) => `w${i}`);
    const gen = makeControllableGenerator({ chunks, failAfter: 5 });

    const conv = store.createConversation();
    const run = store.createRun(conv.id);

    await assert.rejects(() =>
      gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store, bus }),
    );

    const events = store.getEvents(conv.id, 0);
    const types = events.map((e) => e.type);

    assert.ok(!types.includes('run_completed'));
    assert.ok(types.includes('run_failed'));
  });
});
