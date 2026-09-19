/**
 * Test: Service restart — durable state recovery
 * AC4 — events persisted before restart are recoverable; stale runs become interrupted
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import { makeControllableGenerator } from '../generator.js';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';

function tmpDbPath(): string {
  return path.join(os.tmpdir(), `test-restart-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

describe('AC4: Service restart — durable state recovery', () => {
  it('recovers persisted events after store is closed and reopened', async () => {
    const dbPath = tmpDbPath();
    try {
      // Process 1: write events and close
      const store1 = new EventStore(dbPath);
      const bus1 = new RunBus();
      const gen = makeControllableGenerator({
        chunks: Array.from({ length: 20 }, (_, i) => `w${i}`),
      });
      const conv = store1.createConversation('conv-restart-1');
      const run = store1.createRun(conv.id);
      await gen({ runId: run.id, conversationId: conv.id, prompt: 'test', store: store1, bus: bus1 });

      const eventsBefore = store1.getEvents(conv.id, 0);
      assert.equal(eventsBefore.length, 21);
      store1.close();

      // Process 2: reopen and verify
      const store2 = new EventStore(dbPath);
      const eventsAfter = store2.getEvents(conv.id, 0);
      assert.equal(eventsAfter.length, 21);

      const seqsBefore = eventsBefore.map((e) => e.seq);
      const seqsAfter = eventsAfter.map((e) => e.seq);
      assert.deepEqual(seqsAfter, seqsBefore);

      assert.equal(store2.getRun(run.id)?.state, 'completed');

      const midSeq = eventsAfter[9]!.seq;
      const replayed = store2.getEvents(conv.id, midSeq);
      assert.equal(replayed.length, 11);

      store2.close();
    } finally {
      fs.rmSync(dbPath, { force: true });
    }
  });

  it('marks stale running runs as interrupted on restart', async () => {
    const dbPath = tmpDbPath();
    try {
      // Process 1: create a run but do NOT complete it (simulating crash)
      const store1 = new EventStore(dbPath);
      const conv = store1.createConversation();
      const run = store1.createRun(conv.id);
      for (let i = 0; i < 5; i++) {
        store1.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `w${i}` } });
      }
      assert.equal(store1.getRun(run.id)?.state, 'running');
      store1.close();

      // Process 2: startup — must interrupt stale runs
      const store2 = new EventStore(dbPath);
      const interrupted = store2.interruptStaleRuns();
      assert.equal(interrupted, 1);
      assert.equal(store2.getRun(run.id)?.state, 'interrupted');

      const events = store2.getEvents(conv.id, 0);
      // 5 chunks + 1 run_interrupted event appended by interruptStaleRuns
      assert.equal(events.length, 6);
      const lastEvent = events[events.length - 1]!;
      assert.equal(lastEvent.type, 'run_interrupted', 'run_interrupted event must be in history');
      assert.equal((lastEvent.data as { reason?: string }).reason, 'server_restart');

      store2.close();
    } finally {
      fs.rmSync(dbPath, { force: true });
    }
  });

  it('interrupted run cannot transition to completed', () => {
    const store = new EventStore(':memory:');
    const conv = store.createConversation();
    store.createRun(conv.id);
    store.interruptStaleRuns();

    const runs = store.getEvents(conv.id, 0); // get run id another way
    // Get the run via getRun by looking at conversation events - just create fresh
    const store2 = new EventStore(':memory:');
    const conv2 = store2.createConversation();
    const run2 = store2.createRun(conv2.id);
    store2.interruptStaleRuns();
    assert.equal(store2.getRun(run2.id)?.state, 'interrupted');
    assert.throws(() => store2.setRunState(run2.id, 'completed'));
  });
});
