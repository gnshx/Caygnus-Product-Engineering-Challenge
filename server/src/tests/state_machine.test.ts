/**
 * Test: Terminal state machine — failed/completed/interrupted are terminal
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore } from '../store.js';
import type { RunState } from '../store.js';

describe('Terminal state machine', () => {
  const TERMINAL_STATES: RunState[] = ['completed', 'failed', 'interrupted'];

  for (const terminal of TERMINAL_STATES) {
    it(`cannot transition from '${terminal}' to any other state`, () => {
      const store = new EventStore(':memory:');
      const conv = store.createConversation();
      const run = store.createRun(conv.id);

      if (terminal === 'interrupted') {
        store.interruptStaleRuns();
      } else {
        store.setRunState(run.id, terminal);
      }

      assert.equal(store.getRun(run.id)?.state, terminal);

      const transitions: RunState[] = ['running', 'completed', 'failed', 'interrupted'];
      for (const next of transitions) {
        assert.throws(
          () => store.setRunState(run.id, next),
          undefined,
          `Should throw when transitioning from ${terminal} to ${next}`,
        );
      }
    });
  }

  it('allows running → completed', () => {
    const store = new EventStore(':memory:');
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    assert.equal(store.getRun(run.id)?.state, 'running');
    store.setRunState(run.id, 'completed');
    assert.equal(store.getRun(run.id)?.state, 'completed');
  });

  it('allows running → failed', () => {
    const store = new EventStore(':memory:');
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    store.setRunState(run.id, 'failed');
    assert.equal(store.getRun(run.id)?.state, 'failed');
  });

  it('throws on unknown run id', () => {
    const store = new EventStore(':memory:');
    assert.throws(() => store.setRunState('nonexistent', 'completed'), /not found/);
  });
});
