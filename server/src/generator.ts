import type { EventStore, StoredEvent } from './store.js';
import type { RunBus } from './bus.js';

// ─── Generator Interface ──────────────────────────────────────────────────────

export interface GeneratorOptions {
  /** Delay in ms between each chunk. Set to 0 for tests. */
  tickMs?: number;
}

/**
 * A generator function produces text chunks for a given prompt.
 * It writes events to the EventStore and publishes them on the RunBus.
 * It is responsible for setting the terminal run state (completed or failed).
 */
export type GeneratorFn = (params: {
  runId: string;
  conversationId: string;
  prompt: string;
  store: EventStore;
  bus: RunBus;
  opts?: GeneratorOptions;
}) => Promise<void>;

// ─── Fake Generator ───────────────────────────────────────────────────────────
//
// Produces a deterministic 30-word response, one chunk per word.
// Used in tests (tickMs=0) and in the demo (tickMs=80).

const FAKE_RESPONSE_WORDS = [
  'The', 'quick', 'brown', 'fox', 'jumps', 'over', 'the', 'lazy', 'dog',
  'and', 'then', 'runs', 'swiftly', 'through', 'the', 'forest', 'past',
  'ancient', 'oak', 'trees', 'while', 'the', 'sun', 'sets', 'behind',
  'the', 'distant', 'hills', 'glowing', 'orange',
] as const;

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const fakeGenerator: GeneratorFn = async ({
  runId,
  conversationId,
  prompt: _prompt,
  store,
  bus,
  opts = {},
}) => {
  const tickMs = opts.tickMs ?? 80;

  try {
    for (const word of FAKE_RESPONSE_WORDS) {
      await sleep(tickMs);

      const event = store.appendEvent({
        runId,
        conversationId,
        type: 'chunk',
        data: { text: word + ' ' },
      });

      bus.publish(event);
    }

    // Mark completed and emit terminal event
    store.setRunState(runId, 'completed');
    const doneEvent = store.appendEvent({
      runId,
      conversationId,
      type: 'run_completed',
      data: { run_id: runId },
    });
    bus.publish(doneEvent);
  } catch (err) {
    // Mark failed (terminal — cannot become completed later)
    try {
      store.setRunState(runId, 'failed');
    } catch {
      // Already terminal — swallow (e.g. interrupted on restart)
    }

    const failEvent = store.appendEvent({
      runId,
      conversationId,
      type: 'run_failed',
      data: {
        run_id: runId,
        error: err instanceof Error ? err.message : String(err),
      },
    });
    bus.publish(failEvent);

    throw err;
  }
};

// ─── Controllable Fake Generator (for tests) ──────────────────────────────────
//
// Generates `count` chunks then either completes or throws.

export function makeControllableGenerator(params: {
  chunks: string[];
  failAfter?: number; // throw an error after emitting this many chunks
}): GeneratorFn {
  return async ({ runId, conversationId, store, bus }) => {
    try {
      for (let i = 0; i < params.chunks.length; i++) {
        if (params.failAfter !== undefined && i >= params.failAfter) {
          throw new Error(`Simulated generator failure at chunk ${i}`);
        }

        const event = store.appendEvent({
          runId,
          conversationId,
          type: 'chunk',
          data: { text: params.chunks[i]! },
        });
        bus.publish(event);
      }

      store.setRunState(runId, 'completed');
      const doneEvent = store.appendEvent({
        runId,
        conversationId,
        type: 'run_completed',
        data: { run_id: runId },
      });
      bus.publish(doneEvent);
    } catch (err) {
      try {
        store.setRunState(runId, 'failed');
      } catch {
        // Already terminal
      }
      const failEvent = store.appendEvent({
        runId,
        conversationId,
        type: 'run_failed',
        data: {
          run_id: runId,
          error: err instanceof Error ? err.message : String(err),
        },
      });
      bus.publish(failEvent);
      throw err;
    }
  };
}

// Re-export StoredEvent for convenience
export type { StoredEvent };
