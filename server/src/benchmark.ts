/**
 * Verification Benchmark
 *
 * 1. Generates 30 ordered text events for one run.
 * 2. Pauses the generator deterministically after event 15 (no sleep).
 * 3. Simulates client disconnect at cursor=15.
 * 4. Releases the gate — generator continues events 16-30.
 * 5. Client reconnects and replays missed events.
 * 6. Verifies: 30 events, 0 missing, 0 duplicates, final state = completed.
 *
 * WHY NO SLEEP: The generator uses a Promise gate (not a timer) to pause.
 * The benchmark controls when the gate opens. No arbitrary wait, no race.
 *
 * Run with: npm run benchmark
 */
import { EventStore } from './store.js';
import { RunBus } from './bus.js';
import type { StoredEvent } from './store.js';

// ─── Colors ───────────────────────────────────────────────────────────────────
const c = {
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  red:   (s: string) => `\x1b[31m${s}\x1b[0m`,
  bold:  (s: string) => `\x1b[1m${s}\x1b[0m`,
  cyan:  (s: string) => `\x1b[36m${s}\x1b[0m`,
  dim:   (s: string) => `\x1b[2m${s}\x1b[0m`,
};

const TOTAL_CHUNKS = 30;
const PAUSE_AFTER  = 15; // generator pauses here; benchmark disconnects

// ─── Controllable (gated) generator ──────────────────────────────────────────
/**
 * Returns a generator function that pauses after `pauseAfter` chunks.
 * The benchmark calls `release()` to allow the generator to continue.
 * This is fully deterministic — no timers or sleeps involved.
 */
function makeGatedGenerator(params: {
  chunks: string[];
  pauseAfter: number;
}): {
  run: (args: { runId: string; conversationId: string; store: EventStore; bus: RunBus }) => Promise<void>;
  release: () => void;
} {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });

  async function run(args: { runId: string; conversationId: string; store: EventStore; bus: RunBus }) {
    const { runId, conversationId, store, bus } = args;
    try {
      for (let i = 0; i < params.chunks.length; i++) {
        if (i === params.pauseAfter) {
          await gate; // pause until benchmark calls release()
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
      const done = store.appendEvent({ runId, conversationId, type: 'run_completed', data: { run_id: runId } });
      bus.publish(done);
    } catch (err) {
      try { store.setRunState(runId, 'failed'); } catch { /* already terminal */ }
      const fail = store.appendEvent({ runId, conversationId, type: 'run_failed', data: { run_id: runId, error: String(err) } });
      bus.publish(fail);
      throw err;
    }
  }

  return { run, release };
}

// ─── Benchmark ────────────────────────────────────────────────────────────────
async function main() {
  console.log(c.bold('\n━━━ Verification Benchmark: 30-Event Resumable Stream ━━━\n'));

  const store = new EventStore(':memory:');
  const bus   = new RunBus();
  const conv  = store.createConversation();
  const run   = store.createRun(conv.id);

  const chunks = Array.from({ length: TOTAL_CHUNKS }, (_, i) => `word_${String(i + 1).padStart(2, '0')}`);
  const { run: gatedGenerator, release } = makeGatedGenerator({ chunks, pauseAfter: PAUSE_AFTER });

  // ─── Phase 1: Live — receive events 1-15, then disconnect ─────────────────
  console.log(c.cyan(`[Phase 1] Receiving events live (generator pauses after ${PAUSE_AFTER} chunks)\n`));

  const phase1Seqs: number[] = [];
  let interruptCursor = 0;

  let resolvePhase1Done!: () => void;
  const phase1Done = new Promise<void>((r) => { resolvePhase1Done = r; });

  const unsub = bus.subscribe(conv.id, (event: StoredEvent) => {
    if (event.type !== 'chunk') return;
    phase1Seqs.push(event.seq);
    process.stdout.write(c.dim(`  → chunk ${phase1Seqs.length}: ${(event.data as { text: string }).text}\n`));
    if (phase1Seqs.length === PAUSE_AFTER) {
      interruptCursor = event.seq;
      resolvePhase1Done(); // signal: generator has paused, we can disconnect
    }
  });

  // Start generator — it will emit 0..14, then await the gate at i=15
  const genPromise = gatedGenerator({ runId: run.id, conversationId: conv.id, store, bus });

  // Wait until the generator has paused (events 1-15 received)
  await phase1Done;
  unsub(); // disconnect

  console.log(c.red(`\n[Interrupt] Disconnected at cursor=${interruptCursor} (received ${phase1Seqs.length} chunks)\n`));

  // ─── Release the gate — generator continues 16-30 in the background ───────
  release();

  // Wait for generator to finish (events 16-30 + run_completed)
  await genPromise;

  // ─── Phase 2: Reconnect — replay from cursor ───────────────────────────────
  console.log(c.cyan('[Phase 2] Reconnecting and replaying missed events…\n'));

  const replayedSeqs: number[] = [];
  const replayEvents = store.getEvents(conv.id, interruptCursor).filter((e) => e.type === 'chunk');
  for (const e of replayEvents) {
    replayedSeqs.push(e.seq);
    process.stdout.write(c.dim(`  ← replay seq ${e.seq}: ${(e.data as { text: string }).text}\n`));
  }

  // ─── Reconstruct ──────────────────────────────────────────────────────────
  const combined = [...phase1Seqs, ...replayedSeqs];
  combined.sort((a, b) => a - b);
  const deduped = combined.filter((seq, i) => i === 0 || seq !== combined[i - 1]);

  const allChunks = store.getEvents(conv.id, 0).filter((e) => e.type === 'chunk');
  const fullText = deduped
    .map((seq) => {
      const e = allChunks.find((ev) => ev.seq === seq);
      return (e?.data as { text?: string })?.text ?? '';
    })
    .join('');

  const eventCount = deduped.length;
  const duplicates = combined.length - deduped.length;
  const missing    = TOTAL_CHUNKS - eventCount;
  const finalState = store.getRun(run.id)?.state ?? 'unknown';

  console.log('\n' + c.bold('━━━ Benchmark Results ━━━\n'));
  console.log(`  Total chunks expected : ${c.bold(String(TOTAL_CHUNKS))}`);
  console.log(`  Events received       : ${eventCount === TOTAL_CHUNKS ? c.green(String(eventCount)) : c.red(String(eventCount))}`);
  console.log(`  Duplicate events      : ${duplicates === 0 ? c.green('0') : c.red(String(duplicates))}`);
  console.log(`  Missing events        : ${missing    === 0 ? c.green('0') : c.red(String(missing))}`);
  console.log(`  Interrupt at chunk    : ${PAUSE_AFTER}`);
  console.log(`  Reconnect cursor      : ${interruptCursor}`);
  console.log(`  Final run state       : ${finalState === 'completed' ? c.green(finalState) : c.red(finalState)}`);
  console.log(`\n  Reconstructed text    : ${c.cyan('"' + fullText + '"')}`);

  const passed =
    eventCount === TOTAL_CHUNKS &&
    duplicates === 0 &&
    missing === 0 &&
    finalState === 'completed';

  console.log('\n' + (passed ? c.green(c.bold('  ✓ BENCHMARK PASSED')) : c.red(c.bold('  ✗ BENCHMARK FAILED'))));
  console.log('\n' + '━'.repeat(46) + '\n');

  process.exit(passed ? 0 : 1);
}

main().catch((err) => { console.error(err); process.exit(1); });
