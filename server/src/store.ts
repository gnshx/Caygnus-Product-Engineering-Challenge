// Uses the built-in node:sqlite module (Node.js 22.5+, stable in Node 26).
// No native build required — zero gyp compilation.
import { DatabaseSync } from 'node:sqlite';
import { nanoid } from 'nanoid';

// ─── Types ────────────────────────────────────────────────────────────────────

export type RunState = 'running' | 'completed' | 'failed' | 'interrupted';

export type EventType =
  | 'user_message'
  | 'run_started'
  | 'chunk'
  | 'run_completed'
  | 'run_failed'
  | 'run_interrupted'; // appended on server startup for crashed runs

export interface StoredEvent {
  seq: number;
  run_id: string;
  conversation_id: string;
  type: EventType;
  data: Record<string, unknown>;
  created_at: number;
}

export interface Run {
  id: string;
  conversation_id: string;
  state: RunState;
  created_at: number;
  updated_at: number;
}

export interface Conversation {
  id: string;
  created_at: number;
}

// Terminal states — cannot transition out under any circumstances
const TERMINAL_STATES: ReadonlySet<RunState> = new Set(['completed', 'failed', 'interrupted']);

// ─── EventStore ───────────────────────────────────────────────────────────────

export class EventStore {
  private db: DatabaseSync;

  constructor(dbPath: string = ':memory:') {
    this.db = new DatabaseSync(dbPath);
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS conversations (
        id         TEXT    PRIMARY KEY,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS runs (
        id              TEXT    PRIMARY KEY,
        conversation_id TEXT    NOT NULL REFERENCES conversations(id),
        state           TEXT    NOT NULL CHECK(state IN ('running','completed','failed','interrupted')),
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL
      );

      -- seq is a global, monotonically increasing cursor.
      -- AUTOINCREMENT guarantees no reuse of seq values even after deletes.
      -- Synchronous writes (DatabaseSync) mean no two writers can interleave.
      CREATE TABLE IF NOT EXISTS events (
        seq             INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          TEXT    NOT NULL REFERENCES runs(id),
        conversation_id TEXT    NOT NULL,
        type            TEXT    NOT NULL,
        data            TEXT    NOT NULL,
        created_at      INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_events_conv ON events(conversation_id, seq);
      CREATE INDEX IF NOT EXISTS idx_events_run  ON events(run_id, seq);
    `);
  }

  // ── Conversations ────────────────────────────────────────────────────────────

  createConversation(id: string = nanoid()): Conversation {
    const now = Date.now();
    this.db
      .prepare('INSERT INTO conversations (id, created_at) VALUES (?, ?)')
      .run(id, now);
    return { id, created_at: now };
  }

  getConversation(id: string): Conversation | undefined {
    return this.db
      .prepare('SELECT id, created_at FROM conversations WHERE id = ?')
      .get(id) as Conversation | undefined;
  }

  // ── Runs ─────────────────────────────────────────────────────────────────────

  createRun(conversationId: string, id: string = nanoid()): Run {
    const now = Date.now();
    this.db
      .prepare(
        'INSERT INTO runs (id, conversation_id, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
      )
      .run(id, conversationId, 'running', now, now);
    return { id, conversation_id: conversationId, state: 'running', created_at: now, updated_at: now };
  }

  getRun(id: string): Run | undefined {
    return this.db
      .prepare('SELECT id, conversation_id, state, created_at, updated_at FROM runs WHERE id = ?')
      .get(id) as Run | undefined;
  }

  /**
   * Transition a run to a new state.
   * Throws if the current state is terminal (completed | failed | interrupted).
   *
   * State machine:
   *   running → completed    ✓
   *   running → failed       ✓
   *   running → interrupted  ✓
   *   completed → *          ✗ (terminal)
   *   failed → *             ✗ (terminal)
   *   interrupted → *        ✗ (terminal)
   */
  setRunState(runId: string, newState: RunState): void {
    const run = this.getRun(runId);
    if (!run) throw new Error(`Run ${runId} not found`);
    if (TERMINAL_STATES.has(run.state)) {
      throw new Error(
        `Cannot transition run ${runId} from terminal state '${run.state}' to '${newState}'`,
      );
    }
    this.db
      .prepare('UPDATE runs SET state = ?, updated_at = ? WHERE id = ?')
      .run(newState, Date.now(), runId);
  }

  /**
   * Startup recovery (AC4):
   * Find all runs still in 'running' state (crashed mid-generation),
   * mark them 'interrupted', and append a 'run_interrupted' event so
   * reconnecting clients see an explicit terminal event in the history.
   *
   * Returns the count of interrupted runs.
   */
  interruptStaleRuns(): number {
    // Find affected runs first (before updating state)
    const staleRuns = this.db
      .prepare("SELECT id, conversation_id FROM runs WHERE state = 'running'")
      .all() as Array<{ id: string; conversation_id: string }>;

    if (staleRuns.length === 0) return 0;

    const now = Date.now();
    this.db
      .prepare("UPDATE runs SET state = 'interrupted', updated_at = ? WHERE state = 'running'")
      .run(now);

    // Append a run_interrupted event to each affected run's history.
    // This makes the interruption visible to clients replaying from a cursor.
    for (const run of staleRuns) {
      this.appendEvent({
        runId: run.id,
        conversationId: run.conversation_id,
        type: 'run_interrupted',
        data: { run_id: run.id, reason: 'server_restart' },
      });
    }

    return staleRuns.length;
  }

  // ── Events ───────────────────────────────────────────────────────────────────

  /**
   * Append an event synchronously. Returns the assigned seq.
   *
   * ORDERING INVARIANT: every event is persisted (committed to SQLite) before
   * it is published to the live bus. The event store is the source of truth;
   * the bus is only a delivery optimisation.
   *
   * DatabaseSync writes are synchronous — no two concurrent writers can
   * interleave, so the global seq is a strict total order.
   */
  appendEvent(params: {
    runId: string;
    conversationId: string;
    type: EventType;
    data: Record<string, unknown>;
  }): StoredEvent {
    const now = Date.now();
    const result = this.db
      .prepare(
        'INSERT INTO events (run_id, conversation_id, type, data, created_at) VALUES (?, ?, ?, ?, ?)',
      )
      .run(params.runId, params.conversationId, params.type, JSON.stringify(params.data), now);

    const seq = Number(result.lastInsertRowid);
    return {
      seq,
      run_id: params.runId,
      conversation_id: params.conversationId,
      type: params.type,
      data: params.data,
      created_at: now,
    };
  }

  /**
   * Return events for a conversation with seq > afterSeq, ordered ascending.
   * afterSeq = 0 returns all events.
   */
  getEvents(conversationId: string, afterSeq: number = 0): StoredEvent[] {
    const rows = this.db
      .prepare(
        'SELECT seq, run_id, conversation_id, type, data, created_at FROM events WHERE conversation_id = ? AND seq > ? ORDER BY seq ASC',
      )
      .all(conversationId, afterSeq) as Array<Omit<StoredEvent, 'data' | 'seq'> & { data: string; seq: number | bigint }>;

    return rows.map((r) => ({
      ...r,
      seq: Number(r.seq),
      data: JSON.parse(r.data) as Record<string, unknown>,
    }));
  }

  /**
   * Return the maximum seq for a conversation. Returns 0 if none exist.
   */
  getMaxSeq(conversationId: string): number {
    const row = this.db
      .prepare('SELECT COALESCE(MAX(seq), 0) as max_seq FROM events WHERE conversation_id = ?')
      .get(conversationId) as { max_seq: number | bigint };
    return Number(row.max_seq);
  }

  /** Close the underlying database connection. */
  close(): void {
    this.db.close();
  }
}
