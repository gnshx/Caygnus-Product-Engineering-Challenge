// ─── ConnectionManager ────────────────────────────────────────────────────────
//
// Wraps EventSource with exponential backoff reconnection.
// Uses the SSE `Last-Event-ID` header (browser native) as the cursor.
// Exposes observable connection state for the UI.

export type ConnectionState =
  | 'idle'
  | 'connected'
  | 'reconnecting'
  | 'disconnected'
  | 'completed'
  | 'failed'
  | 'interrupted';

export type ConnectionEvent =
  | { type: 'state_change'; state: ConnectionState }
  | { type: 'sse_event'; name: string; data: unknown; seq: number };

export type ConnectionHandler = (event: ConnectionEvent) => void;

interface ConnectionManagerOptions {
  /** Called on every state change or SSE event */
  onEvent: ConnectionHandler;
  /** Base retry delay in ms (default 200) */
  baseRetryMs?: number;
  /** Maximum retry delay in ms (default 30_000) */
  maxRetryMs?: number;
  /** Jitter factor 0..1 (default 0.2) */
  jitter?: number;
}

export class ConnectionManager {
  private es: EventSource | null = null;
  private retryCount = 0;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private lastSeq = 0;
  private state: ConnectionState = 'idle';
  private closed = false;

  private readonly baseRetryMs: number;
  private readonly maxRetryMs: number;
  private readonly jitter: number;
  private readonly onEvent: ConnectionHandler;

  constructor(
    private readonly convId: string,
    opts: ConnectionManagerOptions,
  ) {
    this.onEvent = opts.onEvent;
    this.baseRetryMs = opts.baseRetryMs ?? 200;
    this.maxRetryMs = opts.maxRetryMs ?? 30_000;
    this.jitter = opts.jitter ?? 0.2;
  }

  /** Open the SSE connection (or reconnect from last cursor). */
  connect(): void {
    if (this.closed) return;
    this.openStream();
  }

  /** Permanently close the connection. */
  disconnect(): void {
    this.closed = true;
    this.clearRetryTimer();
    this.es?.close();
    this.es = null;
    this.setState('disconnected');
  }

  /** Programmatically simulate a disconnect (for demo/testing). */
  simulateDisconnect(): void {
    this.es?.close();
    this.es = null;
    // Trigger reconnect logic
    this.handleError();
  }

  getState(): ConnectionState {
    return this.state;
  }

  getLastSeq(): number {
    return this.lastSeq;
  }

  // ── Private ────────────────────────────────────────────────────────────────

  private openStream(): void {
    const url = `/api/conversations/${this.convId}/stream?cursor=${this.lastSeq}`;
    const es = new EventSource(url);
    this.es = es;

    es.onopen = () => {
      this.retryCount = 0;
      this.setState('connected');
    };

    es.onerror = () => {
      this.handleError();
    };

    // Listen to all event types
    const eventTypes = ['chunk', 'run_started', 'run_completed', 'run_failed', 'run_interrupted', 'user_message', 'error'];
    for (const type of eventTypes) {
      es.addEventListener(type, (e: MessageEvent) => {
        this.handleMessage(type, e);
      });
    }
  }

  private handleMessage(type: string, e: MessageEvent): void {
    let data: unknown;
    try {
      data = JSON.parse(e.data as string);
    } catch {
      data = e.data;
    }

    const payload = data as Record<string, unknown>;
    const seq = typeof payload['seq'] === 'number' ? payload['seq'] : 0;

    // Update cursor
    if (seq > this.lastSeq) {
      this.lastSeq = seq;
    }

    this.onEvent({ type: 'sse_event', name: type, data: payload, seq });

    // Terminal events — close permanently
    if (type === 'run_completed') {
      this.es?.close();
      this.es = null;
      this.setState('completed');
    } else if (type === 'run_failed') {
      this.es?.close();
      this.es = null;
      this.setState('failed');
    } else if (type === 'run_interrupted') {
      this.es?.close();
      this.es = null;
      this.setState('interrupted');
    }
  }

  private handleError(): void {
    if (this.closed) return;
    this.es?.close();
    this.es = null;

    if (this.state === 'completed' || this.state === 'failed' || this.state === 'interrupted') return;

    this.setState('reconnecting');
    const delay = this.computeBackoff();
    this.retryTimer = setTimeout(() => {
      if (!this.closed) this.openStream();
    }, delay);
  }

  private computeBackoff(): number {
    const base = Math.min(this.baseRetryMs * 2 ** this.retryCount, this.maxRetryMs);
    const jitterAmount = base * this.jitter * (Math.random() * 2 - 1);
    this.retryCount++;
    return Math.max(0, base + jitterAmount);
  }

  private clearRetryTimer(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  private setState(newState: ConnectionState): void {
    if (this.state === newState) return;
    this.state = newState;
    this.onEvent({ type: 'state_change', state: newState });
  }
}
