import { EventEmitter } from 'node:events';
import type { StoredEvent } from './store.js';

// ─── RunBus ───────────────────────────────────────────────────────────────────
//
// In-process pub/sub for live event delivery.
// This is intentionally transient — it does NOT persist.
// The EventStore (SQLite) is the source of truth.
// On server restart the bus is empty; clients replay from the EventStore.

type EventHandler = (event: StoredEvent) => void;

export class RunBus {
  private emitter = new EventEmitter();

  constructor() {
    // Increase limit to handle many concurrent SSE connections
    this.emitter.setMaxListeners(500);
  }

  /** Publish an event to all listeners subscribed to its conversation. */
  publish(event: StoredEvent): void {
    this.emitter.emit(`conv:${event.conversation_id}`, event);
  }

  /** Subscribe to all events in a conversation. Returns an unsubscribe fn. */
  subscribe(conversationId: string, handler: EventHandler): () => void {
    const channel = `conv:${conversationId}`;
    this.emitter.on(channel, handler);
    return () => this.emitter.off(channel, handler);
  }
}
