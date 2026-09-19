// ─── MessageStore ─────────────────────────────────────────────────────────────
//
// Maintains an ordered, deduplicated list of received chunks.
// Defensive deduplication by seq (server guarantees ordering, but client
// provides a second safety net).

export interface Chunk {
  seq: number;
  text: string;
}

export interface EventLogEntry {
  seq: number;
  type: string;
  timestamp: number;
  data: unknown;
}

export class MessageStore {
  private chunks: Chunk[] = [];
  private seenSeqs = new Set<number>();
  public eventLog: EventLogEntry[] = [];

  /**
   * Add a chunk. Returns true if the chunk was new (not a duplicate).
   */
  addChunk(seq: number, text: string): boolean {
    if (this.seenSeqs.has(seq)) return false;
    this.seenSeqs.add(seq);

    // Insert in sorted position by seq (chunks should arrive in order, but
    // we handle out-of-order defensively)
    const insertAt = this.chunks.findIndex((c) => c.seq > seq);
    if (insertAt === -1) {
      this.chunks.push({ seq, text });
    } else {
      this.chunks.splice(insertAt, 0, { seq, text });
    }

    return true;
  }

  /** Return the full reconstructed text. */
  getText(): string {
    return this.chunks.map((c) => c.text).join('');
  }

  /** Return chunk count. */
  count(): number {
    return this.chunks.length;
  }

  /** Add an entry to the event log (for the debug panel). */
  logEvent(entry: EventLogEntry): void {
    this.eventLog.push(entry);
    // Keep log bounded to last 200 entries
    if (this.eventLog.length > 200) {
      this.eventLog.shift();
    }
  }

  /** Clear all state. */
  clear(): void {
    this.chunks = [];
    this.seenSeqs.clear();
    this.eventLog = [];
  }
}
