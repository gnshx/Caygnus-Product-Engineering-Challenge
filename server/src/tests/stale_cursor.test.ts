/**
 * Test: Unknown or stale cursor handling
 * AC6 — invalid cursors receive explicit recoverable error
 *
 * Uses Fastify's inject() method — no network, no real HTTP server.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../app.js';
import { EventStore } from '../store.js';
import { RunBus } from '../bus.js';
import { makeControllableGenerator } from '../generator.js';

describe('AC6: Unknown or stale cursor handling', () => {
  let store: EventStore;
  let bus: RunBus;
  let app: Awaited<ReturnType<typeof buildApp>>['app'];

  before(async () => {
    store = new EventStore(':memory:');
    bus = new RunBus();
    ({ app } = await buildApp({
      store,
      bus,
      generator: makeControllableGenerator({ chunks: ['a', 'b', 'c'] }),
      logger: false,
    }));
    await app.ready();
  });

  after(async () => {
    await app.close();
  });

  it('returns 404 for unknown conversation', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/api/conversations/nonexistent/stream?cursor=0',
    });
    assert.equal(res.statusCode, 404);
    const body = JSON.parse(res.body) as { error: string };
    assert.equal(body.error, 'conversation_not_found');
  });

  it('returns 409 for cursor ahead of max_seq', async () => {
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    for (let i = 0; i < 5; i++) {
      store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'chunk', data: { text: `w${i}` } });
    }
    const maxSeq = store.getMaxSeq(conv.id);

    const res = await app.inject({
      method: 'GET',
      url: `/api/conversations/${conv.id}/stream?cursor=${maxSeq + 100}`,
    });

    assert.equal(res.statusCode, 409);
    const body = JSON.parse(res.body) as { error: string; max_seq: number };
    assert.equal(body.error, 'stale_cursor');
    assert.equal(body.max_seq, maxSeq);
  });

  it('returns 400 for non-numeric cursor', async () => {
    const conv = store.createConversation();
    const res = await app.inject({
      method: 'GET',
      url: `/api/conversations/${conv.id}/stream?cursor=bogus`,
    });
    assert.equal(res.statusCode, 400);
    const body = JSON.parse(res.body) as { error: string };
    assert.equal(body.error, 'invalid_cursor');
  });

  it('cursor=0 is valid and returns SSE on a conversation with events', async () => {
    const conv = store.createConversation();
    const run = store.createRun(conv.id);
    store.appendEvent({ runId: run.id, conversationId: conv.id, type: 'run_completed', data: {} });
    store.setRunState(run.id, 'completed');

    const res = await app.inject({
      method: 'GET',
      url: `/api/conversations/${conv.id}/stream?cursor=0`,
    });

    assert.equal(res.statusCode, 200);
    assert.ok(res.headers['content-type']?.includes('text/event-stream'));
  });
});
