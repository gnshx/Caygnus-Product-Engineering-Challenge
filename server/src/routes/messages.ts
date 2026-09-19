import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { nanoid } from 'nanoid';
import type { EventStore } from '../store.js';
import type { RunBus } from '../bus.js';
import type { GeneratorFn } from '../generator.js';

const SubmitMessageBody = z.object({
  message_id: z.string().min(1),
  text: z.string().min(1),
});

export async function messagesRoutes(
  app: FastifyInstance,
  {
    store,
    bus,
    generator,
    generatorOpts,
  }: {
    store: EventStore;
    bus: RunBus;
    generator: GeneratorFn;
    generatorOpts?: { tickMs?: number };
  },
) {
  /**
   * POST /conversations/:convId/messages
   *
   * Submit a user message and start a generator run.
   * Idempotent on message_id — returns existing run if already processed.
   */
  app.post<{ Params: { convId: string } }>(
    '/conversations/:convId/messages',
    async (req, reply) => {
      const conv = store.getConversation(req.params.convId);
      if (!conv) return reply.status(404).send({ error: 'conversation_not_found' });

      const body = SubmitMessageBody.safeParse(req.body);
      if (!body.success) {
        return reply.status(400).send({ error: 'invalid_body', details: body.error.issues });
      }

      const { message_id, text } = body.data;

      // ── Idempotency check ──────────────────────────────────────────────────
      // If a user_message event with this message_id already exists, return the
      // existing run_id rather than spawning a second run.
      const existing = store
        .getEvents(req.params.convId, 0)
        .find(
          (e) =>
            e.type === 'user_message' &&
            (e.data as { message_id?: string }).message_id === message_id,
        );

      if (existing) {
        const runId = (existing.data as { run_id: string }).run_id;
        return reply.status(200).send({
          message_id,
          run_id: runId,
          idempotent: true,
        });
      }

      // ── Create run and persist user message ───────────────────────────────
      const runId = nanoid();
      store.createRun(req.params.convId, runId);

      // Persist the user_message event
      const userMsgEvent = store.appendEvent({
        runId,
        conversationId: req.params.convId,
        type: 'user_message',
        data: { message_id, text, run_id: runId },
      });
      bus.publish(userMsgEvent);

      // Persist run_started event
      const startEvent = store.appendEvent({
        runId,
        conversationId: req.params.convId,
        type: 'run_started',
        data: { run_id: runId },
      });
      bus.publish(startEvent);

      // ── Spawn generator (fire-and-forget) ─────────────────────────────────
      // We deliberately do NOT await here so the HTTP response returns
      // immediately while the generator streams in the background.
      generator({
        runId,
        conversationId: req.params.convId,
        prompt: text,
        store,
        bus,
        opts: generatorOpts,
      }).catch((err: unknown) => {
        // Error is already handled inside the generator (sets failed state)
        app.log.error({ err, runId }, 'Generator error');
      });

      return reply.status(201).send({ message_id, run_id: runId });
    },
  );
}
