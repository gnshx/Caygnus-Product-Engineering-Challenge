import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { nanoid } from 'nanoid';
import type { EventStore } from '../store.js';

const CreateConversationBody = z.object({
  id: z.string().optional(),
});

export async function conversationsRoutes(
  app: FastifyInstance,
  { store }: { store: EventStore },
) {
  // POST /conversations — create a new conversation
  app.post('/conversations', async (req, reply) => {
    const body = CreateConversationBody.safeParse(req.body ?? {});
    if (!body.success) {
      return reply.status(400).send({ error: 'invalid_body', details: body.error.issues });
    }

    const conv = store.createConversation(body.data.id ?? nanoid());
    return reply.status(201).send({ conversation_id: conv.id, created_at: conv.created_at });
  });

  // GET /conversations/:convId — fetch conversation info
  app.get<{ Params: { convId: string } }>(
    '/conversations/:convId',
    async (req, reply) => {
      const conv = store.getConversation(req.params.convId);
      if (!conv) return reply.status(404).send({ error: 'not_found' });
      return reply.send(conv);
    },
  );
}
