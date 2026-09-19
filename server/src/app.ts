import Fastify from 'fastify';
import cors from '@fastify/cors';
import type { GeneratorFn } from './generator.js';
import { EventStore } from './store.js';
import { RunBus } from './bus.js';
import { conversationsRoutes } from './routes/conversations.js';
import { messagesRoutes } from './routes/messages.js';
import { streamRoutes } from './routes/stream.js';

export interface AppOptions {
  store?: EventStore;
  bus?: RunBus;
  generator: GeneratorFn;
  generatorOpts?: { tickMs?: number };
  logger?: boolean | object;
}

/**
 * Factory function — returns a configured Fastify app.
 * Accepts injected store/bus for testing (dependency injection).
 */
export async function buildApp(opts: AppOptions) {
  const store = opts.store ?? new EventStore();
  const bus = opts.bus ?? new RunBus();

  const app = Fastify({
    logger: opts.logger ?? { level: 'info' },
  });

  await app.register(cors, { origin: true });

  // Content-type parser for JSON body
  app.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      done(null, JSON.parse(body as string));
    } catch (err) {
      done(err as Error, undefined);
    }
  });

  // Register routes
  await app.register(
    async (instance) => {
      await conversationsRoutes(instance, { store });
      await messagesRoutes(instance, {
        store,
        bus,
        generator: opts.generator,
        generatorOpts: opts.generatorOpts,
      });
      await streamRoutes(instance, { store, bus });
    },
    { prefix: '/api' },
  );

  // Health check
  app.get('/health', async () => ({ ok: true }));

  return { app, store, bus };
}
