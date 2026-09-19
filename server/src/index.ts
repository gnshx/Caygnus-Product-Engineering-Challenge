import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildApp } from './app.js';
import { EventStore } from './store.js';
import { RunBus } from './bus.js';
import { fakeGenerator } from './generator.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, '..', 'chat.db');

const PORT = parseInt(process.env['PORT'] ?? '3001', 10);
const HOST = process.env['HOST'] ?? '0.0.0.0';

async function main() {
  // Create persistent store (file-based SQLite)
  const store = new EventStore(DB_PATH);
  const bus = new RunBus();

  // ── AC4: Service restart — mark stale running runs as interrupted ──────────
  const interrupted = store.interruptStaleRuns();
  if (interrupted > 0) {
    console.warn(
      `[startup] Marked ${interrupted} stale run(s) as 'interrupted' from previous process`,
    );
  }

  const { app } = await buildApp({
    store,
    bus,
    generator: fakeGenerator,
    generatorOpts: { tickMs: 80 },
  });

  try {
    await app.listen({ port: PORT, host: HOST });
    console.log(`Server listening on http://localhost:${PORT}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

main();
