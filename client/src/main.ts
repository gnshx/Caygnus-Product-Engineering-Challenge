import { ConnectionManager, type ConnectionState } from './ConnectionManager.js';
import { MessageStore } from './MessageStore.js';

const API = '/api';

// ─── DOM refs ─────────────────────────────────────────────────────────────────
const form = document.getElementById('msg-form') as HTMLFormElement;
const input = document.getElementById('msg-input') as HTMLInputElement;
const sendBtn = document.getElementById('send-btn') as HTMLButtonElement;
const responseEl = document.getElementById('response-text') as HTMLDivElement;
const statusBadge = document.getElementById('conn-status') as HTMLSpanElement;
const cursorEl = document.getElementById('cursor-val') as HTMLSpanElement;
const chunkCountEl = document.getElementById('chunk-count') as HTMLSpanElement;
const runStateEl = document.getElementById('run-state') as HTMLSpanElement;
const eventLogEl = document.getElementById('event-log') as HTMLUListElement;
const disconnectBtn = document.getElementById('disconnect-btn') as HTMLButtonElement;
const convIdEl = document.getElementById('conv-id') as HTMLSpanElement;

// ─── State ────────────────────────────────────────────────────────────────────
let convId: string | null = null;
let msgStore = new MessageStore();
let conn: ConnectionManager | null = null;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function updateStatus(state: ConnectionState) {
  statusBadge.textContent = state.toUpperCase();
  statusBadge.className = 'status-badge ' + state;
}

function addEventLog(type: string, seq: number, data: unknown) {
  msgStore.logEvent({ seq, type, timestamp: Date.now(), data });

  const li = document.createElement('li');
  li.className = `log-entry log-${type}`;
  li.innerHTML = `<span class="log-seq">#${seq}</span> <span class="log-type">${type}</span>`;
  eventLogEl.prepend(li);

  // Cap log display to 50 entries
  while (eventLogEl.children.length > 50) {
    eventLogEl.removeChild(eventLogEl.lastChild!);
  }
}

function refreshUI() {
  responseEl.textContent = msgStore.getText();
  chunkCountEl.textContent = String(msgStore.count());
  if (conn) {
    cursorEl.textContent = String(conn.getLastSeq());
  }
}

// ─── Start a conversation + connect ───────────────────────────────────────────

async function startConversation(text: string) {
  // Create conversation if we don't have one
  if (!convId) {
    const res = await fetch(`${API}/conversations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const data = await res.json() as { conversation_id: string };
    convId = data.conversation_id;
    convIdEl.textContent = convId;
  }

  // Reset display for new message
  msgStore.clear();
  responseEl.textContent = '';
  runStateEl.textContent = 'running';
  runStateEl.className = 'run-state running';

  // Post the user message
  const msgId = `msg-${Date.now()}`;
  await fetch(`${API}/conversations/${convId}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message_id: msgId, text }),
  });

  // Close any existing connection
  conn?.disconnect();

  // Open SSE stream
  conn = new ConnectionManager(convId, {
    onEvent(event) {
      if (event.type === 'state_change') {
        updateStatus(event.state);

        if (event.state === 'completed') {
          runStateEl.textContent = 'completed';
          runStateEl.className = 'run-state completed';
          sendBtn.disabled = false;
          disconnectBtn.disabled = true;
        } else if (event.state === 'failed') {
          runStateEl.textContent = 'failed';
          runStateEl.className = 'run-state failed';
          sendBtn.disabled = false;
          disconnectBtn.disabled = true;
        } else if (event.state === 'interrupted') {
          runStateEl.textContent = 'interrupted';
          runStateEl.className = 'run-state interrupted';
          sendBtn.disabled = false;
          disconnectBtn.disabled = true;
        }
      } else if (event.type === 'sse_event') {
        const data = event.data as Record<string, unknown>;
        addEventLog(event.name, event.seq, data);

        if (event.name === 'chunk') {
          msgStore.addChunk(event.seq, String(data['text'] ?? ''));
          refreshUI();
        }
      }
    },
  });

  conn.connect();
  sendBtn.disabled = true;
  disconnectBtn.disabled = false;
  updateStatus('connected');
}

// ─── Event listeners ──────────────────────────────────────────────────────────

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  await startConversation(text);
});

disconnectBtn.addEventListener('click', () => {
  if (conn) {
    conn.simulateDisconnect();
    updateStatus('reconnecting');
  }
});

// ─── Init ─────────────────────────────────────────────────────────────────────
updateStatus('idle');
