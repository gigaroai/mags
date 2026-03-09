#!/usr/bin/env node
// services/meridian/index.js — Meridian memory service for Cortex
//
// Direct implementation — talks to Qdrant, Ollama, and SQLite directly.
// No pass-through to another REST service.
//
// Connects to Cortex as the "meridian" service peer.
// Agents send memory requests to "meridian" via Cortex message routing.
//
// Supported actions:
//   - recall:      semantic search across memories (Qdrant vector search)
//   - store:       create a new memory (embed + upsert to Qdrant)
//   - checkpoint:  read/write checkpoints (SQLite)
//   - decisions:   read recent decisions (SQLite)
//   - warnings:    read recent warnings (SQLite)
//   - status:      health check / stats
//
// Data stores:
//   - Qdrant (comms:6333) — vector search, memories_v2 collection
//   - Ollama (meridian-ollama:11434) — mxbai-embed-large embeddings (1024-dim, 512 token ctx)
//   - SQLite (/global/meridian/meridian.db) — checkpoints, decisions, warnings, entities
//
// Usage:
//   CORTEX_URL=ws://localhost:18100/cortex/ws \
//   MERIDIAN_TOKEN=<token> \
//   QDRANT_URL=http://comms:6333 \
//   OLLAMA_URL=http://meridian-ollama:11434 \
//   SQLITE_PATH=/global/meridian/meridian.db \
//   node index.js

const http = require('http');
const { CortexServiceClient } = require('../cortex-client');

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  cortexUrl:     process.env.CORTEX_URL      || 'ws://localhost:18100/cortex/ws',
  meridianToken: process.env.MERIDIAN_TOKEN   || 'meridian-CHANGEME',
  qdrantUrl:     process.env.QDRANT_URL       || 'http://comms:6333',
  ollamaUrl:     process.env.OLLAMA_URL       || 'http://meridian-ollama:11434',
  sqlitePath:    process.env.SQLITE_PATH      || '/global/meridian/meridian.db',
  collection:    process.env.QDRANT_COLLECTION || 'memories_v2',
  embedModel:    process.env.EMBED_MODEL      || 'mxbai-embed-large',
  embedDim:      1024,
  maxEmbedChars: 1500,  // ~400 tokens, stays under mxbai's 512 token ctx
};

// ─────────────────────────────────────────────────────────────────────────────
// SQLite (via better-sqlite3)
// ─────────────────────────────────────────────────────────────────────────────

let Database;
try {
  Database = require('better-sqlite3');
} catch (e) {
  console.error('[meridian] better-sqlite3 not installed. Run: npm install better-sqlite3');
  process.exit(1);
}

class SqliteStore {
  constructor(dbPath) {
    this.db = new Database(dbPath, { readonly: false });
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('busy_timeout = 5000');
  }

  getLatestCheckpoint(projectId = 'default') {
    return this.db.prepare(
      'SELECT * FROM checkpoints WHERE project_id = ? ORDER BY rowid DESC LIMIT 1'
    ).get(projectId) || null;
  }

  getCheckpoints(limit = 5, projectId = 'default') {
    return this.db.prepare(
      'SELECT * FROM checkpoints WHERE project_id = ? ORDER BY rowid DESC LIMIT ?'
    ).all(projectId, limit);
  }

  writeCheckpoint(cp) {
    const id = cp.id || _uuid();
    const now = new Date().toISOString().replace('Z', '');
    this.db.prepare(`
      INSERT INTO checkpoints (id, session_id, project_id, source, task_state, decisions, warnings, next_steps, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      cp.session_id || 'cortex-proxy',
      cp.project_id || 'default',
      cp.source || 'cortex',
      cp.task_state || '',
      JSON.stringify(cp.decisions || []),
      JSON.stringify(cp.warnings || []),
      JSON.stringify(cp.next_steps || []),
      cp.created_at || now
    );
    return { id, created_at: cp.created_at || now };
  }

  getDecisions(limit = 10) {
    return this.db.prepare(
      'SELECT * FROM decisions ORDER BY created_at DESC LIMIT ?'
    ).all(limit);
  }

  getWarnings(limit = 10) {
    return this.db.prepare(
      'SELECT * FROM warnings ORDER BY created_at DESC LIMIT ?'
    ).all(limit);
  }

  close() {
    this.db.close();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Qdrant client (HTTP)
// ─────────────────────────────────────────────────────────────────────────────

class QdrantStore {
  constructor(baseUrl, collection) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.collection = collection;
  }

  async search(vector, opts = {}) {
    const body = {
      vector,
      limit: opts.limit || 10,
      score_threshold: opts.threshold || 0.5,
      with_payload: true,
      with_vector: false,
    };
    if (opts.filter) body.filter = opts.filter;

    const resp = await this._post(
      `/collections/${this.collection}/points/search`, body
    );
    return resp.result || [];
  }

  async upsert(points) {
    return this._put(
      `/collections/${this.collection}/points`, { points }
    );
  }

  async scroll(opts = {}) {
    const body = {
      limit: opts.limit || 20,
      with_payload: true,
      with_vector: false,
    };
    if (opts.filter) body.filter = opts.filter;

    const resp = await this._post(
      `/collections/${this.collection}/points/scroll`, body
    );
    return resp.result || {};
  }

  async collectionInfo() {
    return this._get(`/collections/${this.collection}`);
  }

  _get(path) {
    return new Promise((resolve, reject) => {
      http.get(`${this.baseUrl}${path}`, { timeout: 10000 }, (res) => {
        let data = '';
        res.on('data', (c) => data += c);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch { resolve(data); }
        });
      }).on('error', reject);
    });
  }

  _post(path, body) { return this._req('POST', path, body); }
  _put(path, body) { return this._req('PUT', path, body); }

  _req(method, path, body) {
    return new Promise((resolve, reject) => {
      const url = new URL(`${this.baseUrl}${path}`);
      const payload = JSON.stringify(body);
      const req = http.request({
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method,
        timeout: 30000,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      }, (res) => {
        let data = '';
        res.on('data', (c) => data += c);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch { resolve(data); }
        });
      });
      req.on('error', reject);
      req.write(payload);
      req.end();
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ollama embeddings
// ─────────────────────────────────────────────────────────────────────────────

// Telegraphic compression for embeddings (EXP-008: 38-40% token savings, 0% info loss).
// Embedding models don't care about grammar — strip the fluff, keep the signal.
const STOP_WORDS = new Set([
  'the','a','an','is','was','were','are','be','been','being',
  'have','has','had','having','do','does','did','doing',
  'will','would','could','should','shall','might','may','can',
  'to','of','in','for','on','with','at','by','from','as',
  'into','through','during','before','after','above','below',
  'between','under','again','further','then','once',
  'it','its','this','that','these','those','there','here',
  'i','me','my','we','our','you','your','he','his','she','her','they','them','their',
  'am','just','very','really','actually','basically','essentially',
  'also','however','therefore','furthermore','moreover','additionally',
  'please','certainly','absolutely','definitely','unfortunately',
]);

function compressForEmbed(text) {
  // Phase 1: telegraphic compression — drop stop words, collapse whitespace
  let compressed = text
    .replace(/\n+/g, ' ')                         // collapse newlines
    .replace(/\s+/g, ' ')                          // collapse whitespace
    .replace(/["""'']/g, '')                        // drop quotes
    .trim();

  const words = compressed.split(' ');
  const filtered = words.filter(w => {
    const lower = w.toLowerCase().replace(/[.,;:!?()]/g, '');
    return !STOP_WORDS.has(lower) && lower.length > 0;
  });
  compressed = filtered.join(' ');

  // Phase 2: hard truncate if still over limit
  if (compressed.length > CONFIG.maxEmbedChars) {
    const truncated = compressed.slice(0, CONFIG.maxEmbedChars);
    const lastSpace = truncated.lastIndexOf(' ');
    compressed = lastSpace > CONFIG.maxEmbedChars * 0.8
      ? truncated.slice(0, lastSpace)
      : truncated;
  }

  return compressed;
}

async function embed(text, model = CONFIG.embedModel) {
  const safeText = compressForEmbed(text);
  return new Promise((resolve, reject) => {
    const url = new URL(`${CONFIG.ollamaUrl}/api/embed`);
    const payload = JSON.stringify({ model, input: safeText });
    const req = http.request({
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: 'POST',
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
      },
    }, (res) => {
      let data = '';
      res.on('data', (c) => data += c);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.embeddings && parsed.embeddings.length > 0) {
            resolve(parsed.embeddings[0]);
          } else {
            reject(new Error(`No embeddings in response: ${data.slice(0, 200)}`));
          }
        } catch (e) {
          reject(new Error(`Failed to parse embed response: ${e.message}`));
        }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Utility
// ─────────────────────────────────────────────────────────────────────────────

function _uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Meridian Service
// ─────────────────────────────────────────────────────────────────────────────

class MeridianService {
  constructor() {
    this.sqlite = new SqliteStore(CONFIG.sqlitePath);
    this.qdrant = new QdrantStore(CONFIG.qdrantUrl, CONFIG.collection);

    this.cortex = new CortexServiceClient({
      url: CONFIG.cortexUrl,
      token: CONFIG.meridianToken,
      peerId: 'meridian',
      capabilities: ['memory', 'recall', 'store', 'checkpoint', 'embeddings'],
      metadata: {
        embedding_model: CONFIG.embedModel,
        embedding_dim: CONFIG.embedDim,
        storage: 'qdrant + sqlite',
        qdrant_collection: CONFIG.collection,
        sqlite_path: CONFIG.sqlitePath,
      },
      onMessage: (msg) => this._onMessage(msg),
      onReady: () => this._onReady(),
    });

    this.requestCount = 0;
    this.errorCount = 0;
    this.startTime = Date.now();
  }

  async start() {
    console.log('================================================================');
    console.log('  MERIDIAN — Memory service for Cortex (direct implementation)');
    console.log('');
    console.log(`  Cortex:   ${CONFIG.cortexUrl}`);
    console.log(`  Qdrant:   ${CONFIG.qdrantUrl} (${CONFIG.collection})`);
    console.log(`  Ollama:   ${CONFIG.ollamaUrl} (${CONFIG.embedModel})`);
    console.log(`  SQLite:   ${CONFIG.sqlitePath}`);
    console.log('================================================================\n');

    // Health checks
    try {
      const info = await this.qdrant.collectionInfo();
      const count = info.result?.points_count ?? '?';
      console.log(`[meridian] Qdrant: ${count} points in ${CONFIG.collection}`);
    } catch (e) {
      console.warn(`[meridian] Qdrant health check failed: ${e.message}`);
    }

    try {
      const cp = this.sqlite.getLatestCheckpoint();
      console.log(`[meridian] SQLite: latest checkpoint ${cp?.created_at ?? 'none'}`);
    } catch (e) {
      console.warn(`[meridian] SQLite health check failed: ${e.message}`);
    }

    this.cortex.connect();
  }

  stop() {
    console.log('\n[meridian] Shutting down...');
    this.cortex.reportState('shutting_down');
    this.sqlite.close();
    setTimeout(() => {
      this.cortex.disconnect();
      process.exit(0);
    }, 1000);
  }

  _onReady() {
    console.log('[meridian] Cortex connected — service active');
    this.cortex.reportState('idle');
  }

  async _onMessage(msg) {
    if (msg.type !== 'message') return;

    const payload = msg.payload || {};
    const action = payload.action;
    const requestId = payload.request_id || `mrd-${Date.now()}`;

    this.requestCount++;
    this.cortex.reportState('busy', `${action} from ${msg.from}`);

    try {
      let result;

      switch (action) {
        case 'recall':     result = await this._recall(payload, msg.from); break;
        case 'store':      result = await this._store(payload, msg.from); break;
        case 'checkpoint': result = await this._checkpoint(payload, msg.from); break;
        case 'decisions':  result = this._decisions(payload); break;
        case 'warnings':   result = this._warnings(payload); break;
        case 'status':     result = await this._status(); break;
        default:
          result = {
            error: `Unknown action: ${action}. Available: recall, store, checkpoint, decisions, warnings, status`,
          };
      }

      this.cortex.sendTo(msg.from, {
        type: 'meridian_response',
        request_id: requestId,
        action,
        ...result,
      });
    } catch (err) {
      this.errorCount++;
      console.error(`[meridian] Error handling ${action}:`, err.message);
      this.cortex.sendTo(msg.from, {
        type: 'meridian_error',
        request_id: requestId,
        action,
        error: err.message,
      });
    }

    this.cortex.reportState('idle');
  }

  // ── recall: semantic vector search ────────────────────────────────────

  async _recall(payload, from) {
    const query = payload.query || payload.text;
    if (!query) return { error: 'recall requires "query" field' };

    console.log(`[meridian] Recall from ${from}: "${query.slice(0, 80)}"`);

    const vector = await embed(query);

    let filter;
    if (payload.source || payload.tags || payload.type) {
      const must = [];
      if (payload.source) must.push({ key: 'source', match: { value: payload.source } });
      if (payload.type) must.push({ key: 'type', match: { value: payload.type } });
      if (payload.tags && payload.tags.length > 0) must.push({ key: 'tags', match: { any: payload.tags } });
      if (must.length > 0) filter = { must };
    }

    const results = await this.qdrant.search(vector, {
      limit: payload.limit || 10,
      threshold: payload.threshold || 0.5,
      filter,
    });

    console.log(`[meridian] -> ${results.length} results`);

    return {
      memories: results.map(r => ({
        id: r.id,
        score: r.score,
        content: r.payload?.content,
        type: r.payload?.type,
        source: r.payload?.source,
        tags: r.payload?.tags,
        importance: r.payload?.importance,
        created_at: r.payload?.created_at,
      })),
      count: results.length,
    };
  }

  // ── store: embed + upsert to Qdrant ───────────────────────────────────

  async _store(payload, from) {
    const content = payload.content || payload.text;
    if (!content) return { error: 'store requires "content" field' };

    console.log(`[meridian] Store from ${from}: "${content.slice(0, 80)}"`);

    const vector = await embed(content);
    const id = payload.id || `mem_${_uuid().slice(0, 12)}`;
    const now = new Date().toISOString();

    const point = {
      id,
      vector,
      payload: {
        content,
        type: payload.type || 'note',
        source: payload.source || from,
        tags: payload.tags || [],
        importance: payload.importance || 3,
        created_at: now,
        updated_at: now,
      },
    };

    await this.qdrant.upsert([point]);
    console.log(`[meridian] -> stored ${id}`);

    return { stored: true, id };
  }

  // ── checkpoint: read/write checkpoints ────────────────────────────────

  async _checkpoint(payload, from) {
    if (payload.write) {
      console.log(`[meridian] Checkpoint write from ${from}`);
      const result = this.sqlite.writeCheckpoint(payload.write);
      return { written: true, ...result };
    }

    const limit = payload.limit || 1;
    if (limit === 1) {
      const cp = this.sqlite.getLatestCheckpoint(payload.project_id);
      return { checkpoint: cp };
    }
    const cps = this.sqlite.getCheckpoints(limit, payload.project_id);
    return { checkpoints: cps, count: cps.length };
  }

  // ── decisions: read from SQLite ───────────────────────────────────────

  _decisions(payload) {
    const limit = payload.limit || 10;
    const decisions = this.sqlite.getDecisions(limit);
    return { decisions, count: decisions.length };
  }

  // ── warnings: read from SQLite ────────────────────────────────────────

  _warnings(payload) {
    const limit = payload.limit || 10;
    const warnings = this.sqlite.getWarnings(limit);
    return { warnings, count: warnings.length };
  }

  // ── status: health check ──────────────────────────────────────────────

  async _status() {
    const checks = {};

    try {
      const info = await this.qdrant.collectionInfo();
      checks.qdrant = { ok: true, points: info.result?.points_count, collection: CONFIG.collection };
    } catch (e) {
      checks.qdrant = { ok: false, error: e.message };
    }

    try {
      const cp = this.sqlite.getLatestCheckpoint();
      checks.sqlite = { ok: true, latest_checkpoint: cp?.created_at, path: CONFIG.sqlitePath };
    } catch (e) {
      checks.sqlite = { ok: false, error: e.message };
    }

    try {
      const test = await embed('health check');
      checks.ollama = { ok: test.length === CONFIG.embedDim, model: CONFIG.embedModel, dim: test.length };
    } catch (e) {
      checks.ollama = { ok: false, error: e.message };
    }

    return {
      healthy: checks.qdrant?.ok && checks.sqlite?.ok && checks.ollama?.ok,
      ...checks,
      proxy: { requests: this.requestCount, errors: this.errorCount, uptime_s: Math.round((Date.now() - this.startTime) / 1000) },
    };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

const service = new MeridianService();
service.start().catch(err => { console.error(err); process.exit(1); });

process.on('SIGINT', () => service.stop());
process.on('SIGTERM', () => service.stop());
