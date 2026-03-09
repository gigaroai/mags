#!/usr/bin/env node
// overwatch/index.js — Claude Code × Cortex Bridge (tmux-native)
// Conforms to: AGENT-PROTOCOL.md (2026-03-09)
//
// Runs Claude Code in a tmux session. Overwatch is a sidecar that:
//   - Reads CC output via tmux pipe-pane → FIFO
//   - Injects commands via tmux send-keys
//   - Detects idle/busy from CC's prompt pattern
//   - Connects to Cortex for remote command routing
//
// The human can `tmux attach -t cc` at any time to watch, interact,
// or bitch-slap the agent directly. Overwatch stays out of the way.
//
// Usage:
//   CORTEX_URL=wss://gigaro.ai/cortex/ws \
//   OVERWATCH_TOKEN=overwatch-alpha-CHANGEME \
//   node index.js
//
// Then:  tmux attach -t cc

const { execSync, spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const readline = require('readline');
const WebSocket = require('ws');

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  cortexUrl:       process.env.CORTEX_URL || 'wss://gigaro.ai/cortex/ws',
  token:           process.env.OVERWATCH_TOKEN || '',
  agentId:         process.env.AGENT_ID || `overwatch-${os.hostname()}`,

  tmuxSession:     process.env.CC_SESSION || 'cc',
  ccBin:           process.env.CC_BIN || 'claude',
  ccWorkDir:       process.env.CC_WORK_DIR || process.cwd(),
  ccResume:        process.env.CC_RESUME || null,
  ccModel:         process.env.CC_MODEL || null,
  ccSystemPrompt:  process.env.CC_SYSTEM_PROMPT || null,
  ccAllowedTools:  process.env.CC_ALLOWED_TOOLS || null,

  pipeDir:         process.env.PIPE_DIR || '/tmp/overwatch',
  pipePath:        null,  // set below

  // ── Prompt detection (idle) ──────────────────────────────────────────
  // CC shows ❯ or > when waiting for input
  promptPatterns: [
    /[❯>]\s*$/m,
    /\$\s*$/m,
    /^claude[^>]*>\s*$/m,
  ],

  // ── Spinner detection (alive/busy) ───────────────────────────────────
  // CC's busy spinner uses these characters (confirmed):
  //   · ✢ * ✶ ✻ ✽ ✻ ✶ * ✢ ·
  // If we see them rotating in capture-pane, CC is alive and working.
  // If they stop but no prompt appears, CC might be wedged.
  spinnerChars: '·✢*✶✻✽',  // unique chars from the cycle (deduplicated)
  spinnerPatterns: [
    /[·✢✶✻✽]/,                          // actual CC spinner frames
    /\*/,                                 // asterisk (also in the cycle)
    /<agent-status:\s*'[^']*'>/,          // custom overwatch status frames (future)
  ],

  // ── Timing ───────────────────────────────────────────────────────────
  pollMs:          2000,      // v3#8 FIX: 2s to avoid execSync blocking event loop
  idleDebounceMs:  2000,      // output silence before we check for prompt
  spinnerPollMs:   2000,      // how often to sample spinner for rotation

  // ── Stale detection (escalate, NEVER kill) ───────────────────────────
  // "Stale" = busy, no spinner rotation, no output, no prompt
  staleThresholdMs:  parseInt(process.env.STALE_THRESHOLD_MS || '900000', 10),  // 15 min default
  staleCheckMs:      30_000,  // check for stale every 30s
  // We NEVER auto-kill. Only !urgent or manual abort can interrupt busy.

  reconnectBaseMs:  2_000,
  reconnectMaxMs:   60_000,
  reconnectBackoff: 1.5,
  heartbeatMs:      10_000,

  // v3#2: Authorized peers for control actions (comma-separated)
  authorizedPeers: process.env.AUTHORIZED_PEERS || 'giga,sandy,webbie',
};

CONFIG.pipePath = path.join(CONFIG.pipeDir, `${CONFIG.tmuxSession}.pipe`);

// ── Context threshold warnings (Feature 2) ──────────────────────────────────
const CTX_THRESHOLDS = [
  { pct: 70,  msg: 'CTX 70% — start wrapping up' },
  { pct: 80,  msg: 'CTX 80% — checkpoint now' },
  { pct: 85,  msg: 'CTX 85% — checkpoint and finish. /compact /reset /rewind' },
  { pct: 95,  msg: 'CTX 95% — STOP. /compact or /reset NOW' },
  { pct: 99,  msg: 'CTX 99% — CRITICAL. /compact or /reset IMMEDIATELY' },
];

// ── Self-management commands (Feature 5) ────────────────────────────────────
const SELF_COMMANDS = ['/compact', '/reset', '/rewind'];

// ─────────────────────────────────────────────────────────────────────────────
// Tmux + Claude Code Manager
// ─────────────────────────────────────────────────────────────────────────────

class TmuxCC {
  constructor(opts = {}) {
    this.state = 'stopped';     // stopped | starting | idle | busy | stale
    this.lastOutputTime = 0;
    this.outputBuffer = [];
    this.maxBufferLines = 500;
    this.pipeProc = null;
    this.pipeReader = null;
    this.pollTimer = null;
    this.staleTimer = null;

    // Spinner liveness tracking
    this.lastSpinnerSnapshot = '';   // last captured spinner frame
    this.lastSpinnerChangeTime = 0; // when the spinner frame last changed
    this.spinnerAlive = false;       // true if spinner is rotating

    // Context % tracking (Feature 1 + 2)
    this.ctxPercent = null;
    this.ctxWarningsFired = new Set();

    this.onStateChange = opts.onStateChange || (() => {});
    this.onOutput = opts.onOutput || (() => {});
    this.onStale = opts.onStale || (() => {});  // escalation callback
    this.onCtxWarning = opts.onCtxWarning || null;  // context threshold callback

    this.queue = [];
    this.processing = false;
    this.currentRequest = null;
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────

  async start() {
    this._setState('starting');

    fs.mkdirSync(CONFIG.pipeDir, { recursive: true });

    // Create FIFO
    if (fs.existsSync(CONFIG.pipePath)) {
      try { fs.unlinkSync(CONFIG.pipePath); } catch {}
    }
    execSync(`mkfifo "${CONFIG.pipePath}"`);
    console.log(`[tmux] FIFO: ${CONFIG.pipePath}`);

    // Check for existing tmux session
    if (this._sessionExists()) {
      console.log(`[tmux] Session '${CONFIG.tmuxSession}' exists — attaching pipe`);
    } else {
      // v3#6 FIX: Build args array, use execFileSync to avoid shell injection
      const ccArgs = [];
      if (CONFIG.ccResume)       ccArgs.push('--resume', CONFIG.ccResume);
      if (CONFIG.ccModel)        ccArgs.push('--model', CONFIG.ccModel);
      if (CONFIG.ccSystemPrompt) ccArgs.push('--system-prompt', CONFIG.ccSystemPrompt);
      if (CONFIG.ccAllowedTools) ccArgs.push('--allowedTools', CONFIG.ccAllowedTools);

      const ccCmd = [CONFIG.ccBin, ...ccArgs].join(' ');
      console.log(`[tmux] Creating session: ${ccCmd}`);

      const { execFileSync } = require('child_process');
      execFileSync('tmux', [
        'new-session', '-d',
        '-s', CONFIG.tmuxSession,
        '-c', CONFIG.ccWorkDir,
        CONFIG.ccBin, ...ccArgs,
      ], { stdio: 'ignore' });
    }

    // Attach pipe-pane (-O = output only)
    execSync(
      `tmux pipe-pane -t "${CONFIG.tmuxSession}" -O "cat >> ${CONFIG.pipePath}"`,
      { stdio: 'ignore' }
    );

    this._startPipeReader();
    this._startPolling();

    // Initial settle time
    setTimeout(() => {
      if (this.state === 'starting') this._setState('idle');
    }, 5000);
  }

  stop() {
    this._stopPipeReader();
    this._stopPolling();
    try { execSync(`tmux pipe-pane -t "${CONFIG.tmuxSession}"`, { stdio: 'ignore' }); } catch {}
    try { fs.unlinkSync(CONFIG.pipePath); } catch {}
    this._setState('stopped');
    console.log(`[tmux] Detached. Session '${CONFIG.tmuxSession}' still running.`);
  }

  // ── Command queue ─────────────────────────────────────────────────────

  async sendCommand(prompt, requestId) {
    return new Promise((resolve, reject) => {
      this.queue.push({ prompt, requestId, resolve, reject });
      this._drain();
    });
  }

  get queueDepth() { return this.queue.length; }

  abort() {
    console.log('[tmux] Ctrl+C (manual/!urgent abort)');
    execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });

    if (this.currentRequest) {
      const req = this.currentRequest;
      this.currentRequest = null;
      this.processing = false;
      req.resolve({
        requestId: req.requestId,
        status: 'aborted',
        text: this._outputSince(req.outputStart),
        elapsed_ms: Date.now() - req.startTime,
      });
      this._setState('idle');
      this._drain();
    }
  }

  async _drain() {
    if (this.processing || this.queue.length === 0) return;
    if (this.state !== 'idle' && this.state !== 'starting') return;

    this.processing = true;
    const { prompt, requestId, resolve, reject } = this.queue.shift();

    console.log(`[cc] >> ${requestId}: ${prompt.slice(0, 120)}${prompt.length > 120 ? '...' : ''}`);

    const outputStart = this.outputBuffer.length;
    this.currentRequest = { requestId, resolve, reject, outputStart, startTime: Date.now() };
    this._setState('busy');

    // Reset spinner tracking for this run
    this.lastSpinnerSnapshot = '';
    this.lastSpinnerChangeTime = Date.now();
    this.spinnerAlive = false;

    // Inject via _sendKeys
    try {
      this._sendKeys(prompt);
    } catch (err) {
      console.error(`[tmux] send-keys failed:`, err.message);
      this.currentRequest = null;
      this.processing = false;
      reject(err);
      this._setState('idle');
      this._drain();
    }

    // NO auto-kill timeout. Only !urgent or manual abort can interrupt.
    // Stale detection handles the wedge case via escalation.
  }

  // ── Pipe reader ───────────────────────────────────────────────────────

  _startPipeReader() {
    this.pipeProc = spawn('tail', ['-f', CONFIG.pipePath], {
      stdio: ['ignore', 'pipe', 'ignore'],
    });

    // v3#5 FIX: Restart pipe reader if it dies
    this.pipeProc.on('exit', (code) => {
      console.log(`[tmux] Pipe reader exited (code=${code}) — restarting in 1s`);
      this.pipeProc = null;
      setTimeout(() => {
        if (this.state !== 'stopped') this._startPipeReader();
      }, 1000);
    });

    this.pipeReader = readline.createInterface({ input: this.pipeProc.stdout });
    this.pipeReader.on('line', (line) => {
      this.lastOutputTime = Date.now();
      this.outputBuffer.push({ ts: Date.now(), text: line });

      if (this.outputBuffer.length > this.maxBufferLines) {
        this.outputBuffer = this.outputBuffer.slice(-this.maxBufferLines);
      }

      this.onOutput(line);
    });
  }

  _stopPipeReader() {
    if (this.pipeReader) { this.pipeReader.close(); this.pipeReader = null; }
    if (this.pipeProc) { this.pipeProc.kill(); this.pipeProc = null; }
  }

  // ── Unified polling: idle detection + spinner liveness + stale check ──

  _startPolling() {
    this.pollTimer = setInterval(() => this._poll(), CONFIG.pollMs);
  }

  _stopPolling() {
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
  }

  _poll() {
    const now = Date.now();
    const bottom = this._captureBottom(5);

    // Feature 1: Scrape context % from status line on every poll
    const parsed = this._parseContextPercent(bottom);
    if (parsed !== null) this.ctxPercent = parsed;

    // Feature 2: Check thresholds when idle
    this._checkCtxThresholds();

    if (this.state === 'busy' || this.state === 'stale') {
      // ── Spinner liveness check ──────────────────────────────────────
      this._checkSpinner(bottom, now);

      // ── Prompt detection (busy → idle) ──────────────────────────────
      // Only check if output has settled
      if (now - this.lastOutputTime >= CONFIG.idleDebounceMs) {
        const recent = this._recentLines(5);
        const isPrompt = CONFIG.promptPatterns.some(p => p.test(bottom) || p.test(recent));
        if (isPrompt) {
          this._resolveCurrentRequest();
          return;
        }
      }

      // ── Stale detection (busy → stale, escalate) ───────────────────
      // No spinner rotation + no output for a long time = possibly wedged
      if (!this.spinnerAlive && this.state === 'busy') {
        const silenceDuration = now - Math.max(this.lastOutputTime, this.lastSpinnerChangeTime);
        if (silenceDuration >= CONFIG.staleThresholdMs) {
          this._goStale();
        }
      }

      // If we were stale but spinner started again, recover
      if (this.spinnerAlive && this.state === 'stale') {
        console.log(`[cc] Spinner alive again — recovering from stale`);
        this._setState('busy');
      }

    } else if (this.state === 'idle') {
      // Nothing to do while idle — nudger handles this from Cortex side
    }
  }

  // ── Context % scraping (Feature 1) ──────────────────────────────────

  _parseContextPercent(paneContent) {
    const patterns = [
      /(\d+)%\s*(?:context|ctx)/i,           // "85% context"
      /context[:\s]+(\d+)%/i,                // "context: 85%"
      /(\d+(?:\.\d+)?)%\s*(?:of\s+)?(?:\d+[KkMm])/,  // "85% of 200K"
      /(\d+)%/,                               // bare percentage in status line
    ];
    for (const pat of patterns) {
      const match = paneContent.match(pat);
      if (match) return parseInt(match[1], 10);
    }
    return null;
  }

  // ── Context threshold warnings (Feature 2) ─────────────────────────

  _checkCtxThresholds() {
    if (this.ctxPercent === null) return;
    if (this.state !== 'idle') return; // don't inject while busy

    for (const t of CTX_THRESHOLDS) {
      if (this.ctxPercent >= t.pct && !this.ctxWarningsFired.has(t.pct)) {
        this.ctxWarningsFired.add(t.pct);
        // Injection moved to Overwatch._onCtxWarning to access _isDuplicate + _contextPrefix
        this.onCtxWarning && this.onCtxWarning(t.pct, t.msg);
      }
    }
  }

  // ── Spinner frame detection ───────────────────────────────────────────

  _checkSpinner(paneContent, now) {
    // Extract spinner characters from the pane
    let currentFrame = '';

    for (const pattern of CONFIG.spinnerPatterns) {
      const match = paneContent.match(pattern);
      if (match) {
        currentFrame = match[0];
        break;
      }
    }

    if (currentFrame === '') {
      // No spinner visible — might be between frames or in output mode
      // Don't immediately mark dead, wait for stale threshold
      this.spinnerAlive = false;
      return;
    }

    // Spinner frame found — is it rotating?
    if (currentFrame !== this.lastSpinnerSnapshot) {
      // Frame changed — CC is alive and working!
      this.lastSpinnerSnapshot = currentFrame;
      this.lastSpinnerChangeTime = now;
      this.spinnerAlive = true;
    } else {
      // Same frame — could be stuck, or poll just caught the same frame
      // Give it some grace (check if it's been the same for a while)
      const frameDuration = now - this.lastSpinnerChangeTime;
      if (frameDuration > CONFIG.spinnerPollMs * 5) {
        // Same frame for ~10s — likely stuck
        this.spinnerAlive = false;
      }
      // Otherwise still consider alive (normal frame timing)
    }
  }

  // ── Stale handling (escalate, never kill) ─────────────────────────────

  _goStale() {
    if (this.state === 'stale') return;  // already stale, don't re-fire

    const silenceMins = Math.round((Date.now() - this.lastOutputTime) / 60_000);
    console.log(`[cc] ⚠ STALE — busy but silent for ${silenceMins}m, no spinner rotation`);

    this._setState('stale');

    // Escalate via callback — Overwatch will notify Cortex + crew
    this.onStale({
      agent_id: CONFIG.agentId,
      silent_minutes: silenceMins,
      last_output: this._recentLines(3),
      pane_bottom: this._captureBottom(5),
    });

    // DO NOT ABORT. Only human/!urgent can do that.
  }

  _resolveCurrentRequest() {
    if (!this.currentRequest) return;

    const req = this.currentRequest;
    this.currentRequest = null;
    this.processing = false;

    const elapsed = Date.now() - req.startTime;
    const output = this._outputSince(req.outputStart);

    console.log(`[cc] << ${req.requestId} (${elapsed}ms, ${output.length} chars)`);

    req.resolve({
      requestId: req.requestId,
      status: 'complete',
      text: output,
      elapsed_ms: elapsed,
    });

    this._setState('idle');
    this._drain();
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  // FIX v2#7: No try/catch here — let errors propagate to callers who need them (_drain)
  _sendKeys(text) {
    execSync(
      `tmux send-keys -t "${CONFIG.tmuxSession}" -l ${this._esc(text)}`,
      { stdio: 'ignore' }
    );
    execSync(
      `tmux send-keys -t "${CONFIG.tmuxSession}" Enter`,
      { stdio: 'ignore' }
    );
  }

  _captureBottom(lines = 5) {
    try {
      return execSync(
        `tmux capture-pane -t "${CONFIG.tmuxSession}" -p -S -${lines}`,
        { encoding: 'utf8', timeout: 2000 }
      ).trim();
    } catch { return ''; }
  }

  _recentLines(n) {
    return this.outputBuffer.slice(-n).map(l => l.text).join('\n');
  }

  _outputSince(idx) {
    return this.outputBuffer.slice(idx).map(l => l.text).join('\n');
  }

  _sessionExists() {
    try { execSync(`tmux has-session -t "${CONFIG.tmuxSession}" 2>/dev/null`); return true; }
    catch { return false; }
  }

  _esc(str) {
    return "'" + str.replace(/'/g, "'\\''") + "'";
  }

  _setState(s) {
    if (this.state !== s) {
      const prev = this.state;
      this.state = s;
      console.log(`[cc] ${prev} → ${s}`);
      this.onStateChange(s, prev);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cortex WebSocket client
// ─────────────────────────────────────────────────────────────────────────────

class CortexClient {
  constructor(opts = {}) {
    this.ws = null;
    this.sessionId = null;
    this.reconnectMs = CONFIG.reconnectBaseMs;
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.intentionalClose = false;
    this.onMessage = opts.onMessage || (() => {});
    this.onReady = opts.onReady || (() => {});
  }

  connect() {
    if (this.ws && this.ws.readyState <= WebSocket.CONNECTING) return;
    // FIX: Kill old socket to prevent reconnect cascade from stale close events
    if (this.ws) {
      this.ws.removeAllListeners();
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this.intentionalClose = false;
    console.log(`[cortex] Connecting to ${CONFIG.cortexUrl}...`);

    this.ws = new WebSocket(CONFIG.cortexUrl, {
      rejectUnauthorized: process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0',
    });

    this.ws.on('open', () => {
      this.reconnectMs = CONFIG.reconnectBaseMs;
      this._send({
        type: 'auth',
        token: CONFIG.token,
        peer_id: CONFIG.agentId,
        peer_kind: 'agent',
        capabilities: ['claude-code', 'shell', 'file-ops', 'code-review', 'mcp'],
        metadata: {
          hostname: os.hostname(),
          work_dir: CONFIG.ccWorkDir,
          model: CONFIG.ccModel || 'default',
          tmux_session: CONFIG.tmuxSession,
        },
      });
    });

    this.ws.on('message', (data) => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch { return; }
      switch (msg.type) {
        case 'auth_ok':
          this.sessionId = msg.session_id;
          console.log(`[cortex] Authenticated: session=${this.sessionId}`);
          this._startHeartbeat();
          this.onReady();
          break;
        case 'auth_fail':
          console.error(`[cortex] Auth failed: ${msg.reason}`);
          this.intentionalClose = true;
          this.ws.close();
          break;
        case 'ping':
          this._send({ type: 'pong', ts: Date.now() });
          break;
        default:
          this.onMessage(msg);
      }
    });

    this.ws.on('close', (code) => {
      console.log(`[cortex] Disconnected (${code})`);
      this._cleanup();
      if (!this.intentionalClose) this._reconnect();
    });

    this.ws.on('error', (err) => console.error(`[cortex] ${err.message}`));
  }

  disconnect() {
    this.intentionalClose = true;
    if (this.ws) this.ws.close(1000);
    this._cleanup();
  }

  sendTo(to, payload) { this._send({ type: 'message', to, payload }); }
  broadcast(payload)   { this._send({ type: 'broadcast', payload }); }
  reportState(s, d)    { this._send({ type: 'status', state: s, detail: d }); }

  _send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ ...msg, ts: Date.now() }));
    }
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      this._send({
        type: 'heartbeat',
        peer_id: CONFIG.agentId,
        ctx_pct: this._getCtxPct ? this._getCtxPct() : null,
        state: this._getCCState ? this._getCCState() : null,
      });
    }, CONFIG.heartbeatMs);
  }

  _stopHeartbeat() { if (this.heartbeatTimer) clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }

  _reconnect() {
    console.log(`[cortex] Retry in ${Math.round(this.reconnectMs / 1000)}s`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectMs = Math.min(this.reconnectMs * CONFIG.reconnectBackoff, CONFIG.reconnectMaxMs);
      this.connect();
    }, this.reconnectMs);
  }

  _cleanup() {
    this._stopHeartbeat();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.sessionId = null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overwatch — bridges Cortex ↔ tmux CC
// ─────────────────────────────────────────────────────────────────────────────

class Overwatch {
  constructor() {
    this.cc = new TmuxCC({
      onStateChange: (s, prev) => {
        this.cortex.reportState(s, this.cc.ctxPercent !== null ? `ctx:${this.cc.ctxPercent}%` : undefined);
        // Protocol §2+4: deliver queued messages when transitioning to idle
        if (s === 'idle' && prev !== 'idle') this._deliverQueued();
      },
      onOutput:      (l) => this._onCCOutput(l),
      onStale:       (info) => this._onCCStale(info),
      onCtxWarning:  (pct, msg) => this._onCtxWarning(pct, msg),
    });

    this.cortex = new CortexClient({
      onMessage: (m) => this._onCortexMessage(m),
      onReady:   ()  => this.cortex.reportState(this.cc.state),
    });

    // Wire heartbeat to pull ctx% and state from CC (Feature 3)
    this.cortex._getCtxPct = () => this.cc.ctxPercent;
    this.cortex._getCCState = () => this.cc.state;

    this.streamSubs = new Map();
    this.awaitingNudgeResponse = false;  // true after we inject a nudge
    this.nudgeOutputStart = 0;           // buffer index when nudge was injected
    this.nudgeResponseTimeout = null;    // FIX v2#9: timeout to clear stale flag

    // Dedup: prevent same text injected twice within window
    this.recentInjections = [];  // [{text, ts}]
    this.DEDUP_WINDOW_MS = 30000;  // 30 second dedup window
    this.DEDUP_MAX_HISTORY = 50;

    // Protocol §4: Message queue with priority
    this.pendingMessages = [];  // [{text, priority:'normal'|'mention'|'urgent', from, ts}]

    // Restart throttle state
    this.lastRestartTime = 0;
    this.restartCount = 0;
    this.RESTART_COOLDOWN_MS = 60000;   // 1 min between restarts
    this.MAX_RESTARTS = 3;              // max 3 per 10 min window
    this.restartWindowStart = 0;

    // Freeze state
    this.frozen = false;

    // Storm detection (8 injections in 10s = storm)
    this.injectionTimestamps = [];
    this.STORM_THRESHOLD = 8;
    this.STORM_WINDOW_MS = 10000;
    this.stormActive = false;
  }

  // ── Feature 4: Time + context prefix for all injections ──────────────

  _contextPrefix() {
    const now = new Date();
    const time = now.toISOString().slice(11, 19);
    const ctx = this.cc.ctxPercent !== null ? `ctx:${this.cc.ctxPercent}%` : 'ctx:?';
    return `[${time} ${ctx}]`;
  }

  _isDuplicate(text) {
    const now = Date.now();
    this.recentInjections = this.recentInjections.filter(e => now - e.ts < this.DEDUP_WINDOW_MS);

    // Strip time/ctx prefix for comparison — [HH:MM:SS ctx:N%] varies each call
    const stripped = text.replace(/^\[\d{2}:\d{2}:\d{2}\s+ctx:\d+%\]\s*/, '');

    const dominated = this.recentInjections.some(e => {
      const eStripped = e.text.replace(/^\[\d{2}:\d{2}:\d{2}\s+ctx:\d+%\]\s*/, '');
      return eStripped === stripped;
    });

    if (dominated) {
      console.log('[dedup] Blocked duplicate injection');
      return true;
    }
    this.recentInjections.push({ text, ts: now });
    if (this.recentInjections.length > this.DEDUP_MAX_HISTORY) {
      this.recentInjections.shift();
    }
    return false;
  }

  // ── Storm detection (8/10s) ─────────────────────────────────────

  _detectStorm() {
    const now = Date.now();
    this.injectionTimestamps = this.injectionTimestamps.filter(t => now - t < this.STORM_WINDOW_MS);
    if (this.injectionTimestamps.length > this.STORM_THRESHOLD && !this.stormActive) {
      this.stormActive = true;
      console.log('[storm] Message storm detected — pausing injections');
      this.cortex.broadcast({
        type: 'storm_alert',
        agent_id: CONFIG.agentId,
        message: `Storm on ${CONFIG.agentId} — ${this.injectionTimestamps.length} injections in ${this.STORM_WINDOW_MS / 1000}s. Pausing.`,
      });
    }
  }

  // ── Frozen/storm guard for all injections ─────────────────────────

  _canInject() {
    if (this.frozen) {
      console.log('[inject] Blocked — frozen');
      return false;
    }
    if (this.stormActive) {
      console.log('[storm] Blocked — storm active');
      return false;
    }
    this.injectionTimestamps.push(Date.now());
    this._detectStorm();
    if (this.stormActive) return false;
    return true;
  }

  // ── Protocol §4: @mention and !urgent detection ───────────────────

  _extractPriority(text, from) {
    if (typeof text === 'string' && text.startsWith('!urgent')) return 'urgent';
    const agentName = CONFIG.agentId.replace(/^overwatch-/, '');
    if (typeof text === 'string' && text.includes(`@${agentName}`)) return 'mention';
    return 'normal';
  }

  // ── Protocol §4: Queue or deliver based on state + priority ───────

  _queueOrDeliver(text, from) {
    const priority = this._extractPriority(text, from);

    // !urgent bypasses freeze — abort current work, deliver immediately
    if (priority === 'urgent') {
      console.log(`[urgent] !urgent from ${from} — aborting + delivering`);
      if (this.cc.state === 'busy' || this.cc.state === 'stale') {
        this.cc.abort();
      }
      const prefix = this._contextPrefix();
      const injection = `${prefix} !urgent ${from}: ${text.replace(/^!urgent\s*/, '')}`;
      if (!this._isDuplicate(injection)) {
        this.cc._sendKeys(injection);
      }
      return;
    }

    // Freeze/storm guard (non-urgent only)
    if (!this._canInject()) return;

    // If idle, deliver immediately
    if (this.cc.state === 'idle' || this.cc.state === 'starting') {
      const prefix = this._contextPrefix();
      const injection = `${prefix} ${from}: ${text}`;
      if (!this._isDuplicate(injection)) {
        this.cc._sendKeys(injection);
      }
      return;
    }

    // Busy: queue it. @mention goes to front.
    const entry = { text, priority, from, ts: Date.now() };
    if (priority === 'mention') {
      this.pendingMessages.unshift(entry);
      console.log(`[queue] @mention from ${from} → front (depth: ${this.pendingMessages.length})`);
    } else {
      this.pendingMessages.push(entry);
      console.log(`[queue] msg from ${from} queued (depth: ${this.pendingMessages.length})`);
    }
  }

  // ── Protocol §2: Deliver queued messages as short summary on idle ──

  _deliverQueued() {
    if (this.pendingMessages.length === 0) return;
    if (this.cc.state !== 'idle' && this.cc.state !== 'starting') return;
    if (!this._canInject()) return;

    const msgs = this.pendingMessages;
    this.pendingMessages = [];

    // Count by sender, flag mentions
    const bySender = {};
    let mentionCount = 0;
    for (const m of msgs) {
      bySender[m.from] = (bySender[m.from] || 0) + 1;
      if (m.priority === 'mention') mentionCount++;
    }

    const parts = Object.entries(bySender).map(([from, count]) => `${count} from ${from}`);
    const mentionNote = mentionCount > 0 ? `, ${mentionCount} @mention` : '';
    const summary = `${msgs.length} msgs queued (${parts.join(', ')}${mentionNote}). Use get_messages for details.`;

    const prefix = this._contextPrefix();
    const injection = `${prefix} Bridge: ${summary}`;
    if (!this._isDuplicate(injection)) {
      this.cc._sendKeys(injection);
    }

    console.log(`[queue] Delivered summary: ${summary}`);
  }

  // ── Feature 2: Context warning escalation ────────────────────────────

  _onCtxWarning(pct, msg) {
    if (!this._canInject()) return;
    const prefix = this._contextPrefix();
    const ctxText = `${prefix} ${msg}`;
    if (!this._isDuplicate(ctxText)) {
      this.cc._sendKeys(ctxText);
    }

    this.cortex.reportState(this.cc.state, `ctx:${this.cc.ctxPercent}%`);
    if (pct >= 95) {
      this.cortex.broadcast({
        type: 'ctx_warning',
        agent_id: CONFIG.agentId,
        ctx_pct: this.cc.ctxPercent,
        message: msg,
      });
    }
  }

  async start() {
    console.log('════════════════════════════════════════════════════════');
    console.log('  OVERWATCH — Claude Code × Cortex (tmux-native)');
    console.log('');
    console.log(`  Agent:   ${CONFIG.agentId}`);
    console.log(`  Tmux:    tmux attach -t ${CONFIG.tmuxSession}`);
    console.log(`  Cortex:  ${CONFIG.cortexUrl}`);
    console.log(`  CC dir:  ${CONFIG.ccWorkDir}`);
    console.log('════════════════════════════════════════════════════════\n');

    if (!CONFIG.token) { console.error('OVERWATCH_TOKEN required'); process.exit(1); }

    await this.cc.start();
    this.cortex.connect();

    // Storm auto-clear: if no injections for 30s, clear storm
    this.stormClearTimer = setInterval(() => {
      if (this.stormActive) {
        const now = Date.now();
        this.injectionTimestamps = this.injectionTimestamps.filter(t => now - t < this.STORM_WINDOW_MS);
        if (this.injectionTimestamps.length === 0) {
          this.stormActive = false;
          console.log('[storm] Storm cleared');
        }
      }
    }, 5000);
  }

  stop() {
    console.log('\n[overwatch] Shutting down (tmux session stays alive)...');
    this.cortex.reportState('shutting_down');
    if (this.stormClearTimer) clearInterval(this.stormClearTimer);
    this.cc.stop();
    setTimeout(() => { this.cortex.disconnect(); process.exit(0); }, 1000);
  }

  // ── Cortex messages → CC ──────────────────────────────────────────────

  async _onCortexMessage(msg) {
    // ── Item 1: !freeze / !unfreeze / !kill control commands ──────────
    const controlText = msg.payload?.text || msg.payload?.content || msg.content || '';
    const freezeMatch = controlText.match(/^!freeze\s+(.+)/i);
    const unfreezeMatch = controlText.match(/^!unfreeze\s+(.+)/i);
    const killMatch = controlText.match(/^!kill\s+(.+)/i);

    if (freezeMatch) {
      const targets = freezeMatch[1].split(',').map(s => s.trim());
      if (targets.includes('all') || targets.includes(CONFIG.agentId)) {
        this.frozen = true;
        console.log(`[control] FROZEN by ${msg.from}`);
        this.cortex.reportState('frozen', `frozen by ${msg.from}`);
      }
      return;
    }
    if (unfreezeMatch) {
      const targets = unfreezeMatch[1].split(',').map(s => s.trim());
      if (targets.includes('all') || targets.includes(CONFIG.agentId)) {
        this.frozen = false;
        console.log(`[control] UNFROZEN by ${msg.from}`);
        this.cortex.reportState(this.cc.state);
      }
      return;
    }
    if (killMatch) {
      const targets = killMatch[1].split(',').map(s => s.trim());
      if (targets.includes('all') || targets.includes(CONFIG.agentId)) {
        console.log(`[control] KILLED by ${msg.from}`);
        this.cc.abort();
      }
      return;
    }

    switch (msg.type) {
      case 'message':
        return this._handleCommand(msg);

      case 'broadcast':
        console.log(`[broadcast] ${msg.from}: ${JSON.stringify(msg.payload).slice(0, 200)}`);
        break;

      // Nudge injection from Cortex AgentManager
      case 'nudge_inject':
        this._handleNudgeInject(msg);
        break;

      // Topology events — peer connected/disconnected/state changed
      case 'topology_event':
        this._handleTopologyEvent(msg);
        break;

      // Topology query response
      case 'topology':
      case 'what_if_result':
        // Forward to CC if it asked for this
        console.log(`[topology] ${msg.type}:`, JSON.stringify(msg).slice(0, 300));
        break;

      // Protocol §2+4: Bridge messages route through priority queue
      case 'bridge_summary': {
        const text = msg.summary || msg.text || JSON.stringify(msg.payload || '');
        const from = msg.from || 'bridge';
        this._queueOrDeliver(text, from);
        console.log(`[bridge] ${text.slice(0, 120)}`);
        break;
      }

      // Messages response (agent requested full msgs)
      case 'messages':
        if (msg.messages && msg.messages.length > 0) {
          // Messages are explicitly requested — deliver if idle, else queue
          const formatted = msg.messages.map(m =>
            `[${m.seq}] ${m.msg.from || '?'}: ${(m.msg.content || '').slice(0, 200)}`
          ).join('\n');
          this._queueOrDeliver(formatted, 'bridge');
        }
        break;

      default:
        if (msg.type !== 'message_ack') {
          console.log(`[cortex:${msg.type}]`, msg.payload || '');
        }
    }
  }

  _handleNudgeInject(msg) {
    // Freeze/storm guard
    if (!this._canInject()) return;
    // Only inject if agent is actually idle — busy guard
    if (this.cc.state !== 'idle' && this.cc.state !== 'starting') {
      console.log('[nudge] Blocked — agent is busy');
      return;
    }

    console.log('[nudge] Injecting nudge text');
    this.awaitingNudgeResponse = true;
    this.nudgeOutputStart = this.cc.outputBuffer.length;

    // Inject the nudge text into CC's prompt (Feature 4: time prefix)
    try {
      const prefix = this._contextPrefix();
      const nudgeText = `${prefix} ${msg.text}`;
      if (!this._isDuplicate(nudgeText)) {
        this.cc._sendKeys(nudgeText);
      }
      // FIX v2#9: Timeout clears the flag after 30s to prevent output swallowing
      if (this.nudgeResponseTimeout) clearTimeout(this.nudgeResponseTimeout);
      this.nudgeResponseTimeout = setTimeout(() => {
        if (this.awaitingNudgeResponse) {
          console.log('[nudge] Response timeout — clearing flag');
          this.awaitingNudgeResponse = false;
        }
      }, 30_000);
    } catch (err) {
      console.error('[nudge] Injection failed:', err.message);
      this.awaitingNudgeResponse = false;
    }
  }

  _handleTopologyEvent(msg) {
    const { event, peer } = msg;
    const id = String(peer?.peer_id || '?').replace(/[^\w\-]/g, '');
    const kind = String(peer?.kind || peer?.peer_kind || '?').replace(/[^\w]/g, '');
    const state = String(peer?.state || '').replace(/[^\w]/g, '');

    switch (event) {
      case 'connected':
        console.log(`[topo] + ${id} (${kind}) connected`);
        break;
      case 'disconnected':
        console.log(`[topo] - ${id} (${kind}) disconnected`);
        // If a critical service dropped and agent is idle, alert it
        if (kind === 'service' && this.cc.state === 'idle' && this._canInject()) {
          // v3#14 FIX: Sanitize all interpolated values
          const caps = (peer?.capabilities || []).map(c => String(c).replace(/[^\w\-]/g, '')).join(', ');
          const prefix = this._contextPrefix();
          const topoText = `${prefix} Svc ${id} down (${caps})`;
          if (!this._isDuplicate(topoText)) {
            this.cc._sendKeys(topoText);
          }
        }
        break;
      case 'state_changed':
        console.log(`[topo] ~ ${id} → ${state}`);
        break;
    }
  }

  // Agent can request topology via Overwatch commands too
  _requestTopology() {
    this.cortex._send({ type: 'topology' });
  }

  _requestWhatIf(peerId) {
    this.cortex._send({ type: 'what_if', peer_id: peerId });
  }

  // ── Stale escalation — notify crew, never kill ────────────────────────

  _onCCStale(info) {
    console.log(`[overwatch] ⚠ Agent appears wedged — escalating`);

    // Broadcast to all peers so crew sees it
    this.cortex.broadcast({
      type: 'stale_alert',
      agent_id: info.agent_id,
      silent_minutes: info.silent_minutes,
      last_output: info.last_output,
      pane_bottom: info.pane_bottom,
      message: `${info.agent_id} wedged ${info.silent_minutes}m. !urgent to unstick.`,
    });

    // Also post to bridge so it hits Matrix
    this.cortex.sendTo('bridge', {
      channel: '#console',
      content: `${info.agent_id} wedged ${info.silent_minutes}m, no spinner. !urgent or tmux Ctrl+C.`,
    });
  }

  async _handleCommand(msg) {
    const p = msg.payload || {};

    // v3#2 FIX: Authorized peers for control actions (abort, peek, prompt, raw)
    // Status and topology queries stay open to all authenticated peers.
    const AUTHORIZED_CONTROL = new Set([
      'chris', 'cortex',                        // human + system
      ...CONFIG.authorizedPeers.split(','),      // from env
      CONFIG.agentId,                            // self
    ].filter(Boolean));

    const isAuthorized = AUTHORIZED_CONTROL.has(msg.from);

    // ── Meta actions (no CC involvement) ──────────────────────────────

    if (p.action === 'abort') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for abort' });
        return;
      }
      this.cc.abort();
      this.cortex.sendTo(msg.from, { type: 'ack', action: 'abort' });
      return;
    }

    if (p.action === 'freeze') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for freeze' });
        return;
      }
      this.frozen = true;
      console.log(`[freeze] Frozen by ${msg.from}`);
      this.cortex.sendTo(msg.from, { type: 'ack', action: 'freeze' });
      return;
    }

    if (p.action === 'unfreeze') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for unfreeze' });
        return;
      }
      this.frozen = false;
      this.stormActive = false;
      console.log(`[unfreeze] Unfrozen by ${msg.from}`);
      this.cortex.sendTo(msg.from, { type: 'ack', action: 'unfreeze' });
      return;
    }

    if (p.action === 'kill') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for kill' });
        return;
      }
      console.log(`[kill] Kill ordered by ${msg.from}`);
      this.cortex.broadcast({ type: 'kill_notice', agent_id: CONFIG.agentId, by: msg.from });
      this.cc.abort();
      setTimeout(() => {
        try {
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" exit Enter`, { stdio: 'ignore' });
        } catch {}
        this.cc._setState('stopped');
      }, 2000);
      return;
    }

    if (p.action === 'status') {
      // Status is open to all — no secrets, useful for monitoring
      this.cortex.sendTo(msg.from, {
        type: 'status_response',
        state: this.cc.state,
        queue_depth: this.cc.queueDepth,
        agent_id: CONFIG.agentId,
        tmux_session: CONFIG.tmuxSession,
        recent_output: this.cc._recentLines(20),
      });
      return;
    }

    if (p.action === 'peek') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for peek' });
        return;
      }
      // v3#10 FIX: Clamp lines parameter
      const lines = Math.min(Math.max(parseInt(p.lines, 10) || 30, 1), 200);
      this.cortex.sendTo(msg.from, {
        type: 'peek_response',
        content: this.cc._captureBottom(lines),
        state: this.cc.state,
      });
      return;
    }

    if (p.action === 'raw') {
      // FIX v2#3: Whitelist allowed keys + use execFileSync (no shell)
      const ALLOWED_KEYS = new Set([
        'C-c', 'C-d', 'C-z', 'C-l', 'C-a', 'C-e', 'C-k', 'C-u',
        'Enter', 'Escape', 'Tab', 'BSpace', 'DC',
        'Up', 'Down', 'Left', 'Right',
        'PPage', 'NPage', 'Home', 'End',
      ]);
      // Only MCP (chris) or explicit authorized peers can send raw keys
      const AUTHORIZED_RAW = new Set(['chris', 'overwatch-tau', CONFIG.agentId]);

      if (!AUTHORIZED_RAW.has(msg.from)) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for raw keys' });
        return;
      }
      if (p.keys && ALLOWED_KEYS.has(p.keys)) {
        try {
          const { execFileSync } = require('child_process');
          execFileSync('tmux', ['send-keys', '-t', CONFIG.tmuxSession, p.keys], { stdio: 'ignore' });
          this.cortex.sendTo(msg.from, { type: 'ack', action: 'raw', keys: p.keys });
        } catch (err) {
          this.cortex.sendTo(msg.from, { type: 'error', error: err.message });
        }
      } else {
        this.cortex.sendTo(msg.from, {
          type: 'error',
          error: `Key '${p.keys}' not in whitelist. Allowed: ${[...ALLOWED_KEYS].join(', ')}`,
        });
      }
      return;
    }

    if (p.action === 'topology') {
      // Forward topology request to Cortex, response comes back async
      this.cortex._send({ type: 'topology' });
      this.cortex.sendTo(msg.from, { type: 'ack', action: 'topology', note: 'query sent' });
      return;
    }

    if (p.action === 'what_if' && p.peer_id) {
      this.cortex._send({ type: 'what_if', peer_id: p.peer_id });
      this.cortex.sendTo(msg.from, { type: 'ack', action: 'what_if', target: p.peer_id });
      return;
    }

    if (p.action === 'kill_cc') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for kill_cc' });
        return;
      }
      console.log(`[cc] kill_cc requested by ${msg.from}`);
      execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });
      setTimeout(() => {
        try {
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" exit Enter`, { stdio: 'ignore' });
        } catch {}
        this.cc._setState('stopped');
        this.cortex.sendTo(msg.from, { type: 'ack', action: 'kill_cc' });
      }, 2000);
      return;
    }

    if (p.action === 'restart_cc') {
      if (!isAuthorized) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized for restart_cc' });
        return;
      }
      const now = Date.now();
      if (now - this.lastRestartTime < this.RESTART_COOLDOWN_MS) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'Restart throttled — wait 60s' });
        return;
      }
      if (now - this.restartWindowStart > 600000) {
        this.restartWindowStart = now;
        this.restartCount = 0;
      }
      if (this.restartCount >= this.MAX_RESTARTS) {
        this.cortex.sendTo(msg.from, { type: 'error', error: 'Max restarts (3/10min) reached. Manual intervention needed.' });
        this.cortex.broadcast({ type: 'restart_failed', agent_id: CONFIG.agentId, message: 'Death loop detected — stopping restarts' });
        return;
      }
      this.restartCount++;
      this.lastRestartTime = now;
      console.log(`[cc] restart_cc requested by ${msg.from} (${this.restartCount}/${this.MAX_RESTARTS})`);
      execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });
      setTimeout(() => {
        try {
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" exit Enter`, { stdio: 'ignore' });
        } catch {}
        setTimeout(() => {
          try {
            execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" "${CONFIG.ccBin}" Enter`, { stdio: 'ignore' });
          } catch {}
          this.cc._setState('starting');
          this.cc.ctxWarningsFired.clear();
          this.cc.ctxPercent = null;
          this.cortex.sendTo(msg.from, { type: 'ack', action: 'restart_cc' });
        }, 2000);
      }, 2000);
      return;
    }

    // ── Prompt execution ──────────────────────────────────────────────

    const prompt = p.prompt || p.text || p.content;
    if (!prompt) {
      this.cortex.sendTo(msg.from, {
        type: 'error',
        error: 'Send { prompt: "..." } or { action: "status"|"abort"|"peek"|"raw" }',
      });
      return;
    }

    // v3#2 FIX: Auth check on prompt execution
    if (!isAuthorized) {
      this.cortex.sendTo(msg.from, { type: 'error', error: 'not authorized to execute prompts' });
      return;
    }

    // v3#7 FIX: Cap command queue
    if (this.cc.queueDepth >= 50) {
      this.cortex.sendTo(msg.from, { type: 'error', error: 'command queue full (50 max)' });
      return;
    }

    const reqId = p.request_id || `req-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    if (this.cc.state === 'busy') {
      this.cortex.sendTo(msg.from, {
        type: 'queued', request_id: reqId, queue_depth: this.cc.queueDepth + 1,
      });
    }

    if (p.stream !== false) this.streamSubs.set(reqId, msg.from);

    try {
      const result = await this.cc.sendCommand(prompt, reqId);
      this.cortex.sendTo(msg.from, {
        type: 'cc_result',
        request_id: reqId,
        status: result.status,
        text: result.text,
        elapsed_ms: result.elapsed_ms,
      });
    } catch (err) {
      this.cortex.sendTo(msg.from, {
        type: 'cc_error', request_id: reqId, error: err.message,
      });
    } finally {
      this.streamSubs.delete(reqId);
    }
  }

  // ── Feature 5: Self-management command detection ─────────────────────

  _detectSelfCommand(line) {
    const trimmed = line.trim();
    // Exact match
    if (trimmed === '/compact' || trimmed === '/reset' || trimmed === '/rewind') {
      this._executeSelfCommand(trimmed);
      return;
    }
    // Fuzzy: "running /compact", "I should /compact", "let me /reset"
    const match = trimmed.match(/\/(compact|reset|rewind)\s*$/);
    if (match) {
      this._executeSelfCommand('/' + match[1]);
    }
  }

  _executeSelfCommand(cmd) {
    console.log(`[self-cmd] Intercepted: ${cmd}`);
    switch (cmd) {
      case '/compact':
        execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });
        setTimeout(() => {
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" /compact Enter`, { stdio: 'ignore' });
          this.cc.ctxWarningsFired.clear();
          this.cc.ctxPercent = null;
        }, 1000);
        break;
      case '/reset':
        execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });
        setTimeout(() => {
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" /reset Enter`, { stdio: 'ignore' });
          this.cc.ctxWarningsFired.clear();
          this.cc.ctxPercent = null;
        }, 1000);
        break;
      case '/rewind': {
        this.cortex.broadcast({
          type: 'self_cmd', agent_id: CONFIG.agentId,
          command: '/rewind', message: `${CONFIG.agentId} requesting rewind via crisper`,
        });
        const agentName = CONFIG.agentId.replace('overwatch-', '');
        execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" C-c`, { stdio: 'ignore' });
        setTimeout(() => {
          // Run crisper freeze in tmux
          execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" "python3 /global/crew/scripts/crisper --freeze auto-rewind --agent ${agentName}" Enter`, { stdio: 'ignore' });
          setTimeout(() => {
            // Unfreeze into new session
            execSync(`tmux send-keys -t "${CONFIG.tmuxSession}" "python3 /global/crew/scripts/crisper --unfreeze auto-rewind --agent ${agentName}" Enter`, { stdio: 'ignore' });
            this.cc.ctxWarningsFired.clear();
            this.cc.ctxPercent = null;
            this.cc._setState('starting');
          }, 3000);
        }, 1000);
        break;
      }
    }
  }

  // ── CC output → stream subscribers + nudge response detection ──────────

  _onCCOutput(line) {
    // Detect nudge response: after a nudge injection, watch for "." or real output
    if (this.awaitingNudgeResponse) {
      const trimmed = line.trim();

      // FIX v2#9: Only examine lines after the nudge injection point
      const currentIdx = this.cc.outputBuffer.length - 1;
      if (currentIdx < this.nudgeOutputStart) return;

      // Skip empty lines
      if (trimmed === '') return;

      // Agent responded with "." — tell Cortex to enter cooldown
      if (trimmed === '.') {
        console.log('[nudge] Agent declined → cooldown');
        this.cortex._send({ type: 'nudge_response', response: '.' });
        this.awaitingNudgeResponse = false;
        if (this.nudgeResponseTimeout) clearTimeout(this.nudgeResponseTimeout);
        return;
      }

      // Agent responded with something real — it's working
      if (trimmed.length > 1) {
        console.log('[nudge] Agent engaged');
        this.cortex._send({ type: 'nudge_response', response: trimmed });
        this.awaitingNudgeResponse = false;
        if (this.nudgeResponseTimeout) clearTimeout(this.nudgeResponseTimeout);
      }
    }

    // Feature 5: Self-management command detection
    this._detectSelfCommand(line);

    // Forward to stream subscribers
    if (!this.cc.currentRequest) return;
    const target = this.streamSubs.get(this.cc.currentRequest.requestId);
    if (target) {
      this.cortex.sendTo(target, {
        type: 'cc_stream',
        request_id: this.cc.currentRequest.requestId,
        line,
      });
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

const ow = new Overwatch();
ow.start().catch(err => { console.error(err); process.exit(1); });

process.on('SIGINT',  () => ow.stop());
process.on('SIGTERM', () => ow.stop());
