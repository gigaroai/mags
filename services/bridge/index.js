#!/usr/bin/env node
// services/bridge/index.js — GMB/Matrix ↔ Cortex bridge relay
//
// Connects to:
//   1. GMB WebSocket (wss://gigaro.ai/gmb/ws) — the Matrix bridge
//   2. Cortex (wss://gigaro.ai/cortex/ws) — the hub
//
// Inbound (Matrix → Cortex):
//   - Messages from #console and DMs arrive via GMB
//   - Bridge relay routes them to Cortex as bridge_message
//   - Cortex.AgentManager handles queueing, @mention, !urgent
//
// Outbound (Cortex → Matrix):
//   - Agents send messages to "bridge" peer via Cortex
//   - Bridge relay forwards to GMB for delivery to Matrix
//
// The "." keepalive convention: a single period alone is a silent heartbeat.
// Bridge filters these — never delivered to agents.
//
// Usage:
//   CORTEX_URL=wss://gigaro.ai/cortex/ws \
//   BRIDGE_TOKEN=bridge-CHANGEME \
//   GMB_URL=wss://gigaro.ai/gmb/ws \
//   GMB_TOKEN=bridge-token-from-config \
//   node index.js

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const { CortexServiceClient } = require('../cortex-client');

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  // Cortex
  cortexUrl:    process.env.CORTEX_URL || 'wss://gigaro.ai/cortex/ws',
  bridgeToken:  process.env.BRIDGE_TOKEN || 'bridge-CHANGEME',

  // GMB
  gmbUrl:       process.env.GMB_URL || 'wss://gigaro.ai/gmb/ws',
  gmbToken:     process.env.GMB_TOKEN || '',
  gmbConfigFile: process.env.GMB_CONFIG || '',

  // Channels to relay (empty = all)
  channels:     (process.env.BRIDGE_CHANNELS || '#console').split(',').map(s => s.trim()),

  // Agents to route to (populated from Cortex registry, but can seed here)
  knownAgents:  (process.env.KNOWN_AGENTS || 'giga,sandy,webbie,overwatch-tau').split(',').map(s => s.trim()),

  reconnectMs: 3000,
};

// ─────────────────────────────────────────────────────────────────────────────
// GMB WebSocket client
// ─────────────────────────────────────────────────────────────────────────────

class GMBClient {
  constructor(opts = {}) {
    this.url = CONFIG.gmbUrl;
    this.token = CONFIG.gmbToken;
    this.ws = null;
    this.seq = 0;
    this.reconnectMs = CONFIG.reconnectMs;
    this.reconnectTimer = null;
    this.intentionalClose = false;

    this.onMessage = opts.onMessage || (() => {});
    this.onReady = opts.onReady || (() => {});
  }

  connect() {
    if (this.ws && this.ws.readyState <= WebSocket.CONNECTING) return;
    console.log(`[gmb] Connecting to ${this.url}...`);

    this.ws = new WebSocket(this.url, {
      rejectUnauthorized: process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0',
      headers: this.token ? { 'Authorization': `Bearer ${this.token}` } : {},
    });

    this.ws.on('open', () => {
      console.log('[gmb] Connected');
      this.reconnectMs = CONFIG.reconnectMs;

      // Auth if needed (some GMB setups use a message-based auth)
      if (this.token) {
        this._send({ type: 'auth', token: this.token });
      }

      this.onReady();
    });

    this.ws.on('message', (data) => {
      const raw = data.toString().trim();

      // Silent keepalive — filter out
      if (raw === '.' || raw === '{"type":"keepalive"}') {
        return;
      }

      let msg;
      try {
        msg = JSON.parse(raw);
      } catch {
        // Plain text message from GMB (some bridge modes)
        msg = { type: 'text', content: raw };
      }

      this.onMessage(msg);
    });

    this.ws.on('close', (code) => {
      console.log(`[gmb] Disconnected (${code})`);
      this._cleanup();
      if (!this.intentionalClose) this._reconnect();
    });

    this.ws.on('error', (err) => {
      console.error(`[gmb] ${err.message}`);
    });
  }

  disconnect() {
    this.intentionalClose = true;
    if (this.ws) this.ws.close(1000);
    this._cleanup();
  }

  // Send a message to Matrix via GMB
  send(channel, from, content) {
    this.seq++;
    this._send({
      type: 'message',
      channel: channel,
      from: from,
      content: content,
      seq: this.seq,
    });
  }

  // Send raw to GMB (for protocol-level stuff)
  sendRaw(msg) {
    this._send(msg);
  }

  _send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  _reconnect() {
    console.log(`[gmb] Reconnecting in ${Math.round(this.reconnectMs / 1000)}s...`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectMs = Math.min(this.reconnectMs * 1.5, 60000);
      this.connect();
    }, this.reconnectMs);
  }

  _cleanup() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bridge Relay — GMB ↔ Cortex
// ─────────────────────────────────────────────────────────────────────────────

class BridgeRelay {
  constructor() {
    this.bridgeSeq = 0;  // monotonic sequence for bridge messages

    this.cortex = new CortexServiceClient({
      url: CONFIG.cortexUrl,
      token: CONFIG.bridgeToken,
      peerId: 'bridge',
      capabilities: ['bridge', 'matrix', 'gmb'],
      metadata: {
        gmb_url: CONFIG.gmbUrl,
        channels: CONFIG.channels,
      },
      onMessage: (msg) => this._onCortexMessage(msg),
      onReady: () => this._onCortexReady(),
    });

    this.gmb = new GMBClient({
      onMessage: (msg) => this._onGMBMessage(msg),
      onReady: () => this._onGMBReady(),
    });
  }

  start() {
    console.log('════════════════════════════════════════════════════════');
    console.log('  BRIDGE RELAY — GMB/Matrix ↔ Cortex');
    console.log('');
    console.log(`  GMB:     ${CONFIG.gmbUrl}`);
    console.log(`  Cortex:  ${CONFIG.cortexUrl}`);
    console.log(`  Channels: ${CONFIG.channels.join(', ')}`);
    console.log('════════════════════════════════════════════════════════\n');

    // Connect both ends
    this.gmb.connect();
    this.cortex.connect();
  }

  stop() {
    console.log('\n[bridge] Shutting down...');
    this.cortex.reportState('shutting_down');
    this.gmb.disconnect();
    setTimeout(() => {
      this.cortex.disconnect();
      process.exit(0);
    }, 1000);
  }

  // ── GMB → Cortex (inbound from Matrix) ────────────────────────────────

  _onGMBReady() {
    console.log('[bridge] GMB connected');
  }

  _onGMBMessage(msg) {
    // Parse the GMB message format
    // GMB messages typically look like:
    //   { type: "message", from: "sandy:gigaro.ai", channel: "#console", content: "...", seq: N }
    //   { type: "message", from: "chris:gigaro.ai", channel: "#console", content: "...", seq: N }
    // Or in the [#channel] @user: format as plain text

    const from = msg.from || msg.sender || this._extractSender(msg.content);
    const channel = msg.channel || this._extractChannel(msg.content);
    const content = msg.content || msg.body || msg.text || '';
    const gmbSeq = msg.seq || msg.bridge_seq;

    // Filter: only relay configured channels
    if (channel && CONFIG.channels.length > 0 && !CONFIG.channels.includes(channel)) {
      return;
    }

    // Filter: skip empty or keepalive
    if (!content || content.trim() === '' || content.trim() === '.') {
      return;
    }

    this.bridgeSeq++;

    console.log(`[bridge:in] ${channel || 'DM'} ${from}: ${content.slice(0, 100)}${content.length > 100 ? '...' : ''}`);

    // Determine target agents
    // If it's a DM, route to the specific agent
    // If it's #console, route to all known agents
    const targets = this._routeTargets(channel, content, from);

    const bridgeMsg = {
      from: from,
      content: content,
      channel: channel,
      bridge_seq: this.bridgeSeq,
      gmb_seq: gmbSeq,
    };

    for (const target of targets) {
      this.cortex.bridgeMessage(target, bridgeMsg);
    }
  }

  _routeTargets(channel, content, from) {
    // Extract sender's agent name (before :gigaro.ai)
    const senderAgent = from ? from.split(':')[0].split('@').pop() : '';

    if (channel === '#console' || !channel) {
      // Route to all known agents except the sender
      return CONFIG.knownAgents.filter(a => a !== senderAgent);
    }

    // DM channel — extract target from channel name
    // DM channels might be named like "#dm-giga-sandy" or "!roomid"
    // For now, route to all agents except sender
    return CONFIG.knownAgents.filter(a => a !== senderAgent);
  }

  _extractSender(content) {
    // Parse "[#console] @user:gigaro.ai:" format
    if (!content) return 'unknown';
    const match = content.match(/@(\w+(?::[\w.]+)?)/);
    return match ? match[1] : 'unknown';
  }

  _extractChannel(content) {
    if (!content) return null;
    const match = content.match(/\[([#\w-]+)\]/);
    return match ? match[1] : '#console';
  }

  // ── Cortex → GMB (outbound to Matrix) ─────────────────────────────────

  _onCortexReady() {
    console.log('[bridge] Cortex connected — relay active');
    this.cortex.reportState('idle');
  }

  _onCortexMessage(msg) {
    switch (msg.type) {
      // Agent sends a message to post on Matrix
      case 'message': {
        this._handleOutbound(msg);
        break;
      }

      // Broadcast — relay to Matrix if from an agent
      case 'broadcast': {
        if (msg.payload?.to_bridge || msg.payload?.channel) {
          this._relayToMatrix(
            msg.payload.channel || '#console',
            msg.from,
            msg.payload.content || msg.payload.text || JSON.stringify(msg.payload)
          );
        }
        break;
      }

      default:
        break;
    }
  }

  _handleOutbound(msg) {
    const payload = msg.payload || {};

    // Standard outbound: agent wants to post to a channel
    // { to: "bridge", payload: { channel: "#console", content: "hello" } }
    const channel = payload.channel || '#console';
    const content = payload.content || payload.text || '';
    const from = msg.from || 'unknown';

    if (!content || content.trim() === '') return;

    // Skip keepalives
    if (content.trim() === '.') {
      // Don't relay, but also don't complain — it's the convention
      return;
    }

    console.log(`[bridge:out] ${from} → ${channel}: ${content.slice(0, 100)}${content.length > 100 ? '...' : ''}`);
    this._relayToMatrix(channel, from, content);
  }

  _relayToMatrix(channel, from, content) {
    // Send via GMB
    this.gmb.send(channel, from, content);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

const relay = new BridgeRelay();
relay.start();

process.on('SIGINT', () => relay.stop());
process.on('SIGTERM', () => relay.stop());
