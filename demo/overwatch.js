#!/usr/bin/env node
// demo/overwatch.js — Overwatch agent demo client
// Connects to Cortex, authenticates, sends heartbeats, handles commands

const WebSocket = require('ws');

const CONFIG = {
  cortexUrl: process.env.CORTEX_URL || 'ws://localhost:4000/cortex/ws',
  token:     process.env.AGENT_TOKEN || 'overwatch-alpha-CHANGEME',
  agentId:   process.env.AGENT_ID || `overwatch-${process.pid}`,
};

class OverwatchDemo {
  constructor() {
    this.ws = null;
    this.sessionId = null;
    this.heartbeatTimer = null;
    this.reconnectTimer = null;
    this.reconnectMs = 2000;
    this.state = 'idle';
  }

  connect() {
    console.log(`[overwatch] Connecting to ${CONFIG.cortexUrl}...`);
    this.ws = new WebSocket(CONFIG.cortexUrl);

    this.ws.on('open', () => {
      console.log('[overwatch] Connected — sending auth...');
      this.send({
        type: 'auth',
        token: CONFIG.token,
        peer_id: CONFIG.agentId,
        peer_kind: 'agent',
        capabilities: ['claude-code', 'shell', 'file-ops'],
      });
    });

    this.ws.on('message', (data) => {
      const msg = JSON.parse(data.toString());
      this.handleMessage(msg);
    });

    this.ws.on('close', (code) => {
      console.log(`[overwatch] Disconnected (code=${code})`);
      this.cleanup();
      this.scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      console.error(`[overwatch] Error: ${err.message}`);
    });
  }

  handleMessage(msg) {
    switch (msg.type) {
      case 'auth_ok':
        this.sessionId = msg.session_id;
        console.log(`[overwatch] Authenticated! session=${this.sessionId}`);
        this.startHeartbeat();
        this.startDemo();
        break;

      case 'auth_fail':
        console.error(`[overwatch] Auth failed: ${msg.reason}`);
        process.exit(1);
        break;

      case 'ping':
        this.send({ type: 'pong', ts: Date.now() });
        break;

      case 'message':
        console.log(`[overwatch] Message from ${msg.from}:`, msg.payload);
        break;

      case 'broadcast':
        console.log(`[overwatch] Broadcast from ${msg.from}:`, msg.payload);
        break;

      case 'message_ack':
        console.log(`[overwatch] Message to ${msg.to}: ${msg.status}`);
        break;

      case 'discover_result':
        console.log(`[overwatch] Services found:`, msg.services);
        break;

      case 'inference_routed':
        console.log(`[overwatch] Inference routed to ${msg.routed_to} (${msg.request_id})`);
        break;

      case 'inference_response':
        console.log(`[overwatch] Inference response from ${msg.from}:`, msg.payload);
        break;

      case 'inference_error':
        console.log(`[overwatch] Inference error: ${msg.reason}`);
        break;

      default:
        console.log(`[overwatch] ${msg.type}:`, msg);
    }
  }

  // Demo: cycle through states, discover services, send inference requests
  startDemo() {
    console.log('[overwatch] Starting demo sequence...\n');

    // After 3s, set state to busy
    setTimeout(() => {
      console.log('[overwatch] → Setting state to busy');
      this.send({ type: 'status', state: 'busy', detail: 'Running tests' });
    }, 3000);

    // After 6s, back to idle
    setTimeout(() => {
      console.log('[overwatch] → Setting state back to idle');
      this.send({ type: 'status', state: 'idle' });
    }, 6000);

    // After 8s, discover services
    setTimeout(() => {
      console.log('[overwatch] → Discovering inference services');
      this.send({ type: 'discover', capability: 'inference' });
    }, 8000);

    // After 10s, send an inference request
    setTimeout(() => {
      console.log('[overwatch] → Sending inference request');
      this.send({
        type: 'inference_request',
        model: 'qwen3-32b',
        request_id: `req-${Date.now()}`,
        payload: {
          messages: [{ role: 'user', content: 'Hello from Overwatch!' }],
          max_tokens: 100,
        },
      });
    }, 10000);

    // After 15s, broadcast to all peers
    setTimeout(() => {
      console.log('[overwatch] → Broadcasting to all peers');
      this.send({
        type: 'broadcast',
        payload: { text: 'Overwatch checking in — all systems nominal' },
      });
    }, 15000);
  }

  send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ ...msg, ts: Date.now() }));
    }
  }

  startHeartbeat() {
    this.heartbeatTimer = setInterval(() => {
      this.send({ type: 'heartbeat', peer_id: CONFIG.agentId, status: this.state });
    }, 10000);
  }

  cleanup() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.sessionId = null;
  }

  scheduleReconnect() {
    console.log(`[overwatch] Reconnecting in ${this.reconnectMs / 1000}s...`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectMs = Math.min(this.reconnectMs * 1.5, 60000);
      this.connect();
    }, this.reconnectMs);
  }
}

const agent = new OverwatchDemo();
agent.connect();

process.on('SIGINT', () => {
  console.log('\n[overwatch] Shutting down...');
  if (agent.ws) agent.ws.close();
  process.exit(0);
});
