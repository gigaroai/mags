// services/cortex-client.js — Shared Cortex connection for services
// Both bridge and meridian use this to connect to Cortex as service peers.

const WebSocket = require('ws');

class CortexServiceClient {
  constructor(opts) {
    this.url = opts.url || process.env.CORTEX_URL || 'wss://gigaro.ai/cortex/ws';
    this.token = opts.token || '';
    this.peerId = opts.peerId || 'unnamed-service';
    this.peerKind = 'service';
    this.capabilities = opts.capabilities || [];
    this.metadata = opts.metadata || {};

    this.ws = null;
    this.sessionId = null;
    this.reconnectMs = 2000;
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.intentionalClose = false;

    this.onMessage = opts.onMessage || (() => {});
    this.onReady = opts.onReady || (() => {});
  }

  connect() {
    if (this.ws && this.ws.readyState <= WebSocket.CONNECTING) return;
    console.log(`[${this.peerId}:cortex] Connecting to ${this.url}...`);

    this.ws = new WebSocket(this.url, {
      rejectUnauthorized: process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0',
    });

    this.ws.on('open', () => {
      this.reconnectMs = 2000;
      this._send({
        type: 'auth',
        token: this.token,
        peer_id: this.peerId,
        peer_kind: this.peerKind,
        capabilities: this.capabilities,
        metadata: this.metadata,
      });
    });

    this.ws.on('message', (data) => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch { return; }
      switch (msg.type) {
        case 'auth_ok':
          this.sessionId = msg.session_id;
          console.log(`[${this.peerId}:cortex] Authenticated: session=${this.sessionId}`);
          this._startHeartbeat();
          this.onReady();
          break;
        case 'auth_fail':
          console.error(`[${this.peerId}:cortex] Auth failed: ${msg.reason}`);
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
      console.log(`[${this.peerId}:cortex] Disconnected (${code})`);
      this._cleanup();
      if (!this.intentionalClose) this._reconnect();
    });

    this.ws.on('error', (err) => {
      console.error(`[${this.peerId}:cortex] ${err.message}`);
    });
  }

  disconnect() {
    this.intentionalClose = true;
    if (this.ws) this.ws.close(1000);
    this._cleanup();
  }

  sendTo(to, payload) { this._send({ type: 'message', to, payload }); }
  broadcast(payload) { this._send({ type: 'broadcast', payload }); }
  reportState(s, d) { this._send({ type: 'status', state: s, detail: d }); }

  // Bridge-specific: send a message that gets routed through AgentManager
  bridgeMessage(to, msg) {
    this._send({
      type: 'bridge_message',
      to,
      from: msg.from,
      content: msg.content,
      channel: msg.channel,
      bridge_seq: msg.bridge_seq,
    });
  }

  _send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ ...msg, ts: Date.now() }));
    }
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      this._send({ type: 'heartbeat', peer_id: this.peerId });
    }, 10_000);
  }

  _stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  _reconnect() {
    this.reconnectTimer = setTimeout(() => {
      this.reconnectMs = Math.min(this.reconnectMs * 1.5, 60000);
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

module.exports = { CortexServiceClient };
