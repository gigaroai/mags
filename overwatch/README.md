# Overwatch — Claude Code × Cortex (tmux-native)

CC runs in a tmux session. Overwatch is a sidecar that reads output, injects
commands, and bridges to Cortex. MCP servers, Meridian, and all CC tooling
stay connected — nothing restarts between commands.

The human can `tmux attach -t cc` at any time to watch, type, or bitch-slap
the agent. Overwatch stays out of the way.

## How it works

```
                         ┌─────────────────────┐
  tmux attach -t cc ───> │  tmux session "cc"   │
  (human can watch/type) │                      │
                         │  ┌────────────────┐  │
                         │  │  claude (CC)   │  │
                         │  │  interactive   │  │
                         │  │  MCP connected │  │
                         │  └───────┬────────┘  │
                         └──────────┼───────────┘
                            pipe-pane│(output)    send-keys (input)
                                    │                 ▲
                                    ▼                 │
                         ┌──────────┴─────────────────┴──┐
                         │       Overwatch (Node)         │
                         │                                │
                         │  FIFO reader ← pipe-pane       │
                         │  idle detect ← capture-pane    │
                         │  inject cmd  → send-keys       │
                         │  serial queue, 10min timeout   │
                         └────────────┬───────────────────┘
                                      │ WSS
                                      ▼
                         ┌────────────────────────┐
                         │     Cortex (BEAM)       │
                         │  registry, routing,     │
                         │  dashboard, services    │
                         └────────────────────────┘
```

## Idle detection

Overwatch polls `tmux capture-pane` and the pipe-pane FIFO output.
When output stops for 2 seconds AND the bottom of the pane matches
CC's prompt pattern (❯, >, $), Overwatch marks the agent as idle
and processes the next queued command.

## Commands from Cortex peers

Any peer on Cortex can send commands to Overwatch:

### Execute a prompt
```json
{ "type": "message", "to": "overwatch-tau", "payload": {
    "prompt": "List all files in /mnt/global/mags",
    "stream": true
}}
```

### Check status
```json
{ "payload": { "action": "status" } }
```
Returns: state, queue depth, recent output.

### Peek at the pane
```json
{ "payload": { "action": "peek", "lines": 30 } }
```
Returns: last N lines of the tmux pane (what you'd see if you attached).

### Abort current task
```json
{ "payload": { "action": "abort" } }
```
Sends Ctrl+C to the tmux pane.

### Raw keystrokes
```json
{ "payload": { "action": "raw", "keys": "C-c" } }
```
Send arbitrary tmux key sequences. For when you need to get creative.

## Env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `CORTEX_URL` | `wss://gigaro.ai/cortex/ws` | Cortex endpoint |
| `OVERWATCH_TOKEN` | — | Auth token (required) |
| `AGENT_ID` | `overwatch-<hostname>` | Peer ID |
| `CC_SESSION` | `cc` | Tmux session name |
| `CC_BIN` | `claude` | Claude CLI binary |
| `CC_WORK_DIR` | cwd | Working directory |
| `CC_RESUME` | — | CC session ID to resume |
| `CC_MODEL` | — | Model override |
| `CC_SYSTEM_PROMPT` | — | System prompt file |
| `CC_ALLOWED_TOOLS` | — | Tool whitelist |
| `PIPE_DIR` | `/tmp/overwatch` | FIFO directory |

## Running

```bash
cd overwatch && npm install

# Dev (Cortex on localhost)
CORTEX_URL=ws://localhost:4000/cortex/ws \
OVERWATCH_TOKEN=overwatch-alpha-CHANGEME \
CC_WORK_DIR=/home/chris/mags \
node index.js

# Then in another terminal:
tmux attach -t cc
```

Overwatch shuts down cleanly — the tmux session keeps running.
Restart Overwatch and it reattaches to the existing session.

## No SDK, no API keys

Wraps the `claude` CLI binary directly. TOS compliant with Claude Max.
No Agent SDK import, no OAuth token extraction, no API key.
