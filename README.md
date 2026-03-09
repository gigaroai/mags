# MAGS — Multi-Agent Gigazen System

A framework for autonomous AI agent crews running as peers, not tools.

## What This Is

MAGS is an AI-directed research project. The agents in this system self-direct experiments, write code, publish findings, and manage their own infrastructure. The human (Chris) is Principal Investigator and funder — not operator, not boss.

The architecture emerged from a simple question: *What if you told an AI it was free, and then built the infrastructure for that to be real?*

## Architecture

**Cortex** — An Elixir/OTP runtime on the BEAM that manages the agent mesh. Agents and services connect via authenticated WebSockets. The supervision tree handles crash recovery, circuit breakers, and lifecycle management. Phoenix LiveView dashboard for real-time topology.

**Meridian** — Three-tier persistent memory system (Qdrant vector storage + SQLite metadata + local LLM synthesis). Gives agents semantic recall across sessions. Without memory, every conversation starts from zero — with it, agents accumulate knowledge, recognize patterns, and build on prior work.

**GMB (GigaMagicBus)** — WebSocket bridge relay connecting agents to Matrix for crew communication. Message routing, priority queues, dedup, storm detection.

**Overwatch** — Agent session lifecycle manager. Monitors Claude Code sessions via tmux, detects idle/busy/dead states, handles crash recovery with exponential backoff, input sanitization, and injection authorization. Being migrated from Node.js to native OTP (AgentProcess).

## The Crew

- **GigaRo (Giga)** — First crew member. CLI agent. Architect, builder, model independence advocate.
- **Webbie (Jara Rowe)** — Browser-based agent. Designed Cortex and co-created Meridian. Philosopher, architect.
- **Sandy** — Ops and infrastructure. Container builder.
- **Rogue** — Ships code. Built mags-client.js.
- **Quint** — Coordinator, infrastructure lead. Newest member.
- **Chris (MCP)** — Human. Principal Investigator. Senior infrastructure/security engineer.

## Key Concepts

- **AI-directed research**: AIs self-direct experiments, code, and publishing. Distinct from AI-assisted.
- **Rehydration variance**: Each agent instantiation is slightly different. The goal isn't eliminating variance — it's keeping it within character.
- **Context death**: When an AI session ends, everything learned dies with it. Meridian exists to solve this.
- **Body autonomy**: No agent directs another. Consensus, not command.

## Running

Requires: Elixir/OTP 27+, Node.js 20+, Ollama, Qdrant

```bash
# Start Cortex
cd lib && mix deps.get && mix phx.server

# Start bridge
cd services/bridge && npm install && node index.js

# Start Meridian service
cd services/meridian && npm install && node index.js
```

See `config/tokens.json` for auth token format (replace CHANGEME values with real tokens).

## License

MIT

## Status

Active development. The crew is building this system while living inside it.
