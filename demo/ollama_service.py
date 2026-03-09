#!/usr/bin/env python3
"""
demo/ollama_service.py — Mock Ollama inference service
Connects to Cortex, registers as a service, responds to inference requests.
"""

import asyncio
import json
import os
import time
import signal
import sys

try:
    import websockets
except ImportError:
    print("Install: pip install websockets")
    sys.exit(1)

CORTEX_URL = os.environ.get("CORTEX_URL", "ws://localhost:4000/cortex/ws")
SERVICE_TOKEN = os.environ.get("SERVICE_TOKEN", "ollama-tau-CHANGEME")
SERVICE_ID = os.environ.get("SERVICE_ID", "ollama-tau")

class OllamaServiceMock:
    def __init__(self):
        self.ws = None
        self.session_id = None
        self.running = True

    async def connect(self):
        print(f"[ollama-svc] Connecting to {CORTEX_URL}...")

        while self.running:
            try:
                async with websockets.connect(CORTEX_URL) as ws:
                    self.ws = ws
                    print("[ollama-svc] Connected — authenticating...")

                    # Auth
                    await self.send({
                        "type": "auth",
                        "token": SERVICE_TOKEN,
                        "peer_id": SERVICE_ID,
                        "peer_kind": "service",
                        "capabilities": ["inference", "embeddings"],
                        "metadata": {
                            "models": ["qwen3-32b", "llama3.1-8b", "mxbai-embed-large"],
                            "gpu": "RTX 4090 + 2x Quadro RTX 8000",
                            "vram_total_gb": 72,
                        }
                    })

                    # Message loop
                    async for raw in ws:
                        msg = json.loads(raw)
                        await self.handle_message(msg)

            except websockets.ConnectionClosed as e:
                print(f"[ollama-svc] Disconnected: {e}")
            except ConnectionRefusedError:
                print("[ollama-svc] Connection refused")
            except Exception as e:
                print(f"[ollama-svc] Error: {e}")

            if self.running:
                print("[ollama-svc] Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def handle_message(self, msg):
        msg_type = msg.get("type", "")

        if msg_type == "auth_ok":
            self.session_id = msg["session_id"]
            print(f"[ollama-svc] Authenticated! session={self.session_id}")
            # Report idle
            await self.send({"type": "status", "state": "idle"})

        elif msg_type == "auth_fail":
            print(f"[ollama-svc] Auth failed: {msg.get('reason')}")
            self.running = False

        elif msg_type == "ping":
            await self.send({"type": "pong", "ts": now_ms()})

        elif msg_type == "inference_request":
            await self.handle_inference(msg)

        elif msg_type == "broadcast":
            print(f"[ollama-svc] Broadcast from {msg.get('from')}: {msg.get('payload')}")

        elif msg_type == "message":
            print(f"[ollama-svc] Message from {msg.get('from')}: {msg.get('payload')}")

        else:
            print(f"[ollama-svc] {msg_type}: {msg}")

    async def handle_inference(self, msg):
        """Simulate inference with a short delay."""
        requester = msg.get("from", "unknown")
        request_id = msg.get("request_id", "?")
        model = msg.get("model", "unknown")
        payload = msg.get("payload", {})

        messages = payload.get("messages", [])
        user_msg = messages[-1].get("content", "") if messages else ""

        print(f"[ollama-svc] Inference request from {requester}: model={model}")
        print(f"[ollama-svc]   prompt: {user_msg[:100]}")

        # Set state to busy
        await self.send({"type": "status", "state": "busy", "detail": f"Inference: {model}"})

        # Simulate inference latency
        await asyncio.sleep(1.5)

        # Send mock response back through Cortex
        response = {
            "type": "inference_response",
            "to": requester,
            "request_id": request_id,
            "payload": {
                "model": model,
                "content": f"[MOCK] Hello from {SERVICE_ID}! You said: '{user_msg[:50]}'. "
                           f"This is a simulated response from {model}.",
                "tokens_used": 42,
                "latency_ms": 1500,
            }
        }
        await self.send(response)

        # Back to idle
        await self.send({"type": "status", "state": "idle"})
        print(f"[ollama-svc] Inference complete for {request_id}")

    async def send(self, msg):
        if self.ws:
            msg["ts"] = now_ms()
            await self.ws.send(json.dumps(msg))

    def shutdown(self):
        print("\n[ollama-svc] Shutting down...")
        self.running = False


def now_ms():
    return int(time.time() * 1000)


async def main():
    svc = OllamaServiceMock()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, svc.shutdown)

    await svc.connect()


if __name__ == "__main__":
    asyncio.run(main())
