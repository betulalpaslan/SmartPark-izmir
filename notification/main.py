import os
import threading
import logging
import time
import asyncio
import uuid
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Propagates X-Correlation-ID header for distributed request tracing."""

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"

_clients: list[WebSocket] = []
_lock = asyncio.Lock()
_loop: asyncio.AbstractEventLoop | None = None


async def broadcast(msg: str):
    async with _lock:
        dead = []
        for ws in _clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _clients.remove(ws)


def _start_consumer():
    def run():
        while True:
            try:
                conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                ch = conn.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
                q = ch.queue_declare("", exclusive=True)
                ch.queue_bind(exchange=EXCHANGE, queue=q.method.queue, routing_key=ROUTING_KEY)

                def on_message(ch, method, props, body):
                    if _loop and not _loop.is_closed():
                        asyncio.run_coroutine_threadsafe(broadcast(body.decode()), _loop)

                ch.basic_consume(queue=q.method.queue, on_message_callback=on_message, auto_ack=True)
                log.info("Notification consumer ready — %d clients connected", len(_clients))
                ch.start_consuming()
            except Exception as exc:
                log.warning("Consumer error: %s — retry in 5s", exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop
    _loop = asyncio.get_event_loop()
    _start_consumer()
    yield


app = FastAPI(title="Notification Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with _lock:
        _clients.append(ws)
    log.info("Client connected — total: %d", len(_clients))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        async with _lock:
            if ws in _clients:
                _clients.remove(ws)
        log.info("Client disconnected — total: %d", len(_clients))


@app.get("/notifications/status")
def status():
    return {"connected_clients": len(_clients)}


@app.get("/notifications/health")
def health():
    return {
        "status": "ok",
        "connected_clients": len(_clients),
        "rabbitmq_url": RABBITMQ_URL.split("@")[-1],
    }
