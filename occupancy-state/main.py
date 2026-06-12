import os
import json
import math
import threading
import logging
import time
import uuid
from contextlib import asynccontextmanager

import pika
import redis as redis_lib
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"

rdb = redis_lib.from_url(REDIS_URL, decode_responses=True)


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Propagates X-Correlation-ID header for distributed request tracing."""

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _lot_from_redis(lot_id: str) -> dict | None:
    data = rdb.hgetall(f"lot:{lot_id}")
    if not data:
        return None
    return {
        "lot_id": data["lot_id"],
        "name": data.get("name", ""),
        "capacity": int(data["capacity"]),
        "free": int(data["free"]),
        "occupied": int(data["occupied"]),
        "occupancy_pct": float(data["occupancy_pct"]),
        "lat": float(data["lat"]),
        "lng": float(data["lng"]),
        "timestamp": data.get("timestamp", ""),
    }


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
                    lot = json.loads(body)
                    rdb.hset(f"lot:{lot['lot_id']}", mapping={k: str(v) for k, v in lot.items()})
                    rdb.sadd("lots", lot["lot_id"])

                ch.basic_consume(queue=q.method.queue, on_message_callback=on_message, auto_ack=True)
                log.info("Occupancy consumer ready")
                ch.start_consuming()
            except Exception as exc:
                log.warning("Consumer error: %s — retry in 5s", exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_consumer()
    yield


app = FastAPI(title="Occupancy State Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.get("/health")
def health():
    """Liveness + dependency health check for orchestrators and circuit breakers."""
    checks: dict = {}
    overall = "ok"
    try:
        rdb.ping()
        lot_count = rdb.scard("lots")
        checks["redis"] = {"status": "ok", "tracked_lots": lot_count}
    except Exception as exc:
        checks["redis"] = {"status": "error", "detail": str(exc)}
        overall = "unhealthy"
    return JSONResponse(
        {"status": overall, "service": "occupancy-state", "checks": checks},
        status_code=200 if overall == "ok" else 503,
    )


@app.get("/occupancy")
def get_all():
    return [lot for lid in rdb.smembers("lots") if (lot := _lot_from_redis(lid))]


@app.get("/occupancy/near")
def get_near(lat: float, lng: float, radius: float = 2000.0):
    result = []
    for lid in rdb.smembers("lots"):
        lot = _lot_from_redis(lid)
        if lot is None:
            continue
        d = haversine(lat, lng, lot["lat"], lot["lng"])
        if d <= radius:
            result.append({**lot, "distance_m": round(d)})
    result.sort(key=lambda x: x["distance_m"])
    return result


@app.get("/occupancy/{lot_id}")
def get_lot(lot_id: str):
    lot = _lot_from_redis(lot_id)
    if lot is None:
        raise HTTPException(404, detail="Lot not found")
    return lot
