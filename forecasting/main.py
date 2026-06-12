import os
import json
import threading
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import pika
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://smartpark:smartpark@localhost:5432/forecasting")
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"
ALPHA = 0.3  # EWM smoothing factor

# Per-lot state: {lot_id: {"smoothed": float, "profile": [24 floats], "counts": [24 ints]}}
_state: dict[str, dict] = {}
_lock = threading.Lock()


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Propagates X-Correlation-ID header for distributed request tracing."""

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


def _db():
    return psycopg2.connect(DATABASE_URL)


def _wait_db(retries=20, delay=5):
    for i in range(retries):
        try:
            _db().close()
            log.info("TimescaleDB ready")
            return
        except Exception:
            log.info("Waiting for DB (%d/%d)…", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("DB unavailable")


def _bootstrap():
    try:
        with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT lot_id,
                       EXTRACT(HOUR FROM ts)::int AS hour,
                       AVG(occupancy_pct) AS avg_occ
                FROM occupancy_readings
                GROUP BY lot_id, hour
            """)
            rows = cur.fetchall()
        with _lock:
            for row in rows:
                lid = row["lot_id"]
                if lid not in _state:
                    _state[lid] = {"smoothed": float(row["avg_occ"]), "profile": [50.0] * 24, "counts": [0] * 24}
                h = int(row["hour"])
                _state[lid]["profile"][h] = float(row["avg_occ"])
                _state[lid]["counts"][h] = 1
        log.info("Bootstrap: loaded %d lot profiles", len(_state))
    except Exception as exc:
        log.warning("Bootstrap skipped: %s", exc)


def _update(lot: dict):
    lid = lot["lot_id"]
    occ = float(lot["occupancy_pct"])
    ts = datetime.fromisoformat(lot["timestamp"])
    h = ts.hour

    with _lock:
        if lid not in _state:
            _state[lid] = {"smoothed": occ, "profile": [50.0] * 24, "counts": [0] * 24}
        s = _state[lid]
        s["smoothed"] = ALPHA * occ + (1 - ALPHA) * s["smoothed"]
        n = s["counts"][h] + 1
        s["profile"][h] = (s["profile"][h] * s["counts"][h] + occ) / n
        s["counts"][h] = n

    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO occupancy_readings (lot_id, ts, occupancy_pct, free, capacity)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                (lid, ts, occ, lot.get("free"), lot.get("capacity")),
            )
            conn.commit()
    except Exception as exc:
        log.error("DB insert error: %s", exc)


def _start_consumer():
    def run():
        while True:
            try:
                conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                ch = conn.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
                q = ch.queue_declare("forecasting_q", durable=True)
                ch.queue_bind(exchange=EXCHANGE, queue=q.method.queue, routing_key=ROUTING_KEY)

                def on_message(ch, method, props, body):
                    _update(json.loads(body))
                    ch.basic_ack(method.delivery_tag)

                ch.basic_qos(prefetch_count=10)
                ch.basic_consume(queue=q.method.queue, on_message_callback=on_message, auto_ack=False)
                log.info("Forecasting consumer ready")
                ch.start_consuming()
            except Exception as exc:
                log.warning("Consumer error: %s — retry in 5s", exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _wait_db()
    _bootstrap()
    _start_consumer()
    yield


app = FastAPI(title="Forecasting Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.get("/health")
def health():
    """Liveness + dependency health check for orchestrators and circuit breakers."""
    checks: dict = {}
    overall = "ok"
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM occupancy_readings")
            reading_count = cur.fetchone()[0]
        checks["timescaledb"] = {"status": "ok", "total_readings": reading_count}
    except Exception as exc:
        checks["timescaledb"] = {"status": "error", "detail": str(exc)}
        overall = "unhealthy"
    with _lock:
        checks["in_memory_state"] = {"tracked_lots": len(_state)}
    return JSONResponse(
        {"status": overall, "service": "forecasting", "checks": checks},
        status_code=200 if overall == "ok" else 503,
    )


@app.get("/forecast/{lot_id}")
def forecast(lot_id: str, horizon: str = "30m"):
    minutes = int(horizon.lower().rstrip("m")) if horizon.lower().endswith("m") else 30

    with _lock:
        s = _state.get(lot_id)
    if s is None:
        raise HTTPException(404, detail="No data for this lot yet")

    now = datetime.now(timezone.utc)
    target_hour = (now + timedelta(minutes=minutes)).hour
    blend = min(minutes / 60.0, 1.0)
    predicted = (1 - blend) * s["smoothed"] + blend * s["profile"][target_hour]

    return {
        "lot_id": lot_id,
        "horizon_minutes": minutes,
        "forecast_occupancy_pct": round(predicted, 1),
        "current_smoothed_pct": round(s["smoothed"], 1),
        "profile_pct": round(s["profile"][target_hour], 1),
        "forecast_at": (now + timedelta(minutes=minutes)).isoformat(),
    }
