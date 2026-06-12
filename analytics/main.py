import os
import json
import threading
import logging
import time
import uuid
from contextlib import asynccontextmanager

import pika
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://smartpark:smartpark@analyticsdb:5432/analytics")
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"


def _db():
    return psycopg2.connect(DATABASE_URL)


def _wait_db(retries=20, delay=5):
    for i in range(retries):
        try:
            _db().close()
            log.info("Analytics DB ready")
            return
        except Exception:
            log.info("Waiting for DB (%d/%d)…", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Analytics DB unavailable")


def _update(lot: dict):
    lid = lot["lot_id"]
    occ = float(lot["occupancy_pct"])
    hour = int(lot["timestamp"][11:13])  # UTC hour from ISO string

    try:
        with _db() as conn, conn.cursor() as cur:
            # lot_stats: incremental running average
            cur.execute("""
                INSERT INTO lot_stats (lot_id, avg_occupancy_pct, max_occupancy_pct, min_occupancy_pct, reading_count, last_seen)
                VALUES (%s, %s, %s, %s, 1, NOW())
                ON CONFLICT (lot_id) DO UPDATE SET
                    avg_occupancy_pct = (lot_stats.avg_occupancy_pct * lot_stats.reading_count + EXCLUDED.avg_occupancy_pct)
                                        / (lot_stats.reading_count + 1),
                    max_occupancy_pct = GREATEST(lot_stats.max_occupancy_pct, EXCLUDED.max_occupancy_pct),
                    min_occupancy_pct = LEAST(lot_stats.min_occupancy_pct, EXCLUDED.min_occupancy_pct),
                    reading_count     = lot_stats.reading_count + 1,
                    last_seen         = NOW()
            """, (lid, occ, occ, occ))

            # hourly_stats: incremental running average per hour
            cur.execute("""
                INSERT INTO hourly_stats (lot_id, hour, avg_occupancy_pct, reading_count)
                VALUES (%s, %s, %s, 1)
                ON CONFLICT (lot_id, hour) DO UPDATE SET
                    avg_occupancy_pct = (hourly_stats.avg_occupancy_pct * hourly_stats.reading_count + EXCLUDED.avg_occupancy_pct)
                                        / (hourly_stats.reading_count + 1),
                    reading_count     = hourly_stats.reading_count + 1
            """, (lid, hour, occ))

            conn.commit()
    except Exception as exc:
        log.error("DB update error: %s", exc)


def _start_consumer():
    def run():
        while True:
            try:
                conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                ch = conn.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
                q = ch.queue_declare("analytics_q", durable=True)
                ch.queue_bind(exchange=EXCHANGE, queue=q.method.queue, routing_key=ROUTING_KEY)

                def on_message(ch, method, props, body):
                    _update(json.loads(body))
                    ch.basic_ack(method.delivery_tag)

                ch.basic_qos(prefetch_count=10)
                ch.basic_consume(queue=q.method.queue, on_message_callback=on_message, auto_ack=False)
                log.info("Analytics consumer ready")
                ch.start_consuming()
            except Exception as exc:
                log.warning("Consumer error: %s — retry in 5s", exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _wait_db()
    _start_consumer()
    yield


app = FastAPI(title="Analytics Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.get("/analytics/summary")
def summary():
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COUNT(*)                              AS total_lots,
                ROUND(AVG(avg_occupancy_pct)::numeric, 1) AS avg_occupancy_pct,
                SUM(reading_count)                    AS total_readings,
                MIN(last_seen)                        AS first_reading,
                MAX(last_seen)                        AS last_reading
            FROM lot_stats
        """)
        return dict(cur.fetchone())


@app.get("/analytics/lots")
def lots_stats():
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                lot_id,
                ROUND(avg_occupancy_pct::numeric, 1) AS avg_occupancy_pct,
                ROUND(max_occupancy_pct::numeric, 1) AS max_occupancy_pct,
                ROUND(min_occupancy_pct::numeric, 1) AS min_occupancy_pct,
                reading_count,
                last_seen
            FROM lot_stats
            ORDER BY avg_occupancy_pct DESC
        """)
        return [dict(r) for r in cur.fetchall()]


@app.get("/analytics/hourly/{lot_id}")
def hourly_profile(lot_id: str):
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT hour,
                   ROUND(avg_occupancy_pct::numeric, 1) AS avg_occupancy_pct,
                   reading_count
            FROM hourly_stats
            WHERE lot_id = %s
            ORDER BY hour
        """, (lot_id,))
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(404, detail="Bu lot için veri yok")
    return [dict(r) for r in rows]


@app.get("/analytics/health")
def health():
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM lot_stats")
            count = cur.fetchone()[0]
        return {"status": "ok", "tracked_lots": count}
    except Exception as exc:
        raise HTTPException(503, detail=str(exc))
