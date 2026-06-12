import os
import json
import time
import logging
import requests
import pika
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_URL = "https://openapi.izmir.bel.tr/api/ibb/izum/otoparklar"
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"

_prev: dict[str, str] = {}  # lot_id -> "free:capacity" for delta detection


def normalize(raw: dict) -> dict | None:
    try:
        lot_id = str(
            raw.get("ufid") or raw.get("Id") or raw.get("id") or raw.get("OtoparkId") or ""
        ).strip()
        if not lot_id:
            return None

        occ = raw.get("occupancy", {}).get("total", {})
        free = int(occ.get("free") or raw.get("BosKapasite") or raw.get("Bos") or raw.get("bosKapasite") or 0)
        occupied = int(occ.get("occupied") or 0)
        capacity = int(
            raw.get("Kapasite") or raw.get("kapasite") or raw.get("ToplamKapasite") or (free + occupied) or 0
        )
        if capacity <= 0:
            return None

        name = str(
            raw.get("name") or raw.get("Isim") or raw.get("isim") or raw.get("Ad") or raw.get("OtoparkAdi") or ""
        )
        lat = float(raw.get("lat") or raw.get("Enlem") or raw.get("enlem") or raw.get("KonumX") or 0)
        lng = float(raw.get("lng") or raw.get("Boylam") or raw.get("boylam") or raw.get("KonumY") or 0)

        return {
            "lot_id": lot_id,
            "name": name,
            "capacity": capacity,
            "free": free,
            "occupied": occupied,
            "occupancy_pct": round(occupied / capacity * 100, 1),
            "lat": lat,
            "lng": lng,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except (ValueError, TypeError):
        return None


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))


def poll_and_publish():
    global _prev
    try:
        resp = requests.get(API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("Value") or data.get("value") or data.get("data") or []

        conn = get_connection()
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

        changed = 0
        for raw in data:
            lot = normalize(raw)
            if lot is None:
                continue
            key = f"{lot['free']}:{lot['capacity']}"
            if _prev.get(lot["lot_id"]) == key:
                continue
            _prev[lot["lot_id"]] = key
            ch.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY,
                body=json.dumps(lot),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                ),
            )
            changed += 1

        conn.close()
        log.info("Poll OK — %d lot changed", changed)
    except Exception as exc:
        log.error("Poll error: %s", exc)


def wait_for_rabbitmq(retries: int = 20, delay: int = 5):
    for i in range(retries):
        try:
            c = get_connection()
            c.close()
            log.info("RabbitMQ ready")
            return
        except Exception:
            log.info("Waiting for RabbitMQ (%d/%d)…", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("RabbitMQ unavailable after retries")


if __name__ == "__main__":
    wait_for_rabbitmq()
    poll_and_publish()  # immediate first run to seed all lots

    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_publish, "interval", seconds=POLL_INTERVAL)
    scheduler.start()
    log.info("Data ingestion running — polling every %ds", POLL_INTERVAL)
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
