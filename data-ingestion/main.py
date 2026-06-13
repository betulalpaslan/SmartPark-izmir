import os
import json
import time
import random
import logging
import pika
from curl_cffi import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://host.docker.internal:9999/otoparklar")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"

_prev: dict[str, str] = {}

# Fallback: İzmir'deki gerçek otoparkların statik verisi (API erişilemez olduğunda kullanılır)
_MOCK_LOTS = [
    {"lot_id": "NEDAP-TR-IZM-014", "name": "14 Plevne Bul. Yol Kenarı Otopark",     "capacity": 110, "lat": 38.431993, "lng": 27.141187},
    {"lot_id": "NEDAP-TR-IZM-033", "name": "33 Hürriyet Bulvarı",                    "capacity": 40,  "lat": 38.424774, "lng": 27.141169},
    {"lot_id": "NEDAP-TR-IZM-005", "name": "05 Ali Çetinkaya Yol Kenarı Otopark",    "capacity": 28,  "lat": 38.433452, "lng": 27.147500},
    {"lot_id": "NEDAP-TR-IZM-004", "name": "04 Ziya Gökalp Yol Kenarı Otopark",     "capacity": 59,  "lat": 38.432585, "lng": 27.146680},
    {"lot_id": "NEDAP-TR-IZM-007", "name": "07 26. Ağustos Yol Kenarı Otopark",     "capacity": 14,  "lat": 38.431421, "lng": 27.144755},
    {"lot_id": "NEDAP-TR-IZM-008", "name": "08 Vasıf Çınar Yol Kenarı Otopark",    "capacity": 83,  "lat": 38.430783, "lng": 27.138697},
    {"lot_id": "NEDAP-TR-IZM-031", "name": "31 Şair Eşref Bulvarı",                 "capacity": 55,  "lat": 38.436050, "lng": 27.146689},
    {"lot_id": "NEDAP-TR-IZM-034", "name": "34 Sabancı Kültür Merkezi",              "capacity": 41,  "lat": 38.414551, "lng": 27.122476},
    {"lot_id": "NEDAP-TR-IZM-006", "name": "06 1393 Sk. Yol Kenarı Otopark",        "capacity": 61,  "lat": 38.432968, "lng": 27.145272},
    {"lot_id": "CPS-TR-IZM-M2-04", "name": "Tam Otomatik Alsancak Otopark",          "capacity": 280, "lat": 38.433614, "lng": 27.144863},
    {"lot_id": "CPS-TR-IZM-B2-01", "name": "Hatay Katlı Pazaryeri",                  "capacity": 328, "lat": 38.403563, "lng": 27.110060},
    {"lot_id": "CPS-TR-IZM-M2-02", "name": "Alsancak Yeraltı Otopark",               "capacity": 133, "lat": 38.433917, "lng": 27.147419},
    {"lot_id": "CPS-TR-IZM-K3-01", "name": "Bahriye Üçok Yeraltı Otoparkı",         "capacity": 268, "lat": 38.460267, "lng": 27.114406},
    {"lot_id": "CPS-TR-IZM-N2-01", "name": "Bostanlı Katlı Otopark",                 "capacity": 254, "lat": 38.458214, "lng": 27.099839},
    {"lot_id": "CPS-TR-IZM-M1-01", "name": "Konak Katlı Otopark",                    "capacity": 888, "lat": 38.415959, "lng": 27.129392},
]

# Başlangıç doluluk oranları (gerçek veriye dayalı)
_mock_occupied: dict[str, int] = {
    "NEDAP-TR-IZM-014": 109, "NEDAP-TR-IZM-033": 36,  "NEDAP-TR-IZM-005": 28,
    "NEDAP-TR-IZM-004": 58,  "NEDAP-TR-IZM-007": 13,  "NEDAP-TR-IZM-008": 83,
    "NEDAP-TR-IZM-031": 54,  "NEDAP-TR-IZM-034": 25,  "NEDAP-TR-IZM-006": 60,
    "CPS-TR-IZM-M2-04": 22,  "CPS-TR-IZM-B2-01": 231, "CPS-TR-IZM-M2-02": 133,
    "CPS-TR-IZM-K3-01": 190, "CPS-TR-IZM-N2-01": 173, "CPS-TR-IZM-M1-01": 227,
}


def _mock_data() -> list[dict]:
    """Her çağrıda hafif rastgele dalgalanma ekleyerek gerçekçi veri üretir."""
    result = []
    now = datetime.now(timezone.utc)
    hour = now.hour
    # Gün içi doluluk çarpanı: gece düşük, öğle/akşam yoğun
    peak = 1.0 + 0.3 * max(0.0, 1.0 - abs(hour - 12) / 6)

    for lot in _MOCK_LOTS:
        lid = lot["lot_id"]
        cap = lot["capacity"]
        occ = _mock_occupied[lid]
        # ±%5 rastgele değişim, kapasiteye ve günün saatine göre sınırlı
        delta = random.randint(-max(1, int(cap * 0.05)), max(1, int(cap * 0.05)))
        target = int(min(cap, max(0, occ + delta)) * peak / max(peak, 1.0))
        target = min(cap, max(0, target))
        _mock_occupied[lid] = target
        free = cap - target
        result.append({
            "lot_id": lid,
            "name": lot["name"],
            "capacity": cap,
            "free": free,
            "occupied": target,
            "occupancy_pct": round(target / cap * 100, 1),
            "lat": lot["lat"],
            "lng": lot["lng"],
            "timestamp": now.isoformat(),
        })
    return result


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
    lots: list[dict] = []
    source = "mock"

    try:
        resp = requests.get(API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("Value") or data.get("value") or data.get("data") or []
        lots = [l for raw in data if (l := normalize(raw)) is not None]
        source = "live"
    except Exception as exc:
        log.warning("Canli API erisilemedi (%s) — mock veri kullaniliyor", exc)
        lots = _mock_data()

    try:
        conn = get_connection()
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

        changed = 0
        for lot in lots:
            key = f"{lot['free']}:{lot['capacity']}"
            if source == "live" and _prev.get(lot["lot_id"]) == key:
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
        log.info("Poll OK [%s] — %d lot yayinlandi", source, changed)
    except Exception as exc:
        log.error("RabbitMQ publish hatasi: %s", exc)


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
    poll_and_publish()

    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_publish, "interval", seconds=POLL_INTERVAL)
    scheduler.start()
    log.info("Data ingestion running — polling every %ds", POLL_INTERVAL)
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
