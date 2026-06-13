# SmartPark İzmir

İzmir'deki otopark doluluk verilerini gerçek zamanlı izleyen, dinamik fiyatlandırma ve rota önerisi sunan mikroservis tabanlı akıllı otopark yönetim sistemi.

## Mimari

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kullanıcı (Tarayıcı)                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTP / WebSocket
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      API Gateway (Traefik)                           │
│   /occupancy  /forecast  /recommend  /pricing  /analytics  /ws      │
└───┬───────┬───────┬───────────┬──────────┬──────────────────────────┘
    │       │       │           │          │
    ▼       ▼       ▼           ▼          ▼
┌───────┐ ┌────────────┐ ┌──────────────────┐ ┌─────────────┐ ┌────────────┐
│Occup. │ │Forecasting │ │Pricing & Routing │ │Notification │ │ Analytics  │
│State  │ │  Service   │ │    Service       │ │  Service    │ │  Service   │
│       │ │            │ │ ┌──────────────┐ │ │             │ │            │
│ Redis │ │TimescaleDB │ │ │Circuit Break.│ │ │  WebSocket  │ │ PostgreSQL │
└───┬───┘ └─────┬──────┘ │ │  + Retry    │ │ └──────┬──────┘ └────┬───────┘
    │           │         │ └──────────────┘ │       │             │
    │           │         └──────────────────┘       │             │
    │           │                                     │             │
    └───────────┴─────────────────────────────────────┴─────────────┘
                               ▲ RabbitMQ (Topic Exchange)
                               │ parking.occupancy.changed
                    ┌──────────┴──────────┐
                    │   Data Ingestion    │
                    │  (her 30 saniyede)  │
                    └──────────┬──────────┘
                               │ polling
                    ┌──────────┴──────────┐
                    │  İzmir Büyükşehir   │
                    │  Belediyesi API     │
                    └─────────────────────┘
```

## Uygulanan Mikroservis Kalıpları

| Kalıp | Nerede | Açıklama |
|-------|--------|----------|
| **API Gateway** | Traefik | Tüm dış trafiği tek noktadan yönlendirir |
| **Event-Driven Architecture** | RabbitMQ Topic Exchange | Servisler birbirinden habersiz, mesaj üzerinden haberleşir |
| **Database per Service** | 3× PostgreSQL, 1× TimescaleDB, 1× Redis | Her servis kendi veritabanına sahip, paylaşım yok |
| **Circuit Breaker** | pricing-routing | Downstream servis çökerse kaskad hata yerine fail-fast davranışı |
| **Retry + Exponential Backoff** | pricing-routing | Geçici hatalar için 1s → 2s → 4s bekleme süresiyle yeniden deneme |
| **Health Check** | Tüm servisler | `/health` endpoint; bağımlılık durumunu raporlar |
| **Correlation ID** | Tüm servisler | `X-Correlation-ID` header ile istekler servis zincirinde izlenebilir |
| **Service Discovery** | Docker DNS | Servisler birbirini isim üzerinden (container adı) bulur |

## Servisler

| Servis | Port (iç) | Teknoloji | Görev |
|--------|-----------|-----------|-------|
| data-ingestion | — | Python + APScheduler | İzmir API'sini 30s'de bir çeker, değişimleri RabbitMQ'ya yayar |
| occupancy-state | 8000 | FastAPI + Redis | Anlık doluluk durumunu tutar, coğrafi sorgu sunar |
| forecasting | 8000 | FastAPI + TimescaleDB | EWMA + saatlik profil ile 30dk öngörüsü üretir |
| pricing-routing | 8000 | FastAPI + PostgreSQL | Dinamik fiyat hesaplar, en iyi otoparkı önerir |
| notification | 8000 | FastAPI + WebSocket | Değişiklikleri bağlı tarayıcılara anlık iletir |
| analytics | 8000 | FastAPI + PostgreSQL | Saatlik ve genel istatistik biriktirir |
| frontend | 80 | Nginx + Leaflet.js | Harita, arama ve analitik arayüzü |

## Gereksinimler

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows / macOS)  
  veya Docker Engine + Docker Compose (Linux)

Başka hiçbir şey kurmanıza gerek yok. Python, PostgreSQL, Redis vs. Docker içinde çalışır.

## Kurulum ve Çalıştırma

```bash
git clone https://github.com/<kullanici>/smartpark-izmir.git
cd smartpark-izmir
docker compose up --build
```

İlk açılış image'ları indirdiği için 2-3 dakika sürebilir.

Tarayıcıdan açın: **http://localhost**

### Diğer Arayüzler

| Adres | Ne |
|-------|----|
| http://localhost | SmartPark uygulaması |
| http://localhost:8080 | Traefik dashboard (yönlendirme durumu) |
| http://localhost:15672 | RabbitMQ yönetim paneli (`guest` / `guest`) |

### Durdurma

```bash
docker compose down          # servisleri durdurur
docker compose down -v       # servisleri + veritabanı verilerini siler
```

## Testler

Unit testler servisleri Docker ile ayağa kaldırmadan kritik iş kurallarını kontrol eder:

- İzmir API yanıtlarının normalize edilmesi ve doluluk yüzdesi hesabı
- Dinamik fiyat çarpanı ve mesafe hesabı
- Frontend'in HTML5 geolocation koordinatlarını `/recommend` isteğine eklemesi

Standart Python test runner ile:

```bash
python -m unittest discover -s tests
```

Pytest ile:

```bash
pip install -r requirements-dev.txt
pytest
```

## API Endpoint'leri

### Occupancy State
```
GET /occupancy                          → tüm otoparklar
GET /occupancy/{lot_id}                 → tek otopark
GET /occupancy/near?lat=&lng=&radius=   → yakındaki otoparklar
GET /health                             → servis sağlık durumu
```

### Forecasting
```
GET /forecast/{lot_id}?horizon=30m      → doluluk tahmini
GET /health                             → servis sağlık durumu
```

### Pricing & Routing
```
GET /recommend?userLat=&userLng=&destLat=&destLng=&duration_hours=2
GET /pricing/{lot_id}?duration_hours=1
GET /health                             → circuit breaker durumları dahil
```

### Analytics
```
GET /analytics/summary                  → sistem geneli istatistik
GET /analytics/lots                     → otopark bazlı istatistik
GET /analytics/hourly/{lot_id}          → saatlik doluluk profili
GET /analytics/health
```

### Notification
```
WS  /ws                                 → gerçek zamanlı güncellemeler
GET /notifications/status
GET /notifications/health
```

## Circuit Breaker Davranışı

`pricing-routing` servisi, `occupancy-state` ve `forecasting`'e çağrı yaparken circuit breaker kullanır:

```
Normal:   CLOSED  → servis çağrısı yapılır
5 hata:   OPEN    → çağrı yapılmaz, anında 503 döner (fail-fast)
30 sn:    HALF_OPEN → bir probe çağrısı yapılır
Başarılı: CLOSED  → normal işleme geri döner
```

`GET /health` endpoint'inden anlık circuit durumu görülebilir.

## Teknoloji Yığını

- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Message Broker:** RabbitMQ 3.13
- **Veritabanları:** PostgreSQL 16, TimescaleDB (zaman serisi), Redis 7
- **API Gateway:** Traefik v3
- **Frontend:** Vanilla JS, Leaflet.js, OpenStreetMap
- **Altyapı:** Docker Compose
