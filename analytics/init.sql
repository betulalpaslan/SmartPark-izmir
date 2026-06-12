CREATE TABLE IF NOT EXISTS lot_stats (
    lot_id           TEXT PRIMARY KEY,
    avg_occupancy_pct FLOAT   NOT NULL DEFAULT 0,
    max_occupancy_pct FLOAT   NOT NULL DEFAULT 0,
    min_occupancy_pct FLOAT   NOT NULL DEFAULT 100,
    reading_count     INTEGER NOT NULL DEFAULT 0,
    last_seen         TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hourly_stats (
    lot_id            TEXT    NOT NULL,
    hour              INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    avg_occupancy_pct FLOAT   NOT NULL DEFAULT 0,
    reading_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (lot_id, hour)
);
