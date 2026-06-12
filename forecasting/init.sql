CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS occupancy_readings (
    lot_id        TEXT        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    occupancy_pct FLOAT       NOT NULL,
    free          INTEGER,
    capacity      INTEGER,
    PRIMARY KEY (lot_id, ts)
);

SELECT create_hypertable('occupancy_readings', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_occ_lot ON occupancy_readings (lot_id, ts DESC);
