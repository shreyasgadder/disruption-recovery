-- ETG Analytics — Pipeline state and results tables
-- Runs once on first postgres boot via docker-entrypoint-initdb.d

-- Idempotency: tracks which partitions have been processed
CREATE TABLE IF NOT EXISTS processing_state (
    state_key       VARCHAR(255) PRIMARY KEY,
    partition_date  DATE,
    status          VARCHAR(50) DEFAULT 'IN_PROGRESS',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold layer: airline-level metrics per partition
CREATE TABLE IF NOT EXISTS airline_metrics (
    airline         VARCHAR(100),
    partition_date  DATE,
    total_events    BIGINT,
    total_revenue   NUMERIC(14, 2),
    anomaly_count   BIGINT,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (airline, partition_date)
);

-- Gold layer: flagged bookings for CS team review
CREATE TABLE IF NOT EXISTS flagged_bookings (
    booking_id      VARCHAR(255),
    partition_date  DATE,
    customer_name   VARCHAR(255),
    tier            VARCHAR(50),
    region          VARCHAR(50),
    airline         VARCHAR(100),
    event_count     BIGINT,
    total_revenue   NUMERIC(14, 2),
    latest_status   VARCHAR(100),
    conflict_type   VARCHAR(255),
    priority_score  INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_id, partition_date)
);

CREATE INDEX IF NOT EXISTS idx_flagged_priority
    ON flagged_bookings (partition_date, priority_score DESC);

CREATE INDEX IF NOT EXISTS idx_flagged_tier
    ON flagged_bookings (tier, partition_date);
