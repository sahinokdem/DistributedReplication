-- =====================================================================
-- Data Replication in a Single-Leader Environment
-- Schema & replication logging (domain: simple product / stock catalog)
--
-- NOTE: Run this ONLY on the LEADER. It replicates to the follower
--       automatically, since the follower is a read-only physical standby.
-- =====================================================================

-- Clean start (safe to re-run during development)
DROP TABLE IF EXISTS consistency_observations CASCADE;
DROP TABLE IF EXISTS replication_lag_samples  CASCADE;
DROP TABLE IF EXISTS read_log                 CASCADE;
DROP TABLE IF EXISTS write_log                CASCADE;
DROP TABLE IF EXISTS experiment_runs          CASCADE;
DROP TABLE IF EXISTS products                 CASCADE;


-- ---------------------------------------------------------------------
-- 1) products : the main entity the experiments operate on.
--    Carries the tracking fields the experiments need: a version number,
--    a last_updated timestamp, and an operation_id. stock_qty is the
--    value that changes on UPDATE.
-- ---------------------------------------------------------------------
CREATE TABLE products (
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          TEXT NOT NULL UNIQUE,                          -- product code
    name         TEXT NOT NULL,
    stock_qty    INTEGER NOT NULL DEFAULT 0,                    -- changes on UPDATE
    price        NUMERIC(10,2) NOT NULL DEFAULT 0,
    version      INTEGER NOT NULL DEFAULT 1,                    -- monotonic reads test
    operation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    is_deleted   BOOLEAN NOT NULL DEFAULT FALSE                 -- soft delete (visibility analysis)
);


-- ---------------------------------------------------------------------
-- 2) write_log : every write on products is logged here AUTOMATICALLY
--    (via the trigger below), with a timestamp and the WAL LSN at write
--    time. This is the replication-logging requirement: it makes the
--    order in which writes hit the WAL explicit.
-- ---------------------------------------------------------------------
CREATE TABLE write_log (
    log_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id     BIGINT,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('INSERT','UPDATE','DELETE')),
    version_after  INTEGER,
    operation_id   UUID,
    client_id      TEXT,                                        -- which client issued the write
    written_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    wal_lsn        PG_LSN DEFAULT pg_current_wal_lsn()          -- WAL position on the leader
);


-- ---------------------------------------------------------------------
-- 3) read_log : every read performed DURING an experiment is logged.
--    Reads may come from the leader or the follower; this is what lets
--    us detect monotonic-reads violations (a later read seeing an older
--    version).
--
--    NOTE: written on the LEADER even when reading the follower,
--          because the follower is read-only.
-- ---------------------------------------------------------------------
CREATE TABLE read_log (
    read_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       BIGINT,
    product_id   BIGINT,
    version_seen INTEGER,
    read_from    TEXT NOT NULL CHECK (read_from IN ('leader','follower')),
    client_id    TEXT,
    read_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);


-- ---------------------------------------------------------------------
-- 4) replication_lag_samples : periodic snapshots of replication lag,
--    in bytes (LSN difference) and in time. Makes lag measurable and
--    chartable instead of merely asserted.
-- ---------------------------------------------------------------------
CREATE TABLE replication_lag_samples (
    sample_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    replica_addr TEXT,
    state        TEXT,
    sent_lsn     PG_LSN,
    replay_lsn   PG_LSN,
    lag_bytes    BIGINT,                                        -- pg_wal_lsn_diff(sent_lsn, replay_lsn)
    write_lag    INTERVAL,
    flush_lag    INTERVAL,
    replay_lag   INTERVAL,
    sampled_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);


-- ---------------------------------------------------------------------
-- 5) experiment_runs : metadata for each experiment execution, so every
--    log row and observation can be tied back to a specific run.
-- ---------------------------------------------------------------------
CREATE TABLE experiment_runs (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    experiment_name TEXT NOT NULL,                              -- 'eventual','monotonic','raw','concurrent'
    parameters      JSONB,                                      -- e.g. {"poll_interval_s":1,"timeout_s":60}
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ended_at        TIMESTAMPTZ,
    notes           TEXT
);


-- ---------------------------------------------------------------------
-- 6) consistency_observations : computed results / metrics per run
--    (convergence time, monotonic violation count, RAW success, ...).
-- ---------------------------------------------------------------------
CREATE TABLE consistency_observations (
    obs_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id      BIGINT REFERENCES experiment_runs(run_id),
    product_id  BIGINT,
    metric      TEXT NOT NULL,                                  -- 'convergence_seconds','monotonic_violations', ...
    value_num   DOUBLE PRECISION,
    value_text  TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);


-- ---------------------------------------------------------------------
-- Logging mechanism: a trigger that fills write_log on every
-- INSERT / UPDATE / DELETE against products.
--
-- A client may tag itself with:  SET app.client_id = 'writer-1';
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION log_write() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO write_log(product_id, operation_type, version_after, operation_id, client_id)
        VALUES (OLD.product_id, 'DELETE', OLD.version, OLD.operation_id,
                current_setting('app.client_id', true));
        RETURN OLD;
    ELSE
        INSERT INTO write_log(product_id, operation_type, version_after, operation_id, client_id)
        VALUES (NEW.product_id, TG_OP, NEW.version, NEW.operation_id,
                current_setting('app.client_id', true));
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_write
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW EXECUTE FUNCTION log_write();


-- Quick sanity data
INSERT INTO products(sku, name, stock_qty, price)
VALUES ('SKU-001', 'Wireless Mouse', 100, 19.90);