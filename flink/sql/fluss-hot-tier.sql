-- Phase 4: Stream raw ticks from Iggy into Fluss for sub-second SQL queries
--
-- Submit via: /opt/flink/bin/sql-client.sh embedded -f /opt/flink/sql/fluss-hot-tier.sql（渲染后，见 playbook_jobs.yaml）

SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar;file:///opt/flink/lib/fluss-flink-1.20-0.9.0-incubating.jar';

-- Source: raw ticks from Iggy
CREATE TABLE iggy_ticks (
  pair          STRING,
  price         STRING,
  best_bid      STRING,
  best_ask      STRING,
  volume_24h    STRING,
  `time`        STRING,
  `sequence`    BIGINT
) WITH (
  'connector' = 'iggy',
  'host'      = 'iggy',
  'stream'    = 'crypto',
  'topic'     = 'prices',
  'format'    = 'json'
);

-- Sink: Fluss log table for real-time SQL
CREATE CATALOG fluss_catalog WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
);

CREATE DATABASE IF NOT EXISTS fluss_catalog.crypto;

CREATE TABLE IF NOT EXISTS fluss_catalog.crypto.ticks (
  pair          STRING,
  price         DOUBLE,
  best_bid      DOUBLE,
  best_ask      DOUBLE,
  volume_24h    DOUBLE,
  event_time    TIMESTAMP(3),
  `sequence`    BIGINT
) WITH (
  'bucket.num' = '4'
);

-- Continuous job: Iggy -> Fluss (casts strings to proper types)
INSERT INTO fluss_catalog.crypto.ticks
SELECT
  pair,
  CAST(price AS DOUBLE),
  CAST(best_bid AS DOUBLE),
  CAST(best_ask AS DOUBLE),
  CAST(volume_24h AS DOUBLE),
  TO_TIMESTAMP(REPLACE(`time`, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
  `sequence`
FROM iggy_ticks;
