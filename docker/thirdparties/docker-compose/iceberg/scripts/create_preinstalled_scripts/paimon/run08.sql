use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

SET TIME ZONE '+08:00';

DROP TABLE IF EXISTS t_ts_ntz;
CREATE TABLE t_ts_ntz (
  id INT,
  ts TIMESTAMP,
  ts_ntz TIMESTAMP_NTZ
) USING paimon;

INSERT INTO t_ts_ntz VALUES
  (1, CAST('2025-08-12 06:00:00+00:00' AS TIMESTAMP), CAST('2025-08-12 06:00:00' AS TIMESTAMP_NTZ)),
  (2, CAST('2025-08-12 14:00:00+08:00' AS TIMESTAMP), CAST('2025-08-12 14:00:00' AS TIMESTAMP_NTZ));
