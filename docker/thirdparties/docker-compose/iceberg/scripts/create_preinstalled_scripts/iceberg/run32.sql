-- DORIS-29047: float/double tables holding NaN, for the range-filter pushdown regression.
-- Doris orders NaN above every other value, while Iceberg's file metrics exclude NaN from the
-- lower/upper bounds and mark an all-NaN column via nan_value_counts, so a bare `greaterThan`
-- pushdown prunes files that actually hold matching rows.
-- This script is sourced on every Iceberg container start, so keep it repeatable.

CREATE DATABASE IF NOT EXISTS demo.test_db;
USE demo.test_db;

DROP TABLE IF EXISTS nan_filter_double;

CREATE TABLE nan_filter_double (
    id INT,
    d DOUBLE
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

-- One file per INSERT (COALESCE(1) keeps each append to a single task), so the four metrics
-- shapes the evaluators branch on stay separable, and inputSplitNum pins which ones got pruned:
--   file 1: {1.0, NaN}  -> bounds [1.0, 1.0], nan_value_count=1 (the NaN is invisible in bounds)
--   file 2: {NaN}       -> no bounds at all, nan_value_count=1 (all-NaN shortcut)
--   file 3: {8.0}       -> bounds [8.0, 8.0], nan_value_count=0
--   file 4: {1.0}       -> bounds [1.0, 1.0], nan_value_count=0 (must STILL be pruned by d > 5)
INSERT INTO nan_filter_double
SELECT /*+ COALESCE(1) */ * FROM VALUES (1, CAST(1.0 AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)) AS v(id, d);
INSERT INTO nan_filter_double
SELECT /*+ COALESCE(1) */ * FROM VALUES (3, CAST('NaN' AS DOUBLE)) AS v(id, d);
INSERT INTO nan_filter_double
SELECT /*+ COALESCE(1) */ * FROM VALUES (4, CAST(8.0 AS DOUBLE)) AS v(id, d);
INSERT INTO nan_filter_double
SELECT /*+ COALESCE(1) */ * FROM VALUES (5, CAST(1.0 AS DOUBLE)) AS v(id, d);

DROP TABLE IF EXISTS nan_filter_float;

CREATE TABLE nan_filter_float (
    id INT,
    f FLOAT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

--   file 1: {1.0, NaN}  -> bounds [1.0, 1.0], nan_value_count=1
--   file 2: {2.0}       -> bounds [2.0, 2.0], nan_value_count=0 (must STILL be pruned by f > 5)
INSERT INTO nan_filter_float
SELECT /*+ COALESCE(1) */ * FROM VALUES (1, CAST(1.0 AS FLOAT)), (2, CAST('NaN' AS FLOAT)) AS v(id, f);
INSERT INTO nan_filter_float
SELECT /*+ COALESCE(1) */ * FROM VALUES (3, CAST(2.0 AS FLOAT)) AS v(id, f);
