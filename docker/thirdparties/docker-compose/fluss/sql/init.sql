-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Regression fixtures for the fluss catalog. Data is static: the suites read
-- it, they never write. __FLUSS_BOOTSTRAP_SERVERS__ is substituted by
-- scripts/run-init-sql.sh.
--
-- This is the first of two scripts. Everything a lake table should hold in
-- PAIMON is written here; init-lake-tail.sql then writes the rows that must
-- stay in the fluss log. scripts/run-init-sql.sh stops the tiering service in
-- between, which is what freezes the split -- otherwise the tail would drift
-- into the lake at the next tiering round and every assertion about how the
-- two halves divide would decay into "it depends when you ran it".

SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '2';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = '__FLUSS_BOOTSTRAP_SERVERS__'
);

USE CATALOG fluss_catalog;

-- Recreated from scratch so that rerunning this script is idempotent.
DROP DATABASE IF EXISTS fluss_test CASCADE;
CREATE DATABASE fluss_test;
USE fluss_test;

-- ---------------------------------------------------------------------------
-- log_basic: plain append-only table, carries table and column comments so the
-- catalog suite can check that they survive the metadata mapping.
-- ---------------------------------------------------------------------------
CREATE TABLE log_basic (
    id INT COMMENT 'row id',
    name STRING COMMENT 'row name',
    price DECIMAL(10, 2)
) COMMENT 'fluss log table for regression'
WITH (
    'bucket.num' = '3'
);

INSERT INTO log_basic VALUES
    (1, 'alice', CAST(10.10 AS DECIMAL(10, 2))),
    (2, 'bob', CAST(20.20 AS DECIMAL(10, 2))),
    (3, 'carol', CAST(30.30 AS DECIMAL(10, 2)));

-- ---------------------------------------------------------------------------
-- log_types: one column per fluss data type that the connector maps, plus an
-- all-NULL row. TIME is deliberately absent: it has no Doris counterpart and
-- gets its own negative fixture later.
-- ---------------------------------------------------------------------------
CREATE TABLE log_types (
    id INT,
    f_boolean BOOLEAN,
    f_tinyint TINYINT,
    f_smallint SMALLINT,
    f_int INT,
    f_bigint BIGINT,
    f_float FLOAT,
    f_double DOUBLE,
    f_decimal DECIMAL(20, 4),
    f_char CHAR(5),
    f_string STRING,
    f_binary BINARY(3),
    f_bytes BYTES,
    f_date DATE,
    f_timestamp TIMESTAMP(6),
    f_timestamp_ltz TIMESTAMP_LTZ(3),
    f_array ARRAY<INT>,
    f_map MAP<STRING, INT>,
    f_row ROW<r_int INT, r_string STRING>
) WITH (
    'bucket.num' = '1'
);

INSERT INTO log_types VALUES
    (
        1,
        TRUE,
        CAST(1 AS TINYINT),
        CAST(2 AS SMALLINT),
        3,
        CAST(4 AS BIGINT),
        CAST(1.5 AS FLOAT),
        CAST(2.5 AS DOUBLE),
        CAST(123.4567 AS DECIMAL(20, 4)),
        CAST('char1' AS CHAR(5)),
        'string1',
        CAST(X'010203' AS BINARY(3)),
        CAST(X'0a0b' AS BYTES),
        DATE '2026-01-01',
        TIMESTAMP '2026-01-01 01:02:03.456789',
        CAST(TIMESTAMP '2026-01-01 01:02:03.456' AS TIMESTAMP_LTZ(3)),
        ARRAY[1, 2, 3],
        MAP['k1', 1, 'k2', 2],
        CAST(ROW(1, 'nested1') AS ROW<r_int INT, r_string STRING>)
    ),
    (
        2,
        FALSE,
        CAST(-1 AS TINYINT),
        CAST(-2 AS SMALLINT),
        -3,
        CAST(-4 AS BIGINT),
        CAST(-1.5 AS FLOAT),
        CAST(-2.5 AS DOUBLE),
        CAST(-123.4567 AS DECIMAL(20, 4)),
        CAST('char2' AS CHAR(5)),
        'string2',
        CAST(X'040506' AS BINARY(3)),
        CAST(X'0c0d' AS BYTES),
        DATE '2026-01-02',
        TIMESTAMP '2026-01-02 01:02:03.456789',
        CAST(TIMESTAMP '2026-01-02 01:02:03.456' AS TIMESTAMP_LTZ(3)),
        ARRAY[4, 5],
        MAP['k3', 3],
        CAST(ROW(2, 'nested2') AS ROW<r_int INT, r_string STRING>)
    ),
    (
        3,
        CAST(NULL AS BOOLEAN),
        CAST(NULL AS TINYINT),
        CAST(NULL AS SMALLINT),
        CAST(NULL AS INT),
        CAST(NULL AS BIGINT),
        CAST(NULL AS FLOAT),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DECIMAL(20, 4)),
        CAST(NULL AS CHAR(5)),
        CAST(NULL AS STRING),
        CAST(NULL AS BINARY(3)),
        CAST(NULL AS BYTES),
        CAST(NULL AS DATE),
        CAST(NULL AS TIMESTAMP(6)),
        CAST(NULL AS TIMESTAMP_LTZ(3)),
        CAST(NULL AS ARRAY<INT>),
        CAST(NULL AS MAP<STRING, INT>),
        CAST(NULL AS ROW<r_int INT, r_string STRING>)
    );

-- ---------------------------------------------------------------------------
-- log_part: partitioned append-only table. Partitions are created on write
-- (fluss dynamic partitioning), so the partition set is exactly the one below.
-- ---------------------------------------------------------------------------
CREATE TABLE log_part (
    id INT,
    name STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket.num' = '2'
);

INSERT INTO log_part VALUES
    (1, 'p1a', '20260101'),
    (2, 'p1b', '20260101'),
    (3, 'p2a', '20260102'),
    (4, 'p3a', '20260103');

-- ---------------------------------------------------------------------------
-- log_empty: never written to. Every bucket's latest offset is 0, so planning
-- must emit no scan range at all rather than ranges that read nothing.
-- ---------------------------------------------------------------------------
CREATE TABLE log_empty (
    id INT,
    name STRING
) WITH (
    'bucket.num' = '2'
);

-- ---------------------------------------------------------------------------
-- pk_basic: primary-key table. Row 2 is updated and row 3 deleted, so a
-- correct read returns the merged view, not the raw change log.
-- ---------------------------------------------------------------------------
CREATE TABLE pk_basic (
    id INT NOT NULL,
    name STRING,
    score DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

INSERT INTO pk_basic VALUES
    (1, 'k1', CAST(1.5 AS DOUBLE)),
    (2, 'k2', CAST(2.5 AS DOUBLE)),
    (3, 'k3', CAST(3.5 AS DOUBLE)),
    (4, 'k4', CAST(4.5 AS DOUBLE));

INSERT INTO pk_basic VALUES
    (2, 'k2-updated', CAST(22.5 AS DOUBLE));

SET 'execution.runtime-mode' = 'batch';
DELETE FROM pk_basic WHERE id = 3;
SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- pk_types: same type coverage as log_types, but stored in the kv (compacted)
-- row format that primary-key tables use.
-- ---------------------------------------------------------------------------
CREATE TABLE pk_types (
    id INT NOT NULL,
    f_boolean BOOLEAN,
    f_tinyint TINYINT,
    f_smallint SMALLINT,
    f_int INT,
    f_bigint BIGINT,
    f_float FLOAT,
    f_double DOUBLE,
    f_decimal DECIMAL(20, 4),
    f_char CHAR(5),
    f_string STRING,
    f_binary BINARY(3),
    f_bytes BYTES,
    f_date DATE,
    f_timestamp TIMESTAMP(6),
    f_timestamp_ltz TIMESTAMP_LTZ(3),
    f_array ARRAY<INT>,
    f_map MAP<STRING, INT>,
    f_row ROW<r_int INT, r_string STRING>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '1'
);

INSERT INTO pk_types VALUES
    (
        1,
        TRUE,
        CAST(1 AS TINYINT),
        CAST(2 AS SMALLINT),
        3,
        CAST(4 AS BIGINT),
        CAST(1.5 AS FLOAT),
        CAST(2.5 AS DOUBLE),
        CAST(123.4567 AS DECIMAL(20, 4)),
        CAST('char1' AS CHAR(5)),
        'string1',
        CAST(X'010203' AS BINARY(3)),
        CAST(X'0a0b' AS BYTES),
        DATE '2026-01-01',
        TIMESTAMP '2026-01-01 01:02:03.456789',
        CAST(TIMESTAMP '2026-01-01 01:02:03.456' AS TIMESTAMP_LTZ(3)),
        ARRAY[1, 2, 3],
        MAP['k1', 1, 'k2', 2],
        CAST(ROW(1, 'nested1') AS ROW<r_int INT, r_string STRING>)
    ),
    (
        2,
        CAST(NULL AS BOOLEAN),
        CAST(NULL AS TINYINT),
        CAST(NULL AS SMALLINT),
        CAST(NULL AS INT),
        CAST(NULL AS BIGINT),
        CAST(NULL AS FLOAT),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DECIMAL(20, 4)),
        CAST(NULL AS CHAR(5)),
        CAST(NULL AS STRING),
        CAST(NULL AS BINARY(3)),
        CAST(NULL AS BYTES),
        CAST(NULL AS DATE),
        CAST(NULL AS TIMESTAMP(6)),
        CAST(NULL AS TIMESTAMP_LTZ(3)),
        CAST(NULL AS ARRAY<INT>),
        CAST(NULL AS MAP<STRING, INT>),
        CAST(NULL AS ROW<r_int INT, r_string STRING>)
    );

-- ---------------------------------------------------------------------------
-- pk_part: partitioned primary-key table. A partitioned primary-key table is
-- snapshotted per partition, so reading it wrong -- asking the table for its
-- snapshots instead of the partition -- resumes the change log at another
-- partition's offset. One row of one partition is updated and one deleted, so
-- the merge has to happen inside a partition and not across the table.
-- ---------------------------------------------------------------------------
CREATE TABLE pk_part (
    id INT NOT NULL,
    name STRING,
    dt STRING NOT NULL,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'bucket.num' = '2'
);

INSERT INTO pk_part VALUES
    (1, 'q1a', '20260101'),
    (2, 'q1b', '20260101'),
    (3, 'q2a', '20260102'),
    (4, 'q2b', '20260102');

INSERT INTO pk_part VALUES
    (2, 'q1b-updated', '20260101');

SET 'execution.runtime-mode' = 'batch';
DELETE FROM pk_part WHERE id = 4 AND dt = '20260102';
SET 'execution.runtime-mode' = 'streaming';

-- ===========================================================================
-- Lake tables. 'table.datalake.enabled' makes the fluss coordinator create a
-- matching paimon table and lets the tiering service move data into it; the
-- lake settings themselves (warehouse, metastore) come from the cluster config
-- and are copied into each table's properties, which is where the Doris
-- connector reads them from.
--
-- Freshness is the lag the tiering service is asked to keep. Three minutes by
-- default, which every environment start would then have to wait out.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- lake_log: the ordinary union-read fixture. Three buckets and only two rows in
-- the tail, so at least one bucket is fully tiered and must contribute no log
-- range at all, while the others resume where the lake stops.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_log (
    id INT,
    name STRING,
    price DECIMAL(10, 2)
) COMMENT 'fluss log table tiered into paimon'
WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_log VALUES
    (1, 'lake1', CAST(1.10 AS DECIMAL(10, 2))),
    (2, 'lake2', CAST(2.20 AS DECIMAL(10, 2))),
    (3, 'lake3', CAST(3.30 AS DECIMAL(10, 2))),
    (4, 'lake4', CAST(4.40 AS DECIMAL(10, 2)));

-- ---------------------------------------------------------------------------
-- lake_cold: written once and never again, so after the tiering service has
-- caught up the lake holds the whole table and planning must emit no log range
-- whatsoever. One bucket, so "no log range" is an exact number and not a range.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_cold (
    id INT,
    name STRING
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_cold VALUES
    (1, 'cold1'),
    (2, 'cold2'),
    (3, 'cold3');

-- ---------------------------------------------------------------------------
-- lake_types: the type-parity fixture. The connector's fluss->Doris mapping is
-- required to equal fluss->paimon->Doris, so that the table and its $lake
-- sibling present one schema and not two; this is the only place that identity
-- is checked against the real paimon connector instead of by reading both
-- mappings side by side. One bucket keeps the split between the two halves
-- exact: everything here is tiered, the all-NULL row stays in the log.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_types (
    id INT,
    f_boolean BOOLEAN,
    f_tinyint TINYINT,
    f_smallint SMALLINT,
    f_int INT,
    f_bigint BIGINT,
    f_float FLOAT,
    f_double DOUBLE,
    f_decimal DECIMAL(20, 4),
    f_char CHAR(5),
    f_string STRING,
    f_binary BINARY(3),
    f_bytes BYTES,
    f_date DATE,
    f_timestamp TIMESTAMP(6),
    f_timestamp_ltz TIMESTAMP_LTZ(3),
    f_array ARRAY<INT>,
    f_map MAP<STRING, INT>,
    f_row ROW<r_int INT, r_string STRING>
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_types VALUES
    (
        1,
        TRUE,
        CAST(1 AS TINYINT),
        CAST(2 AS SMALLINT),
        3,
        CAST(4 AS BIGINT),
        CAST(1.5 AS FLOAT),
        CAST(2.5 AS DOUBLE),
        CAST(123.4567 AS DECIMAL(20, 4)),
        CAST('char1' AS CHAR(5)),
        'string1',
        CAST(X'010203' AS BINARY(3)),
        CAST(X'0a0b' AS BYTES),
        DATE '2026-01-01',
        TIMESTAMP '2026-01-01 01:02:03.456789',
        CAST(TIMESTAMP '2026-01-01 01:02:03.456' AS TIMESTAMP_LTZ(3)),
        ARRAY[1, 2, 3],
        MAP['k1', 1, 'k2', 2],
        CAST(ROW(1, 'nested1') AS ROW<r_int INT, r_string STRING>)
    );

-- ---------------------------------------------------------------------------
-- lake_part: partitioned lake table. The tail goes into one partition only, so
-- the other one is served entirely from the lake -- the two halves have to be
-- stitched per (partition, bucket) and not per table.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_part (
    id INT,
    name STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_part VALUES
    (1, 'lp1a', '20260101'),
    (2, 'lp1b', '20260101'),
    (3, 'lp2a', '20260102');

-- ---------------------------------------------------------------------------
-- lake_pk: primary-key table tiered into paimon. Merging a lake with a change
-- log BY KEY is not implemented, so the connector refuses this table unless the
-- lake is switched off -- the fixture exists to pin that refusal, to check that
-- the fluss-only read still returns the whole table, and to let $lake read the
-- paimon side directly. Row 2 is updated before tiering, so the lake already
-- holds a merged view rather than a raw change log.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_pk (
    id INT NOT NULL,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_pk VALUES
    (1, 'lp1'),
    (2, 'lp2'),
    (3, 'lp3');

INSERT INTO lake_pk VALUES
    (2, 'lp2-lake');
