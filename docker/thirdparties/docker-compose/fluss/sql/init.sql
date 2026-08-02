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
-- Tables backed by the data lake are added together with the tiering service
-- when union read lands; everything here is fluss-only.

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
