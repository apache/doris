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
-- log_nested: complex types nested inside complex types. log_types covers one
-- column per type but never more than one level, and a decoder can be right
-- about a MAP<STRING,INT> and wrong about the MAP<STRING,ROW<..>> beside it --
-- the element decoder is chosen per level, and only a nested column asks for it
-- to be chosen twice. Every combination of the three constructors is here, plus
-- an all-NULL row, because a null at the outer level and a null element inside a
-- present collection are different things to get wrong.
-- ---------------------------------------------------------------------------
CREATE TABLE log_nested (
    id INT,
    f_arr_arr ARRAY<ARRAY<INT>>,
    f_arr_map ARRAY<MAP<STRING, INT>>,
    f_arr_row ARRAY<ROW<a INT, b STRING>>,
    f_map_arr MAP<STRING, ARRAY<INT>>,
    f_map_row MAP<STRING, ROW<a INT, b STRING>>,
    f_row_deep ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>,
    f_arr_arr_arr ARRAY<ARRAY<ARRAY<INT>>>
) WITH (
    'bucket.num' = '1'
);

INSERT INTO log_nested VALUES
    (
        1,
        ARRAY[ARRAY[1, 2], ARRAY[3]],
        ARRAY[MAP['a', 1], MAP['b', 2]],
        ARRAY[CAST(ROW(1, 'x') AS ROW<a INT, b STRING>), CAST(ROW(2, 'y') AS ROW<a INT, b STRING>)],
        MAP['k1', ARRAY[1, 2], 'k2', ARRAY[3]],
        MAP['k1', CAST(ROW(9, 'z') AS ROW<a INT, b STRING>)],
        CAST(ROW(1, ARRAY[7, 8], MAP['m', 3], ROW(5, 'deep'))
             AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>),
        ARRAY[ARRAY[ARRAY[1], ARRAY[2, 3]], ARRAY[ARRAY[4]]]
    ),
    (
        2,
        ARRAY[CAST(NULL AS ARRAY<INT>)],
        ARRAY[CAST(NULL AS MAP<STRING, INT>)],
        ARRAY[CAST(NULL AS ROW<a INT, b STRING>)],
        MAP['k1', CAST(NULL AS ARRAY<INT>)],
        MAP['k1', CAST(NULL AS ROW<a INT, b STRING>)],
        CAST(ROW(CAST(NULL AS INT), CAST(NULL AS ARRAY<INT>), CAST(NULL AS MAP<STRING, INT>),
                 CAST(NULL AS ROW<x INT, y STRING>))
             AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>),
        ARRAY[CAST(NULL AS ARRAY<ARRAY<INT>>)]
    ),
    (
        3,
        CAST(NULL AS ARRAY<ARRAY<INT>>),
        CAST(NULL AS ARRAY<MAP<STRING, INT>>),
        CAST(NULL AS ARRAY<ROW<a INT, b STRING>>),
        CAST(NULL AS MAP<STRING, ARRAY<INT>>),
        CAST(NULL AS MAP<STRING, ROW<a INT, b STRING>>),
        CAST(NULL AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>),
        CAST(NULL AS ARRAY<ARRAY<ARRAY<INT>>>)
    );

-- ---------------------------------------------------------------------------
-- pk_nested: the same nesting in the kv row format a primary-key table stores.
-- The two formats have separate readers for every constructor, so covering one
-- says nothing about the other.
-- ---------------------------------------------------------------------------
CREATE TABLE pk_nested (
    id INT NOT NULL,
    f_arr_arr ARRAY<ARRAY<INT>>,
    f_arr_row ARRAY<ROW<a INT, b STRING>>,
    f_map_arr MAP<STRING, ARRAY<INT>>,
    f_map_row MAP<STRING, ROW<a INT, b STRING>>,
    f_row_deep ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '1'
);

INSERT INTO pk_nested VALUES
    (
        1,
        ARRAY[ARRAY[1, 2], ARRAY[3]],
        ARRAY[CAST(ROW(1, 'x') AS ROW<a INT, b STRING>)],
        MAP['k1', ARRAY[1, 2]],
        MAP['k1', CAST(ROW(9, 'z') AS ROW<a INT, b STRING>)],
        CAST(ROW(1, ARRAY[7, 8], MAP['m', 3], ROW(5, 'deep'))
             AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>)
    ),
    (
        2,
        CAST(NULL AS ARRAY<ARRAY<INT>>),
        CAST(NULL AS ARRAY<ROW<a INT, b STRING>>),
        CAST(NULL AS MAP<STRING, ARRAY<INT>>),
        CAST(NULL AS MAP<STRING, ROW<a INT, b STRING>>),
        CAST(NULL AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>)
    );

-- The update is what makes this a primary-key fixture rather than a second log
-- one: the nested values of key 1 have to be the SECOND set, not the first.
INSERT INTO pk_nested VALUES
    (
        1,
        ARRAY[ARRAY[10, 20]],
        ARRAY[CAST(ROW(11, 'xx') AS ROW<a INT, b STRING>)],
        MAP['k9', ARRAY[10]],
        MAP['k9', CAST(ROW(99, 'zz') AS ROW<a INT, b STRING>)],
        CAST(ROW(2, ARRAY[70], MAP['mm', 30], ROW(50, 'deeper'))
             AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>)
    );

-- ---------------------------------------------------------------------------
-- part_types: one partition column of every type fluss allows AND Doris can
-- read back. A partition's value is kept nowhere but in its name, so the type
-- decides whether it survives at all -- STRING is the only one the rest of these
-- fixtures use, and it is also the only one that could not tell a rendering bug
-- from a working one.
--
-- Two rows, two partitions: one value of each column per partition, so a
-- predicate on any single column prunes to exactly one.
--
-- p_bin is the type whose verdict depends on the catalog: fluss names its
-- partitions with the hex text of the bytes, which reads back as the text it is
-- unless 'enable.mapping.varbinary' asks for a VARBINARY column instead.
-- ---------------------------------------------------------------------------
CREATE TABLE part_types (
    id INT,
    name STRING,
    p_str STRING,
    p_char CHAR(2),
    p_bool BOOLEAN,
    p_tiny TINYINT,
    p_small SMALLINT,
    p_int INT,
    p_big BIGINT,
    p_date DATE,
    p_bin BINARY(2)
) PARTITIONED BY (p_str, p_char, p_bool, p_tiny, p_small, p_int, p_big, p_date, p_bin)
WITH (
    'bucket.num' = '2'
);

INSERT INTO part_types VALUES
    (1, 'pt1', 'cn', CAST('c1' AS CHAR(2)), TRUE, CAST(1 AS TINYINT), CAST(10 AS SMALLINT), 100,
     CAST(1000 AS BIGINT), DATE '2026-01-01', CAST(X'0102' AS BINARY(2))),
    (2, 'pt2', 'us', CAST('c2' AS CHAR(2)), FALSE, CAST(2 AS TINYINT), CAST(20 AS SMALLINT), 200,
     CAST(2000 AS BIGINT), DATE '2026-01-02', CAST(X'0304' AS BINARY(2)));

-- ---------------------------------------------------------------------------
-- part_ts: a partition column whose value fluss cannot store verbatim. Its name
-- for 2026-01-01 01:02:03 is 2026-01-01-01-02-03_0 -- every character a
-- partition name may not hold rewritten, many-to-one, unrecoverable. Fluss
-- creates the table without complaint, which is why the refusal has to come from
-- the connector and has to name the column.
-- ---------------------------------------------------------------------------
CREATE TABLE part_ts (
    id INT,
    name STRING,
    p_ts TIMESTAMP(3)
) PARTITIONED BY (p_ts)
WITH (
    'bucket.num' = '1'
);

INSERT INTO part_ts VALUES
    (1, 'ts1', TIMESTAMP '2026-01-01 01:02:03.000');

-- ---------------------------------------------------------------------------
-- log_time: a column of the one fluss type Doris has nowhere to put. The column
-- reads as UNSUPPORTED, so naming it -- or asking for * -- must fail, while the
-- rest of the table stays perfectly readable. A connector that mapped it to a
-- string or to elapsed millis instead would hand back a value meaning something
-- else, which no error would ever reveal.
-- ---------------------------------------------------------------------------
CREATE TABLE log_time (
    id INT,
    name STRING,
    f_time TIME(0)
) WITH (
    'bucket.num' = '1'
);

INSERT INTO log_time VALUES
    (1, 'time1', TIME '01:02:03'),
    (2, 'time2', TIME '04:05:06');

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

-- ---------------------------------------------------------------------------
-- pk_empty: a primary-key table nothing was ever written to. It is not the same
-- shape as an empty log table: a primary-key read starts from a kv snapshot,
-- and there is none, so the path taken here is the one that has to notice the
-- table is empty rather than the one that reads no log records.
-- ---------------------------------------------------------------------------
CREATE TABLE pk_empty (
    id INT NOT NULL,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '2'
);

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
-- log BY KEY is not implemented, so this table is read from fluss alone -- which
-- is the WHOLE table rather than a part of it, because fluss keeps a primary-key
-- table's state in full. That read is the baseline the future merge has to
-- reproduce: $lake shows what paimon holds, the front door shows the answer.
-- Row 2 is updated before tiering, so the lake already holds a merged view
-- rather than a raw change log.
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

-- ---------------------------------------------------------------------------
-- lake_pk_multi: the same table over three buckets. A primary-key table is
-- merged with its log tail per BUCKET, and a single-bucket fixture cannot tell
-- that apart from merging per table: with one bucket the two are the same
-- arrangement. Here they are not -- the tail below touches some buckets and not
-- others, so a lake split may only be filtered by the tail of ITS OWN bucket.
-- Binding one bucket's tail to another's split suppresses nothing (a key lives
-- in exactly one bucket), and the rows that tail was meant to replace come back
-- as duplicates.
--
-- Nine keys, because which bucket a key lands in is fluss's hash of it and not
-- ours to choose: enough of them to be spread over all three, few enough to
-- read. Row 5 is updated before tiering, so the lake half already holds a
-- merged view here too.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_pk_multi (
    id INT NOT NULL,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_pk_multi VALUES
    (1, 'm1'),
    (2, 'm2'),
    (3, 'm3'),
    (4, 'm4'),
    (5, 'm5'),
    (6, 'm6'),
    (7, 'm7'),
    (8, 'm8'),
    (9, 'm9');

INSERT INTO lake_pk_multi VALUES
    (5, 'm5-lake');

-- ---------------------------------------------------------------------------
-- lake_pk_part: partitioned primary-key table tiered into paimon. The tail is
-- written into one partition only and a third partition is created after
-- tiering has stopped, so one query reads all three ways a partition can stand
-- with respect to the lake: 20260101 is lake plus tail, 20260102 is lake alone,
-- and 20260103 exists in fluss only -- the lake has never heard of it, so its
-- buckets have to be read whole from fluss inside the very same scan.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_pk_part (
    id INT NOT NULL,
    name STRING,
    dt STRING NOT NULL,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'bucket.num' = '2',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_pk_part VALUES
    (1, 'pp1a', '20260101'),
    (2, 'pp1b', '20260101'),
    (3, 'pp2a', '20260102'),
    (4, 'pp2b', '20260102');

-- ---------------------------------------------------------------------------
-- lake_pk_cold: a primary-key lake table with nothing left in the log. It gets
-- no tail at all, so planning must wrap no lake split and emit no tail range:
-- the merge has to cost nothing when there is nothing to merge, and a reader
-- that always builds a suppression set would still return the right rows here
-- while doing the work -- only the plan shows the difference.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_pk_cold (
    id INT NOT NULL,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '2',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_pk_cold VALUES
    (1, 'c1'),
    (2, 'c2'),
    (3, 'c3');

-- ---------------------------------------------------------------------------
-- lake_nested: nested complex types that cross the seam. The type mapping this
-- connector applies has to equal fluss->paimon->Doris for these as well, and
-- nesting is where the two could most easily part ways: each level is converted
-- by its own rule on both sides. The populated row is tiered, the all-NULL row
-- stays in the log, so one union read decodes the same nested column through
-- paimon and through fluss.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_nested (
    id INT,
    f_arr_arr ARRAY<ARRAY<INT>>,
    f_arr_row ARRAY<ROW<a INT, b STRING>>,
    f_map_arr MAP<STRING, ARRAY<INT>>,
    f_map_row MAP<STRING, ROW<a INT, b STRING>>,
    f_row_deep ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_nested VALUES
    (
        1,
        ARRAY[ARRAY[1, 2], ARRAY[3]],
        ARRAY[CAST(ROW(1, 'x') AS ROW<a INT, b STRING>)],
        MAP['k1', ARRAY[1, 2]],
        MAP['k1', CAST(ROW(9, 'z') AS ROW<a INT, b STRING>)],
        CAST(ROW(1, ARRAY[7, 8], MAP['m', 3], ROW(5, 'deep'))
             AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>)
    );

-- ---------------------------------------------------------------------------
-- lake_empty: tiering is on and has never committed anything, because nothing
-- was ever written. It is the state every lake table passes through, and the
-- one where "read the lake plus the log" has no lake to read: auto has to fall
-- back to the fluss-only read, required has to say why it will not.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_empty (
    id INT,
    name STRING
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

-- ---------------------------------------------------------------------------
-- lake_part_int: a tiered log table partitioned by an INT. Concatenating a lake
-- with a log tail needs no partition value matched across the halves -- each
-- half prunes on its own -- so a non-STRING partition column has to work here,
-- and this table is what says the rule that stops the primary-key merge (below)
-- was not applied to everything partitioned.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_part_int (
    id INT,
    name STRING,
    p_int INT
) PARTITIONED BY (p_int)
WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_part_int VALUES
    (1, 'li1a', 1),
    (2, 'li1b', 1),
    (3, 'li2a', 2);

-- ---------------------------------------------------------------------------
-- lake_pk_part_int: a tiered PRIMARY-KEY table partitioned by an INT. Merging
-- its halves by key means matching a paimon split to a fluss partition by the
-- text each side renders that value as, and only STRING is guaranteed to render
-- alike -- so this table must NOT be merged. Under auto it falls back to the
-- fluss-only read, which returns every row anyway; under required it is an
-- error. Without this fixture the rule is only ever exercised on tables that had
-- no lake to merge in the first place.
-- ---------------------------------------------------------------------------
CREATE TABLE lake_pk_part_int (
    id INT NOT NULL,
    name STRING,
    p_int INT NOT NULL,
    PRIMARY KEY (id, p_int) NOT ENFORCED
) PARTITIONED BY (p_int)
WITH (
    'bucket.num' = '2',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO lake_pk_part_int VALUES
    (1, 'pi1a', 1),
    (2, 'pi1b', 1),
    (3, 'pi2a', 2),
    (4, 'pi2b', 2);

-- ===========================================================================
-- The large fixtures. Everything above is a handful of rows chosen so that a
-- wrong answer is visible by eye; these two are the opposite question -- whether
-- the same machinery still returns the right answer when a scan is split across
-- many batches, a suppression set holds more keys than a debugger would print,
-- and a bucket's log runs to five figures.
--
-- Both are lake tables with a tail, so ONE table answers three of the scenarios
-- at once: read through a union-read catalog it is lake plus log, read through a
-- disabled one it is the pure fluss read of the same rows -- a full log replay
-- for big_log, a kv snapshot plus its log for big_pk. Two catalogs over one
-- fixture also means the two paths can be compared to each other rather than to
-- a number someone wrote down.
--
-- The values are derived from the sequence rather than randomly generated, so
-- every aggregate over them is a closed form: 1..100000 sums to 5000050000, and
-- a suite asserting that is checking arithmetic rather than repeating whatever
-- the fixture happened to produce.
-- ===========================================================================

CREATE TEMPORARY TABLE big_seq (id INT) WITH (
    'connector' = 'datagen',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '100000'
);

-- ---------------------------------------------------------------------------
-- big_log: 100000 rows over three buckets, all of them tiered. The tail written
-- afterwards is small on purpose -- that is the shape of a real tiered table,
-- where the tail is only what the freshness window has not yet taken.
-- ---------------------------------------------------------------------------
CREATE TABLE big_log (
    id INT,
    name STRING,
    price DECIMAL(10, 2),
    grp INT
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO big_log
SELECT id, CONCAT('n', CAST(id AS STRING)), CAST(id AS DECIMAL(10, 2)), MOD(id, 7) FROM big_seq;

-- ---------------------------------------------------------------------------
-- big_pk: 100000 keys over three buckets, all tiered. Its tail updates 500 of
-- them, deletes five and adds 500 more, so the suppression set that filters the
-- lake half holds a thousand-odd keys instead of the two or three every other
-- primary-key fixture here has. A merge that is right for three keys and wrong
-- for a thousand -- a set built per split, a cache keyed too coarsely -- has
-- nowhere to hide in the counts this produces.
-- ---------------------------------------------------------------------------
CREATE TABLE big_pk (
    id INT NOT NULL,
    name STRING,
    grp INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO big_pk
SELECT id, CONCAT('p', CAST(id AS STRING)), MOD(id, 7) FROM big_seq;

-- No deletion-vector fixture, and the reason is worth recording where the next
-- person to want one will look. Fluss does forward a 'paimon.*' table property
-- into the paimon table it creates, so a table declared with
-- 'paimon.deletion-vectors.enabled' = 'true' really is created with deletion
-- vectors on -- that much was verified against this environment. What the
-- tiering service then writes into it is data files and no index: no deletion
-- vector is ever produced, and paimon's own reader answers COUNT(*) with 0 on a
-- table whose files are sitting right there. A fixture that reads as empty
-- proves nothing, so deletion vectors under the merge stay covered by the BE
-- unit tests until the tiering side writes them.
