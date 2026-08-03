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

-- The second half of the lake fixtures: the rows that must stay in the fluss
-- log rather than reach paimon. scripts/run-init-sql.sh runs this only after
-- the tiering service has committed everything init.sql wrote AND has been
-- stopped, so these rows stay where they land and a union read has both halves
-- to stitch for as long as the environment lives.
--
-- lake_cold gets nothing on purpose: it is the fixture for a table the lake
-- already holds in full.

SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '2';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = '__FLUSS_BOOTSTRAP_SERVERS__'
);

USE CATALOG fluss_catalog;
USE fluss_test;

-- Two rows over three buckets: at least one bucket keeps an empty tail.
INSERT INTO lake_log VALUES
    (5, 'hot5', CAST(5.50 AS DECIMAL(10, 2))),
    (6, 'hot6', CAST(6.60 AS DECIMAL(10, 2)));

-- The all-NULL row, so the union read covers NULLs arriving from the log half
-- while the non-NULL values of every type come from the lake half.
INSERT INTO lake_types VALUES
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

-- Only 20260101 gets a tail; 20260102 stays entirely in the lake.
INSERT INTO lake_part VALUES
    (4, 'lp1c', '20260101');

-- The tail carries one of each way a change log can disagree with the lake it
-- follows: row 3 is updated, row 1 is deleted, and row 4 exists only here. All
-- three are what a merge of the two halves has to get right, and the fluss-only
-- read of this table -- which is the whole table, because fluss keeps a
-- primary-key table's state in full -- is the answer that merge must reproduce.
INSERT INTO lake_pk VALUES
    (3, 'lp3-hot'),
    (4, 'lp4-hot');

-- Three keys out of nine, so the tail reaches some buckets and not others --
-- which is the whole point of this table. Key 10 is new here, so it also lands
-- in a bucket the lake already holds rows for and must be added to that bucket
-- rather than replacing anything in it.
INSERT INTO lake_pk_multi VALUES
    (2, 'm2-hot'),
    (10, 'm10-hot');

-- 20260101 gets a tail, 20260102 keeps none, and 20260103 is created here --
-- after tiering stopped -- so the lake never sees it.
INSERT INTO lake_pk_part VALUES
    (1, 'pp1a-hot', '20260101');

INSERT INTO lake_pk_part VALUES
    (5, 'pp3a', '20260103');

-- The all-NULL row of the nested fixture: every nested column arrives from the
-- log half while its populated counterpart comes through paimon.
INSERT INTO lake_nested VALUES
    (
        2,
        CAST(NULL AS ARRAY<ARRAY<INT>>),
        CAST(NULL AS ARRAY<ROW<a INT, b STRING>>),
        CAST(NULL AS MAP<STRING, ARRAY<INT>>),
        CAST(NULL AS MAP<STRING, ROW<a INT, b STRING>>),
        CAST(NULL AS ROW<r_int INT, r_arr ARRAY<INT>, r_map MAP<STRING, INT>, r_row ROW<x INT, y STRING>>)
    );

-- A tail in one INT-named partition and none in the other.
INSERT INTO lake_part_int VALUES
    (4, 'li1c', 1);

-- A tail on the primary-key table whose partition column is an INT. It exists so
-- that falling back to the fluss-only read is a real choice rather than a
-- distinction without a difference: were the halves merged by key here, this row
-- would be the one the merge got wrong.
INSERT INTO lake_pk_part_int VALUES
    (1, 'pi1a-hot', 1);

-- The large tails. 500 updated keys, 500 new ones and five deletions, against a
-- lake of 100000 -- so the suppression set is a thousand-odd keys wide and the
-- rows it must drop are scattered through every bucket rather than sitting at
-- the front of one.
CREATE TEMPORARY TABLE tail_seq (id INT) WITH (
    'connector' = 'datagen',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '500'
);

CREATE TEMPORARY TABLE tail_seq_log (id INT) WITH (
    'connector' = 'datagen',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '100001',
    'fields.id.end' = '101000'
);

-- 1000 rows appended after the lake stopped: the log half of the big log table.
-- Ids stay contiguous with the lake half (1..101000), and every derived column
-- is the same function of the id on both sides, so an aggregate over the union
-- has a closed form and a missing or duplicated half is arithmetic, not opinion.
INSERT INTO big_log
SELECT id, CONCAT('hot', CAST(id AS STRING)), CAST(id AS DECIMAL(10, 2)), MOD(id, 7)
FROM tail_seq_log;

-- 500 keys the lake already holds, written again with a different name: each one
-- must suppress exactly the lake row it replaces.
INSERT INTO big_pk
SELECT id, CONCAT('hot', CAST(id AS STRING)), MOD(id, 7) FROM tail_seq;

-- 500 keys the lake has never seen, in buckets it does hold rows for: they are
-- added, not substituted for anything.
INSERT INTO big_pk
SELECT 100000 + id, CONCAT('new', CAST(id AS STRING)), MOD(100000 + id, 7) FROM tail_seq;

-- Deletions of rows the lake holds. They are the case a suppression set has to
-- cover but a merge of surviving rows cannot: the key is gone from the tail's
-- own result, and the lake row it removes would otherwise stay.
--
-- The big table's five are spelled out one at a time because a fluss DELETE
-- resolves a full primary key, not a range; they are spread across the key space
-- so they cannot all land in one bucket.
SET 'execution.runtime-mode' = 'batch';
DELETE FROM lake_pk WHERE id = 1;
DELETE FROM lake_pk_multi WHERE id = 7;
DELETE FROM lake_pk_part WHERE id = 2 AND dt = '20260101';
DELETE FROM big_pk WHERE id = 7;
DELETE FROM big_pk WHERE id = 19999;
DELETE FROM big_pk WHERE id = 50000;
DELETE FROM big_pk WHERE id = 77777;
DELETE FROM big_pk WHERE id = 99999;
SET 'execution.runtime-mode' = 'streaming';

-- lake_pk_cold gets nothing: it is the fixture for a primary-key table the lake
-- already holds in full, where planning must wrap no split and read no tail.
