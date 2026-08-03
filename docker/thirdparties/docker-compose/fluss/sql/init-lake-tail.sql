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

SET 'execution.runtime-mode' = 'batch';
DELETE FROM lake_pk WHERE id = 1;
SET 'execution.runtime-mode' = 'streaming';
