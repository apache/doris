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

-- Counts the rows the tiering service has committed to paimon, read straight
-- from the warehouse rather than through fluss. scripts/run-init-sql.sh runs
-- this in a loop and greps the markers until every table reports the count
-- init.sql wrote.
--
-- Counting is what makes the environment deterministic. "A paimon snapshot
-- exists" would not: the service commits what it has consumed so far, so a
-- fixture could be frozen half-tiered, and then a table meant to be entirely in
-- the lake would still have a log tail -- for some runs and not others.
--
-- __FLUSS_PAIMON_WAREHOUSE__ is substituted by scripts/run-init-sql.sh.

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '__FLUSS_PAIMON_WAREHOUSE__'
);

USE CATALOG paimon_catalog;
USE fluss_test;

-- One statement, so one Flink job per poll instead of one per table. The marker
-- is a single token with no spaces because it is matched with a literal grep.
SELECT CONCAT('LAKEROWS:lake_log=', CAST(COUNT(*) AS STRING)) AS marker FROM lake_log
UNION ALL
SELECT CONCAT('LAKEROWS:lake_cold=', CAST(COUNT(*) AS STRING)) FROM lake_cold
UNION ALL
SELECT CONCAT('LAKEROWS:lake_types=', CAST(COUNT(*) AS STRING)) FROM lake_types
UNION ALL
SELECT CONCAT('LAKEROWS:lake_part=', CAST(COUNT(*) AS STRING)) FROM lake_part
UNION ALL
SELECT CONCAT('LAKEROWS:lake_pk=', CAST(COUNT(*) AS STRING)) FROM lake_pk
UNION ALL
SELECT CONCAT('LAKEROWS:lake_pk_multi=', CAST(COUNT(*) AS STRING)) FROM lake_pk_multi
UNION ALL
SELECT CONCAT('LAKEROWS:lake_pk_part=', CAST(COUNT(*) AS STRING)) FROM lake_pk_part
UNION ALL
SELECT CONCAT('LAKEROWS:lake_pk_cold=', CAST(COUNT(*) AS STRING)) FROM lake_pk_cold;
