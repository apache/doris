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

-- Read the same fixtures through the Fluss catalog. Planning each source asks
-- the coordinator for getReadableLakeSnapshot; run-init-sql.sh also inspects
-- the corresponding JobManager log slice and rejects Fluss-only fallback.

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'fluss-readable-snapshot-probe';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = '__FLUSS_BOOTSTRAP_SERVERS__',
    'paimon.metastore' = 'filesystem',
    'paimon.warehouse' = '__FLUSS_PAIMON_WAREHOUSE__',
    'paimon.s3.endpoint' = '__FLUSS_LAKE_S3_ENDPOINT__',
    'paimon.s3.path.style.access' = 'true',
    'paimon.s3.access-key' = '__FLUSS_LAKE_S3_ACCESS_KEY__',
    'paimon.s3.secret-key' = '__FLUSS_LAKE_S3_SECRET_KEY__'
);

USE CATALOG fluss_catalog;
USE fluss_test;

SELECT CONCAT('READABLE:lake_log=', CAST(COUNT(*) AS STRING)) AS marker FROM lake_log
UNION ALL
SELECT CONCAT('READABLE:lake_cold=', CAST(COUNT(*) AS STRING)) FROM lake_cold
UNION ALL
SELECT CONCAT('READABLE:lake_types=', CAST(COUNT(*) AS STRING)) FROM lake_types
UNION ALL
SELECT CONCAT('READABLE:lake_part=', CAST(COUNT(*) AS STRING)) FROM lake_part
UNION ALL
SELECT CONCAT('READABLE:lake_pk=', CAST(COUNT(*) AS STRING)) FROM lake_pk
UNION ALL
SELECT CONCAT('READABLE:lake_pk_multi=', CAST(COUNT(*) AS STRING)) FROM lake_pk_multi
UNION ALL
SELECT CONCAT('READABLE:lake_pk_part=', CAST(COUNT(*) AS STRING)) FROM lake_pk_part
UNION ALL
SELECT CONCAT('READABLE:lake_pk_cold=', CAST(COUNT(*) AS STRING)) FROM lake_pk_cold
UNION ALL
SELECT CONCAT('READABLE:lake_nested=', CAST(COUNT(*) AS STRING)) FROM lake_nested
UNION ALL
SELECT CONCAT('READABLE:lake_part_int=', CAST(COUNT(*) AS STRING)) FROM lake_part_int
UNION ALL
SELECT CONCAT('READABLE:lake_pk_part_int=', CAST(COUNT(*) AS STRING)) FROM lake_pk_part_int
UNION ALL
SELECT CONCAT('READABLE:big_log=', CAST(COUNT(*) AS STRING)) FROM big_log
UNION ALL
SELECT CONCAT('READABLE:big_pk=', CAST(COUNT(*) AS STRING)) FROM big_pk;
