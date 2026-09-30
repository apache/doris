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

-- Header for the exact-readable-snapshot probe. run-init-sql.sh asks the
-- ZooKeeper sidecar for the snapshot ID the Fluss coordinator has published
-- for each fixture, appends one count with a scan.snapshot-id hint per table,
-- then runs the resulting statement. Counting the latest Paimon snapshot would
-- prove only that tiering committed; counting through the Fluss catalog would
-- let an older readable snapshot plus its log tail satisfy the full row count.

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '__FLUSS_PAIMON_WAREHOUSE__',
    's3.endpoint' = '__FLUSS_LAKE_S3_ENDPOINT__',
    's3.path.style.access' = 'true',
    's3.access-key' = '__FLUSS_LAKE_S3_ACCESS_KEY__',
    's3.secret-key' = '__FLUSS_LAKE_S3_SECRET_KEY__'
);

USE CATALOG paimon_catalog;
USE fluss_test;
