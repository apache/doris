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

DROP TABLE IF EXISTS ${TABLE_NAME};

CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
    k1 BIGINT NOT NULL,
    c1 VARCHAR(64) NOT NULL
)
DUPLICATE KEY(k1)
PARTITION BY RANGE(k1) (
    PARTITION p1 VALUES LESS THAN ("1000"),
    PARTITION p2 VALUES LESS THAN ("2000")
)
DISTRIBUTED BY HASH(k1) BUCKETS 8
PROPERTIES (
    "file_cache_ttl_seconds" = "300",
    "disable_auto_compaction" = "true"
);

