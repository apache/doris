// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_ivm_baseline_rebuild_prepare", "p0,mtmv,restart_fe") {
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_restart_rebuild_mv"""
    sql """DROP TABLE IF EXISTS ivm_restart_rebuild_t"""
    sql """
        CREATE TABLE ivm_restart_rebuild_t (
            dt DATE NOT NULL,
            id INT NOT NULL,
            v INT
        )
        UNIQUE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_restart_rebuild_t VALUES
            ('2024-01-10', 1, 10), ('2024-02-10', 2, 20)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_restart_rebuild_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, id, v FROM ivm_restart_rebuild_t
    """
    sql """REFRESH MATERIALIZED VIEW ivm_restart_rebuild_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_restart_rebuild_mv")
    sql """TRUNCATE TABLE ivm_restart_rebuild_t PARTITION(p202401)"""
}
