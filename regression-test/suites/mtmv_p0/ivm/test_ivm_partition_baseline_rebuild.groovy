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

suite("test_ivm_partition_baseline_rebuild", "nonConcurrent") {
    def tableName = "ivm_part_rebuild_t"
    def mvName = "ivm_part_rebuild_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
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
    sql """INSERT INTO ${tableName} VALUES
            ('2024-01-10', 1, 10), ('2024-02-10', 2, 20)"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, id, v FROM ${tableName}
    """
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)

    sql """TRUNCATE TABLE ${tableName} PARTITION(p202401)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    qt_truncate_mode """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_after_truncate """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""

    sql """ALTER TABLE ${tableName} DROP PARTITION p202402"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_after_drop """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""

    sql """RECOVER PARTITION p202402 FROM ${tableName}"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    qt_recover_mode """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_after_recover """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""

    sql """ALTER TABLE ${tableName} ADD TEMPORARY PARTITION tp202401
            VALUES [('2024-01-01'), ('2024-02-01'))"""
    sql """INSERT INTO ${tableName} TEMPORARY PARTITION(tp202401)
            VALUES ('2024-01-20', 3, 30)"""
    sql """ALTER TABLE ${tableName} REPLACE PARTITION (p202401)
            WITH TEMPORARY PARTITION (tp202401)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    qt_replace_mode """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_after_replace """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""
}
