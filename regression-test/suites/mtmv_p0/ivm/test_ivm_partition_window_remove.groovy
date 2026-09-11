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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_partition_window_remove") {

    // DORIS-28376: removing or enlarging ivm_partition_window_limit brings previously
    // lossy partitions back into the refresh range. Their stream backlog was skipped by
    // the windowed refreshes, so a strict INCREMENTAL refresh right after the ALTER used
    // to be wrongly skipped as "all partitions are synced" and returned SUCCESS with
    // stale data. The ALTER must now force the next refresh to rebuild a complete baseline.

    sql """drop materialized view if exists test_ivm_pwr_mv;"""
    sql """drop table if exists test_ivm_pwr_s;"""

    sql """
        CREATE TABLE test_ivm_pwr_s (
            dt DATE NOT NULL,
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(dt, k1)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES LESS THAN ("2026-01-02"),
            PARTITION p2 VALUES LESS THAN ("2026-01-03"),
            PARTITION p3 VALUES LESS THAN ("2026-01-04")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO test_ivm_pwr_s VALUES ("2026-01-01", 1, 10), ("2026-01-02", 2, 20), ("2026-01-03", 3, 30);"""

    // Window = last 1 partition (p3). p1/p2 are outside the window.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_pwr_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_partition_window_limit' = 'test_ivm_pwr_s:1'
        )
        AS SELECT dt, k1, v1 FROM test_ivm_pwr_s;
    """
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pwr_mv")
    order_qt_pwr_initial """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    // p1 is outside the window; its change is ignored by the windowed refresh.
    sql """INSERT INTO test_ivm_pwr_s VALUES ("2026-01-01", 1, 11);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pwr_mv")
    order_qt_pwr_window_ignores_p1 """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    // Remove the window. A strict manual INCREMENTAL cannot replay the lossy backlog
    // safely: it must fail explicitly instead of returning SUCCESS with stale data.
    sql """ALTER MATERIALIZED VIEW test_ivm_pwr_mv SET ("ivm_partition_window_limit" = "");"""
    def previousTaskId = sql("""
        SELECT TaskId FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_pwr_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """)[0][0].toString()
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv INCREMENTAL"""
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        def task = sql_return_maparray("""
            SELECT TaskId, Status FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_pwr_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """)
        return !task.isEmpty()
                && task[0].TaskId.toString() != previousTaskId
                && task[0].Status.toString() != 'PENDING'
                && task[0].Status.toString() != 'RUNNING'
    })
    order_qt_pwr_strict_after_remove """
        SELECT Status,
               ErrorMsg LIKE '%baseline rebuild is pending%'
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_pwr_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_pwr_stale_after_strict """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    // The next AUTO refresh rebuilds a complete baseline and replays the p1 backlog.
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv AUTO"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pwr_mv")
    order_qt_pwr_removed_refresh_mode """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_pwr_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_pwr_window_removed_catchup """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    // Re-arm a window, create a fresh outside-window backlog, then enlarge the window:
    // the newly included partition's backlog must also be replayed by a full refresh.
    sql """ALTER MATERIALIZED VIEW test_ivm_pwr_mv SET ("ivm_partition_window_limit" = "test_ivm_pwr_s:1");"""
    sql """INSERT INTO test_ivm_pwr_s VALUES ("2026-01-01", 1, 12);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pwr_mv")
    order_qt_pwr_window_rearmed """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    // Enlarge the window to 2 partitions (p2 + p3): p1 is now inside the window, and
    // its backlog (12) must be replayed by the next refresh.
    sql """ALTER MATERIALIZED VIEW test_ivm_pwr_mv SET ("ivm_partition_window_limit" = "test_ivm_pwr_s:2");"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pwr_mv AUTO"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pwr_mv")
    order_qt_pwr_enlarged_refresh_mode """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_pwr_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_pwr_window_enlarged_catchup """SELECT dt, k1, v1 FROM test_ivm_pwr_mv ORDER BY dt"""

    sql """drop materialized view if exists test_ivm_pwr_mv;"""
    sql """drop table if exists test_ivm_pwr_s;"""
}
