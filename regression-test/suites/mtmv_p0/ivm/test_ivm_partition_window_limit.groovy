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

suite("test_ivm_partition_window_limit") {

    // The ivm_partition_window_limit property restricts IVM incremental
    // refresh to the last N partitions (by partition value) of each configured
    // base table. Changes outside the window are ignored (lossy, user opt-in).

    def baseTable = { String name ->
        sql """
            CREATE TABLE ${name} (
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
                "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
    }

    sql """drop materialized view if exists test_ivm_pw_mv;"""
    sql """drop table if exists test_ivm_pw_s;"""
    sql """drop table if exists test_ivm_pw_t;"""
    sql """drop materialized view if exists test_ivm_pw_nonivm_mv;"""

    baseTable("test_ivm_pw_s")
    baseTable("test_ivm_pw_t")

    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-01", 1, 10), ("2026-01-02", 2, 20), ("2026-01-03", 3, 30);"""
    sql """INSERT INTO test_ivm_pw_t VALUES ("2026-01-01", 1, 100), ("2026-01-02", 2, 200), ("2026-01-03", 3, 300);"""

    // Window = last 2 partitions of both base tables by value: p2 (2026-01-02) and p3 (2026-01-03).
    // p1 (2026-01-01) is outside the window; its changes must be ignored.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_pw_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_partition_window_limit' = 'test_ivm_pw_s:2,test_ivm_pw_t:2'
        )
        AS SELECT s.dt, s.k1, s.v1 + t.v1 AS v
           FROM test_ivm_pw_s s
           JOIN test_ivm_pw_t t ON s.dt = t.dt AND s.k1 = t.k1;
    """

    // Initial refresh is a full baseline: all partitions are present. (A strict
    // INCREMENTAL initial refresh would only build the windowed partitions.)
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_initial """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // Change inside the window (p2): the incremental refresh maintains it.
    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-02", 2, 21);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_window_change """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // Change outside the window (p1): ignored by the incremental refresh.
    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-01", 1, 11);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_outside_window_change """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // Shrink the window to 1 partition (p3): p2 changes are now ignored too.
    sql """ALTER MATERIALIZED VIEW test_ivm_pw_mv SET ("ivm_partition_window_limit" = "test_ivm_pw_s:1,test_ivm_pw_t:1");"""
    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-02", 2, 22);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_shrunk_window """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // Remove the window: a strict INCREMENTAL would now be rejected (baseline rebuild
    // pending, see test_ivm_partition_window_remove), so the AUTO refresh rebuilds a
    // complete baseline and p1's accumulated binlog is replayed (11 -> 12).
    sql """ALTER MATERIALIZED VIEW test_ivm_pw_mv SET ("ivm_partition_window_limit" = "");"""
    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-01", 1, 12);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv AUTO"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_window_removed_catchup """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // A table can be both excluded (no delta) and windowed (its snapshot side is
    // still limited): the combination must not break incremental refresh.
    sql """ALTER MATERIALIZED VIEW test_ivm_pw_mv SET (
        "excluded_trigger_tables" = "test_ivm_pw_s",
        "ivm_partition_window_limit" = "test_ivm_pw_s:2,test_ivm_pw_t:2");"""
    sql """INSERT INTO test_ivm_pw_s VALUES ("2026-01-03", 3, 31);"""
    sql """INSERT INTO test_ivm_pw_t VALUES ("2026-01-03", 3, 301);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_pw_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_pw_mv")
    order_qt_pw_excluded_and_windowed """SELECT dt, k1, v FROM test_ivm_pw_mv ORDER BY dt"""

    // The property error cases (non-IVM create/alter, unknown base table) are
    // covered by AlterMTMVTest in FE unit tests, not here.

    sql """drop materialized view if exists test_ivm_pw_mv;"""
    sql """drop table if exists test_ivm_pw_s;"""
    sql """drop table if exists test_ivm_pw_t;"""
}
