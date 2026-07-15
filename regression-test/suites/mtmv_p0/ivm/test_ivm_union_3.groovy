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

suite("test_ivm_union_3") {

    // =========================================================
    // Part 11: Nested UNION ALL — (a UNION ALL b) UNION ALL c
    // Verifies incremental refresh propagates correctly through nested UNION.
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_3_nested_mv;"""
    sql """drop table if exists test_ivm_union_3_nested_t1;"""
    sql """drop table if exists test_ivm_union_3_nested_t2;"""
    sql """drop table if exists test_ivm_union_3_nested_t3;"""

    sql """
        CREATE TABLE test_ivm_union_3_nested_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_3_nested_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_3_nested_t3 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_3_nested_t1 VALUES (1, 10), (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_3_nested_t2 VALUES (3, 30);
    """
    sql """
        INSERT INTO test_ivm_union_3_nested_t3 VALUES (4, 40);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_3_nested_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        (SELECT k1, v1 FROM test_ivm_union_3_nested_t1
         UNION ALL
         SELECT k1, v1 FROM test_ivm_union_3_nested_t2)
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_3_nested_t3;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_nested_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_nested_mv")
    advance_ivm_stream_offset("test_ivm_union_3_nested_mv")
    order_qt_nested_after_complete """
        SELECT k1, v1 FROM test_ivm_union_3_nested_mv
    """

    sql """INSERT INTO test_ivm_union_3_nested_t2 VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_nested_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_nested_mv")
    order_qt_nested_after_inc_inner """
        SELECT k1, v1 FROM test_ivm_union_3_nested_mv
    """

    sql """INSERT INTO test_ivm_union_3_nested_t3 VALUES (6, 60);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_nested_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_nested_mv")
    order_qt_nested_after_inc_outer """
        SELECT k1, v1 FROM test_ivm_union_3_nested_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_nested_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_nested_mv")
    advance_ivm_stream_offset("test_ivm_union_3_nested_mv")
    order_qt_nested_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_3_nested_mv
    """

    // =========================================================
    // Part 12: Self-union with DELETE (MOW with binlog_op)
    // Same table appears in both arms. Verifies delete propagation
    // only affects the delta arm being refreshed.
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_3_self_del_mv;"""
    sql """drop table if exists test_ivm_union_3_self_del_t;"""

    sql """
        CREATE TABLE test_ivm_union_3_self_del_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_3_self_del_t VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_3_self_del_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_3_self_del_t
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_3_self_del_t;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_self_del_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_self_del_mv")
    advance_ivm_stream_offset("test_ivm_union_3_self_del_mv")
    order_qt_self_del_after_complete """
        SELECT k1, v1 FROM test_ivm_union_3_self_del_mv
    """

    // Delete row k1=2 (binlog_op=1) and insert k1=4 (binlog_op=0)
    sql """
        INSERT INTO test_ivm_union_3_self_del_t VALUES
            (4, 40);
    """
    sql """DELETE FROM test_ivm_union_3_self_del_t WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_self_del_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_self_del_mv")
    order_qt_self_del_after_inc """
        SELECT k1, v1 FROM test_ivm_union_3_self_del_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_3_self_del_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_3_self_del_mv")
    advance_ivm_stream_offset("test_ivm_union_3_self_del_mv")
    order_qt_self_del_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_3_self_del_mv
    """

    // =========================================================
    // Part 13: UNION DISTINCT rejection — should fail at MV creation
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_3_distinct_mv;"""
    sql """drop table if exists test_ivm_union_3_distinct_t1;"""
    sql """drop table if exists test_ivm_union_3_distinct_t2;"""

    sql """
        CREATE TABLE test_ivm_union_3_distinct_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_3_distinct_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    test {
        sql """
            CREATE MATERIALIZED VIEW test_ivm_union_3_distinct_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT k1, v1 FROM test_ivm_union_3_distinct_t1
            UNION
            SELECT k1, v1 FROM test_ivm_union_3_distinct_t2;
        """
        exception "UNION DISTINCT"
    }

    // =========================================================
    // Part 14: Constant expression arm rejection
    // UNION ALL with constant SELECT (no base table) is not supported for IVM.
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_3_const_mv;"""
    sql """drop table if exists test_ivm_union_3_const_t1;"""

    sql """
        CREATE TABLE test_ivm_union_3_const_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    test {
        sql """
            CREATE MATERIALIZED VIEW test_ivm_union_3_const_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT k1, v1 FROM test_ivm_union_3_const_t1
            UNION ALL
            SELECT 1, 2;
        """
        exception "IVM"
    }
}
