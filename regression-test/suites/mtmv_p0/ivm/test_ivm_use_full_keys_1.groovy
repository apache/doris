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

suite("test_ivm_use_full_keys_1") {
    // Returns the MV key column names (including hidden columns) in physical schema order.
    def getMvKeyColumns = { String mvName ->
        sql """SET show_hidden_columns = true;"""
        def desc = sql """DESC ${mvName}"""
        sql """SET show_hidden_columns = false;"""
        return desc.findAll { row -> row[3].toString() == "true" }
                .collect { row -> row[0].toString() }
    }

    // =========================================================
    // Part 1: Agg MV with ivm_use_full_keys
    //   Keys = (k1, k2, row_id); SHOW CREATE shows KEY(k1, k2) + property
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk1_agg_mv;"""
    sql """drop table if exists test_ivm_uk1_agg_base;"""

    sql """
        CREATE TABLE test_ivm_uk1_agg_base (
            k1 INT,
            k2 INT,
            v1 INT
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_uk1_agg_base VALUES
            (1, 1, 10),
            (2, 2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_uk1_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_use_full_keys' = 'true'
        )
        AS SELECT k1, k2, SUM(v1) AS sum_v1 FROM test_ivm_uk1_agg_base GROUP BY k1, k2;
    """

    // Keys include the group-by columns plus the hidden row-id.
    assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk1_agg_mv"))

    // SHOW CREATE shows the auto-added visible keys and the property.
    def showCreate = sql """show create materialized view test_ivm_uk1_agg_mv"""
    assertTrue(showCreate.toString().contains("KEY(`k1`, `k2`)"))
    assertTrue(showCreate.toString().contains("ivm_use_full_keys"))

    // Replaying SHOW CREATE produces an identical MV.
    sql """
        ${showCreate[0][1].toString()
            .replace("test_ivm_uk1_agg_mv", "test_ivm_uk1_agg_mv_replay")}
    """
    assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk1_agg_mv_replay"))
    sql """drop materialized view if exists test_ivm_uk1_agg_mv_replay;"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_agg_mv")

    order_qt_uk1_agg_initial """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_agg_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_agg_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_agg_mv")

    order_qt_uk1_agg_initial_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_agg_mv"""

    sql """INSERT INTO test_ivm_uk1_agg_base VALUES (1, 1, 5), (3, 3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_agg_mv")

    order_qt_uk1_agg_incremental """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_agg_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_agg_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_agg_mv")

    order_qt_uk1_agg_incremental_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_agg_mv"""

    // =========================================================
    // Part 2: user-specified partial KEY + ivm_use_full_keys
    //   User declares KEY(k1); useFullKeys appends the remaining
    //   group-by key k2 (dedup: k1 is not repeated).
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk1_partial_mv;"""
    sql """drop table if exists test_ivm_uk1_partial_base;"""

    sql """
        CREATE TABLE test_ivm_uk1_partial_base (
            k1 INT,
            k2 INT,
            v1 INT
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_uk1_partial_base VALUES
            (1, 1, 10),
            (2, 2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_uk1_partial_mv (k1, k2, sum_v1)
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_use_full_keys' = 'true'
        )
        AS SELECT k1, k2, SUM(v1) AS sum_v1 FROM test_ivm_uk1_partial_base GROUP BY k1, k2;
    """

    // User key k1 kept, k2 auto-added by useFullKeys, row_id last.
    assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk1_partial_mv"))

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_partial_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_partial_mv")

    order_qt_uk1_partial_initial """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_partial_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_partial_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_partial_mv")

    order_qt_uk1_partial_initial_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_partial_mv"""

    sql """INSERT INTO test_ivm_uk1_partial_base VALUES (1, 1, 5), (3, 3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_partial_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_partial_mv")

    order_qt_uk1_partial_incremental """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_partial_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_partial_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_partial_mv")

    order_qt_uk1_partial_incremental_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk1_partial_mv"""

    // =========================================================
    // Part 3: Inner join MV with ivm_use_full_keys
    //   Keys = (left uk, right uk, row_id)
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk1_join_mv;"""
    sql """drop table if exists test_ivm_uk1_join_l;"""
    sql """drop table if exists test_ivm_uk1_join_r;"""

    sql """
        CREATE TABLE test_ivm_uk1_join_l (
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
        CREATE TABLE test_ivm_uk1_join_r (
            k2 INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_uk1_join_l VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_uk1_join_r VALUES
            (2, 200),
            (3, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_uk1_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_use_full_keys' = 'true'
        )
        AS SELECT test_ivm_uk1_join_l.k1, test_ivm_uk1_join_r.k2,
                  test_ivm_uk1_join_l.v1, test_ivm_uk1_join_r.v2
           FROM test_ivm_uk1_join_l JOIN test_ivm_uk1_join_r
           ON test_ivm_uk1_join_l.k1 = test_ivm_uk1_join_r.k2;
    """

    // Left and right unique keys are both identity keys.
    assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk1_join_mv"))

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_join_mv")

    order_qt_uk1_join_initial """SELECT k1, k2, v1, v2 FROM test_ivm_uk1_join_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_join_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_join_mv")

    order_qt_uk1_join_initial_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk1_join_mv"""

    // Insert a matching right row k2=1 and a new matching pair k1=4/k2=4.
    sql """INSERT INTO test_ivm_uk1_join_r VALUES (1, 100);"""
    sql """INSERT INTO test_ivm_uk1_join_l VALUES (4, 40);"""
    sql """INSERT INTO test_ivm_uk1_join_r VALUES (4, 400);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_join_mv")

    order_qt_uk1_join_incremental """SELECT k1, k2, v1, v2 FROM test_ivm_uk1_join_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk1_join_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk1_join_mv")

    order_qt_uk1_join_incremental_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk1_join_mv"""
}
