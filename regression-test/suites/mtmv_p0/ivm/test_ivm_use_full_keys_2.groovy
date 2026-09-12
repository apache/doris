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

suite("test_ivm_use_full_keys_2") {
    // Returns the MV key column names (including hidden columns) in physical schema order.
    def getMvKeyColumns = { String mvName ->
        sql """SET show_hidden_columns = true;"""
        def desc = sql """DESC ${mvName}"""
        sql """SET show_hidden_columns = false;"""
        return desc.findAll { row -> row[3].toString() == "true" }
                .collect { row -> row[0].toString() }
    }

    // =========================================================
    // Part 1: Union ALL MV with ivm_use_full_keys
    //   Identity keys are hidden: (arm_index, positional key, row_id)
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk2_union_mv;"""
    sql """drop table if exists test_ivm_uk2_union_l;"""
    sql """drop table if exists test_ivm_uk2_union_r;"""

    sql """
        CREATE TABLE test_ivm_uk2_union_l (
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
        CREATE TABLE test_ivm_uk2_union_r (
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
        INSERT INTO test_ivm_uk2_union_l VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_uk2_union_r VALUES
            (2, 200),
            (3, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_uk2_union_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'ivm_use_full_keys' = 'true'
        )
        AS SELECT k1, v1 FROM test_ivm_uk2_union_l
           UNION ALL
           SELECT k1, v1 FROM test_ivm_uk2_union_r;
    """

    // Union identity keys are the hidden arm-index + positional key + row_id.
    assertEquals(["__DORIS_IVM_UNION_ARM_INDEX_0_COL__",
                  "__DORIS_IVM_UNION_KEY_0_0_COL__",
                  "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk2_union_mv"))

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_union_mv")

    order_qt_uk2_union_initial """SELECT k1, v1 FROM test_ivm_uk2_union_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_union_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_union_mv")

    order_qt_uk2_union_initial_complete """SELECT k1, v1 FROM test_ivm_uk2_union_mv"""

    sql """INSERT INTO test_ivm_uk2_union_l VALUES (4, 40);"""
    sql """INSERT INTO test_ivm_uk2_union_r VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_union_mv")

    order_qt_uk2_union_incremental """SELECT k1, v1 FROM test_ivm_uk2_union_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_union_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_union_mv")

    order_qt_uk2_union_incremental_complete """SELECT k1, v1 FROM test_ivm_uk2_union_mv"""

    // =========================================================
    // Part 2: default (ivm_use_full_keys=false) keeps row_id-only keys
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk2_default_mv;"""
    sql """drop table if exists test_ivm_uk2_default_base;"""

    sql """
        CREATE TABLE test_ivm_uk2_default_base (
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
        INSERT INTO test_ivm_uk2_default_base VALUES
            (1, 10),
            (2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_uk2_default_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, v1 FROM test_ivm_uk2_default_base;
    """

    // Default behavior: only the hidden row-id is a key.
    assertEquals(["__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk2_default_mv"))

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_default_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_default_mv")

    order_qt_uk2_default_initial """SELECT k1, v1 FROM test_ivm_uk2_default_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_default_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_default_mv")

    order_qt_uk2_default_initial_complete """SELECT k1, v1 FROM test_ivm_uk2_default_mv"""

    sql """INSERT INTO test_ivm_uk2_default_base VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_default_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_default_mv")

    order_qt_uk2_default_incremental """SELECT k1, v1 FROM test_ivm_uk2_default_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_uk2_default_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_uk2_default_mv")

    order_qt_uk2_default_incremental_complete """SELECT k1, v1 FROM test_ivm_uk2_default_mv"""

    // =========================================================
    // Part 3: ALTER is rejected for ivm_use_full_keys
    // =========================================================

    test {
        sql """
            ALTER MATERIALIZED VIEW test_ivm_uk2_default_mv
            SET ("ivm_use_full_keys" = "true");
        """
        exception "cannot be altered"
    }
}
