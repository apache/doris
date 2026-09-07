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

suite("test_ivm_use_full_keys_5", "nonConcurrent") {
    String debugPoint = "IvmUtil.buildRowIdHash.force_collision"
    GetDebugPoint().disableDebugPointForAllFEs(debugPoint)

    // Returns the MV key column names (including hidden columns) in physical schema order.
    def getMvKeyColumns = { String mvName ->
        sql """SET show_hidden_columns = true;"""
        def desc = sql """DESC ${mvName}"""
        sql """SET show_hidden_columns = false;"""
        return desc.findAll { row -> row[3].toString() == "true" }
                .collect { row -> row[0].toString() }
    }

    // =========================================================
    // Collision Case C — inner join MV with ivm_use_full_keys
    //   Debug point forces every row_id to 1. The MV keys are
    //   (left uk, right uk, row_id): distinct (k1, k2) join rows
    //   survive even though every row shares row_id=1, and a
    //   delete delta only touches the matching join row.
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk5_join_mv;"""
    sql """drop table if exists test_ivm_uk5_join_l;"""
    sql """drop table if exists test_ivm_uk5_join_r;"""

    sql """
        CREATE TABLE test_ivm_uk5_join_l (
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
        CREATE TABLE test_ivm_uk5_join_r (
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
        INSERT INTO test_ivm_uk5_join_l VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_uk5_join_r VALUES
            (1, 100),
            (2, 200);
    """

    try {
        // Must be enabled before the first refresh so the stored row_id is also 1.
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk5_join_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true'
            )
            AS SELECT test_ivm_uk5_join_l.k1, test_ivm_uk5_join_r.k2,
                      test_ivm_uk5_join_l.v1, test_ivm_uk5_join_r.v2
               FROM test_ivm_uk5_join_l JOIN test_ivm_uk5_join_r
               ON test_ivm_uk5_join_l.k1 = test_ivm_uk5_join_r.k2;
        """

        // Left and right unique keys + row_id become the MV keys.
        assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"],
                getMvKeyColumns("test_ivm_uk5_join_mv"))

        // Dump the physical schema (including hidden columns) into the .out for inspection.
        sql """SET show_hidden_columns = true;"""
        qt_uk5_join_mv_desc """DESC test_ivm_uk5_join_mv"""
        sql """SET show_hidden_columns = false;"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk5_join_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk5_join_mv")

        // Both join rows survive despite identical row_id=1.
        order_qt_uk5_join_collision_initial """SELECT k1, k2, v1, v2 FROM test_ivm_uk5_join_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk5_join_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk5_join_mv")

        order_qt_uk5_join_collision_initial_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk5_join_mv"""

        // Insert a new matching pair, delete k1=1 and add a dirty row so the partition has
        // new data: the delete must only remove the (1, 1) join row despite row_id collisions.
        sql """INSERT INTO test_ivm_uk5_join_l VALUES (3, 30);"""
        sql """INSERT INTO test_ivm_uk5_join_r VALUES (3, 300);"""
        sql """DELETE FROM test_ivm_uk5_join_l WHERE k1 = 1;"""
        sql """INSERT INTO test_ivm_uk5_join_l VALUES (4, 40);"""
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk5_join_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk5_join_mv")

        order_qt_uk5_join_collision_after_delete """SELECT k1, k2, v1, v2 FROM test_ivm_uk5_join_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk5_join_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk5_join_mv")

        order_qt_uk5_join_collision_after_delete_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk5_join_mv"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
}
