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

suite("test_ivm_dup_keys_mtmv") {
    sql """drop materialized view if exists test_ivm_dup_keys_mtmv_mv;"""
    sql """drop table if exists test_ivm_dup_keys_mtmv_base;"""

    // 1. Create base table (DUPLICATE KEY)
    //    For DUP_KEYS base tables, IVM generates row_id = uuid_numeric() (non-deterministic).
    //    This case validates COMPLETE refresh on a simple scan MV, where INSERT OVERWRITE
    //    replaces all MV data and preserves the correct DUP_KEYS rows.
    sql """
        CREATE TABLE test_ivm_dup_keys_mtmv_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    // 2. Insert initial rows (DUP_KEYS allows duplicates on k1)
    sql """
        INSERT INTO test_ivm_dup_keys_mtmv_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    // 3. Create IVM MV (simple scan, no aggregation)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_keys_mtmv_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM test_ivm_dup_keys_mtmv_base;
    """

    // 4. Verify MV metadata
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_dup_keys_mtmv_mv'"""
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS (IVM MV is always MOW regardless of base table type)
    def descResult = sql """desc test_ivm_dup_keys_mtmv_mv all"""
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    // 6. First COMPLETE refresh
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_keys_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_keys_mtmv_mv")

    order_qt_dup_after_first_complete """SELECT k1, v1, v2 FROM test_ivm_dup_keys_mtmv_mv"""

    // 7. Insert more rows into base table (including a duplicate k1=1)
    sql """
        INSERT INTO test_ivm_dup_keys_mtmv_base VALUES
            (4, 40, 'ddd'),
            (1, 11, 'aaa_dup');
    """

    // 8. Second COMPLETE refresh — should reflect all 5 rows (DUP_KEYS allows k1 duplicates)
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_keys_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_keys_mtmv_mv")

    order_qt_dup_after_second_complete """SELECT k1, v1, v2 FROM test_ivm_dup_keys_mtmv_mv"""

    // 9. Verify row count is exactly 5 (not duplicated)
    def rowCount = sql """SELECT COUNT(*) FROM test_ivm_dup_keys_mtmv_mv"""
    assertEquals(5, rowCount[0][0] as int)

    // 10. Verify row_ids changed between the two COMPLETE refreshes
    //     (DUP_KEYS row_id is uuid_numeric, non-deterministic)
    sql """SET show_hidden_columns = true;"""
    def rowIds = sql """SELECT __DORIS_IVM_ROW_ID_COL__ FROM test_ivm_dup_keys_mtmv_mv ORDER BY __DORIS_IVM_ROW_ID_COL__"""
    sql """SET show_hidden_columns = false;"""
    assertEquals(5, rowIds.size())
    // All row_ids should be distinct
    def uniqueRowIds = rowIds.collect { it[0].toString() }.toSet()
    assertEquals(5, uniqueRowIds.size(), "All row_ids should be unique")
}
