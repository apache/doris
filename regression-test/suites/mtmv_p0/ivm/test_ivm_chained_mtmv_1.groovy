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

suite("test_ivm_chained_mtmv_1") {
    assert false,  "binlog not support row id now, this case will cause BE coredump, so skip it"

    sql """drop materialized view if exists mv_ivm_chained_2;"""
    sql """drop materialized view if exists mv_ivm_chained_1;"""
    sql """drop table if exists t_ivm_chained_base;"""

    // 1. Create base table (MOW — UNIQUE_KEYS with merge-on-write)
    sql """
        CREATE TABLE t_ivm_chained_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
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

    // 2. Insert initial rows
    sql """
        INSERT INTO t_ivm_chained_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    // 3. Create MV1 on base table (IVM, with binlog enabled)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_chained_1
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW',
            'binlog.need_historical_value' = 'true'
        )
        AS SELECT * FROM t_ivm_chained_base;
    """

    // 4. Full refresh MV1 and advance stream offset so chained incremental refreshes
    //    consume only the changes produced after the initial snapshot.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_chained_1 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_chained_1")

    order_qt_mv1_after_first_refresh """SELECT k1, v1, v2 FROM mv_ivm_chained_1"""

    // 5. Create MV2 on MV1 (chained — MV2's base table is MV1 which has binlog enabled)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_chained_2
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, v1 FROM mv_ivm_chained_1;
    """

    // 6. Full refresh MV2 and advance stream offset to establish the chained baseline.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_chained_2 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_chained_2")

    order_qt_mv2_after_first_refresh """SELECT k1, v1 FROM mv_ivm_chained_2"""

    // 7. Insert new data into base table
    sql """
        INSERT INTO t_ivm_chained_base VALUES
            (4, 40, 'ddd'),
            (5, 50, 'eee');
    """

    // 8. Incremental refresh MV1, then incremental refresh MV2 — verify row-id columns
    //    now flow through row binlog and chained IVM can reuse upstream row ids.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_chained_1 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_chained_1")

    sql """REFRESH MATERIALIZED VIEW mv_ivm_chained_2 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_chained_2")

    sql """SET show_hidden_columns = true;"""
    def mv2RowIdCount = sql """SELECT COUNT(__DORIS_IVM_ROW_ID_COL__) FROM mv_ivm_chained_2"""
    sql """SET show_hidden_columns = false;"""
    assertEquals(5, mv2RowIdCount[0][0] as int)

    order_qt_mv1_after_second_refresh """SELECT k1, v1, v2 FROM mv_ivm_chained_1"""
    order_qt_mv2_after_second_refresh """SELECT k1, v1 FROM mv_ivm_chained_2"""

    // 9. Verify MV2 row count is correct
    def mv2Result = sql """SELECT COUNT(*) AS cnt FROM mv_ivm_chained_2"""
    assertEquals(5, mv2Result[0][0] as int)
}
