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

suite("test_ivm_basic_mtmv") {
    sql """drop materialized view if exists mv_ivm_basic;"""
    sql """drop table if exists t_ivm_basic_base;"""

    // 1. Create base table (MOW — UNIQUE_KEYS with merge-on-write)
    //    IVM generates row_id = hash(unique_keys) for MOW base tables,
    //    which is deterministic across refreshes, so the mock
    //    (reading full base table) correctly upserts existing rows.
    sql """
        CREATE TABLE t_ivm_basic_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // 2. Insert initial rows
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    // 3. Create IVM materialized view (BUILD DEFERRED, REFRESH INCREMENTAL, ON MANUAL)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_basic
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_basic_base;
    """

    // 4. Verify MV metadata — state should be INIT (not yet refreshed)
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'mv_ivm_basic'"""
    logger.info("mv_infos after create: " + mvInfos.toString())
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS with MOW (via desc command)
    def descResult = sql """desc mv_ivm_basic all"""
    logger.info("desc mv: " + descResult.toString())
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    // 6. First COMPLETE refresh (full overwrite, skips IVM path)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    // 7. Verify data after first refresh (exclude __IVM_ROW_ID__ column)
    order_qt_after_first_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 8. Insert more rows into base table
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (4, 40, 'ddd'),
            (5, 50, 'eee');
    """

    // 9. Second refresh via IVM incremental path (mock reads full base table,
    //    deterministic row_id upserts correctly for MOW base table)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_second_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 10. Update existing rows in base table
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (2, 22, 'bbb_updated'),
            (3, 33, 'ccc_updated');
    """

    // 11. Third refresh via IVM incremental — should reflect updated rows
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_third_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 12. Complete refresh after incremental — should produce same result (full overwrite)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_complete_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

}
