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

suite("test_ivm_mtmv_changelog") {
    sql """DROP MATERIALIZED VIEW IF EXISTS test_ivm_mtmv_changelog_mv;"""
    sql """DROP TABLE IF EXISTS test_ivm_mtmv_changelog_base;"""

    sql """
        CREATE TABLE test_ivm_mtmv_changelog_base (
            k1 INT,
            v1 INT,
            jv VARIANT NULL
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        );
    """

    sql """INSERT INTO test_ivm_mtmv_changelog_base VALUES (1, 10, parse_to_variant('{"k": 1, "d": {"row": "1 10"}}')), (2, 20, parse_to_variant('{"k": 2, "d": {"row": "2 20"}}'));"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_mtmv_changelog_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
        AS SELECT k1, v1 FROM test_ivm_mtmv_changelog_base;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_mtmv_changelog_mv COMPLETE;"""
    waitingMTMVTaskFinishedByMvName("test_ivm_mtmv_changelog_mv")
    sql """sync"""

    order_qt_mv_rows """SELECT k1, v1 FROM test_ivm_mtmv_changelog_mv ORDER BY k1;"""

    qt_mv_changelog_rows """
        SELECT __DORIS_CHANGELOG_OP__, k1, v1
        FROM changelog("table" = "test_ivm_mtmv_changelog_mv")
        ORDER BY __DORIS_CHANGELOG_LSN__;
    """

    sql """SET show_hidden_columns = true;"""
    qt_mv_changelog_row_ids """
        SELECT __DORIS_IVM_ROW_ID_COL__, k1, v1
        FROM changelog("table" = "test_ivm_mtmv_changelog_mv")
        ORDER BY k1;
    """
    def mvRowIdRows = sql """
        SELECT __DORIS_IVM_ROW_ID_COL__, k1, v1
        FROM test_ivm_mtmv_changelog_mv
        ORDER BY k1;
    """
    def changelogRowIdRows = sql """
        SELECT __DORIS_IVM_ROW_ID_COL__, k1, v1
        FROM changelog("table" = "test_ivm_mtmv_changelog_mv")
        ORDER BY k1;
    """
    assertTrue(mvRowIdRows.size() > 0)
    assertTrue(mvRowIdRows.every { it[0] != null })
    assertEquals(mvRowIdRows, changelogRowIdRows)
    sql """SET show_hidden_columns = false;"""
}
