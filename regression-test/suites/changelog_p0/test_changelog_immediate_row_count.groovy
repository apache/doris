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

suite("test_changelog_immediate_row_count", "nonConcurrent") {
    // The debug point changes global statistics publication; run on both SN and cloud.
    def pauseReports = "FE.TabletStatMgr.updateIndexRowCount.disabled"
    def recordIndexRows = { tag ->
        // SHOW DATA reads the index's retained-count estimate directly. Project by column name
        // so deployment-dependent byte sizes and replica counts do not enter the golden file.
        quickRunTest(tag, "SHOW DATA FROM changelog_immediate_rows", true, { row, meta ->
            def columns = (1..meta.getColumnCount()).collectEntries { column ->
                [(meta.getColumnLabel(column)): row[column - 1]]
            }
            def indexName = columns.IndexName
            def kind = indexName == "changelog_immediate_rows" ? "base" :
                    (indexName == "Total" ? "total" : "changelog")
            return [kind, kind == "total" ? "-" : columns.RowCount]
        })
    }

    GetDebugPoint().enableDebugPointForAllFEs(pauseReports)
    try {
        sql "DROP STREAM IF EXISTS changelog_report_gap_stream FORCE"
        sql "DROP TABLE IF EXISTS changelog_report_gap_sink FORCE"
        sql "DROP STREAM IF EXISTS changelog_immediate_stream FORCE"
        sql "DROP TABLE IF EXISTS changelog_immediate_rows FORCE"
        sql """
            CREATE TABLE changelog_immediate_rows (k INT, v INT)
            UNIQUE KEY(k)
            PARTITION BY RANGE(k) (
                PARTITION p1 VALUES LESS THAN (2),
                PARTITION p2 VALUES LESS THAN (4)
            )
            DISTRIBUTED BY HASH(k) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true",
                "disable_auto_compaction" = "true"
            )
        """
        sql """
            CREATE STREAM changelog_immediate_stream ON TABLE changelog_immediate_rows
            PROPERTIES ("type" = "detail", "show_initial_rows" = "false")
        """

        // No periodic row-count publication can initialize the index or supply these counts.
        sql "INSERT INTO changelog_immediate_rows VALUES (1, 10), (2, 20), (3, 30)"
        recordIndexRows("order_index_rows_after_append")
        qt_physical_rows_after_append """
            SELECT count(*) FROM changelog("table" = "changelog_immediate_rows")
        """
        order_qt_stream_append """
            SELECT k, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM changelog_immediate_stream
        """

        // Historical images produced by MoW are counted after publish, once per physical record.
        sql "UPDATE changelog_immediate_rows SET v = v + 1 WHERE k IN (1, 2)"
        recordIndexRows("order_index_rows_after_update")
        qt_physical_rows_after_update """
            SELECT count(*) FROM changelog("table" = "changelog_immediate_rows")
        """
        order_qt_stream_update """
            SELECT k, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM changelog_immediate_stream
        """

        sql "DELETE FROM changelog_immediate_rows WHERE k = 3"
        recordIndexRows("order_index_rows_after_delete")
        qt_physical_rows_after_delete """
            SELECT count(*) FROM changelog("table" = "changelog_immediate_rows")
        """
        order_qt_stream_delete """
            SELECT k, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM changelog_immediate_stream
        """
        order_qt_base_after_delete "SELECT k, v FROM changelog_immediate_rows"

        sql "DROP STATS changelog_immediate_rows"
        recordIndexRows("order_index_rows_after_drop_stats")
        sql "TRUNCATE TABLE changelog_immediate_rows"
        recordIndexRows("order_index_rows_after_truncate")
        sql "INSERT INTO changelog_immediate_rows VALUES (1, 100), (2, 200)"
        recordIndexRows("order_index_rows_after_replacement_load")
        qt_physical_rows_after_replacement_load """
            SELECT count(*) FROM changelog("table" = "changelog_immediate_rows")
        """

        // Two successful consumptions between periodic reports: each visible load updates the
        // current count independently of the stream's consumption offset and baseline.
        sql """
            CREATE TABLE changelog_report_gap_sink (k INT, v INT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE STREAM changelog_report_gap_stream ON TABLE changelog_immediate_rows
            PROPERTIES ("type" = "detail", "show_initial_rows" = "false")
        """
        sql "INSERT INTO changelog_immediate_rows VALUES (3, 300)"
        recordIndexRows("order_index_rows_before_first_consumption")
        sql "INSERT INTO changelog_report_gap_sink SELECT k, v FROM changelog_report_gap_stream"
        qt_empty_after_first_consumption "SELECT count(*) FROM changelog_report_gap_stream"
        recordIndexRows("order_index_rows_after_first_consumption")
        sql "INSERT INTO changelog_immediate_rows VALUES (0, 400)"
        recordIndexRows("order_index_rows_between_consumptions")
        order_qt_rows_after_second_load """
            SELECT k, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM changelog_report_gap_stream
        """
        sql "INSERT INTO changelog_report_gap_sink SELECT k, v FROM changelog_report_gap_stream"
        qt_empty_after_second_consumption "SELECT count(*) FROM changelog_report_gap_stream"
        order_qt_consumed_rows_between_reports "SELECT k, v FROM changelog_report_gap_sink"
        recordIndexRows("order_index_rows_after_second_consumption")
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(pauseReports)
    }
}
