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

suite("test_cloud_changelog_subtransaction_row_count", "nonConcurrent") {
    // This exercises the MS subtransaction commit response; SN uses a different publisher.
    if (!isCloudMode()) {
        return
    }
    def pauseReports = "FE.TabletStatMgr.updateIndexRowCount.disabled"
    def recordIndexRows = { tag ->
        quickRunTest(tag, "SHOW DATA FROM cloud_changelog_subtransaction_rows", true, { row, meta ->
            def columns = (1..meta.getColumnCount()).collectEntries { column ->
                [(meta.getColumnLabel(column)): row[column - 1]]
            }
            def kind = columns.IndexName == "cloud_changelog_subtransaction_rows" ? "base" :
                    (columns.IndexName == "Total" ? "total" : "changelog")
            return [kind, kind == "total" ? "-" : columns.RowCount]
        })
    }
    GetDebugPoint().enableDebugPointForAllFEs(pauseReports)
    try {
        sql "DROP TABLE IF EXISTS cloud_changelog_subtransaction_rows FORCE"
        sql """
            CREATE TABLE cloud_changelog_subtransaction_rows (k INT, v INT)
            DUPLICATE KEY(k)
            PARTITION BY RANGE(k) (
                PARTITION p1 VALUES LESS THAN (3),
                PARTITION p2 VALUES LESS THAN (6)
            )
            DISTRIBUTED BY HASH(k) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "changelog.enable" = "true",
                "disable_auto_compaction" = "true"
            )
        """
        // INSERT SELECT creates separate subtransactions; INSERT VALUES can use one buffered load.
        sql "BEGIN"
        sql "INSERT INTO cloud_changelog_subtransaction_rows SELECT 1, 10 UNION ALL SELECT 3, 30"
        sql "INSERT INTO cloud_changelog_subtransaction_rows SELECT 2, 20 UNION ALL SELECT 4, 40"
        sql "COMMIT"
        recordIndexRows("order_index_rows_after_commit")
        qt_physical_rows_after_commit "SELECT count(*) FROM changelog('table' = 'cloud_changelog_subtransaction_rows')"
        order_qt_rows_after_commit "SELECT * FROM cloud_changelog_subtransaction_rows"

        sql "BEGIN"
        sql "INSERT INTO cloud_changelog_subtransaction_rows SELECT 5, 50"
        sql "ROLLBACK"
        recordIndexRows("order_index_rows_after_rollback")
        qt_physical_rows_after_rollback "SELECT count(*) FROM changelog('table' = 'cloud_changelog_subtransaction_rows')"
        order_qt_rows_after_rollback "SELECT * FROM cloud_changelog_subtransaction_rows"

        sql "BEGIN"
        sql "INSERT INTO cloud_changelog_subtransaction_rows SELECT 5, 50"
        sql "COMMIT"
        recordIndexRows("order_index_rows_after_next_commit")
        qt_physical_rows_after_next_commit "SELECT count(*) FROM changelog('table' = 'cloud_changelog_subtransaction_rows')"
        order_qt_rows_after_next_commit "SELECT * FROM cloud_changelog_subtransaction_rows"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(pauseReports)
    }
}
