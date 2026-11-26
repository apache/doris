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

suite("test_row_ttl_cluster_key", "nonConcurrent") {
    // Cluster keys require vertical compaction; exercise different value-group widths.
    for (int columnsPerGroup : [1, 3]) {
        setBeConfigTemporary([
            enable_vertical_compaction: true,
            vertical_compaction_num_columns_per_group: columnsPerGroup,
            enable_delete_when_cumu_compaction: false,
            enable_missing_rows_correctness_check: true,
            enable_mow_compaction_correctness_check_fail: true
        ]) {
            for (boolean sourceColumn : [false, true]) {
                sql "DROP TABLE IF EXISTS row_ttl_cluster_key"
                def ttlProperties = sourceColumn ? ', "function_column.ttl_col"="event_time", "function_column.ttl"="0", "function_column.ttl_time_zone"="+00:00"' : ''
                sql """
                    CREATE TABLE row_ttl_cluster_key (
                        k INT, event_time DATETIMEV2(6), v INT
                    ) UNIQUE KEY(k)
                    ORDER BY(v)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES (
                        "replication_num"="1", "disable_auto_compaction"="true",
                        "enable_unique_key_merge_on_write"="true",
                        "function_column.enable_row_ttl"="true" ${ttlProperties}
                    )
                """
                // Cluster-key scans apply delete bitmaps before TTL filtering. The compaction
                // correctness check must count these two causes separately.
                // First build a base rowset containing the old live version of key 1.
                sql "INSERT INTO row_ttl_cluster_key VALUES (1, '9999-01-01', 10)"
                sql "INSERT INTO row_ttl_cluster_key VALUES (2, '9999-01-01', 20)"
                trigger_and_wait_compaction("row_ttl_cluster_key", "full")
                // The latest key 1 expires, and cumulative compaction can remove it.
                // Its old base version still relies on the delete bitmap to stay hidden.
                for (int value : 11..14) {
                    if (sourceColumn) {
                        sql "INSERT INTO row_ttl_cluster_key VALUES (1, '1970-01-01', ${value})"
                    } else {
                        sql """INSERT INTO row_ttl_cluster_key(k, event_time, v, __DORIS_TTL_COL__)
                            VALUES (1, '9999-01-01', ${value}, 0)"""
                    }
                }
                sql "INSERT INTO row_ttl_cluster_key VALUES (3, '9999-01-01', 30)"
                "order_qt_before_${columnsPerGroup}_${sourceColumn}"("SELECT k, v FROM row_ttl_cluster_key ORDER BY k")
                trigger_and_wait_compaction("row_ttl_cluster_key", "cumulative")
                if (!isCloudMode()) {
                    def statusUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_cluster_key")[0].CompactionStatus
                    def (code, out, err) = curl("GET", statusUrl)
                    assertEquals(0, code)
                    def status = parseJson(out.trim())["last cumulative status"].replace("'", "''")
                    "qt_cumulative_status_${columnsPerGroup}_${sourceColumn}"("SELECT '${status}' AS compaction_status")
                }
                "order_qt_cumulative_${columnsPerGroup}_${sourceColumn}"("SELECT k, v FROM row_ttl_cluster_key ORDER BY k")
                trigger_and_wait_compaction("row_ttl_cluster_key", "full")
                "order_qt_full_${columnsPerGroup}_${sourceColumn}"("SELECT k, v FROM row_ttl_cluster_key ORDER BY k")
                if (!isCloudMode()) {
                    def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_cluster_key")[0].MetaUrl
                    def (code, out, err) = curl("GET", metaUrl)
                    assertEquals(0, code)
                    def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                    "qt_physical_${columnsPerGroup}_${sourceColumn}"("SELECT ${physicalRows} AS physical_rows")
                }
            }
        }
    }
}
