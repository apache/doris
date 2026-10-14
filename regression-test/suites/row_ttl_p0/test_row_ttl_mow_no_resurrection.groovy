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

suite("test_row_ttl_mow_no_resurrection", "nonConcurrent") {
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: vertical,
            vertical_compaction_num_columns_per_group: 1
        ]) {
            for (boolean sourceColumn : [false, true]) {
                sql "DROP TABLE IF EXISTS row_ttl_mow_no_resurrection"
                def ttlProperties = sourceColumn ? ', "function_column.ttl_col"="event_time", "function_column.ttl"="0", "function_column.ttl_time_zone"="+00:00"' : ''
                sql """
                    CREATE TABLE row_ttl_mow_no_resurrection (
                        k INT, event_time DATETIMEV2(6), v INT
                    ) UNIQUE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES (
                        "replication_num"="1", "disable_auto_compaction"="true",
                        "enable_unique_key_merge_on_write"="true",
                        "function_column.enable_row_ttl"="true" ${ttlProperties}
                    )
                """
                // First build a base rowset containing the old live version of key 1.
                sql "INSERT INTO row_ttl_mow_no_resurrection VALUES (1, '9999-01-01', 10)"
                sql "INSERT INTO row_ttl_mow_no_resurrection VALUES (2, '9999-01-01', 20)"
                trigger_and_wait_compaction("row_ttl_mow_no_resurrection", "full")
                // The latest key 1 expires, and cumulative compaction can remove it.
                // Its old base version still relies on the delete bitmap to stay hidden.
                for (int value : 11..14) {
                    if (sourceColumn) {
                        sql "INSERT INTO row_ttl_mow_no_resurrection VALUES (1, '1970-01-01', ${value})"
                    } else {
                        sql """INSERT INTO row_ttl_mow_no_resurrection(k, event_time, v, __DORIS_TTL_COL__)
                            VALUES (1, '9999-01-01', ${value}, 0)"""
                    }
                }
                sql "INSERT INTO row_ttl_mow_no_resurrection VALUES (3, '9999-01-01', 30)"
                "order_qt_before_${vertical}_${sourceColumn}"("SELECT k, v FROM row_ttl_mow_no_resurrection ORDER BY k")
                trigger_and_wait_compaction("row_ttl_mow_no_resurrection", "cumulative")
                "order_qt_cumulative_${vertical}_${sourceColumn}"("SELECT k, v FROM row_ttl_mow_no_resurrection ORDER BY k")
                trigger_and_wait_compaction("row_ttl_mow_no_resurrection", "full")
                "order_qt_full_${vertical}_${sourceColumn}"("SELECT k, v FROM row_ttl_mow_no_resurrection ORDER BY k")
                if (!isCloudMode()) {
                    def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_mow_no_resurrection")[0].MetaUrl
                    def (code, out, err) = curl("GET", metaUrl)
                    assertEquals(0, code)
                    def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                    "qt_physical_${vertical}_${sourceColumn}"("SELECT ${physicalRows} AS physical_rows")
                }
            }
        }
    }
}
