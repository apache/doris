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

suite("test_row_ttl_ordered_compaction", "nonConcurrent") {
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: vertical,
            enable_ordered_data_compaction: true,
            ordered_data_compaction_min_segment_size: 0
        ]) {
            sql "DROP TABLE IF EXISTS row_ttl_ordered_compaction"
            sql """
                CREATE TABLE row_ttl_ordered_compaction(k INT, v INT) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
                    "replication_num"="1", "disable_auto_compaction"="true",
                    "function_column.enable_row_ttl"="true")
            """
            // Disjoint sorted key ranges qualify for link-file compaction. Expired rows
            // must still be physically removed by the cumulative compaction's row reader.
            for (int key : 1..6) {
                def expiration = key % 2 == 0 ? Long.MAX_VALUE : 0L
                sql """INSERT INTO row_ttl_ordered_compaction(k, v, __DORIS_TTL_COL__)
                    VALUES (${key}, ${key}, ${expiration})"""
            }
            "order_qt_before_${vertical}"("SELECT k, v FROM row_ttl_ordered_compaction ORDER BY k")
            trigger_and_wait_compaction("row_ttl_ordered_compaction", "cumulative")
            "order_qt_after_${vertical}"("SELECT k, v FROM row_ttl_ordered_compaction ORDER BY k")
            if (!isCloudMode()) {
                def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_ordered_compaction")[0].MetaUrl
                def (code, out, err) = curl("GET", metaUrl)
                assertEquals(0, code)
                def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                "qt_physical_${vertical}"("SELECT ${physicalRows} AS physical_rows")
            }
        }
    }
}
