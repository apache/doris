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

suite("test_row_ttl_empty_blocks", "nonConcurrent") {
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: vertical,
            vertical_compaction_num_columns_per_group: 1,
            inverted_index_compaction_enable: true,
            // Opening every output index at close catches missing compacted string indexes.
            enable_write_index_searcher_cache: true
        ]) {
            for (boolean unique : [false, true]) {
                sql "DROP TABLE IF EXISTS row_ttl_empty_blocks"
                def keyType = unique ? "UNIQUE" : "DUPLICATE"
                def mowProperty = unique ? ', "enable_unique_key_merge_on_write"="true"' : ''
                sql """
                    CREATE TABLE row_ttl_empty_blocks(
                        k BIGINT, v1 BIGINT, v2 STRING,
                        INDEX idx_v1(v1) USING INVERTED,
                        INDEX idx_v2(v2) USING INVERTED
                    )
                    ${keyType} KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES (
                        "replication_num"="1", "disable_auto_compaction"="true",
                        "function_column.enable_row_ttl"="true" ${mowProperty}
                    )
                """
                // Multiple complete reader blocks expire before the first surviving key.
                // An empty filtered block must not end the compaction or misalign value groups.
                sql """INSERT INTO row_ttl_empty_blocks(k,v1,v2,__DORIS_TTL_COL__)
                    SELECT number, number + 1, number + 2, 0 FROM numbers("number"="8192")"""
                sql """INSERT INTO row_ttl_empty_blocks(k,v1,v2,__DORIS_TTL_COL__)
                    VALUES (10000,10001,10002,NULL),(20000,20001,20002,9223372036854775807)"""
                trigger_and_wait_compaction("row_ttl_empty_blocks", "full")
                "order_qt_survivors_${vertical}_${unique}"(
                    "SELECT k,v1,v2 FROM row_ttl_empty_blocks ORDER BY k")
                "order_qt_index_survivors_${vertical}_${unique}"(
                    "SELECT k FROM row_ttl_empty_blocks WHERE v1 >= 10001 ORDER BY k")
                "order_qt_text_index_survivors_${vertical}_${unique}"(
                    "SELECT k FROM row_ttl_empty_blocks WHERE v2 IN ('10002','20002','30002') ORDER BY k")
                if (!isCloudMode()) {
                    def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_empty_blocks")[0].MetaUrl
                    def (code, out, err) = curl("GET", metaUrl)
                    assertEquals(0, code)
                    def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                    "qt_physical_survivors_${vertical}_${unique}"("SELECT ${physicalRows} AS physical_rows")
                }
                // Updates after rowid conversion must still hide the previous MoW version.
                sql """INSERT INTO row_ttl_empty_blocks(k,v1,v2,__DORIS_TTL_COL__)
                    VALUES (10000,7,8,0),(30000,30001,30002,NULL)"""
                trigger_and_wait_compaction("row_ttl_empty_blocks", "full")
                "order_qt_after_write_${vertical}_${unique}"(
                    "SELECT k,v1,v2 FROM row_ttl_empty_blocks ORDER BY k")
                "order_qt_index_after_write_${vertical}_${unique}"(
                    "SELECT k FROM row_ttl_empty_blocks WHERE v1 >= 10001 ORDER BY k")
                "order_qt_text_index_after_write_${vertical}_${unique}"(
                    "SELECT k FROM row_ttl_empty_blocks WHERE v2 IN ('10002','20002','30002') ORDER BY k")
                if (!isCloudMode()) {
                    def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_empty_blocks")[0].MetaUrl
                    def (code, out, err) = curl("GET", metaUrl)
                    assertEquals(0, code)
                    def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                    "qt_physical_after_write_${vertical}_${unique}"("SELECT ${physicalRows} AS physical_rows")
                }
            }
        }
    }
}
