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

suite("topn_runtime_filter_storage_merge") {
    sql "DROP TABLE IF EXISTS topn_storage_merge_agg"
    sql """CREATE TABLE topn_storage_merge_agg (k INT NOT NULL, v BIGINT SUM NOT NULL)
        AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'disable_auto_compaction'='true')"""
    // Separate rowsets keep the two contributions visible to the storage merge.
    sql "INSERT INTO topn_storage_merge_agg VALUES (1,10)"
    sql "INSERT INTO topn_storage_merge_agg VALUES (1,20)"

    sql "DROP TABLE IF EXISTS topn_storage_merge_control"
    sql """CREATE TABLE topn_storage_merge_control (k INT NOT NULL, v BIGINT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_storage_merge_control VALUES (1,999)"

    sql "DROP TABLE IF EXISTS topn_storage_merge_mor"
    sql """CREATE TABLE topn_storage_merge_mor (k INT NOT NULL, v BIGINT NOT NULL)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='false',
                    'disable_auto_compaction'='true')"""
    sql "INSERT INTO topn_storage_merge_mor VALUES (1,10)"
    sql "INSERT INTO topn_storage_merge_mor VALUES (1,20)"

    sql "DROP TABLE IF EXISTS topn_storage_merge_mow"
    sql """CREATE TABLE topn_storage_merge_mow (k INT NOT NULL, v BIGINT NOT NULL)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true')"""
    sql "INSERT INTO topn_storage_merge_mow VALUES (1,10)"
    sql "INSERT INTO topn_storage_merge_mow VALUES (1,20)"

    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET force_sort_algorithm='heap'"
    sql "SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_UNION'"

    def checkMerge = { String name, String source, String validPayload ->
        for (String key : ["k", "abs(k)"]) {
            for (String direction : ["ASC", "DESC"]) {
                String tag = "${name}_${key == 'k' ? 'slot' : 'expr'}_${direction.toLowerCase()}"
                // A second ordering key would disable strict bounds. Accept either complete tie,
                // but reject partial SUM values, stale unique rows, or invented changelog events.
                String query = """SELECT k, (${validPayload}) AS valid_row FROM (
                    SELECT k,v FROM (
                        SELECT k,v FROM ${source}
                        UNION ALL
                        SELECT k,v FROM topn_storage_merge_control
                    ) u ORDER BY ${key} ${direction} NULLS LAST LIMIT 1
                ) selected"""
                sql "SET topn_filter_ratio=0"
                "order_qt_${tag}_disabled"(query)
                sql "SET topn_filter_ratio=1000000"
                explain {
                    sql query
                    contains "UNION"
                    contains "TOP-N"
                    // Keep both targets. BE retains ties on the merging scan only.
                    multiContains "TOPN OPT:", 2
                }
                "order_qt_${tag}_enabled"(query)
            }
        }
    }
    checkMerge("agg", "topn_storage_merge_agg", "v <=> 30 OR v <=> 999")
    checkMerge("mor", "topn_storage_merge_mor", "v <=> 20 OR v <=> 999")
    // Ordinary MOW reads do not merge contributions at query time and remain eligible.
    checkMerge("mow", "topn_storage_merge_mow", "v <=> 20 OR v <=> 999")
}
