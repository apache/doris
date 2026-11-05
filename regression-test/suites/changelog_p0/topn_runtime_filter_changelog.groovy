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

suite("topn_runtime_filter_changelog") {
    sql "DROP TABLE IF EXISTS topn_storage_merge_log"
    sql """CREATE TABLE topn_storage_merge_log (k INT NOT NULL, v BIGINT NOT NULL)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                    'disable_auto_compaction'='true', 'changelog.enable'='true',
                    'changelog.need_historical_value'='true')"""
    sql "INSERT INTO topn_storage_merge_log VALUES (1,10)"
    sql "DELETE FROM topn_storage_merge_log WHERE k=1"

    sql "DROP TABLE IF EXISTS topn_changelog_control"
    sql """CREATE TABLE topn_changelog_control (k INT NOT NULL, v BIGINT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_changelog_control VALUES (1,999)"

    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET force_sort_algorithm='heap'"
    sql "SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_UNION'"

    for (String key : ["k", "abs(k)"]) {
        for (String direction : ["ASC", "DESC"]) {
            String tag = "${key == 'k' ? 'slot' : 'expr'}_${direction.toLowerCase()}"
            // INSERT followed by DELETE cancels out. Only (1,999) is valid.
            String query = """SELECT k,v FROM (
                SELECT k,v FROM topn_storage_merge_log@incr('incrementType'='MIN_DELTA')
                UNION ALL SELECT k,v FROM topn_changelog_control
            ) u ORDER BY ${key} ${direction} NULLS LAST LIMIT 1"""
            sql "SET topn_filter_ratio=0"
            "order_qt_${tag}_disabled"(query)
            sql "SET topn_filter_ratio=1000000"
            explain {
                sql query
                contains "UNION"
                contains "TOP-N"
                // The control scan remains a target; the changelog reader must see every event.
                multiContains "TOPN OPT:", 1
            }
            "order_qt_${tag}_enabled"(query)
        }
    }
}
