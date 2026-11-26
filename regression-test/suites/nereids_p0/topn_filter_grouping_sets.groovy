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

suite("topn_filter_grouping_sets") {
    sql "DROP TABLE IF EXISTS topn_repeat_t"
    sql """CREATE TABLE topn_repeat_t (id BIGINT NOT NULL, k INT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num" = "1")"""
    sql "DROP TABLE IF EXISTS topn_repeat_b"
    sql """CREATE TABLE topn_repeat_b (id BIGINT NOT NULL, k INT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO topn_repeat_t SELECT number, 20 FROM numbers('number' = '1000000')"
    sql "INSERT INTO topn_repeat_b VALUES (0, 10)"

    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    // Keep a single parent TopN so the small UNION branch can publish a bound
    // before the other branch has finished reading the aggregate's input.
    sql "SET disable_nereids_rules = 'PUSH_DOWN_TOP_N_THROUGH_UNION'"
    sql "SET push_topn_to_agg = false"
    sql "SET force_sort_algorithm = 'heap'"
    sql "SET batch_size = 1024"
    sql "SET topn_filter_ratio = 1000000"

    explain {
        sql """SELECT k, COUNT(*) FROM topn_repeat_t
               GROUP BY GROUPING SETS ((k), ()) ORDER BY k NULLS FIRST LIMIT 1"""
        contains "VREPEAT"
        notContains "TOPN OPT:"
    }
    // A key present in every grouping set can still be filtered at the scan.
    explain {
        sql """SELECT k, id, COUNT(*) FROM topn_repeat_t
               GROUP BY GROUPING SETS ((k, id), (k)) ORDER BY k, id NULLS FIRST LIMIT 1"""
        contains "VREPEAT"
        contains "TOPN OPT:"
    }

    for (String grouping : ["GROUPING SETS ((k), ())", "ROLLUP(k)", "CUBE(k)"]) {
        for (String ordering : ["k ASC NULLS FIRST", "k DESC NULLS FIRST", "coalesce(k, 0)"]) {
            String query = """SELECT k, c FROM (
                    SELECT k, COUNT(*) AS c FROM topn_repeat_t GROUP BY ${grouping}
                    UNION ALL
                    SELECT k, CAST(0 AS BIGINT) AS c FROM topn_repeat_b
                ) u ORDER BY ${ordering} LIMIT 1"""
            explain {
                sql query
                contains "VREPEAT"
                contains "TOPN OPT:"
            }
            sql "SET topn_filter_ratio = 0"
            qt_without_filter query
            sql "SET topn_filter_ratio = 1000000"
            qt_with_filter query
        }
    }
    qt_common_grouping_key """SELECT k, id, COUNT(*) FROM topn_repeat_t
            GROUP BY GROUPING SETS ((k, id), (k)) ORDER BY k, id NULLS FIRST LIMIT 1"""
}
