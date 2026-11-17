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

suite("topn_filter_assert_num_rows") {
    sql "DROP TABLE IF EXISTS topn_assert_t"
    sql """CREATE TABLE topn_assert_t (id BIGINT NOT NULL, k INT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num" = "1")"""
    sql "DROP TABLE IF EXISTS topn_assert_b"
    sql """CREATE TABLE topn_assert_b (id BIGINT NOT NULL, k INT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO topn_assert_t SELECT number, 20 FROM numbers('number' = '1000000')"
    sql "INSERT INTO topn_assert_b VALUES (0, 10)"
    // Statistics allow the scalar branch to become the nested loop join's probe side.
    sql "ANALYZE TABLE topn_assert_t WITH SYNC"
    sql "ANALYZE TABLE topn_assert_b WITH SYNC"

    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET disable_nereids_rules = 'PUSH_DOWN_TOP_N_THROUGH_UNION'"
    sql "SET force_sort_algorithm = 'heap'"
    sql "SET batch_size = 1024"

    for (int target : [999999, 1000000]) {
        for (String expression : ["k", "coalesce(k, -1)"]) {
            for (String nullOrder : ["FIRST", "LAST"]) {
                // The scalar returns one row for 999999 and no rows for 1000000.
                // Filtering its input must not manufacture or suppress its NULL result.
                String query = """SELECT ${expression} AS sort_key FROM (
                        SELECT (SELECT k FROM topn_assert_t WHERE abs(id - ${target}) = 0) AS k
                        UNION ALL SELECT k FROM topn_assert_b
                    ) u ORDER BY sort_key NULLS ${nullOrder} LIMIT 1"""
                sql "SET topn_filter_ratio = 1000000"
                explain {
                    sql query
                    contains "VASSERT NUMBER OF ROWS"
                    contains "assert number of rows: EQ 1"
                    // Keep the other UNION scan as a target, without reaching the scalar's scan.
                    multiContains "TOPN OPT:", 1
                }
                qt_enabled query
                sql "SET topn_filter_ratio = 0"
                qt_disabled query
            }
        }
    }

    for (String expression : ["k", "coalesce(k, -1)"]) {
        for (String nullOrder : ["FIRST", "LAST"]) {
            for (int ratio : [0, 1000000]) {
                sql "SET topn_filter_ratio = ${ratio}"
                // Both rows must reach Assert: even with NULLS LAST, dropping them
                // could turn a required scalar-cardinality error into a successful query.
                test {
                    sql """SELECT ${expression} AS sort_key FROM (
                            SELECT (SELECT k FROM topn_assert_t WHERE abs(id - 999999) <= 1) AS k
                            UNION ALL SELECT k FROM topn_assert_b
                        ) u ORDER BY sort_key NULLS ${nullOrder} LIMIT 1"""
                    exception "Expected EQ 1"
                }
            }
        }
    }
}
