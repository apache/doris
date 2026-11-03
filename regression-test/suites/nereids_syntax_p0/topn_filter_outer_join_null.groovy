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

suite("topn_filter_outer_join_null", "p0") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET disable_join_reorder = true"
    sql "SET parallel_pipeline_task_num = 1"
    sql "DROP TABLE IF EXISTS topn_filter_outer_join_null"
    sql """CREATE TABLE topn_filter_outer_join_null (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO topn_filter_outer_join_null VALUES (0), (1)"

    for (String joinType : ["FULL OUTER", "RIGHT OUTER"]) {
        // The inequalities match the same integer keys while requiring a nested loop join.
        for (String condition : ["l.k = r.k", "l.k < r.k + 1 AND l.k > r.k - 1"]) {
            for (String direction : ["ASC", "DESC"]) {
                int firstKey = direction == "ASC" ? 0 : 1
                int fallback = direction == "ASC" ? -1 : 2
                String probe = """(
                    SELECT k FROM topn_filter_outer_join_null WHERE k = ${firstKey}
                    UNION ALL
                    SELECT k FROM topn_filter_outer_join_null
                    WHERE k != ${firstKey} AND sleep(k + ${firstKey}) = 0
                )"""
                for (String expression : ["coalesce(l.k, ${fallback})",
                        "ifnull(l.k, ${fallback})",
                        "if(l.k is null, ${fallback}, l.k)",
                        "case when l.k is null then ${fallback} else l.k end",
                        "coalesce(l.k, ${fallback}) - 1"]) {
                    String query = """SELECT ${expression} AS sort_key, coalesce(r.k, -1)
                        FROM ${probe} l ${joinType} JOIN topn_filter_outer_join_null r ON ${condition}
                        ORDER BY sort_key ${direction} NULLS LAST, 2 NULLS LAST LIMIT 1"""
                    sql "SET topn_filter_ratio = 1000000"
                    explain {
                        sql query
                        contains(condition == "l.k = r.k" ? "HASH JOIN" : "NESTED LOOP JOIN")
                        notContains "TOPN OPT:"
                    }
                    qt_enabled query
                    sql "SET topn_filter_ratio = 0"
                    qt_disabled query
                }
            }
        }
    }

    sql "SET topn_filter_ratio = 1000000"
    for (String nullOrder : ["FIRST", "LAST"]) {
        String query = """SELECT l.k, r.k FROM (
            SELECT k FROM topn_filter_outer_join_null WHERE k = 0
            UNION ALL
            SELECT k FROM topn_filter_outer_join_null WHERE k > 0 AND sleep(k) = 0
        ) l FULL OUTER JOIN topn_filter_outer_join_null r ON l.k = r.k
        ORDER BY l.k NULLS ${nullOrder}, r.k NULLS LAST LIMIT 1"""
        explain {
            sql query
            if (nullOrder == "FIRST") {
                notContains "TOPN OPT:"
            } else {
                contains "TOPN OPT:"
            }
        }
        qt_bare_key query
    }
}
