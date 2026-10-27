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

suite("topn_evaluation_safety", "p0") {
    sql "DROP TABLE IF EXISTS topn_error_input"
    sql "DROP TABLE IF EXISTS topn_error_build"
    sql """CREATE TABLE topn_error_input (id INT, k INT, d DATE, ts BIGINT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """CREATE TABLE topn_error_build (id INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    // The first physical key carries the error; later batches contain the best TopN keys.
    sql """INSERT INTO topn_error_input
        SELECT 0, 100, CAST('9999-12-31' AS DATE), CAST(-1 AS BIGINT)
        UNION ALL
        SELECT number + 1, 0, CAST('2020-01-01' AS DATE), CAST(0 AS BIGINT)
        FROM numbers("number"="8192")"""
    sql "INSERT INTO topn_error_build SELECT id FROM topn_error_input"
    sql "SET topn_filter_ratio = 1000000"
    // Isolate runtime filtering from ordered scan/TopN pushdown and fix the join's probe side.
    sql "SET topn_opt_limit_threshold = 0"
    sql "SET disable_join_reorder = true"

    String dateQuery = """SELECT k, weeks_add(d, 1) AS v FROM topn_error_input
        ORDER BY k, v LIMIT 1"""
    explain {
        sql dateQuery
        notContains "TOPN OPT:"
    }
    test {
        sql dateQuery
        exception "out of range"
    }

    for (String comparison : ['=', '<']) {
        String joinQuery = """SELECT l.k FROM topn_error_input l JOIN [broadcast] topn_error_build r
            ON l.id ${comparison} r.id AND assert_true(l.k < 3, 'topn join error')
            ORDER BY l.k LIMIT 1"""
        explain {
            sql joinQuery
            notContains "TOPN OPT:"
        }
        test {
            sql joinQuery
            exception "topn join error"
        }
    }

    // Residual WHERE predicates are attached to the join only after the TopN visitor runs.
    String whereQuery = """SELECT l.k FROM topn_error_input l JOIN [broadcast] topn_error_build r
        ON l.id = r.id WHERE assert_true(l.k < 3, 'topn where error') ORDER BY l.k LIMIT 1"""
    explain {
        sql whereQuery
        notContains "TOPN OPT:"
    }
    test {
        sql whereQuery
        exception "topn where error"
    }

    String generateQuery = """SELECT k, checked FROM topn_error_input
        LATERAL VIEW explode(array(assert_true(k < 3, 'topn generator error'))) lv AS checked
        ORDER BY k, checked LIMIT 1"""
    explain {
        sql generateQuery
        notContains "TOPN OPT:"
    }
    test {
        sql generateQuery
        exception "topn generator error"
    }

    // These deterministic conversions can throw and are not in the date-add/subtract hierarchy.
    for (String function : ['from_second', 'from_millisecond', 'from_microsecond']) {
        String epochQuery = "SELECT k, ${function}(ts) AS v FROM topn_error_input ORDER BY k, v LIMIT 1"
        explain {
            sql epochQuery
            notContains "TOPN OPT:"
        }
        test {
            sql epochQuery
            exception "out of range"
        }
    }

    // Literal-zero date arithmetic is total and must keep the optimization available.
    explain {
        sql "SELECT k, weeks_add(d, 0) AS v FROM topn_error_input ORDER BY k, v LIMIT 1"
        contains "TOPN OPT:"
    }
}
