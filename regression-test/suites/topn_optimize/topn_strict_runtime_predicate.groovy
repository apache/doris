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

suite("topn_strict_runtime_predicate") {
    sql "DROP TABLE IF EXISTS topn_strict_runtime_predicate"
    sql """CREATE TABLE topn_strict_runtime_predicate (
        id BIGINT NOT NULL, k VARCHAR(32) NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO topn_strict_runtime_predicate
        SELECT number, CASE WHEN number % 17 = 0 THEN NULL
                            WHEN number % 3 = 0 THEN ''
                            WHEN number % 3 = 1 THEN 'middle' ELSE 'zulu' END
        FROM numbers('number'='100000')"""

    sql "DROP TABLE IF EXISTS topn_strict_runtime_predicate_join"
    sql """CREATE TABLE topn_strict_runtime_predicate_join (
        id BIGINT NOT NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO topn_strict_runtime_predicate_join
        SELECT number FROM numbers('number'='10') UNION ALL SELECT 100001"""

    // Keep the offset candidates and the requested number of equal boundary values.
    sql "SET topn_filter_ratio=0"
    qt_asc_baseline "SELECT k FROM topn_strict_runtime_predicate ORDER BY k ASC NULLS LAST LIMIT 20 OFFSET 7"
    qt_desc_baseline "SELECT k FROM topn_strict_runtime_predicate ORDER BY k DESC NULLS LAST LIMIT 20 OFFSET 7"
    qt_nulls_first_baseline "SELECT k FROM topn_strict_runtime_predicate ORDER BY k NULLS FIRST LIMIT 20 OFFSET 7"
    qt_multi_baseline "SELECT k,id FROM topn_strict_runtime_predicate ORDER BY k,id LIMIT 20 OFFSET 7"
    qt_expr_asc_baseline """SELECT abs(id % 13 - 6) AS v FROM topn_strict_runtime_predicate
        ORDER BY v LIMIT 20 OFFSET 7"""
    qt_expr_desc_baseline """SELECT abs(id % 13 - 6) AS v FROM topn_strict_runtime_predicate
        ORDER BY v DESC LIMIT 20 OFFSET 7"""
    qt_join_asc_baseline """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k ASC NULLS LAST LIMIT 20 OFFSET 7"""
    qt_join_desc_baseline """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k DESC NULLS LAST LIMIT 20 OFFSET 7"""
    qt_join_multi_baseline """SELECT /*+ leading(a b) */ a.k,a.id,b.id
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k,a.id,b.id LIMIT 20 OFFSET 7"""
    qt_right_join_last_baseline """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a RIGHT OUTER JOIN topn_strict_runtime_predicate_join b
        ON a.id = b.id ORDER BY a.k NULLS LAST LIMIT 5 OFFSET 1"""
    qt_right_join_first_baseline """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a RIGHT OUTER JOIN topn_strict_runtime_predicate_join b
        ON a.id = b.id ORDER BY a.k NULLS FIRST LIMIT 5 OFFSET 1"""
    qt_grouped_baseline """SELECT k,count(*) FROM topn_strict_runtime_predicate
        GROUP BY k ORDER BY k NULLS LAST LIMIT 3"""

    sql "SET topn_filter_ratio=1000000"
    qt_asc_strict "SELECT k FROM topn_strict_runtime_predicate ORDER BY k ASC NULLS LAST LIMIT 20 OFFSET 7"
    qt_desc_strict "SELECT k FROM topn_strict_runtime_predicate ORDER BY k DESC NULLS LAST LIMIT 20 OFFSET 7"
    qt_nulls_first_strict "SELECT k FROM topn_strict_runtime_predicate ORDER BY k NULLS FIRST LIMIT 20 OFFSET 7"
    qt_multi_inclusive "SELECT k,id FROM topn_strict_runtime_predicate ORDER BY k,id LIMIT 20 OFFSET 7"
    qt_expr_asc_strict """SELECT abs(id % 13 - 6) AS v FROM topn_strict_runtime_predicate
        ORDER BY v LIMIT 20 OFFSET 7"""
    qt_expr_desc_strict """SELECT abs(id % 13 - 6) AS v FROM topn_strict_runtime_predicate
        ORDER BY v DESC LIMIT 20 OFFSET 7"""
    qt_join_asc_strict """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k ASC NULLS LAST LIMIT 20 OFFSET 7"""
    qt_join_desc_strict """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k DESC NULLS LAST LIMIT 20 OFFSET 7"""
    qt_join_multi_inclusive """SELECT /*+ leading(a b) */ a.k,a.id,b.id
        FROM topn_strict_runtime_predicate a JOIN topn_strict_runtime_predicate_join b
        ON a.id % 5 = b.id % 5 ORDER BY a.k,a.id,b.id LIMIT 20 OFFSET 7"""
    qt_right_join_last_strict """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a RIGHT OUTER JOIN topn_strict_runtime_predicate_join b
        ON a.id = b.id ORDER BY a.k NULLS LAST LIMIT 5 OFFSET 1"""
    qt_right_join_first_no_filter """SELECT /*+ leading(a b) */ a.k
        FROM topn_strict_runtime_predicate a RIGHT OUTER JOIN topn_strict_runtime_predicate_join b
        ON a.id = b.id ORDER BY a.k NULLS FIRST LIMIT 5 OFFSET 1"""
    qt_grouped_strict """SELECT k,count(*) FROM topn_strict_runtime_predicate
        GROUP BY k ORDER BY k NULLS LAST LIMIT 3"""

    // Group-key targets keep inclusive bounds, so boundary groups retain all contributions.
    explain {
        sql """SELECT /*+ SET_VAR(push_topn_to_agg=false) */ k,count(*)
            FROM topn_strict_runtime_predicate GROUP BY k ORDER BY k NULLS LAST LIMIT 3"""
        contains "AGGREGATE"
        contains "TOPN OPT:"
    }
    explain {
        sql """SELECT /*+ SET_VAR(push_topn_to_agg=false) */ k,sum(id + 1)
            FROM topn_strict_runtime_predicate GROUP BY k ORDER BY k NULLS LAST LIMIT 3"""
        contains "AGGREGATE"
        contains "TOPN OPT:"
    }
}
