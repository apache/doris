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

suite("topn_runtime_filter_safety") {
    sql "DROP TABLE IF EXISTS topn_runtime_filter_safety_a"
    sql """CREATE TABLE topn_runtime_filter_safety_a (
        id INT NOT NULL, k INT NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    // One row fills a LIMIT 1 heap and publishes the strict boundary abs(k) < 10.
    // This is sufficient to reproduce stale-bound reuse: after the fragment is rebuilt,
    // that old boundary would reject the only input of the new, still-empty heap.
    // One recursive step is sufficient to rebuild the source. If reset_bounds() is
    // skipped, only the seed (0,0) survives instead of the expected (0,0),(1,10).
    sql "INSERT INTO topn_runtime_filter_safety_a VALUES (1,10)"
    sql "SET force_sort_algorithm='heap'"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"

    def recursiveQuery = """WITH RECURSIVE r(depth, v) AS (
        SELECT CAST(0 AS INT), CAST(0 AS BIGINT)
        UNION ALL
        SELECT CAST(r.depth + 1 AS INT), s.v
        FROM r CROSS JOIN (
            SELECT abs(k) AS v FROM topn_runtime_filter_safety_a
            ORDER BY v NULLS FIRST LIMIT 1
        ) s WHERE r.depth < 1
    ) SELECT depth, v FROM r ORDER BY depth"""

    sql "SET topn_filter_ratio=0"
    qt_recursive_baseline recursiveQuery

    sql "SET topn_filter_ratio=1000000"
    explain {
        sql recursiveQuery
        contains "TOPN OPT:"
        contains "TOP-N"
    }
    qt_recursive_enabled recursiveQuery
}
