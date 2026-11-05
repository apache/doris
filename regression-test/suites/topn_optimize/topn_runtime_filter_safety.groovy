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
    sql """INSERT INTO topn_runtime_filter_safety_a
        SELECT CAST(number AS INT), 10 FROM numbers('number'='1000000')"""

    def recursiveQuery = """WITH RECURSIVE r(depth, v) AS (
        SELECT CAST(0 AS INT), CAST(0 AS BIGINT)
        UNION ALL
        SELECT CAST(r.depth + 1 AS INT), s.v
        FROM r CROSS JOIN (
            SELECT abs(k) AS v FROM topn_runtime_filter_safety_a
            ORDER BY v NULLS FIRST LIMIT 1
        ) s WHERE r.depth < 3
    ) SELECT depth, v FROM r ORDER BY depth"""

    sql "SET topn_filter_ratio=0"
    qt_recursive_baseline recursiveQuery

    sql "SET topn_filter_ratio=1000000"
    qt_recursive_enabled recursiveQuery
}
