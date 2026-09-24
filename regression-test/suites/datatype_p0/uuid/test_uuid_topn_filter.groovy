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

// Checklist: C08 E08 F07 J07.
suite("test_uuid_topn_filter", "p0") {
    sql "DROP TABLE IF EXISTS uuid_topn_filter"
    sql """CREATE TABLE uuid_topn_filter (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1')"""
    // UUID is a non-key value column; each page mixes both sides of the unsigned boundary.
    sql """INSERT INTO uuid_topn_filter SELECT number,
           IF(number % 65536 = 0,CAST(NULL AS UUID),
              CAST(CONCAT(CASE number % 3 WHEN 0 THEN '7fffffffffffffff'
                               WHEN 1 THEN '8000000000000000' ELSE 'ffffffffffffffff' END,
                          LPAD(HEX(number),16,'0')) AS UUID))
           FROM numbers('number'='131072')"""
    sql "SET enable_sql_cache = false"
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_two_phase_read_opt = false"
    // Exercise the runtime filter independently of ordered scan/limit pushdown.
    sql "SET topn_opt_limit_threshold = 0"
    // Two NULLs leave a non-NULL TopN bound even for NULLS FIRST.
    for (String direction : ['ASC', 'DESC']) {
        for (String nullOrder : ['FIRST', 'LAST']) {
            String query = "SELECT id,u FROM uuid_topn_filter ORDER BY u ${direction} NULLS ${nullOrder},id LIMIT 10"
            for (boolean enabled : [false, true]) {
                sql "SET topn_filter_ratio = ${enabled ? 1000000 : 0}"
                explain {
                    sql query
                    if (enabled) {
                        contains "TOPN OPT:"
                    } else {
                        notContains "TOPN OPT:"
                    }
                }
                // Scanning may finish before the sorter publishes a bound, so a positive
                // RowsVectorPredFiltered is not guaranteed. RuntimePredicateTest covers UUID
                // scanner filtering before and after publishing a bound deterministically.
                qt_result query
            }
        }
    }
}
