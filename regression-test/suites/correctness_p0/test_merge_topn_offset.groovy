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

suite("test_merge_topn_offset") {
    // DORIS-26301: when MERGE_TOP_N merges a parent TopN that carries a non-zero OFFSET into its
    // child TopN, the merged limit must be clamped by (childLimit - parentOffset). Otherwise rows
    // beyond the child's limit leak through. Before the fix, an outer "LIMIT 3 OFFSET 4" over an
    // inner "LIMIT 5" wrongly returned 3 rows (k=5,6,7) instead of the single correct row (k=5).
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tableName = "test_merge_topn_offset"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            k INT NOT NULL,
            v INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ INSERT INTO ${tableName} VALUES
        (1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
        (6, 60), (7, 70), (8, 80), (9, 90), (10, 100) """

    // MERGE_TOP_N is enabled by default; stacking an outer TopN with an OFFSET over an inner TopN
    // triggers it. The inner "ORDER BY k LIMIT 5" yields k = 1..5.

    // outer offset (4) consumes 4 of the inner's 5 rows -> only k=5 remains
    qt_merge_topn_off4lim3 """ SELECT * FROM (SELECT k, v FROM ${tableName} ORDER BY k LIMIT 5) s ORDER BY k LIMIT 3 OFFSET 4 """
    // outer offset (2) is within the inner limit -> k = 3,4,5
    qt_merge_topn_off2lim3 """ SELECT * FROM (SELECT k, v FROM ${tableName} ORDER BY k LIMIT 5) s ORDER BY k LIMIT 3 OFFSET 2 """
    // outer offset (6) exceeds the inner limit (5) -> empty
    qt_merge_topn_off6lim3 """ SELECT * FROM (SELECT k, v FROM ${tableName} ORDER BY k LIMIT 5) s ORDER BY k LIMIT 3 OFFSET 6 """
}
