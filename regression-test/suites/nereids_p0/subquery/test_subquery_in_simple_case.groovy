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

// Regression test for duplicate RelationId bug in simple CASE WHEN with subqueries.
// The bug caused "groupExpression already exists in memo" error because the simple
// case value (a subquery) was duplicated into multiple EqualTo nodes at parse time.

suite("test_subquery_in_simple_case") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_simple_case_subquery"
    sql """
        CREATE TABLE test_simple_case_subquery (
            big_key int,
            int_1 int
        )
        DISTRIBUTED BY HASH(big_key) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql "INSERT INTO test_simple_case_subquery VALUES (1, 1), (2, 3), (3, 5)"

    // Case 1: Exact reproduce case from bug report.
    // sum(int_1)=9, CASE 9 WHEN 1 THEN 2 WHEN 3 THEN 4 ELSE 5 END → 5 for all 3 rows.
    order_qt_case1 """
        select case (select sum(int_1) from test_simple_case_subquery) when 1 then 2 when 3 then 4 else 5 end a3 from test_simple_case_subquery
    """

    // Case 2: Simple case with scalar subquery, multiple WHEN clauses, no ELSE.
    // sum(int_1)=9, CASE 9 WHEN 9 THEN 'match' WHEN 0 THEN 'zero' END → 'match' for all 3 rows.
    order_qt_case2 """
        select case (select sum(int_1) from test_simple_case_subquery) when 9 then 'match' when 0 then 'zero' end from test_simple_case_subquery
    """

    // Case 3: Simple case with column reference (non-subquery baseline).
    // Row-by-row: int_1=1→'one', int_1=3→'three', int_1=5→'other'.
    order_qt_case3 """
        select case int_1 when 1 then 'one' when 3 then 'three' else 'other' end from test_simple_case_subquery
    """

    // Case 4: Simple case with subquery in value AND subquery in result.
    // sum(int_1)=9, CASE 9 WHEN 9 THEN max(int_1)=5 ELSE 0 END → 5 for all 3 rows.
    order_qt_case4 """
        select case (select sum(int_1) from test_simple_case_subquery) when 9 then (select max(int_1) from test_simple_case_subquery) else 0 end from test_simple_case_subquery
    """

    // Case 5: Single WHEN clause with subquery value (edge case).
    // sum(int_1)=9, CASE 9 WHEN 9 THEN 'yes' ELSE 'no' END → 'yes' for all 3 rows.
    order_qt_case5 """
        select case (select sum(int_1) from test_simple_case_subquery) when 9 then 'yes' else 'no' end from test_simple_case_subquery
    """

    // Case 6: Nested case with subquery.
    // count(*)=3, sum(int_1)=9.
    // CASE 3 WHEN 3 THEN (CASE 9 WHEN 9 THEN 'nested_match' ELSE 'nested_other' END) ELSE 'outer_other' END
    // → 'nested_match' for all 3 rows.
    order_qt_case6 """
        select case (select count(*) from test_simple_case_subquery) when 3 then case (select sum(int_1) from test_simple_case_subquery) when 9 then 'nested_match' else 'nested_other' end else 'outer_other' end from test_simple_case_subquery
    """

    // Case 7: Searched case with subquery (should still work, not affected by change).
    // sum(int_1)=9, CASE WHEN 9 > 5 THEN 'big' ELSE 'small' END → 'big' for all 3 rows.
    order_qt_case7 """
        select case when (select sum(int_1) from test_simple_case_subquery) > 5 then 'big' else 'small' end from test_simple_case_subquery
    """
}
