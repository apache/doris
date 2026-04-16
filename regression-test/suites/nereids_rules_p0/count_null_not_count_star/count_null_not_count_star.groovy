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

suite("count_null_not_count_star") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS count_null_test"

    sql """
        create table count_null_test(pk int, a int, b int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1');
    """

    sql """
        insert into count_null_test values(1, 1, 1), (2, null, 2), (3, 3, null), (4, null, null);
    """
    sql "sync"

    // count(null) should always return 0 regardless of row count
    order_qt_count_null """select count(null) from count_null_test"""

    // count(*) should return total number of rows
    order_qt_count_star """select count(*) from count_null_test"""

    // count(1) should be equivalent to count(*)
    order_qt_count_literal """select count(1) from count_null_test"""

    // count(null) with group by should return 0 for each group
    order_qt_count_null_group_by """select pk, count(null) from count_null_test group by pk order by pk"""

    // mixed: count(null) vs count(*) vs count(column)
    order_qt_count_mixed """select count(null), count(*), count(a), count(b) from count_null_test"""

    // count(null) in subquery with union all
    order_qt_count_null_union """
        select count(null) from (
            select a from count_null_test
            union all
            select a from count_null_test
        ) t
    """

    // count(null) as window function should still return 0
    order_qt_count_null_window """
        select pk, count(null) over (order by pk) from count_null_test order by pk
    """

    // count(null) through join with GROUP BY: exercises PushDownAggThroughJoin path.
    // With the bug, count(null) would be treated as count(*) and pushed down through join,
    // producing wrong non-zero results. With the fix, it correctly returns 0.
    sql "DROP TABLE IF EXISTS count_null_test2"
    sql """
        create table count_null_test2(pk int, c int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1');
    """
    sql """insert into count_null_test2 values(1, 10), (2, 20);"""
    sql "sync"

    order_qt_count_null_join_grouped """
        select t1.pk, count(null), count(*)
        from count_null_test t1 inner join count_null_test2 t2 on t1.pk = t2.pk
        group by t1.pk order by t1.pk
    """

    // count(null) vs count(*) through join without GROUP BY for comparison
    order_qt_count_star_join """
        select count(*), count(null) from count_null_test t1 inner join count_null_test2 t2 on t1.pk = t2.pk
    """

    // COUNT_ON_INDEX path: count(null) on a table with inverted index should NOT use COUNT_ON_MATCH
    sql "DROP TABLE IF EXISTS count_null_idx_test"
    sql """
        create table count_null_idx_test(
            pk int,
            content varchar(200),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties('replication_num' = '1');
    """
    sql """insert into count_null_idx_test values(1, 'hello world'), (2, 'doris test'), (3, 'hello doris');"""
    sql "sync"

    // count(*) with MATCH predicate may use COUNT_ON_MATCH optimization
    // count(null) with same predicate must NOT — verify via explain
    def explainCountStar = sql """
        explain shape plan select count(*) from count_null_idx_test where content match 'hello'
    """
    def explainCountNull = sql """
        explain shape plan select count(null) from count_null_idx_test where content match 'hello'
    """
    // count(null) plan must not contain COUNT_ON_MATCH
    def countNullPlan = explainCountNull.collect { it[0] }.join("\n")
    assertFalse(countNullPlan.contains("COUNT_ON_MATCH"),
        "count(null) should not use COUNT_ON_MATCH optimization, plan:\n${countNullPlan}")

    // result correctness: count(null) should be 0 even with MATCH filter
    order_qt_count_null_index """
        select count(null), count(*) from count_null_idx_test where content match 'hello'
    """
}
