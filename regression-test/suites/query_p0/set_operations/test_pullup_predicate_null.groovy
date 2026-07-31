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

// Regression test for PullUpPredicates.getFiltersFromUnionConstExprs() incorrectly
// removing NULL from pulled-up predicates without compensating with OR IS NULL.
// This caused INTERSECT/EXCEPT to lose NULL rows when InferPredicates pushed down
// the incomplete IN(...) filter to set operation children.
suite("test_pullup_predicate_null", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // Original reproducing case: INTERSECT with NULL in CTE constants
    // tbl0 n*2 = {2, 6, NULL}, tbl1 n*2 = {4, NULL, 2}
    // INTERSECT should return {2, NULL}
    order_qt_intersect_null """
        WITH
            tbl0(n) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(n) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT (n * 2) AS n FROM tbl0
            INTERSECT
            SELECT (n * 2) AS n FROM tbl1
        )
    """

    // EXCEPT with NULL: {2, 6, NULL} EXCEPT {4, NULL, 2} = {6}
    order_qt_except_null """
        WITH
            tbl0(n) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(n) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT (n * 2) AS n FROM tbl0
            EXCEPT
            SELECT (n * 2) AS n FROM tbl1
        )
    """

    // EXCEPT where NULL is only in first set: {1, NULL} EXCEPT {1, 2} = {NULL}
    order_qt_except_null_only_left """
        WITH
            tbl0(n) AS (SELECT 1 UNION ALL SELECT NULL),
            tbl1(n) AS (SELECT 1 UNION ALL SELECT 2)
        (
            SELECT n FROM tbl0
            EXCEPT
            SELECT n FROM tbl1
        )
    """

    // All-NULL INTERSECT: {NULL} INTERSECT {NULL, 1} = {NULL}
    order_qt_all_null_intersect """
        SELECT * FROM (SELECT NULL a UNION ALL SELECT NULL) t1
        INTERSECT
        SELECT * FROM (SELECT NULL a UNION ALL SELECT 1) t2
    """

    // UNION DISTINCT with NULL from both sides
    order_qt_union_distinct_null """
        WITH
            tbl0(n) AS (SELECT 1 UNION ALL SELECT NULL),
            tbl1(n) AS (SELECT NULL UNION ALL SELECT 2)
        (
            SELECT n FROM tbl0
            UNION
            SELECT n FROM tbl1
        )
    """

    // Non-null case: verify no regression
    order_qt_no_null_intersect """
        WITH
            tbl0(n) AS (SELECT 1 UNION ALL SELECT 3),
            tbl1(n) AS (SELECT 2 UNION ALL SELECT 1)
        (
            SELECT (n * 2) AS n FROM tbl0
            INTERSECT
            SELECT (n * 2) AS n FROM tbl1
        )
    """

    // Multi-column with NULL in one column
    order_qt_multi_col_null """
        WITH
            t1(a, b) AS (SELECT 1, NULL UNION ALL SELECT 2, 'y'),
            t2(a, b) AS (SELECT 1, NULL UNION ALL SELECT 3, 'z')
        (
            SELECT a, b FROM t1
            INTERSECT
            SELECT a, b FROM t2
        )
    """

    // Table-based test with NULL values
    sql "DROP TABLE IF EXISTS test_pullup_null_t1"
    sql """
        CREATE TABLE test_pullup_null_t1 (
            k1 INT NULL
        ) DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_pullup_null_t1 VALUES (1), (2), (NULL)"

    sql "DROP TABLE IF EXISTS test_pullup_null_t2"
    sql """
        CREATE TABLE test_pullup_null_t2 (
            k1 INT NULL
        ) DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_pullup_null_t2 VALUES (2), (3), (NULL)"

    order_qt_table_intersect_null """
        SELECT k1 FROM test_pullup_null_t1
        INTERSECT
        SELECT k1 FROM test_pullup_null_t2
    """

    order_qt_table_except_null """
        SELECT k1 FROM test_pullup_null_t1
        EXCEPT
        SELECT k1 FROM test_pullup_null_t2
    """
}
