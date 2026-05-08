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

// Regression test for ExprId reuse between set operation output and child output.
// When Nereids optimizer produces a UNION with a single regular child where the
// output ExprId equals the child's output ExprId, the plan translator must translate
// child result expressions before creating the output tuple descriptor to avoid
// the ExprId->SlotRef mapping being overwritten.
suite("set_operation_exprid_reuse", "query,p0") {
    sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
    """

    // Original reproducing query from CIR-19889:
    // CTE + INTERSECT with expression computation causes ExprId reuse
    order_qt_intersect_cte_expr """
        WITH
            tbl0(`n`) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(`n`) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT (`n` * 2) AS `n` FROM tbl0
            INTERSECT
            SELECT (`n` * 2) AS `n` FROM tbl1
        )
    """

    // EXCEPT with CTE and expression
    order_qt_except_cte_expr """
        WITH
            tbl0(`n`) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(`n`) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT (`n` * 2) AS `n` FROM tbl0
            EXCEPT
            SELECT (`n` * 2) AS `n` FROM tbl1
        )
    """

    // UNION DISTINCT with CTE and expression
    order_qt_union_distinct_cte_expr """
        WITH
            tbl0(`n`) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(`n`) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT (`n` * 2) AS `n` FROM tbl0
            UNION
            SELECT (`n` * 2) AS `n` FROM tbl1
        )
    """

    // CAST projection through Union (PushProjectThroughUnion path)
    order_qt_intersect_cast """
        WITH
            tbl0(`n`) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(`n`) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT CAST(`n` AS BIGINT) AS `n` FROM tbl0
            INTERSECT
            SELECT CAST(`n` AS BIGINT) AS `n` FROM tbl1
        )
    """

    // Plain rename projection through Union (PushProjectThroughUnion slot path)
    order_qt_intersect_rename """
        WITH
            tbl0(`n`) AS (SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT NULL),
            tbl1(`n`) AS (SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 1)
        (
            SELECT `n` AS `m` FROM tbl0
            INTERSECT
            SELECT `n` AS `m` FROM tbl1
        )
    """

    // Nested set operations
    order_qt_nested_set_ops """
        SELECT * FROM (SELECT 1 a INTERSECT SELECT 1 a) t1
        UNION
        SELECT * FROM (SELECT 2 a EXCEPT SELECT 3 a) t2
    """

    // INTERSECT with multiple columns and expressions
    order_qt_intersect_multi_col """
        WITH
            t1(a, b) AS (SELECT 1, 'x' UNION ALL SELECT 2, 'y' UNION ALL SELECT 3, 'z'),
            t2(a, b) AS (SELECT 2, 'y' UNION ALL SELECT 3, 'z' UNION ALL SELECT 4, 'w')
        (
            SELECT a * 10 AS a, upper(b) AS b FROM t1
            INTERSECT
            SELECT a * 10 AS a, upper(b) AS b FROM t2
        )
    """

    // INTERSECT with NULL handling
    order_qt_intersect_nulls """
        WITH
            t1(a) AS (SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1),
            t2(a) AS (SELECT NULL UNION ALL SELECT 1 UNION ALL SELECT 2)
        (
            SELECT a FROM t1
            INTERSECT
            SELECT a FROM t2
        )
    """

    // Multiple INTERSECT chained
    order_qt_chained_intersect """
        WITH
            t1(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3),
            t2(n) AS (SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4),
            t3(n) AS (SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5)
        (
            SELECT n * 2 AS n FROM t1
            INTERSECT
            SELECT n * 2 AS n FROM t2
            INTERSECT
            SELECT n * 2 AS n FROM t3
        )
    """

    // INTERSECT with table data (not just constants)
    sql "drop table if exists set_op_exprid_t1"
    sql "drop table if exists set_op_exprid_t2"

    sql """
        CREATE TABLE set_op_exprid_t1 (
            id INT NOT NULL,
            val VARCHAR(50) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE set_op_exprid_t2 (
            id INT NOT NULL,
            val VARCHAR(50) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO set_op_exprid_t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    sql "INSERT INTO set_op_exprid_t2 VALUES (2, 'b'), (3, 'c'), (4, 'd')"

    order_qt_intersect_table """
        SELECT id * 2, upper(val) FROM set_op_exprid_t1
        INTERSECT
        SELECT id * 2, upper(val) FROM set_op_exprid_t2
    """

    order_qt_except_table """
        SELECT id * 2, upper(val) FROM set_op_exprid_t1
        EXCEPT
        SELECT id * 2, upper(val) FROM set_op_exprid_t2
    """
}
