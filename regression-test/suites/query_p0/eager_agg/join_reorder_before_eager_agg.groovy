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

suite("join_reorder_before_eager_agg") {
    sql "set disable_join_reorder=false;"
    sql "set enable_cost_based_join_reorder=false;"
    sql "set memo_max_group_expression_size=1;"
    sql "set runtime_filter_mode=OFF;"
    sql 'set ignore_shape_nodes="PhysicalProject, PhysicalDistribute";'

    multi_sql """
        DROP TABLE IF EXISTS eager_reorder_a;
        DROP TABLE IF EXISTS eager_reorder_b;
        DROP TABLE IF EXISTS eager_reorder_c;

        CREATE TABLE eager_reorder_a (
            k INT NOT NULL,
            g INT NOT NULL,
            x INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");

        CREATE TABLE eager_reorder_b (
            a_k INT NOT NULL,
            c_k INT NOT NULL,
            v BIGINT NOT NULL
        )
        DUPLICATE KEY(a_k, c_k)
        DISTRIBUTED BY HASH(a_k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");

        CREATE TABLE eager_reorder_c (
            k INT NOT NULL,
            x INT NOT NULL,
            v BIGINT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");

        INSERT INTO eager_reorder_a VALUES
            (1, 10, 5),
            (2, 20, 7),
            (3, 30, 9);

        INSERT INTO eager_reorder_b VALUES
            (1, 101, 2),
            (1, 102, 3),
            (2, 101, 4),
            (3, 103, 5),
            (3, 104, 6);

        INSERT INTO eager_reorder_c VALUES
            (101, 7, 10),
            (101, 8, 20),
            (102, 8, 30),
            (103, 14, 40),
            (104, 99, 50);

        ALTER TABLE eager_reorder_a MODIFY COLUMN k SET STATS (
            'row_count'='1000000', 'ndv'='1000000', 'num_nulls'='0',
            'min_value'='1', 'max_value'='1000000');
        ALTER TABLE eager_reorder_b MODIFY COLUMN a_k SET STATS (
            'row_count'='1000', 'ndv'='1000', 'num_nulls'='0',
            'min_value'='1', 'max_value'='1000');
        ALTER TABLE eager_reorder_b MODIFY COLUMN c_k SET STATS (
            'row_count'='1000', 'ndv'='1000', 'num_nulls'='0',
            'min_value'='101', 'max_value'='1100');
        ALTER TABLE eager_reorder_c MODIFY COLUMN k SET STATS (
            'row_count'='100', 'ndv'='100', 'num_nulls'='0',
            'min_value'='101', 'max_value'='200');
    """

    // The original tree is (A join B) join C, so SUM(b.v * c.v) cannot be
    // pushed below the top join. Reordering to A join (B join C) makes the
    // expression local to the B-C subtree and enables eager aggregation.
    qt_reorder_enabled_plan """
        EXPLAIN SHAPE PLAN
        SELECT /*+ SET_VAR(eager_aggregation_mode=1, enable_join_reorder_before_eager_agg=true) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c ON b.c_k = c.k
        GROUP BY a.g;
    """

    qt_reorder_disabled_plan """
        EXPLAIN SHAPE PLAN
        SELECT /*+ SET_VAR(eager_aggregation_mode=1, enable_join_reorder_before_eager_agg=false) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c ON b.c_k = c.k
        GROUP BY a.g;
    """

    order_qt_eager_off_result """
        SELECT /*+ SET_VAR(eager_aggregation_mode=-1) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c ON b.c_k = c.k
        GROUP BY a.g
        ORDER BY a.g;
    """

    order_qt_eager_on_reordered_result """
        SELECT /*+ SET_VAR(eager_aggregation_mode=1, enable_join_reorder_before_eager_agg=true) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c ON b.c_k = c.k
        GROUP BY a.g
        ORDER BY a.g;
    """

    // This predicate references all three atoms. The data contains both matching
    // and non-matching rows, so a lost predicate changes the query result.
    order_qt_hyperedge_eager_off """
        SELECT /*+ SET_VAR(eager_aggregation_mode=-1) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c
            ON b.c_k = c.k AND a.x + b.v = c.x
        GROUP BY a.g
        ORDER BY a.g;
    """

    order_qt_hyperedge_eager_on """
        SELECT /*+ SET_VAR(eager_aggregation_mode=1, enable_join_reorder_before_eager_agg=true) */
            a.g, SUM(b.v * c.v)
        FROM eager_reorder_a a
        JOIN eager_reorder_b b ON a.k = b.a_k
        JOIN eager_reorder_c c
            ON b.c_k = c.k AND a.x + b.v = c.x
        GROUP BY a.g
        ORDER BY a.g;
    """
}
