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

suite("join_pull_up_union") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ SET inline_cte_referenced_threshold=0 """
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql """
        -- Create tables
    DROP TABLE IF EXISTS table_a;
    CREATE TABLE table_a (
        id INT,
        name VARCHAR(50),
        value VARCHAR(50)
    )ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    DROP TABLE IF EXISTS table_b;
    CREATE TABLE table_b (
        id INT,
        name VARCHAR(50),
        value VARCHAR(50)
    )ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    DROP TABLE IF EXISTS table_c;
    CREATE TABLE table_c (
        id INT,
        name VARCHAR(50),
        value VARCHAR(50),
        value1 VARCHAR(50),
        value2 VARCHAR(50),
    )ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    DROP TABLE IF EXISTS table_d;
    CREATE TABLE table_d (
        id INT,
        name VARCHAR(50),
        value VARCHAR(50)
    )ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    // Simple case with two tables joined in a union
    qt_basic_join_union """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Three-way union with common join
    qt_three_way_union """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id
    UNION ALL
    SELECT a.id, a.name, d.value FROM table_a a JOIN table_d d ON a.id = d.id) t
    """

    // Union with projections above joins
    qt_union_with_projections """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, UPPER(b.value) AS upper_value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, LOWER(c.value) AS lower_value FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with constant expressions
    qt_union_with_constants """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with loss slots
    qt_union_with_loss_slots """
    explain shape plan
    SELECT t.id FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with different join conditions
    qt_different_join_conditions """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.name = c.name) t
    """

    // Union with multi-column join conditions
    qt_multi_column_join """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id AND a.name = b.name
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id AND a.name = c.name) t
    """

    // Union with other joins
    qt_left_joins """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a LEFT JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a LEFT JOIN table_c c ON a.id = c.id) t
    """

    // Union with subqueries in join
    qt_subquery_join """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN (SELECT id, MAX(value) AS value FROM table_b GROUP BY id) b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN (SELECT id, MAX(value) AS value FROM table_c GROUP BY id) c ON a.id = c.id) t
    """

    // Union with complex expressions in join condition
    qt_complex_join_condition1 """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON CAST(a.id AS INT) + 1 = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON CAST(a.id AS INT) + 1 = c.id) t
    """

    // Union with complex expressions in join condition
    qt_complex_join_condition2 """
    explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON CAST(a.id AS INT) + 1 = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON CAST(a.id AS DOUBLE) + 1 = c.id) t
    """

    // how to construct union expr
    // qt_constant_union """
        
    // """

    qt_union_filter1 """
        explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id where a.id = 1
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id where a.id = 1) t
    """

        qt_union_filter2 """
        explain shape plan
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id where a.value = 1
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id where a.value = 1) t
    """
}