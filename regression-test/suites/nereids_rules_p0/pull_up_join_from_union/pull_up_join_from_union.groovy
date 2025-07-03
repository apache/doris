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
    sql "set runtime_filter_mode=OFF"
    sql """ SET inline_cte_referenced_threshold=0 """
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"

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
        value2 VARCHAR(50)
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
    sql """INSERT INTO table_a (id, name, value) VALUES
    (1, 'Alice', 'Value_A1'),
    (2, 'Bob', 'Value_A2'),
    (3, 'Charlie', 'Value_A3'),
    (5, 'Eva', 'Value_A5');"""

    sql """INSERT INTO table_b (id, name, value) VALUES
    (1, 'Alice', 'Value_B1'),
    (2, 'Bob', 'Value_B2'),
    (4, 'Daniel', 'Value_B4'),
    (6, 'Fiona', 'Value_B6');"""

    sql """INSERT INTO table_c (id, name, value, value1, value2) VALUES
    (1, 'Alice', 'Value_C1', 'Extra_C1_1', 'Extra_C1_2'),
    (3, 'Charlie', 'Value_C3', 'Extra_C3_1', 'Extra_C3_2'),
    (4, 'Daniel', 'Value_C4', 'Extra_C4_1', 'Extra_C4_2'),
    (7, 'Grace', 'Value_C7', 'Extra_C7_1', 'Extra_C7_2');"""

    sql """INSERT INTO table_d (id, name, value) VALUES
    (1, 'Alice', 'Value_D1'),
    (2, 'Bob', 'Value_D2'),
    (3, 'Charlie', 'Value_D3'),
    (8, 'Henry', 'Value_D8');"""


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


    // Simple case with two tables joined in a union
    order_qt_basic_join_union_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Three-way union with common join
    order_qt_three_way_union_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id
    UNION ALL
    SELECT a.id, a.name, d.value FROM table_a a JOIN table_d d ON a.id = d.id) t
    """

    // Union with projections above joins
    order_qt_union_with_projections_res """
    SELECT * FROM
    (SELECT a.id, a.name, UPPER(b.value) AS upper_value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, LOWER(c.value) AS lower_value FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with constant expressions
    order_qt_union_with_constants_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with loss slots
    order_qt_union_with_loss_slots_res """
    SELECT t.id FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id) t
    """

    // Union with different join conditions
    order_qt_different_join_conditions_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.name = c.name) t
    """

    // Union with multi-column join conditions
    order_qt_multi_column_join_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON a.id = b.id AND a.name = b.name
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON a.id = c.id AND a.name = c.name) t
    """

    // Union with other joins
    order_qt_left_joins_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a LEFT JOIN table_b b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a LEFT JOIN table_c c ON a.id = c.id) t
    """

    // Union with subqueries in join
    order_qt_subquery_join_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN (SELECT id, MAX(value) AS value FROM table_b GROUP BY id) b ON a.id = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN (SELECT id, MAX(value) AS value FROM table_c GROUP BY id) c ON a.id = c.id) t
    """

    // Union with complex expressions in join condition
    order_qt_complex_join_condition1_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON CAST(a.id AS INT) + 1 = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON CAST(a.id AS INT) + 1 = c.id) t
    """

    // Union with complex expressions in join condition
    order_qt_complex_join_condition2_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value FROM table_a a JOIN table_b b ON CAST(a.id AS INT) + 1 = b.id
    UNION ALL
    SELECT a.id, a.name, c.value FROM table_a a JOIN table_c c ON CAST(a.id AS DOUBLE) + 1 = c.id) t
    """

    order_qt_union_filter1_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id where a.id = 1
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id where a.id = 1) t
    """

    order_qt_union_filter2_res """
    SELECT * FROM
    (SELECT a.id, a.name, b.value, 'B' AS source FROM table_a a JOIN table_b b ON a.id = b.id where a.value = 1
    UNION ALL
    SELECT a.id, a.name, c.value, 'C' AS source FROM table_a a JOIN table_c c ON a.id = c.id where a.value = 1) t
    """

    sql """drop table if exists test_like1"""
    sql """CREATE TABLE `test_like1` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );"""
    sql """drop table if exists test_like2"""
    sql """CREATE TABLE `test_like2` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );"""
    sql """drop table if exists test_like3"""
    sql """CREATE TABLE `test_like3` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );"""
    sql "drop table if exists test_like4"
    sql """create table test_like4 (a bigint, b varchar(10), c int, d int) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );"""
    sql "insert into test_like1 values(100,'d2',3,5),(0,'d2',3,5),(null,null,9,3),(33,'d2',2,5),(null,'d2',3,55),(78,null,9,3),(12,null,9,3);"
    sql "insert into test_like2 values(10,'d2',2,2),(0,'d2',2,2),(100,'d2',3,null),(null,null,9,3),(78,'d2',23,5),(33,'d2',23,5);"
    sql "insert into test_like3 values(1,'d2',2,2),(33,'d2',99,5),(33,'d2',23,6),(33,'d2',3,5);"
    sql "insert into test_like4 values(11,'d2',3,5),(1,'d2',3,5),(79,null,9,3),(33,'d2',2,5),(null,'d2',3,55),(78,null,9,3),(12,null,9,3);"

    qt_expr """select c1,c2 from 
    (select t2.a+1 c1,t2.c+2 c2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a union ALL
    select t3.a+1,t1.a+2 from test_like1 t1 join test_like3 t3 on t1.a=t3.a) t order by 1,2"""
    qt_const """select c1,c2 from
    (select t2.a+1 c1,2 c2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a union ALL
    select t3.a+1,3 from test_like1 t1 join test_like3 t3 on t1.a=t3.a) t order by 1,2"""

    qt_multi_condition """select c1,c2 from (
    select t2.a+1 c1,2 c2 from test_like1 t1 join test_like2 t2 on t1.c=t2.d AND t1.c=t2.d union ALL
    select t3.a+1,3 from test_like1 t1 join test_like3 t3 on t1.c=t3.d AND t1.c=t3.d) t order by 1,2"""

    qt_multi_condition2 """select c1,c2 from (
    select t2.a+1 c1 ,2 c2 from test_like1 t1 join test_like2 t2 on t1.c=t2.d AND t1.c=t2.c union ALL
    select t3.a+1,3 from test_like1 t1 join test_like3 t3 on t1.c=t3.d AND t1.c=t3.c) t order by 1,2"""

    qt_multi_differenct_count_condition """select c1,c2 from (
    select t2.a+1 c1,2 c2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a AND t1.a=t2.c union ALL
    select t3.a+1 c3,3 c4 from test_like1 t1 join test_like3 t3 on t1.a=t3.c) t order by 1,2"""
    qt_no_common_side_project """select c1,c2 from (
    select t2.a+1 c1,t2.c+2 c2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a union ALL
    select t3.a+1,t3.a+2 from test_like1 t1 join test_like3 t3 on t1.a=t3.a) t order by 1,2"""

    qt_common_slot_differnt """select c1,c2 from (
    select t2.a+1 c1,t2.c+2 c2 from test_like1 t1 join test_like2 t2 on t1.a+1=t2.a union ALL
    select t3.a+1,t3.c+2 from test_like1 t1 join test_like3 t3 on t1.a=t3.a) t order by 1,2"""

    qt_other_expr_differnt """select c1,c2 from (
    select t2.a+1 c1,t2.c+2 c2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a union ALL
    select t3.a+1,t3.c+100 from test_like1 t1 join test_like3 t3 on t1.a=t3.a) t order by 1,2"""

    qt_2_same_tables """select c1,c2 from (
    select t1.a c1,1 c2 from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a
    union ALL
    select t1.a,t2.a from test_like1 t1  inner join test_like2 t2 on t1.a=t2.a) t order by 1,2"""

    qt_simple_column """select c1,c2 from (
    select t1.a c1,t2.a c2 from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a
    union ALL
    select t1.a,t2.a from test_like1 t1  inner join test_like2 t2 on t1.a=t2.a) t order by 1,2"""

    qt_func_column """select c1,c2 from (
    select t1.a+1 c1,length(t2.b) c2 from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a
    union ALL
    select t1.a+1,length(t2.b)+1 from test_like1 t1  inner join test_like3 t2 on t1.a=t2.a) t order by 1,2"""

    qt_other_join_slot_differnt """select c1 from (
    select t1.a+1 c1 from test_like1 t1 inner join test_like2 t2 on t1.a=t2.c
    union ALL
    select t1.a+1 from test_like1 t1  inner join test_like3 t2 on t1.a=t2.a) t order by 1"""

    qt_join_common_slot_has_expr """select c1 from (
    select t1.a+1 c1 from test_like1 t1 inner join test_like2 t2 on t1.a+1=t2.a
    union ALL
    select t1.a+1 from test_like1 t1  inner join test_like2 t2 on t1.a+1=t2.a) t order by 1"""

    qt_can_not_transform """select c1,c2 from (
    select t2.c c1,t1.a c2 from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a
    union ALL
    select t1.a,t2.c from test_like1 t1  inner join test_like2 t2 on t1.a=t2.a) t order by 1,2"""

    qt_other_side_condition_slot_has_expr_do_transform """
    select c1 from (
    select t1.a+1 c1 from test_like4 t1 inner join test_like2 t2 on t1.a=t2.a+2
    union ALL
    select t1.a+1 from test_like4 t1  inner join test_like3 t2 on t1.a=t2.a+1) t order by 1"""
}