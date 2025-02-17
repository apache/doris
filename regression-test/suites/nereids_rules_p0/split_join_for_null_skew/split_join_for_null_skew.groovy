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

suite("split_join_for_null_skew") {
    sql "drop table if exists split_join_for_null_skew_t"
    sql """create table split_join_for_null_skew_t(a int null, b int not null, c varchar(10) null, d date, dt datetime)
    distributed by hash(a) properties("replication_num"="1");
    """
    sql """
    INSERT INTO split_join_for_null_skew_t (a, b, c, d, dt) VALUES
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (2, 101, 'banana', '2023-01-02', '2023-01-02 11:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'), 
    (NULL, 103, 'date', '2023-01-04', '2023-01-04 13:00:00'),
    (4, 104, 'elderberry', '2023-01-05', '2023-01-05 14:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (6, 106, 'fig', '2023-01-07', '2023-01-07 16:00:00'),
    (NULL, 107, 'grape', '2023-01-08', '2023-01-08 17:00:00');
    """

    // left join on slot
    qt_int "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a order by 1,2,3,4"
    qt_on_condition_has_plus_expr "select/*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.a,t2.b  from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a+1=t2.a order by 1,2,3,4"
    qt_on_condition_has_abs_expr "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.a,t2.b  from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on abs(t1.a)=t2.a order by 1,2,3,4"
    qt_varchar "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.c  from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.c=t2.c order by 1,2,3,4"
    qt_datetime "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t2.b,t1.b,t2.a,t2.c  from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.dt=t2.dt order by 1,2,3,4"

    // left join child has filter
    qt_int_has_filter "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.b from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a where t1.a =1 order by 1,2,3,4"
    qt_int_has_is_not_null "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.dt,t1.b,t2.a,t2.b from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a where t1.a is not null order by 1,2,3,4"
    qt_int_has_is_null "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.dt,t1.b,t2.a,t2.b from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a where t1.a is null order by 1,2,3,4"

    // split expr has multi slots
    qt_multi_slots_split_expr """select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1
    left join split_join_for_null_skew_t t2 on t1.a+t1.b=t2.a order by 1,2,3,4;"""
    qt_have_nonequal_join_conjuncts """select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1
    left join split_join_for_null_skew_t t2 on t1.a=t2.a and t1.b<t2.b order by 1,2,3,4;"""
    qt_test_multi_left_join """
    select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1 
    left join split_join_for_null_skew_t t2 on t1.a=t2.a and t1.b<t2.b
    left join split_join_for_null_skew_t t3 on t1.a=t3.a
    order by 1,2,3,4; """
    qt_test_upper_ref_agg """
    select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/  a,b from (
    select t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1 
    left join split_join_for_null_skew_t t2 on t1.a=t2.a  where t1.a is not null) t group by a,b order by 1,2;
    """

    // right join
    qt_right_join_varchar "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.c  from split_join_for_null_skew_t t1 right join split_join_for_null_skew_t t2 on t1.c=t2.c order by 1,2,3,4"
    qt_right_join_datetime "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t2.b,t1.b,t2.a,t2.c  from split_join_for_null_skew_t t1 right join split_join_for_null_skew_t t2 on t1.dt=t2.dt order by 1,2,3,4"

    qt_right_join_varchar_has_filter "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.c  from (select * from split_join_for_null_skew_t where a in (1,2)) t1 right join (select * from split_join_for_null_skew_t where a in (1,3))t2 on t1.c=t2.c order by 1,2,3,4"
    qt_right_join_has_filter_int "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.c  from (select * from split_join_for_null_skew_t where a in (1,2)) t1 right join (select * from split_join_for_null_skew_t where a in (1,3))t2 on t1.a=t2.a order by 1,2,3,4"
    qt_right_join_split_expt_not_null_not_transform "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.dt,t2.c  from (select * from split_join_for_null_skew_t where a in (1,2)) t1 right join (select * from split_join_for_null_skew_t where a in (1,3))t2 on t1.b=t2.b order by 1,2,3,4"
    qt_on_condition_has_plus_expr_right_join "select/*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.a,t2.b  from split_join_for_null_skew_t t1 right join split_join_for_null_skew_t t2 on t1.a+1=t2.a+9 order by 1,2,3,4"
    qt_on_condition_has_abs_expr_right_join "select /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ t1.a,t1.b,t2.a,t2.b  from split_join_for_null_skew_t t1 right join split_join_for_null_skew_t t2 on abs(t1.a)=abs(t2.a) order by 1,2,3,4"


    sql "drop table if exists null_skew_table1"
    sql """
    CREATE TABLE `null_skew_table1` (
    `guid` int NULL,
    `dt` varchar(65533) NULL,
    `tracking_type` varchar(65533) NULL,
    `clue_id` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`guid`)
    DISTRIBUTED BY HASH(`dt`) BUCKETS 3
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "drop table if exists null_skew_table2"
    sql """CREATE TABLE `null_skew_table2` (
    `clue_id` int NOT NULL,
    `new_category_name` text NOT NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`clue_id`)
    DISTRIBUTED BY HASH(`clue_id`) BUCKETS 10
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );"""

    qt_test_join_key_has_cast """SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/
    COUNT(1) AS c1,
            t2.new_category_name AS c2,
            t1.dt AS c3
    FROM
    null_skew_table1 as t1
    LEFT JOIN [broadcast] null_skew_table2 AS t2 ON t1.clue_id = t2.clue_id
    WHERE
    t1.dt BETWEEN '2024-08-19'
    AND '2024-08-19 14:44:28'
    AND t1.tracking_type = 'beseen'
    GROUP BY
    2,
    3
    ORDER BY
    3 asc
    LIMIT
    10000;"""

    sql "drop table if exists t3"
    sql """
    CREATE TABLE t3 (
        x INT NULL,
        y INT NOT NULL,
        z VARCHAR(10),
        dt DATETIME
    ) DISTRIBUTED BY HASH(x) PROPERTIES("replication_num"="1");
    """

    sql """
    INSERT INTO t3 (x, y, z, dt) VALUES
    (1, 200, 'apple', '2023-01-01 10:00:00'),
    (2, 201, 'banana', '2023-01-02 11:00:00'),
    (NULL, 202, 'cherry', '2023-01-03 12:00:00'),
    (4, 203, NULL, '2023-01-04 13:00:00'),
    (5, 204, 'elderberry', '2023-01-05 14:00:00');
    """
    sql "drop table if exists t4"
    sql """
    CREATE TABLE t4 (
        a INT NULL,
        b INT NOT NULL,
        c VARCHAR(10),
        d DATE
    ) DISTRIBUTED BY HASH(a) PROPERTIES("replication_num"="1");
    """

    sql """
    INSERT INTO t4 (a, b, c, d) VALUES
    (1, 300, 'apple', '2023-01-01'),
    (2, 301, 'banana', '2023-01-02'),
    (NULL, 302, 'cherry', '2023-01-03'),
    (4, 303, NULL, '2023-01-04'),
    (5, 304, 'elderberry', '2023-01-05');
    """

    qt_multi_left_join_and_non_equal """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t1.b, t2.c, t3.z, t4.d
    FROM split_join_for_null_skew_t t1
    LEFT JOIN split_join_for_null_skew_t t2 ON t1.a = t2.a AND t1.b < t2.b
    LEFT JOIN t3 ON t1.a = t3.x AND t3.y > 200
    LEFT JOIN t4 ON t1.d = t4.d
    ORDER BY 1,2,3,4,5;
    """

    qt_multi_right_join_and_non_equal """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, t3.x, t4.c
    FROM split_join_for_null_skew_t t1
    RIGHT JOIN t3 ON t1.a = t3.x
    RIGHT JOIN t4 ON t3.z = t4.c
    RIGHT JOIN split_join_for_null_skew_t t2 ON t4.a = t2.a
    ORDER BY 1,2,3,4;
    """
    qt_mixed_join_types """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, t3.z, t4.d
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a = t3.x
    RIGHT JOIN t4 ON t1.d = t4.d
    LEFT JOIN split_join_for_null_skew_t t2 ON t4.a = t2.a
    ORDER BY 1,2,3,4;
    """

    qt_non_equi_join_chain """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, t3.y, t4.c
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a < t3.x
    LEFT JOIN t4 ON t3.y > t4.b
    LEFT JOIN split_join_for_null_skew_t t2 ON t4.a = t2.a
    ORDER BY 1,2,3,4;
    """

    qt_mixed_inner_left """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, t3.z, t4.d
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a = t3.x
    INNER JOIN t4 ON t1.d = t4.d
    LEFT JOIN split_join_for_null_skew_t t2 ON t4.a = t2.a
    ORDER BY 1,2,3,4;
    """

    qt_mixed_inner_right """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, t3.z, t4.d
    FROM split_join_for_null_skew_t t1
    RIGHT JOIN t3 ON t1.a = t3.x
    INNER JOIN t4 ON t3.z = t4.c
    RIGHT JOIN split_join_for_null_skew_t t2 ON t4.a = t2.a
    ORDER BY 1,2,3,4;
    """

    qt_join_with_agg """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, AVG(t3.y)
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a = t3.x
    LEFT JOIN t4 ON t3.x = t4.a
    GROUP BY t1.a
    ORDER BY 1,2;
    """

    qt_join_with_window """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t2.b, 
        ROW_NUMBER() OVER (PARTITION BY t1.a ORDER BY t2.dt)
    FROM split_join_for_null_skew_t t1
    LEFT JOIN split_join_for_null_skew_t t2 ON t1.a = t2.a
    ORDER BY 1,2,3;
    """

    qt_join_with_sort_filter """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t4.b, t3.z
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a = t3.x
    RIGHT JOIN t4 ON t3.z = t4.c
    WHERE t4.b > 300
    ORDER BY t1.a DESC, t4.b;
    """

    qt_multi_column_expr_join """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t1.b, t3.x, t3.y
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a + t3.x = 10 AND t1.b < t3.y
    ORDER BY 1,2,3,4;
    """

    qt_mixed_left_right_condition """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t4.b, t3.z
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON t1.a = t3.x AND t3.y > t1.b
    RIGHT JOIN t4 ON t1.d = t4.d AND t4.a = t3.x
    ORDER BY 1,2,3;
    """

    qt_complex_expr_join """
    SELECT /*+use_cbo_rule(JOIN_SPLIT_FOR_NULL_SKEW)*/ 
        t1.a, t1.b, t3.z
    FROM split_join_for_null_skew_t t1
    LEFT JOIN t3 ON COALESCE(t1.a, 0) = COALESCE(t3.x, 0)
    ORDER BY 1,2,3;
    """
}