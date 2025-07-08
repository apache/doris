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

suite("test_pull_up_agg") {
//    sql "set disable_nereids_rules='INFER_PREDICATES'"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql 'set runtime_filter_mode=off'
    sql 'set disable_join_reorder=true'

    sql "drop table if exists test_pull_up_agg_t1"
    sql "drop table if exists test_pull_up_agg_t2"

    sql """
    CREATE TABLE `test_pull_up_agg_t1` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE `test_pull_up_agg_t2` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    INSERT INTO `test_pull_up_agg_t1` (`a`, `b`, `c`, `d`) VALUES
    (1, 'Apple', 100, 10),
    (1, 'Banana', 200, 20),
    (1, 'Apple', 150, 15),
    (1, 'Cherry', 250, 25),
    (1, 'Banana', 250, 12),
    (1, 'Durian', 250, 22),
    (1, 'Elder', 180, 18),
    (1, 'Fig', 280, 28),
    (1, 'Grape', 250, 13),
    (1, 'Honey', 250, 23),
    (2, 'Kiwi', 250, 19),
    (2, 'Lemon', 250, 29),
    (2, 'Mango', 140, 14),
    (2, 'Necta', 240, 24),
    (2, 'Orange', 150, 16),
    (3, 'Peach', 150, 26),
    (3, 'Quince', 150, 17),
    (3, 'Rasp', 260, 27),
    (3, 'Straw', 150, 11),
    (3, 'Tomato', 150, 21);"""

    sql """
    INSERT INTO `test_pull_up_agg_t2` (`a`, `b`, `c`, `d`) VALUES
    (1, 'Apple', 100, 10),
    (1, 'Berry', 150, 21),
    (1, 'Apple', 100, 16),
    (1, 'Cherry', 255, 26),
    (1, 'Banana', 125, 13),
    (1, 'Date', 100, 23),
    (1, 'Elder', 185, 19),
    (2, 'Fig', 285, 29),
    (2, 'Grape', 150, 14),
    (2, 'Honey', 235, 24),
    (2, 'Kiwi', 100, 20),
    (2, 'Lime', 260, 30),
    (2, 'Mango', 140, 15),
    (3, 'Necta', 250, 25),
    (3, 'Orange', 250, 17),
    (3, 'Pear', 260, 27),
    (3, 'Quince', 150, 18),
    (3, 'Rasp', 150, 28),
    (3, 'Straw', 140, 12),
    (3, 'Tamar', 180, 22);"""

    qt_min_less_less_equal_with_group_by """select t.col1,t.col2,t2.a from (select min(a) col1, min(c) col2 from test_pull_up_agg_t1 where a<=10 and c<200 group by a) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""
    qt_min_less_less_equal_without_group_by """select t.col1,t.col2,t2.a from (select min(a) col1, min(c) col2 from test_pull_up_agg_t1 where a<=20 and c<200 ) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""

    qt_max_less_less_equal_with_group_by """select t.col1,t.col2,t2.a from (select max(a) col1, max(c) col2 from test_pull_up_agg_t1 where a<=10 and c<200 group by a) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""
    qt_max_less_less_equal_without_group_by """select t.col1,t.col2,t2.a from (select max(a) col1, max(c) col2 from test_pull_up_agg_t1 where a<=20 and c<200 ) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""

    qt_mix_greater_greater_equal_with_group_by """select t.col1,t.col2,t2.a from (select min(a) col1, max(c) col2 from test_pull_up_agg_t1 where a>=0 and c>1 group by a) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""
    qt_mix_greater_greater_equal_without_group_by """select t.col1,t.col2,t2.a from (select min(a) col1, max(c) col2 from test_pull_up_agg_t1 where a>=0 and c>1) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""

    qt_mix_equal_with_group_by """select t.col1,t.col2,t2.a from (select min(a) col1, max(c) col2 from test_pull_up_agg_t1 where a<=>1 and c=200 group by a) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""
    qt_mix_equal_with_group_by """select t.col1,t.col2,t2.a from (select max(a) col1, min(c) col2 from test_pull_up_agg_t1 where a<=>1 and c=200) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""

    qt_max_less_less_equal_with_group_by_shape """explain shape plan select t.col1,t.col2,t2.a from (select max(a) col1, any_value(c) col2 from test_pull_up_agg_t1 where a<=10 and c<200 group by a) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""
    qt_max_less_less_equal_without_group_by_shape """explain shape plan  select t.col1,t.col2,t2.a from (select max(a) col1, max(c) col2 from test_pull_up_agg_t1 where a<=20 and c<200 ) t
    inner join test_pull_up_agg_t2 t2 on t.col1=t2.a and t.col2=t2.c order by 1,2,3;"""

    qt_pull_up_from_agg_to_filter_with_same_cond_shape """explain shape plan select max(a) as col1 from test_pull_up_agg_t1 where a >1 having col1 > 1 order by 1;"""
    qt_pull_up_from_agg_to_filter_with_same_cond """select max(a) as col1 from test_pull_up_agg_t1 where a >1 having col1 > 1 order by 1;"""

    qt_pull_up_from_agg_to_filter_with_same_cond_shape """explain shape plan select max(a) as col1 from test_pull_up_agg_t1 where a >1 having col1 is null or col1 > 1 order by 1;"""
    qt_pull_up_from_agg_to_filter_with_same_cond """select max(a) as col1 from test_pull_up_agg_t1 where a >1 having col1 is null or col1 > 1 order by 1;"""
}