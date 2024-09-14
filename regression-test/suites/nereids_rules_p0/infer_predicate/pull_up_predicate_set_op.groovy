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

suite("test_pull_up_predicate_set_op") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql 'set runtime_filter_mode=off'
    sql 'set enable_fold_constant_by_be=true'
    sql 'set debug_skip_fold_constant=false'
    sql 'set disable_join_reorder=true'

    sql "drop table if exists test_pull_up_predicate_set_op1"
    sql "drop table if exists test_pull_up_predicate_set_op2"
    sql "drop table if exists test_pull_up_predicate_set_op3"

    sql """
    CREATE TABLE `test_pull_up_predicate_set_op1` (
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
    CREATE TABLE `test_pull_up_predicate_set_op2` (
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
    CREATE TABLE `test_pull_up_predicate_set_op3` (
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
    insert into test_pull_up_predicate_set_op1 values(1,'d2',3,5),(0,'d2',3,5),(-3,'d2',2,2),(-2,'d2',2,2);
    """

    sql """
    insert into test_pull_up_predicate_set_op2 values(1,'d2',2,2),(-3,'d2',2,2),(0,'d2',3,5);
    """

    sql """
    insert into test_pull_up_predicate_set_op3 values(1,'d2',2,2),(-2,'d2',2,2),(0,'d2',3,5);
    """

    sql "drop table if exists test_pull_up_predicate_set_op4"
    sql "create table test_pull_up_predicate_set_op4(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2) properties('replication_num'='1');"
    sql """insert into test_pull_up_predicate_set_op4 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11')
            ,(14,'01234567890123456789', 33,23,'2024-01-04','2020-01-11 10:00:00.99','2020-01-11'),
            (14,'01234567890123456789', 33,23,'2024-01-03 10:00:00','2020-01-11 10:00:00.99','2020-01-11');"""

    qt_intersect """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_one_side_constant_one_side_column """
    explain shape plan 
    select t.a,t3.b from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_one_side """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_one_side_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_except_first_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_second_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 except select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_different_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<2 union all select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_one_side_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1  union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_with_const """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union all select a,b from test_pull_up_predicate_set_op2 where a<1 union all select 2,2) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_all_const """explain shape plan
    select t.a,t3.b from      (select 133333 as a,'aa' as b union all select 2,'dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""
    qt_union_all_const_tinyint_int """explain shape plan
    select t.a,t3.b from      (select 3 as a,'aa' as b union all select 2,'dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""

    qt_union_all_const_empty_relation """ explain shape plan select t.a,t3.b from (select 3 as a,'aa' as b from test_pull_up_predicate_set_op3 limit 0 offset 0 union all select 2,'dd' ) t 
    inner join test_pull_up_predicate_set_op3 t3 on t3.a=t.a and t3.b=t.b;"""

    qt_union_all_const2_has_cast_to_null_different_type """ explain shape plan
    select t.a,t3.b from (select 3 as a,'aa' as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b; """

    qt_union_all_const2_has_cast_to_null_different_type """ explain shape plan
    select t.a,t3.b from (select 'abcd' as a,12 as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b; """
    qt_union_all_different_type_int_cast_to_char """ explain shape plan
    select t.a,t3.b from (select 'abcd' as a,12 as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""

    qt_union_all_const_char3_char2 """explain shape plan
    select t.a,t3.b from (select 3 as a,'aa' as b union all select 2,'dd' union all select 5,'aab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_child_and_const_exprs """
    explain shape plan
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) union select a,b from test_pull_up_predicate_set_op2 where a in (1,2) union select 2,2 union select 2,2) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_child_and_const_exprs_andpredicates """
    explain shape plan
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) and b in ('2d','3') union select 2,'2d' union select 2,'3') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_child_and_const_exprs_orpredicates """
    explain shape plan
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) or b in ('2d','3') union select 2,'2d' union select 2,'3') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""

    // test_different join type
    qt_intersect_one_side_constant_one_side_column_left_join """
    explain shape plan 
    select t.a,t3.b from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_second_no_filter_left_join """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t left join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_left_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t left join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_right_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t right join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_one_side_constant_one_side_column_left_semi_join """
    explain shape plan 
    select t.a from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t left semi join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_second_no_filter_right_semi_join """
    explain shape plan 
    select t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t right semi join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_left_anti_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t left anti join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_right_anti_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t right anti join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_all_const_full_join """explain shape plan
    select t.a,t3.b from      (select 133333 as a,'aa' as b union all select 2,'dd' ) t full outer join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""
    qt_union_all_const_tinyint_int_cross_join """explain shape plan
    select t.a,t3.b from      (select 3 as a,'aa' as b union all select 2,'dd' ) t cross join test_pull_up_predicate_set_op3 t3
    """

    // join right is join or union
    qt_intersect_union """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join 
    (select a,b from test_pull_up_predicate_set_op2  union all select a,b from test_pull_up_predicate_set_op2 ) t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_agg """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join 
    (select a,b from test_pull_up_predicate_set_op1 group by a,b) t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_cross_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join 
    (select t1.a,t2.b from test_pull_up_predicate_set_op3 t1 cross join test_pull_up_predicate_set_op1 t2 ) t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union_inner_join """    
    explain shape plan
    select * from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join 
    (select t1.a,t2.b from test_pull_up_predicate_set_op3 t1 inner join test_pull_up_predicate_set_op1 t2 on t1.a=t2.a ) t3
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_all_const_datetime """
    explain shape plan
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_datetimev2=t.b;
    """
    qt_union_all_const_date """
    explain shape plan
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_datev2=t.b;
    """
    qt_union_all_const_char100 """
    explain shape plan
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_char100=t.b;
    """
    qt_union_all_const_char10 """    explain shape plan
    select t.a,t3.d_char10 from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-04') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_char10=t.b;"""

    qt_union_all_and_project_pull_up """explain shape plan
    select t.a,t3.b from      (select 3 as a,b from test_pull_up_predicate_set_op3 union all select 3,b from test_pull_up_predicate_set_op3 ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    // need pull up from agg support
    qt_union_and_const """explain shape plan
    select c2 from (select 2 id,'abc' c2  union  select 1 ,'abbbb' c4  ) t inner join test_pull_up_predicate_set_op3 t2 on t.id=t2.a"""

    // test sql res
    qt_intersect_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b  order by 1,2;
    """
    qt_union_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b  order by 1,2; """

    qt_intersect_one_side_constant_one_side_column_res """
    select t.a,t3.b from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_intersect_one_side_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_intersect_one_side_no_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_except_first_no_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_second_no_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_no_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 except select a,b from test_pull_up_predicate_set_op2 ) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_union_different_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<2 union all select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_union_one_side_filter_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1  union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_union_with_const_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union all select a,b from test_pull_up_predicate_set_op2 where a<1 union all select 2,2) t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_union_all_const_res """select t.a,t3.b from      (select 133333 as a,'aa' as b union all select 2,'dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b;"""
    qt_union_all_const_tinyint_int_res """select t.a,t3.b from      (select 3 as a,'aa' as b union all select 2,'dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;"""

    qt_union_all_const_empty_relation_res """ select t.a,t3.b from (select 3 as a,'aa' as b from test_pull_up_predicate_set_op3 limit 0 offset 0 union all select 2,'dd' ) t 
    inner join test_pull_up_predicate_set_op3 t3 on t3.a=t.a and t3.b=t.b order by 1,2;"""

    qt_union_all_const2_has_cast_to_null_different_type_res """ select t.a,t3.b from (select 3 as a,'aa' as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2; """

    qt_union_all_const2_has_cast_to_null_different_type_res """ select t.a,t3.b from (select 'abcd' as a,12 as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2; """
    qt_union_all_different_type_int_cast_to_char_res """ select t.a,t3.b from (select 'abcd' as a,12 as b union all select 'abc','dd' ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;"""

    qt_union_all_const_char3_char2_res """select t.a,t3.b from (select 3 as a,'aa' as b union all select 2,'dd' union all select 5,'aab') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_intersect_one_side_constant_one_side_column_left_join_res """
    select t.a,t3.b from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_second_no_filter_left_join_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t left join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_union_left_join_res """    
    select t.a,t3.a from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t left join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_union_right_join_res """    
    select t.a,t3.a from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t right join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_intersect_one_side_constant_one_side_column_left_semi_join_res """
    select t.a from      (select 1 as a,b from test_pull_up_predicate_set_op1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t left semi join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1;
    """
    qt_except_second_no_filter_right_semi_join_res """
    select t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a>1 except select a,b from test_pull_up_predicate_set_op2 ) t right semi join test_pull_up_predicate_set_op3 t3 
    on t3.a=t.a and t3.b=t.b order by 1;
    """
    qt_union_left_anti_join_res """    
    select t.a,t.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t left anti join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_union_right_anti_join_res """    
    select t3.a from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t right anti join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1;
    """
    qt_union_all_const_full_join_res """select t.a,t3.b from      (select 133333 as a,'aa' as b union all select 2,'dd' ) t full outer join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;"""
    qt_union_all_const_tinyint_int_cross_join_res """select t.a,t3.b from      (select 3 as a,'aa' as b union all select 2,'dd' ) t cross join test_pull_up_predicate_set_op3 t3 order by 1,2
    """

    qt_intersect_union_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 intersect select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join 
    (select a,b from test_pull_up_predicate_set_op2  union all select a,b from test_pull_up_predicate_set_op2 ) t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_agg_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 except select a,b from test_pull_up_predicate_set_op2 where b>'ab') t inner join 
    (select a,b from test_pull_up_predicate_set_op1 group by a,b) t3
    on t3.a=t.a and t3.b=t.b  order by 1,2;
    """
    qt_union_cross_join_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join 
    (select t1.a,t2.b from test_pull_up_predicate_set_op3 t1 cross join test_pull_up_predicate_set_op1 t2 ) t3
    on t3.a=t.a and t3.b=t.b  order by 1,2;
    """
    qt_union_inner_join_res """    
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a<1 union select a,b from test_pull_up_predicate_set_op2 where a<1) t inner join 
    (select t1.a,t2.b from test_pull_up_predicate_set_op3 t1 inner join test_pull_up_predicate_set_op1 t2 on t1.a=t2.a ) t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """

    qt_union_all_const_datetime_res """
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_datetimev2=t.b order by 1;
    """
    qt_union_all_const_date_res """    
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_datev2=t.b  order by 1;
    """
    qt_union_all_const_char100_res """    
    select t.a from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-03') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_smallint=t.a and t3.d_char100=t.b order by 1;
    """
    qt_union_all_const_char10_res """    select t.a,t3.d_char10 from (select 12222222 as a,'2024-01-03 10:00:00' as b union all select 2,'2024-01-04') t inner join test_pull_up_predicate_set_op4 t3
    on t3.d_char10=t.b order by 1,2;"""

    qt_union_all_and_project_pull_up_res """select t.a,t3.b from  (select 3 as a,b from test_pull_up_predicate_set_op3 union all select 3,b from test_pull_up_predicate_set_op3 ) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    // need pull up from agg support
    qt_union_and_const_res """select c2 from (select 2 id,'abc' c2  union  select 1 ,'abbbb' c4  ) t inner join test_pull_up_predicate_set_op3 t2 on t.id=t2.a  order by 1"""

    qt_union_child_and_const_exprs_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) union select a,b from test_pull_up_predicate_set_op2 where a in (1,2) union select 2,2 union select 2,2) t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_union_child_and_const_exprs_andpredicates_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) and b in ('2d','3') union select 2,'2d' union select 2,'3') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_union_child_and_const_exprs_orpredicates_res """
    select t.a,t3.b from      (select a,b from test_pull_up_predicate_set_op1 where a in (1,2) or b in ('2d','3') union select 2,'2d' union select 2,'3') t inner join test_pull_up_predicate_set_op3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;"""
}