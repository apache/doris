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

    sql "drop table if exists test_like1"
    sql "drop table if exists test_like2"
    sql "drop table if exists test_like3"

    sql """
    CREATE TABLE `test_like1` (
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
    CREATE TABLE `test_like2` (
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
    CREATE TABLE `test_like3` (
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
    insert into test_like1 values(1,'d2',3,5),(0,'d2',3,5),(-3,'d2',2,2),(-2,'d2',2,2);
    """

    sql """
    insert into test_like2 values(1,'d2',2,2),(-3,'d2',2,2),(0,'d2',3,5);
    """

    sql """
    insert into test_like3 values(1,'d2',2,2),(-2,'d2',2,2),(0,'d2',3,5);
    """

    qt_intersect """    
    explain shape plan
    select * from      (select a,b from test_like1 where a<1 intersect select a,b from test_like2 where b>'ab') t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except """    
    explain shape plan
    select * from      (select a,b from test_like1 where a<1 except select a,b from test_like2 where b>'ab') t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b;
    """
    qt_union """    
    explain shape plan
    select * from      (select a,b from test_like1 where a<1 union select a,b from test_like2 where a<1) t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_res """    
    select t.a,t3.b from      (select a,b from test_like1 where a<1 intersect select a,b from test_like2 where b>'ab') t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b order by 1,2;
    """
    qt_except_res """    
    select t.a,t3.b from      (select a,b from test_like1 where a<1 except select a,b from test_like2 where b>'ab') t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b  order by 1,2;
    """
    qt_union_res """    
    select t.a,t3.b from      (select a,b from test_like1 where a<1 union select a,b from test_like2 where a<1) t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b  order by 1,2; """

    qt_intersect_one_side """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 intersect select a,b from test_like2 where b>'ab') t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_intersect_one_side_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 intersect select a,b from test_like2 ) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_except_first_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 except select a,b from test_like2 where b>'ab') t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_second_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 where a>1 except select a,b from test_like2 ) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """
    qt_except_no_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 except select a,b from test_like2 ) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_different_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 where a<2 union all select a,b from test_like2 where a<1) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_one_side_filter """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1  union select a,b from test_like2 where a<1) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_with_const """
    explain shape plan 
    select t.a,t3.b from      (select a,b from test_like1 where a<1 union all select a,b from test_like2 where a<1 union all select 2,2) t inner join test_like3 t3 
    on t3.a=t.a and t3.b=t.b;
    """

    qt_union_all_const """explain shape plan
    select t.a,t3.b from      (select 133333 as a,'aa' as b union all select 2,'dd' ) t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b;"""
    qt_union_all_const2_has_cast_not_support """explain shape plan
    select t.a,t3.b from      (select 3 as a,'aa' as b union all select 2,'dd' ) t inner join test_like3 t3
    on t3.a=t.a and t3.b=t.b;"""
}