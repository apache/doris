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

suite("extract_from_disjunction_in_join") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set runtime_filter_mode=OFF"


    sql "drop table if exists extract_from_disjunction_in_join_t1"
    sql "drop table if exists extract_from_disjunction_in_join_t2"
    sql """
    CREATE TABLE `extract_from_disjunction_in_join_t1` (
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
    CREATE TABLE `extract_from_disjunction_in_join_t2` (
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

    sql "insert into extract_from_disjunction_in_join_t1 values(1,'d2',3,5),(2,'d2',3,5),(3,'d2',3,5);"
    sql "insert into extract_from_disjunction_in_join_t2 values(7,'d2',2,2),(8,'d2',2,2),(9,'d2',2,2);"
    qt_left_semi """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 left semi join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_right_semi """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 right semi join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_left """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 left join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_right """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 right join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_left_anti """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 left anti join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_right_anti """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 right anti join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_inner """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 inner join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8);"""
    qt_outer """explain shape plan
    select * from extract_from_disjunction_in_join_t1 t1 full join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8)
    where t1.c=3;"""

    qt_left_semi_res "select t1.a from extract_from_disjunction_in_join_t1 t1 left semi join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_right_semi_res "select t2.a from extract_from_disjunction_in_join_t1 t1 right semi join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_left_res "select t1.a from extract_from_disjunction_in_join_t1 t1 left join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_right_res "select t1.a from extract_from_disjunction_in_join_t1 t1 right join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_left_anti_res "select t1.a from extract_from_disjunction_in_join_t1 t1 left anti join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_right_anti_res "select t2.a from extract_from_disjunction_in_join_t1 t1 right anti join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_inner_res "select t1.a from extract_from_disjunction_in_join_t1 t1 inner join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8) order by 1;"
    qt_outer_res """select t1.a from extract_from_disjunction_in_join_t1 t1 full join extract_from_disjunction_in_join_t2 t2 on t1.b=t2.b and (t2.a=9 && t1.a=1 || t1.a=2 && t2.a=8)
    where t1.c=3 order by 1;"""
}