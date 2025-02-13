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
    insert into test_pull_up_agg_t1 values(1,'d2',3,5),(0,'d2',3,5),(-3,'d2',2,2),(-2,'d2',2,2);
    """

    sql """
    insert into test_pull_up_agg_t2 values(1,'d2',2,2),(-3,'d2',2,2),(0,'d2',3,5);
    """

    qt_test_max_less "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a<10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_max_greater "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a>10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_max_less_equal "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a<=10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_max_greater_equal "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a>=10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_max_equal "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a=10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_max_null_safe_equal "explain shape plan select * from (select max(a) c1 from test_pull_up_agg_t1 where a<=>10 group by a having max(a)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"

    // test min(non-groupby-column)
    qt_test_min_less "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c<10 group by a having min(c)<1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_min_greater "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c>10 group by a having min(c)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_min_less_equal "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c<=10 group by a having min(c)>=1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_min_greater_equal "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c>=10 group by a having min(c)<=1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_min_equal "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c=10 group by a having min(c)>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_min_null_safe_equal "explain shape plan select * from (select min(c) c1 from test_pull_up_agg_t1 where c<=>10 group by a having min(c)=1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"

    qt_test_avg_less "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c<10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_avg_greater "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c>10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_avg_less_equal "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c<=10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_avg_greater_equal "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c>=10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_avg_equal "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c=10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
    qt_test_avg_null_safe_equal "explain shape plan select * from (select avg(c) c1 from test_pull_up_agg_t1 where c<=>10 group by a having a>1) t inner join test_pull_up_agg_t2 t2 on t.c1=t2.a;"
}