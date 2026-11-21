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

suite("single_tablet_gather") {
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set parallel_pipeline_task_num=1"
    sql "set enable_single_tablet_gather=true"
    sql "set enable_parallel_result_sink=false"

    sql "drop table if exists single_tablet_gather_t"
    sql """create table single_tablet_gather_t (k int, v int null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")"""
    sql "insert into single_tablet_gather_t values (1, 10), (2, null), (3, 30), (4, 0)"

    def scalar = "select count(case when cast(v as boolean) then 1 end), count(v), sum(v) from single_tablet_gather_t"
    explain {
        sql scalar
        contains "AGGREGATE"
        notContains "EXCHANGE"
    }
    qt_scalar scalar
    qt_inline """select count(case when b then 1 end) from
        (select cast(v as boolean) b from single_tablet_gather_t) s"""
    qt_filter "select count(v), sum(v) from single_tablet_gather_t where k > 1"
    qt_filtered_empty "select count(v), sum(v) from single_tablet_gather_t where v < 0"
    qt_count_star "select count(*) from single_tablet_gather_t"
    qt_distinct "select count(distinct v), sum(distinct v) from single_tablet_gather_t"
    order_qt_group "select v, count(*) from single_tablet_gather_t group by v order by v"

    sql "set disable_nereids_rules=ELIMINATE_AGG_CASE_WHEN"
    explain {
        sql scalar
        notContains "EXCHANGE"
    }
    qt_case_rule_disabled scalar
    sql "set disable_nereids_rules=''"

    sql "set enable_single_tablet_gather=false"
    explain {
        sql scalar
        contains "EXCHANGE"
    }
    qt_disabled scalar
    sql "set enable_single_tablet_gather=true"

    sql "set parallel_pipeline_task_num=2"
    explain {
        sql scalar
        contains "EXCHANGE"
    }
    qt_dop_two scalar
    sql "set parallel_pipeline_task_num=1"

    sql "set enable_query_cache=true"
    explain {
        sql scalar
        contains "EXCHANGE"
    }
    qt_query_cache scalar
    sql "set enable_query_cache=false"

    // Model a multi-BE optimizer environment without requiring extra running BEs.
    sql "set be_number_for_test=3"
    explain {
        sql scalar
        notContains "EXCHANGE"
    }
    qt_multi_be_planning scalar
    sql "set be_number_for_test=-1"

    sql "drop table if exists single_tablet_gather_multi"
    sql """create table single_tablet_gather_multi (k int, v int null)
        duplicate key(k) distributed by hash(k) buckets 3
        properties("replication_num"="1")"""
    sql "insert into single_tablet_gather_multi select * from single_tablet_gather_t"
    explain {
        sql "select count(case when v > 0 then 1 end) from single_tablet_gather_multi"
        contains "EXCHANGE"
    }
    qt_multi_tablet "select count(v), sum(v) from single_tablet_gather_multi"
    explain {
        sql "select count(case when v > 0 then 1 end) from single_tablet_gather_multi where k=1"
        notContains "EXCHANGE"
    }
    qt_tablet_pruned "select count(v), sum(v) from single_tablet_gather_multi where k=1"

    sql "drop table if exists single_tablet_gather_part"
    sql """create table single_tablet_gather_part (k int, v int null)
        duplicate key(k) partition by range(k) (
            partition p1 values less than (3), partition p2 values less than (10))
        distributed by hash(k) buckets 1 properties("replication_num"="1")"""
    sql "insert into single_tablet_gather_part select * from single_tablet_gather_t"
    explain {
        sql "select count(case when v > 0 then 1 end) from single_tablet_gather_part"
        contains "EXCHANGE"
    }
    qt_multi_partition "select count(v), sum(v) from single_tablet_gather_part"
    explain {
        sql "select count(case when v > 0 then 1 end) from single_tablet_gather_part where k < 3"
        notContains "EXCHANGE"
    }
    qt_partition_pruned "select count(v), sum(v) from single_tablet_gather_part where k < 3"

    // Storage gathers must retain exchanges at fragment-merging boundaries.
    qt_join """select count(a.v), sum(b.v) from single_tablet_gather_t a
        join single_tablet_gather_multi b on a.k=b.k"""
    qt_scalar_join """select a.c+b.c from
        (select count(v) c from single_tablet_gather_t) a cross join
        (select count(v) c from single_tablet_gather_multi) b"""
    order_qt_union """select count(v) c from single_tablet_gather_t
        union all select count(v) c from single_tablet_gather_multi order by c"""
    qt_cte """with s as (select k, v from single_tablet_gather_t)
        select count(a.v), sum(b.v) from s a join s b on a.k=b.k"""

    qt_recursive_cte """with recursive s as (
        select k, v, cast(1 as int) depth from single_tablet_gather_t union all
        select k, v, cast(depth+1 as int) from s where depth < 3)
        select count(v), sum(v) from s"""
    qt_sorted_scalar_join """select count(*) from
        (select v from single_tablet_gather_t order by v limit 2) a cross join
        (select v from single_tablet_gather_t order by v limit 2) b"""

    sql "drop table if exists single_tablet_gather_empty"
    sql """create table single_tablet_gather_empty (k int, v int null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")"""
    qt_empty "select count(*), count(v), sum(v) from single_tablet_gather_empty"
}
