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

suite("distinct_split_constant_join") {
    sql "set enable_nereids_planner=true"
    sql "set be_number_for_test=1"
    // the project above the join materializes the constant of a column which the join condition still refers to.
    // Such an item must not be pushed into the join, otherwise both children of the join output the same exprId
    // and the required hash properties of one side stay empty, see ProjectJoinReorderHelper#normalize.
    sql "set enable_dphyp_optimizer=true"
    // force the shuffle plan, so the required distribution of the join children is derived instead of being
    // satisfied by the colocate plan; that is the path where a duplicated exprId across the join children shows up
    sql "set disable_colocate_plan=true"
    sql "set multi_distinct_strategy=2"

    sql "drop table if exists test_split_constant_base"
    sql """create table test_split_constant_base (
              org_code varchar(20),
              dc_code varchar(20),
              date_key date,
              people_count bigint,
              optimum_store_count bigint
            ) engine=olap
            duplicate key(org_code, dc_code, date_key)
            distributed by hash(org_code) buckets 10
            properties("replication_num"="1")"""

    sql "drop table if exists test_split_constant_sign"
    sql """create table test_split_constant_sign (
              org_code varchar(20),
              dc_code varchar(20),
              date_key date,
              poi_code varchar(40),
              if_optimum int,
              sign_duration bigint
            ) engine=olap
            duplicate key(org_code, dc_code, date_key, poi_code)
            distributed by hash(org_code) buckets 10
            properties("replication_num"="1")"""

    sql "drop table if exists test_split_constant_shop"
    sql """create table test_split_constant_shop (
              org_code varchar(20),
              dc_code varchar(20),
              dc_name varchar(100)
            ) engine=olap
            duplicate key(org_code, dc_code, dc_name)
            distributed by hash(org_code) buckets 10
            properties("replication_num"="1")"""

    sql """insert into test_split_constant_base
           select '014', 'C22035', date_add('2026-10-01', number % 31), number % 7, number % 11
           from numbers("number"="2000")"""
    sql """insert into test_split_constant_sign
           select '014', 'C22035', date_add('2026-10-01', number % 31),
                  concat('poi_', number % 100), number % 2, number % 100
           from numbers("number"="20000")"""
    sql """insert into test_split_constant_shop
           select '014', 'C22035', concat('shop_', number)
           from numbers("number"="100")"""

    sql "analyze table test_split_constant_base with sync"
    sql "analyze table test_split_constant_sign with sync"
    sql "analyze table test_split_constant_shop with sync"

    // the inner aggregate is split into one cte per distinct function, and every split group by the same
    // org_code / dc_code columns, so the outer query joins on a column whose exprId is defined twice.
    // `org_code in ('014')` and `dc_code in ('C22035')` make those columns constant, so constant propagation
    // materializes them as `'014' AS org_code` in the project above the join.
    order_qt_constant_predicate_join """
        select
            t1.org_code,
            t1.dc_code,
            max(t2.dc_name) as dc_name,
            sum(ifnull(t1.people_count, 0)) as people_count,
            sum(ifnull(t1.optimum_store_count, 0)) as optimum_store_count,
            max(ifnull(t2.sign_count, 0)) as sign_count,
            max(ifnull(t2.sign_optimum_store_count, 0)) as sign_optimum_store_count,
            max(ifnull(t2.sign_store_count, 0)) as sign_store_count
        from test_split_constant_base t1
        inner join (
            select
                t1.org_code,
                t1.dc_code,
                max(t2.dc_name) as dc_name,
                count(1) as sign_count,
                count(distinct if(t1.if_optimum = 1, t1.poi_code, null)) as sign_optimum_store_count,
                count(distinct t1.poi_code) as sign_store_count
            from test_split_constant_sign t1
            left join (
                select distinct org_code, dc_code, dc_name from test_split_constant_shop
            ) t2 on t1.org_code = t2.org_code and t1.dc_code = t2.dc_code
            where t1.date_key >= '2026-10-01' and t1.date_key <= '2026-10-31'
              and t1.org_code in ('014')
              and t1.dc_code in ('C22035')
            group by t1.org_code, t1.dc_code
        ) t2 on t1.org_code = t2.org_code and t1.dc_code = t2.dc_code
        where t1.date_key = '2026-10-24'
          and t1.org_code in ('014')
          and t1.dc_code in ('C22035')
        group by t1.org_code, t1.dc_code
        order by 1, 2
    """
}
