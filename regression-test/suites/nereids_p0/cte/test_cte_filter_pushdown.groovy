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
suite("test_cte_filter_pushdown") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set ignore_shape_nodes='PhysicalDistribute, PhysicalProject'"
    // CTE filter pushing down with the same filter
    qt_cte_filter_pushdown_1 """
            explain shape plan
            with main AS (
               select k1, row_number() over (partition by k1) rn
               from nereids_test_query_db.test
           )
           select * from (
               select m1.* from main m1, main m2
               where m1.k1 = m2.k1
           ) temp
           where k1 = 1;
    """
    qt_cte_filter_pushdown_2 """
            explain shape plan
            with main AS (
               select k1, row_number() over (partition by k2) rn
               from nereids_test_query_db.test
           )
           select * from (
               select m1.* from main m1, main m2
               where m1.k1 = m2.k1
           ) temp
           where k1 = 1;
    """
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set runtime_filter_mode=off'
    sql 'set dump_nereids_memo=false'
    sql "set disable_join_reorder=true"
    qt_cte_filter_pushdown_3 """
            explain shape plan
            with tmp as (
                select 
                    k1,
                    k3,
                    sum(k2) over (partition by l.k1 order by l.k3 ) pay_num
                from ( select * from nereids_test_query_db.test)l
            ),
            tmp2 as (
                select 
                    tt.*
                from 
                tmp tt join (select k3 from nereids_test_query_db.baseall ) dd
                on tt.k3=dd.k3
            )
            SELECT * from tmp2
            where k3=0 and k1=1;
    """
}
