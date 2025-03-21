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

suite("push_count_into_union_all") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql """
          DROP TABLE IF EXISTS mal_test_push_count
         """

    sql """
         create table mal_test_push_count(pk int, a int, b int) distributed by hash(pk) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into mal_test_push_count values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
        ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);
      """
    sql "sync"

    qt_count_group_by """
        select a,count(a) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a order by 1,2;"""

    qt_count_group_by_shape """explain shape plan
        select a,count(a) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a;"""

    qt_count_group_by_none """
        select count(a) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t order by 1"""

    qt_count_group_by_none_shape """explain shape plan
        select count(a) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t"""

    qt_count_expr_group_by_none """
        select count(a+1) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t order by 1"""

    qt_count_expr_group_by_none_shape """explain shape plan
        select count(a+1) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t"""

    qt_union_const """
        select count(a+1) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select 1,2 ) t order by 1"""

    qt_union_const_shape """explain shape plan
        select count(a+1) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select 1,2 ) t"""

    qt_count_expr_group_by_ditinct_none_shape """explain shape plan
        select count(distinct a+1) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t"""

    qt_union_all_child_alias """
        select c1,count(c1)  from (select a as c1,b as c2 from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100
        union all select a,b from mal_test_push_count where a=1 ) t group by c1 order by 1,2;"""

    qt_union_all_child_alias_shape """
        explain shape plan
        select c1,count(c1)  from (select a as c1,b as c2 from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100
        union all select a,b from mal_test_push_count where a=1 ) t group by c1;"""

    qt_union_all_child_expr """
        select c1,count(c1)  from (select a+1 as c1,b as c2 from mal_test_push_count where a>1 union all select a+100,b from mal_test_push_count where a<100
        union all select abs(a),b from mal_test_push_count where a=1 ) t group by c1 order by 1,2;"""

    qt_count_group_by_count_other """
        select a,count(b) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a order by 1,2;"""

    qt_count_group_by_count_other_shape """
        explain shape plan
        select a,count(b) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a order by 1,2;"""

    qt_count_group_by_multi_col """
        select a,count(b) c1 from (select a,b,pk from mal_test_push_count where a>1 union all select a,b,pk from mal_test_push_count where a<100 
        union all select a,b,pk from mal_test_push_count where a=1 ) t group by a,pk order by 1,2;"""

    qt_count_group_by_multi_col_shape """
        explain shape plan
        select a,count(b) c1 from (select a,b,pk from mal_test_push_count where a>1 union all select a,b,pk from mal_test_push_count where a<100
        union all select a,b,pk from mal_test_push_count where a=1 ) t group by a,pk order by 1,2;"""

    qt_test_upper_refer """
        select a,c1 from (
        select a,count(b) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a) outer_table order by 1,2;"""

    qt_test_upper_refer_shape """
        explain shape plan
        select a,c1 from (
        select a,count(b) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a) outer_table order by 1,2;"""

    qt_unsupport_agg_func """
        explain shape plan
        select a,count(b) c1,sum(b) from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a order by 1,2,3;"""

    qt_test_upper_refer_count_star """
        select a,c1 from (
        select a,count(*) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a) outer_table order by 1,2;"""

    qt_test_upper_refer_count_star_shape """
        explain shape plan
        select a,c1 from (
        select a,count(*) c1 from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1 ) t group by a) outer_table order by 1,2;"""

    qt_test_count_star """
        select count(*) from (select a,b from mal_test_push_count where a>1 union all select a,b from mal_test_push_count where a<100 
        union all select a,b from mal_test_push_count where a=1) t order by 1,2;"""
}