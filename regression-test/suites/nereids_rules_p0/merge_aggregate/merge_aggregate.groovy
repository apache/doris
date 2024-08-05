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
suite("merge_aggregate") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set enable_parallel_result_sink=false;"

    sql """
          DROP TABLE IF EXISTS mal_test1
         """

    sql """
         create table mal_test1(pk int, a int, b int) distributed by hash(pk) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into mal_test1 values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
        ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);
      """
    sql "drop table if exists mal_test2"
    sql """
        create table mal_test2(pk int, a int, b int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1');
    """

    sql "sync"


    qt_sumCount_empty_table """
        select sum(col) from (select count(a) col from mal_test2 group by a) t;
    """

    qt_maxMax_minMin_sumSum_sumCount """
        select max(col1), min(col2), sum(col3), sum(col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 
        from mal_test1 group by pk,a) t group by a order by 1,2,3,4;
     """

    qt_maxGroupKey_minGroupKey """
        select max(a),min(a),max(pk),min(pk) from 
        (select pk,a from mal_test1 group by pk,a) t 
        group by a order by 1,2,3,4;
    """

    qt_agg_project_agg """
        select col2, max(col2),min(col2),sum(col3),sum(col4) from 
        (select pk as col1,a as col2,sum(b) col3, count(b) col4 from mal_test1 group by pk,a) t 
        group by col2 order by 1,2,3,4;
    """

    qt_upper_plan_can_use_name """
        select c1+1 from (
        select max(col1) c1, min(col2) c2, sum(col3) c3, sum(col4) c4 from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by a order by 1,2,3,4) outert order by 1;
    """

    qt_outer_agg_has_distinct_same_keys """
        select max(col1), min(col2), sum(col3), sum(DISTINCT col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by pk,a order by 1,2,3,4;    
    """

    qt_inner_agg_has_distinct_same_keys """
        select max(col1), min(col2), sum(col3), sum(col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(distinct b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by a,pk order by 1,2,3,4;
    """

    qt_sumCount_empty_table_shape """
        explain shape plan select sum(col) from (select count(a) col from mal_test2 group by a) t;
    """

    qt_agg_project_agg_shape """
        explain shape plan select max(col2),min(col2),sum(col3),sum(col4) from 
        (select pk as col1,a as col2,sum(b) col3, count(b) col4 from mal_test1 group by pk,a) t 
        group by col2 order by 1,2,3,4;
    """

    qt_maxMax_minMin_sumSum_sumCount_shape """
        explain shape plan select max(col1), min(col2), sum(col3), sum(col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 
        from mal_test1 group by pk,a) t group by a order by 1,2,3,4;
     """

    qt_maxGroupKey_minGroupKey_shape """
        explain shape plan select max(a),min(a),max(pk),min(pk) from 
        (select pk,a from mal_test1 group by pk,a) t 
        group by a order by 1,2,3,4;
    """

    qt_outer_agg_has_distinct_same_keys_shape """
        explain shape plan
        select max(col1), min(col2), sum(col3), sum(DISTINCT col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by pk,a order by 1,2,3,4;    
    """

    qt_inner_agg_has_distinct_same_keys_shape """
        explain shape plan
        select max(col1), min(col2), sum(col3), sum(col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(distinct b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by a,pk order by 1,2,3,4;
    """

    qt_middle_project_has_expression_cannot_merge_shape1 """
        explain shape plan 
        select max(col1),min(col1) from 
        (select pk+1 as col1,a from mal_test1 group by pk,a) t 
        group by a order by 1,2;
    """

    qt_middle_project_has_expression_cannot_merge_shape2 """
        explain shape plan
        select max(col1), min(col2), sum(col3), sum(col4) from
        (select pk,a,max(b)+1 as col1, min(b) as col2, sum(b) as col3, count(b) as col4 from mal_test1 group by pk,a) t
        group by a order by 1,2,3,4;
    """

    qt_maxGroupKey_minGroupKey_sumGroupKey_cannot_merge_shape """
        explain shape plan select max(a),min(a),max(pk),min(pk),sum(pk) from 
        (select pk,a from mal_test1 group by pk,a) t 
        group by a;
    """

    qt_maxMin_cannot_merge_shape """
        explain shape plan select max(col), max(col2) from 
        (select pk,a,min(b) col,max(b) col2 from mal_test1 group by pk,a) t 
        group by a;
    """

    qt_group_key_not_contain_cannot_merge_shape """
        explain shape plan select max(col2) from 
        (select pk,a,max(b) col2 from mal_test1 group by pk,a) t 
        group by a,col2;
    """

    qt_outer_agg_has_distinct_cannot_merge_shape """
        explain shape plan
        select max(col1), min(col2), sum(col3), sum(DISTINCT col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by a order by 1,2,3,4;    
    """

    qt_inner_agg_has_distinct_cannot_merge_shape """
        explain shape plan
        select max(col1), min(col2), sum(col3), sum(col4) from 
        (select pk,a,max(b) as col1, min(b) as col2, sum(distinct b) as col3, count(b) as col4 from mal_test1 group by pk,a) t 
        group by a order by 1,2,3,4;
    """

    qt_agg_with_expr_cannot_merge_shape1 """
        explain shape plan select max(col1+a),min(col1) from 
        (select pk as col1, a from mal_test1 group by pk,a) t 
        group by a order by 1,2;
    """

    qt_agg_with_expr_cannot_merge_shape2 """
        explain shape plan select max(col1+1),min(col1) from 
        (select pk as col1, a from mal_test1 group by pk,a) t 
        group by a order by 1,2;
    """

    sql "drop table if exists mal_test_merge_agg"
    sql """
         create table mal_test_merge_agg(
            k1 int null,
            k2 int not null,
            k3 string null,
            k4 varchar(100) null
        )
        duplicate key (k1,k2)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql "insert into mal_test_merge_agg select 1,1,'1','a';"
    sql "insert into mal_test_merge_agg select 2,2,'2','b';"
    sql "insert into mal_test_merge_agg select 3,-3,null,'c';"
    sql "sync"

    qt_test_has_project_distinct_cant_transform """
        select max(count_col)
        from (
            select k4,
            count(distinct case when k3 is null then 1 else 0 end) as count_col
            from mal_test_merge_agg group by k4
        ) t ;
    """
    qt_test_has_project_distinct_cant_transform_shape """
        explain shape plan
        select max(count_col)
        from (
            select k4,
            count(distinct case when k3 is null then 1 else 0 end) as count_col
            from mal_test_merge_agg group by k4
        ) t ;
    """

    qt_test_distinct_expr_transform """
        select max(count_col)
        from (
            select k4,
            max(-abs(k1)) as count_col
            from mal_test_merge_agg group by k4
        ) t ;
    """
    qt_test_distinct_expr_transform_shape """
        explain shape plan
        select max(count_col)
        from (
            select k4,
            max(-abs(k1)) as count_col
            from mal_test_merge_agg group by k4
        ) t ;
    """

    qt_test_has_project_distinct_expr_transform """
        select sum(count_col)
        from (
            select k4,
            count(distinct case when k3 is null then 1 else 0 end) as count_col
            from mal_test_merge_agg group by k4
        ) t  group by k4;
    """

    qt_test_has_project_distinct_expr_transform """
        explain shape plan
        select sum(count_col)
        from (
            select k4,
            count(distinct case when k3 is null then 1 else 0 end) as count_col
            from mal_test_merge_agg group by k4
        ) t  group by k4;
    """

    qt_test_sum_empty_table """
        select sum(col1),min(col2),max(col3) from (select sum(a) col1, min(b) col2, max(pk) col3 from mal_test2 group by a) t;
    """

    qt_test_sum_empty_table_shape """
        explain shape plan
        select sum(col1),min(col2),max(col3) from (select sum(a) col1, min(b) col2, max(pk) col3 from mal_test2 group by a) t;
    """

    qt_agg_project_agg_the_project_has_duplicate_slot_output """
    select max(col1), col10, col11 from 
        (select a,max(b) as col1, count(b) as col4, a as col10, a as col11 
        from mal_test1 group by a) t group by col10, col11 order by 1,2,3;
    """
}
