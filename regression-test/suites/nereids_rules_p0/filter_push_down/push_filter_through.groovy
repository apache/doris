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

suite("push_filter_through") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql "set disable_join_reorder=true"
    sql 'set be_number_for_test=3'
    sql "SET ignore_shape_nodes='PhysicalDistribute, PhysicalProject'"
    sql "set enable_fold_nondeterministic_fn=false"
    sql "set enable_fold_constant_by_be=false"//plan shape will be different
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    // push filter through alias
    qt_filter_project_alias""" 
     explain shape plan select * from (select id as alia from t1) t where alia = 1;
    """
    // push filter through constant alias
    qt_filter_project_constant""" 
     explain shape plan select * from (select 2 as alia from t1) t where alia = 1;
    """

    // push filter through project with arithmetic expression
    qt_filter_project_arithmetic"""
    explain shape plan select * from (select id + 1 as alia from t1) t where alia = 2;
    """

    // push filter through order by
    qt_filter_order_by"""
    explain shape plan select * from (select id from t1 order by id) t where t.id = 1;
    """

    // push filter through order by
    qt_filter_order_by_limit"""
    explain shape plan select * from (select id from t1 order by id limit 1) t where id = 1;
    """

    // push filter through order by constant
    qt_filter_order_by_constant"""
    explain shape plan select * from (select id from t1 order by 1) t where id = 1;
    """

    // push filter through inner join
    qt_filter_join_inner"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.id = 1;
    """
    qt_filter_join_inner"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id and t1.id = 1;
    """
    qt_filter_join_inner"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id and t1.msg = "";
    """
    // push filter through inner join with and cond
    qt_filter_join_inner"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.id = 1 and t2.id = 2;
    """

    // push filter through inner join with or cond
    qt_filter_join_inner"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.id = 1 or t2.id = 2;
    """
    // push filter through left join
    qt_filter_join_left"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through left join with and cond
    qt_filter_join_left"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id where t1.id = 1 and t2.id = 2;
    """

    // push filter through left join with or cond
    qt_filter_join_left"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id where t1.id = 1 or t2.id = 2;
    """
    // push filter through right join
    qt_filter_join_right"""
    explain shape plan select * from t1 right outer join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through full join
    qt_filter_join_full"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through full join with and cond
    qt_filter_join_left"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id where t1.id = 1 and t2.id = 2;
    """

    // push filter through full join with or cond
    qt_filter_join_left"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id where t1.id = 1 or t2.id = 2;
    """
    // push filter through cross join
    qt_filter_join_cross"""
    explain shape plan select * from t1 cross join t2 where t1.id = 1;
    """
    
    
    // push filter through left anti join
    qt_filter_join_left_anti"""
    explain shape plan select * from t1 left anti join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through left semi join
    qt_filter_join_left_semi"""
    explain shape plan select * from t1 left semi join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through right anti join
    qt_filter_join_right_anti"""
    explain shape plan select * from t1 right anti join t2 on t1.id = t2.id where t2.id = 1;
    """
    // push filter through right semi join
    qt_filter_join_right_semi"""
    explain shape plan select * from t1 right semi join t2 on t1.id = t2.id where t2.id = 1;
    """
    // push filter through right semi join
    qt_filter_join_right_semi"""
    explain shape plan select * from t1 right semi join t2 on t1.id = t2.id where t2.id = 1;
    """

    // Push filter through multiple inner joins
    qt_filter_multi_inner"""
    explain shape plan
    select *
    from t1
    inner join t2 on t1.id = t2.id
    inner join t3 on t1.id = t3.id
    where t1.id = 1;
    """

    // Push filter through mixed inner and left joins
    qt_filter_mixed_inner_left"""
    explain shape plan
    select *
    from t1
    inner join t2 on t1.id = t2.id
    left join t3 on t1.id = t3.id
    where t1.id = 1 and t2.id = 2 or t3.id = 2;
    """

    // Push filter through multiple left joins
    qt_filter_multi_left"""
    explain shape plan
    select *
    from t1
    left join t2 on t1.id = t2.id
    left join t3 on t1.id = t3.id
    where t1.id = 1 and t2.id > 1 or t3.id < 4;
    """

    // Push filter through multiple outer joins
    qt_filter_multi_outer"""
    explain shape plan
    select *
    from t1
    full join t2 on t1.id = t2.id
    left join t3 on t1.id = t3.id
    where t1.id = 1;
    """

    // Push filter through multiple cross joins
    qt_filter_multi_cross"""
    explain shape plan
    select *
    from t1
    cross join t2
    cross join t3
    where t1.id = 1;
    """

    // Push filter through a mix of inner, left, and right joins
    qt_filter_multi_mixed"""
    explain shape plan
    select *
    from t1
    inner join t2 on t1.id = t2.id
    left join t3 on t1.id = t3.id
    right join t4 on t1.id = t4.id
    where t1.id = 1;
    """

    // Push filter of agg function through aggregated filter
    qt_filter_aggregation_filtered_agg_func"""
    explain shape plan select count() from t1 group by msg having count() > 10;
    """
    // Push filter through group by set
    qt_filter_aggregation_group_set"""
    explain shape plan select count() from t1 group by grouping sets ((msg, id), (id)) having id > 10 and msg = 1;
    """
    qt_filter_aggregation_group_set"""
    explain shape plan select count() from t1 group by grouping sets ((msg, id), (id)) having id > 10 or msg = 1;
    """
    // Push filter of group by key through aggregated filter
    qt_filter_aggregation_filtered_key"""
    explain shape plan select count() from t1 group by msg having msg = "1";
    """

    // Push filter to subquery with alias
    qt_filter_aggregation_filtered_part_key"""
    explain shape plan select * from (select msg, count() as c from t1 group by msg) t where c > 10;
    """
    // Push filter through UNION
    qt_push_filter_union"""
    explain shape plan select * from ( select * from t1 UNION select * from t2) t where t.id = 2;
    """
    // Push filter through UNION ALL
    qt_push_filter_union_all"""
    explain shape plan select * from ( select * from t1 UNION ALL select * from t2) t where t.id = 2;
    """
    // Push filter through INTERSECT
    qt_push_filter_intersect"""
    explain shape plan select * from ( select * from t1 INTERSECT select * from t2) t where t.id = 2;
    """
    // Push filter through EXCEPT
    qt_push_filter_except"""
    explain shape plan select * from ( select * from t1 EXCEPT select * from t2) t where t.id = 2;
    """
    // Push filter through UNION with constant
    qt_push_filter_union"""
    explain shape plan select id from ( select cast(rand() as int) as id UNION select id from t2) t where id = 2;
    """
    // Push filter through UNION ALL with constant
    qt_push_filter_union_all"""
    explain shape plan select id from ( select cast(rand() as int) as id UNION ALL select id from t2) t where id = 2 or id = 3;
    """
    // Push filter through INTERSECT with constant
    qt_push_filter_intersect"""
    explain shape plan select id from ( select cast(rand() as int) as id INTERSECT select id from t2) t where id = 2 or id = 3;
    """
    // Push filter through EXCEPT with constant
    qt_push_filter_except"""
    explain shape plan select id from ( select cast(rand() as int) as id, msg from t1 EXCEPT select id, msg from t2) t where id = 2 and msg = '';
    """

     qt_push_filter_except"""
    explain shape plan select t.id from ( select id, msg from t1 where id = 2 EXCEPT select id, msg from t2 where id = 2) t inner join t3 on t3.id = t.id;
    """

     qt_push_filter_subquery"""
    explain shape plan select t.id from ( select id, msg from t1 where id = 2 EXCEPT select id, msg from t2 where id = 2) t inner join t3 on t3.id = t.id;
    """

    // push filter through window function with partition - ROW_NUMBER()
    qt_filter_window_row_number"""
    explain shape plan SELECT ROW_NUMBER() OVER (PARTITION BY id) AS row_num from t1 WHERE id <= 5;
    """
    // push filter through window function with partition and order by - ROW_NUMBER()
    qt_filter_window_order_row_number"""
    explain shape plan SELECT ROW_NUMBER() OVER (PARTITION BY id order by id) AS row_num from t1 WHERE id <= 5;
    """
    // push filter through window function with partition and order by and complex predicate - ROW_NUMBER()
    qt_filter_window_row_number_complex_predicate"""
    explain shape plan SELECT ROW_NUMBER() OVER (PARTITION BY id + msg order by id) AS row_num from t1 WHERE id + msg = "";
    """
    // push filter through window function with partition and order by and complex predicate - ROW_NUMBER()
    qt_filter_multi_window"""
    explain shape plan SELECT ROW_NUMBER() OVER (PARTITION BY id + msg) AS row_num,
     ROW_NUMBER() OVER (PARTITION BY id order by id) AS row_num2
    from t1 WHERE msg = "" or id = 2;
    """
}