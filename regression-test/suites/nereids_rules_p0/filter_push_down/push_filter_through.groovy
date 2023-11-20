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
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"

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
    // push filter through left join
    qt_filter_join_left"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through right join
    qt_filter_join_right"""
    explain shape plan select * from t1 right outer join t2 on t1.id = t2.id where t1.id = 1;
    """
    // push filter through full join
    qt_filter_join_full"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id where t1.id = 1;
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
    where t1.id = 1;
    """

    // Push filter through multiple left joins
    qt_filter_multi_left"""
    explain shape plan
    select *
    from t1
    left join t2 on t1.id = t2.id
    left join t3 on t1.id = t3.id
    where t1.id = 1;
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
    explain shape plan select count() from t1 group by ROLLUP(msg, id) having count() > 10;
    """
    // Push filter of group by key through aggregated filter
    qt_filter_aggregation_filtered_key"""
    explain shape plan select count() from t1 group by id having id > 10;
    """
    // Push filter of part of group by key through aggregated filter
    qt_filter_aggregation_filtered_part_key"""
    explain shape plan select count() from t1 group by id, msg having id > 10;
    """
    // Push filter to subquery with constant
    qt_filter_aggregation_filtered_part_key"""
    select * from (select count(), now() as c from t1 group by id) t where c > 10;
    """
    // Push filter to subquery with alias
    qt_filter_aggregation_filtered_part_key"""
    explain shape plan select * from (select id + 1, count() as c from t1 group by id) t where c > 10;
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
    // // Push filter through UNION with constant
    // qt_push_filter_union"""
    // explain shape plan select id from ( select cast(now() as int) as id UNION select id from t2) t where c = 2;
    // """
    // // Push filter through UNION ALL with constant
    // qt_push_filter_union_all"""
    // explain shape plan select id from ( select cast(now() as int) as id UNION ALL select id from t2) t where c = 2;
    // """
    // // Push filter through INTERSECT with constant
    // qt_push_filter_intersect"""
    // explain shape plan select id from ( select cast(now() as int) as id INTERSECT select id from t2) t where c = 2;
    // """
    // // Push filter through EXCEPT with constant
    // qt_push_filter_except"""
    // explain shape plan select id from ( select cast(now() as int) as id EXCEPT select id from t2) t where c = 2;
    // """
    
}