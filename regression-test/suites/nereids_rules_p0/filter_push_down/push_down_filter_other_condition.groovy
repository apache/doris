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

suite("push_down_filter_other_condition") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "use regression_test_nereids_rules_p0"
    sql "set disable_join_reorder=true"
    sql 'set be_number_for_test=3'
    // Push down join condition to inner join child
    qt_pushdown_inner_join"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to left semi join child
    qt_pushdown_left_semi_join"""
    explain shape plan select * from t1 left semi join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to right semi join child
    qt_pushdown_right_semi_join"""
    explain shape plan select * from t1 right semi join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to left outer join child
    qt_pushdown_left_outer_join"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to right outer join child
    qt_pushdown_right_outer_join"""
    explain shape plan select * from t1 right outer join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to full outer join child
    qt_pushdown_full_outer_join"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to left anti join child
    qt_pushdown_left_anti_join"""
    explain shape plan select * from t1 left anti join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to right anti join child
    qt_pushdown_right_anti_join"""
    explain shape plan select * from t1 right anti join t2 on t1.id = t2.id and t1.id > 1;
    """
    // Push down join condition to cross join child
    qt_pushdown_cross_join"""
    explain shape plan select * from t1 cross join t2 where t1.id = t2.id and t1.id > 1;
    """

    // Push down join condition to inner join child in a combined join query
    qt_pushdown_inner_join_combined"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to left semi join child in a combined join query
    qt_pushdown_left_semi_join_combined"""
    explain shape plan select * from t1 left semi join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t1.id = t3.id and t1.id < 10;
    """
    // Push down join condition to right semi join child in a combined join query
    qt_pushdown_right_semi_join_combined"""
    explain shape plan select * from t1 right semi join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to left outer join child in a combined join query
    qt_pushdown_left_outer_join_combined"""
    explain shape plan select * from t1 left outer join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to right outer join child in a combined join query
    qt_pushdown_right_outer_join_combined"""
    explain shape plan select * from t1 right outer join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to full outer join child in a combined join query
    qt_pushdown_full_outer_join_combined"""
    explain shape plan select * from t1 full outer join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to left anti join child in a combined join query
    qt_pushdown_left_anti_join_combined"""
    explain shape plan select * from t1 left anti join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t1.id = t3.id and t1.id < 10;
    """
    // Push down join condition to right anti join child in a combined join query
    qt_pushdown_right_anti_join_combined"""
    explain shape plan select * from t1 right anti join t2 on t1.id = t2.id and t1.id > 1
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """
    // Push down join condition to cross join child in a combined join query
    qt_pushdown_cross_join_combined"""
    explain shape plan select * from t1 cross join t2
    inner join t3 on t2.id = t3.id and t2.id < 10;
    """

    // Push down join condition to null-aware anti join
    qt_pushdown_null_aware_anti_join_combined"""
    explain shape plan select * from t1 where t1.id not in (select id from t2 where t2.id > 0);
    """

    // Push down join condition to inner join child in a subquery within the WHERE clause
    qt_pushdown_inner_join_subquery"""
    explain shape plan select * from t1 where t1.id = (select sum(id) from t2 where t1.id = 1);
    """
    // Push down join condition to left semi join child in a subquery within the WHERE clause
    qt_pushdown_left_semi_join_subquery"""
    explain shape plan select * from t1 where t1.id in (select id from t2 where t1.id > 1);
    """
    // Push down join condition to left outer join child in a subquery within the WHERE clause
    qt_pushdown_left_outer_join_subquery"""
    explain shape plan select * from t1 where t1.id = (select sum(id) from t2 where t1.id = 1) or t1.id is null;
    """
    // Push down join condition to left anti join child in a subquery within the WHERE clause
    qt_pushdown_left_anti_join_subquery"""
    explain shape plan select * from t1 where t1.id not in (select id from t2 where t1.id > 1);
    """
    // Push down join condition to cross join child in a subquery within the WHERE clause
    qt_pushdown_cross_subquery"""
    explain shape plan select * from t1 where exists (select 1 from t2 where t1.id > 1);
    """
    // Push down join condition to inner join child outside the subquery
    qt_pushdown_inner_join_subquery_outer"""
    explain shape plan select * from t1 where t1.id = (select id from t2) and t1.id > 1;
    """
    // Push down join condition to left semi join child outside the subquery
    qt_pushdown_left_semi_join_subquery_outer"""
    explain shape plan select * from t1 where t1.id in (select id from t2) and t1.id > 1;
    """
    // Push down join condition to left outer join child outside the subquery
    qt_pushdown_left_outer_join_subquery_outer"""
    explain shape plan select * from t1 where t1.id = (select id from t2) or t1.id is null and t1.id > 1;
    """
    // Push down join condition to left anti join child outside the subquery
    qt_pushdown_left_anti_join_subquery_outer"""
    explain shape plan select * from t1 where t1.id not in (select id from t2) and t1.id > 1;
    """
    // Push down join condition to cross join child outside the subquery
    qt_pushdown_cross_join_subquery_outer"""
    explain shape plan select * from t1 where exists (select 1 from t2) and t1.id > 1;
    """
}

