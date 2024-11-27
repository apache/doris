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

suite("push_down_expression_in_hash_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql "set ignore_shape_nodes='PhysicalDistribute, PhysicalProject'"
    sql "set disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    // Push down arithmetic expression in inner join
    qt_push_arithmetic_inner_join"""
    explain shape plan select * from t1 join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in left semi join
    qt_push_arithmetic_left_semi_join"""
    explain shape plan select * from t1 left semi join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in right semi join
    qt_push_arithmetic_right_semi_join"""
    explain shape plan select * from t1 right semi join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in left outer join
    qt_push_arithmetic_left_outer_join"""
    explain shape plan select * from t1 left outer join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in right outer join
    qt_push_arithmetic_right_outer_join"""
    explain shape plan select * from t1 right outer join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in full outer join
    qt_push_arithmetic_full_outer_join"""
    explain shape plan select * from t1 full outer join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in left anti join
    qt_push_arithmetic_left_anti_join"""
    explain shape plan select * from t1 left anti join t2 on t1.id + 1 = t2.id;
    """
    // Push down arithmetic expression in right anti join
    qt_push_arithmetic_right_anti_join"""
    explain shape plan select * from t1 right anti join t2 on t1.id + 1 = t2.id;
    """
    // Push down join condition to null-aware anti join
    qt_pushdown_null_aware_anti_join_combined"""
    explain shape plan select * from t1 where t1.id not in (select id from t2 where t2.id > 0);
    """
    // Push down join condition to inner join child in a subquery within the WHERE clause
    qt_pushdown_inner_join_subquery"""
    explain shape plan select * from t1 where t1.id + 1 = (select sum(id) from t2);
    """
    // Push down join condition to left semi join child in a subquery within the WHERE clause
    qt_pushdown_left_semi_join_subquery"""
    explain shape plan select * from t1 where t1.id + 1 in (select id from t2);
    """
    // Push down join condition to left outer join child in a subquery within the WHERE clause
    qt_pushdown_left_outer_join_subquery"""
    explain shape plan select * from t1 where t1.id + 1 = (select sum(id) from t2) or t1.id is null;
    """
    // Push down join condition to left anti join child in a subquery within the WHERE clause
    qt_pushdown_left_anti_join_subquery"""
    explain shape plan select * from t1 where t1.id + 1 not in (select id from t2);
    """
}

