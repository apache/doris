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

suite("push_down_alias_through_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql 'set be_number_for_test=3'
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql "set disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set enable_parallel_result_sink=false;"

    // Push alias through inner join where condition not use alias
    qt_pushdown_inner_join"""
    explain shape plan select t1.id as id1, t2.id as id2, t1.msg, t2.msg from t1 inner join t2 on t1.id > t2.id;
    """

    // Push alias through left outer join where condition not using alias
    qt_pushdown_left_outer_join"""
    explain shape plan select t1.id as id1, t2.id as id2, t1.msg, t2.msg from t1 left outer join t2 on t1.id > t2.id;
    """

    // Push alias through right outer join where condition not using alias
    qt_pushdown_right_outer_join"""
    explain shape plan select t1.id as id1, t2.id as id2, t1.msg, t2.msg from t1 right outer join t2 on t1.id > t2.id;
    """

    // Push alias through full outer join where condition not using alias
    qt_pushdown_full_outer_join"""
    explain shape plan select t1.id as id1, t2.id as id2, t1.msg, t2.msg from t1 full outer join t2 on t1.id > t2.id;
    """

    // Push alias through left semi join where condition not using alias
    qt_pushdown_left_semi_join"""
    explain shape plan select t1.id as id1, t1.msg from t1 left semi join t2 on t1.id > t2.id;
    """

    // Push alias through right semi join where condition not using alias
    qt_pushdown_right_semi_join"""
    explain shape plan select t2.msg, t2.id as id2 from t1 right semi join t2 on t1.id > t2.id;
    """

    // Push alias through right anti join where condition not using alias
    qt_pushdown_right_anti_join"""
    explain shape plan select t2.msg, t2.id as id2 from t1 right anti join t2 on t1.id > t2.id;
    """

    // Push alias through left anti join where condition not using alias
    qt_pushdown_left_anti_join"""
    explain shape plan select t1.id as id1, t1.msg from t1 left anti join t2 on t1.id > t2.id;
    """

    // Push alias through cross join where condition not using alias
    qt_pushdown_cross_join"""
    explain shape plan select t1.id as id1, t2.id, t1.msg, t2.msg as id2 from t1 cross join t2 where t1.id > t2.id;
    """

    // Push alias through multiple joins where conditions not using aliases
    qt_pushdown_multiple_joins"""
    explain shape plan
    select t1.id as id1, t2.id as id2, t3.id as id3
    from t1
    inner join t2 on t1.id = t2.id
    left outer join t3 on t2.id = t3.id
    where t1.id > t2.id and t2.id < t3.id;
    """

    // Push alias through multiple joins with different join types and conditions
    qt_pushdown_multiple_joins"""
    explain shape plan
    select t1.id as id1, t2.id as id2, t3.id as id3
    from t1
    inner join t2 on t1.id = t2.id
    right outer join t3 on t2.id = t3.id
    left semi join t4 on t3.id = t4.id;
    """
}

