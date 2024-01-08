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

suite("push_filter_inside_join") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql 'set be_number_for_test=3'

    // Push down > condition to cross join
    qt_pushdown_cross_join"""
    explain shape plan select * from t1, t2 where t1.msg > t2.msg;
    """
    
    // Push down hash condition to cross join
    qt_pushdown_cross_join"""
    explain shape plan select * from t1, t2 where t1.msg = t2.msg;
    """

    // Push > condition into inner join
    qt_pushdown_inner_join"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.msg > t2.msg;
    """

    // Push > condition into left join
    qt_pushdown_left_join"""
    explain shape plan select * from t1 left join t2 on t1.id = t2.id where t1.msg > t2.msg;
    """

    // Push < condition into right join
    qt_pushdown_right_join"""
    explain shape plan select * from t1 right join t2 on t1.id = t2.id where t2.msg < t1.msg;
    """

    // Push < condition into full join
    qt_pushdown_full_join"""
    explain shape plan select * from t1 full join t2 on t1.id = t2.id where t1.msg < t2.msg;
    """

    // Push < condition into cross join
    qt_pushdown_cross_join"""
    explain shape plan select * from t1 cross join t2 where t1.msg < t2.msg;
    """

    // Push = condition into inner join
    qt_pushdown_inner_join_hash"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.msg = t2.msg;
    """

    // Push = condition into left join
    qt_pushdown_left_join_hash"""
    explain shape plan select * from t1 left join t2 on t1.id = t2.id where t1.msg = t2.msg;
    """

    // Push = condition into right join
    qt_pushdown_right_join_hash"""
    explain shape plan select * from t1 right join t2 on t1.id = t2.id where t2.msg = t1.msg;
    """

    // Push = condition into full join
    qt_pushdown_full_join_hash"""
    explain shape plan select * from t1 full join t2 on t1.id = t2.id where t1.msg = t2.msg;
    """

    // Push combine condition into cross join
    qt_pushdown_cross_join_combine"""
    explain shape plan select * from t1 cross join t2 where t1.msg = t2.msg and t1.msg + t2.msg = "";
    """

    // Push combine condition into inner join
    qt_pushdown_inner_join_combine"""
    explain shape plan select * from t1 inner join t2 on t1.id = t2.id where t1.msg = t2.msg and t1.msg + t2.msg = "";
    """

    // Push combine condition into left join
    qt_pushdown_left_join_combine"""
    explain shape plan select * from t1 left join t2 on t1.id = t2.id where t1.msg = t2.msg and t1.msg + t2.msg = "";
    """

    // Push combine condition into right join
    qt_pushdown_right_join_combine"""
    explain shape plan select * from t1 right join t2 on t1.id = t2.id where t2.msg = t1.msg and t1.msg + t2.msg = "";
    """

    // Push combine condition into full join
    qt_pushdown_full_join_combine"""
    explain shape plan select * from t1 full join t2 on t1.id = t2.id where t1.msg = t2.msg and t1.msg + t2.msg = "";
    """

    // Push combine condition into cross join
    qt_pushdown_cross_join_combine"""
    explain shape plan select * from t1 cross join t2 where t1.msg = t2.msg and t1.msg + t2.msg = "";
    """
}

