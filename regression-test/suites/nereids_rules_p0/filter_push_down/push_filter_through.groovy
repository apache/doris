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
}