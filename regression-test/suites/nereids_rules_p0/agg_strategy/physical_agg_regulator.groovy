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

suite("physical_agg_regulator_request_deriver") {
    multi_sql"""
    SET ignore_shape_nodes='PhysicalProject';
    set runtime_filter_mode=OFF;
    set enable_parallel_result_sink=false;
    set be_number_for_test=1;
    """
    multi_sql """
    analyze table t1025_skew5000 with sample rows 7000 with sync;
    --drop cached stats t1025;
    """
    // 这个预期应该使用cte之后any
    qt_skew """explain shape plan
    select count(distinct a_1), count(distinct b_5) from t1025_skew5000 group by d_1025;"""

    // TODO 这个预期应该使用cte之后shuffle
    multi_sql """
    analyze table t1025 with sample rows 4000 with sync;
    --drop cached stats t1025;
    """
    qt_not_skew """explain shape plan
    select count(distinct a_1), count(distinct b_5) from t1025 group by d_1025;"""

    // request deriver
    qt_request_deriver_parent_ndv_high """explain shape plan
    select count(distinct b_5) from t1025 group by d_1025;"""
    qt_request_deriver_parent_ndv_low """explain shape plan
    select count(distinct d_1025) from t1025 group by b_5;"""
    qt_split_multi_agg_use_three_phase """explain shape plan
    select count(distinct b_5),avg(b_5) from t1025 group by d_1025;"""
    qt_split_multi_agg_use_four_phase """explain shape plan
    select count(distinct d_1025),avg(b_5) from t1025 group by b_5;"""
}
