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

suite("distinct_agg_rewriter") {
    multi_sql"""
    SET ignore_shape_nodes='PhysicalProject';
    set runtime_filter_mode=OFF;
    set enable_parallel_result_sink=false;
    set be_number_for_test=1;
    """
    multi_sql """
    analyze table t1000_2 with sync;
    """
    qt_use_multi_phase1 """explain shape plan
    select count(distinct b_5) from t1000_2 group by d_200;"""
    qt_use_multi_phase2 """explain shape plan
    select count(distinct b_5) from t1000_2 group by b_5;"""
    qt_use_multi_phase3 """explain shape plan
    select count(distinct d_200) from t1000_2 group by b_5;"""
    qt_use_multi_distinct """explain shape plan
    select count(distinct d_200) from t1000_2 group by a_1;"""
}