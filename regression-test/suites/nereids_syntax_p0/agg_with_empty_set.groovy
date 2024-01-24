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

suite("agg_with_empty_set") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "use regression_test_nereids_syntax_p0"

    qt_select1 "select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where 1=2"
    qt_select2 "select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where 1=1"
    qt_select3 "select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where 1=2 group by c_custkey"
    qt_select4 "select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where 1=1 group by c_custkey order by c_custkey"
    qt_select5 """select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where c_custkey < 
        (select min(c_custkey) from customer)"""
    qt_select6 """select count(c_custkey), max(c_custkey), min(c_custkey), avg(c_custkey), sum(c_custkey) from customer where c_custkey < 
        (select min(c_custkey) from customer) having min(c_custkey) is null"""
    qt_ditinct_sum """select sum(distinct ifnull(c_custkey, 0)) from customer where 1 = 0"""
}