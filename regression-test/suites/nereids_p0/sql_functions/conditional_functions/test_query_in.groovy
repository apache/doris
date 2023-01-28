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

suite("test_query_in", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"
    
    def tableName1 = "test"
    def tableName2 = "baseall"
    
    qt_in1 """select * from ${tableName1} where k1 in (1, -1, 5, 0.1, 3.000) order by k1, k2, k3, k4"""
    qt_in2 """select * from ${tableName1} where k6 in ("true") order by k1, k2, k3, k4"""
    qt_in3 """select * from ${tableName1} where k7 in ("wangjuoo4") order by k1, k2, k3, k4"""
    qt_in4 """select * from ${tableName1} where k7 in ("wjj") order by k1, k2, k3, k4"""
    qt_in5 """select * from ${tableName1} where k8 in (1, -1, 0.100, 0) order by k1, k2, k3, k4"""
    qt_in6 """select * from ${tableName1} where k9 in (-365, 100) order by k1, k2, k3, k4"""
    qt_in7 """select * from ${tableName1} where k5 in
		    (123.123, 1243.5, 100, -654,6540, "0", "-0.1230")
		    order by k1, k2, k3, k4"""
    qt_in8 """select * from test where k4 in
		    (-9016414291091581975, -1, 100000000000000000000000000000000000) order by k1, k2, k3, k4"""
    qt_in9 """select * from ${tableName1} where k1 not in (1, -1, 5, 0.1, 3.000) order by k1, k2, k3, k4"""
    qt_in10 """select * from ${tableName1} where k6 not in ("true") order by k1, k2, k3, k4"""
    qt_in11 """select * from ${tableName1} where k7 not in ("wangjuoo4") order by k1, k2, k3, k4"""
    qt_in12 """select * from ${tableName1} where k7 not in ("wjj") order by k1, k2, k3, k4"""
    qt_in13 """select * from ${tableName1} where k8 not in (1, -1, 0.100, 0) order by k1, k2, k3, k4"""
    qt_in14 """select * from ${tableName1} where k9 not in (-365, 100) order by k1, k2, k3, k4"""
    qt_in15 """select * from ${tableName1} where k5 not in
		    (123.123, 1243.5, 100, -654,6540, "0", "-0.1230")
		    order by k1, k2, k3, k4"""
    qt_in16 """select * from test where k4 not in
		    (-9016414291091581975, -1, 100000000000000000000000000000000000) order by k1, k2, k3, k4"""
    qt_in17 """select NULL in (1, 2, 3)"""
    qt_in18 """select NULL in (1, NULL, 3)"""
    qt_in19 """select 1 in (2, NULL, 1)"""
    qt_in20 """select 1 in (1, NULL, 2)"""
    qt_in21 """select 1 in (2, NULL, 3)"""
    qt_in22 """select 1 in (2, 3, 4)"""
    qt_in23 """select NULL not in (1, 2, 3)"""
    qt_in24 """select NULL not in (1, NULL, 3)"""
    qt_in25 """select 1 not in (2, NULL, 1)"""
    qt_in26 """select 1 not in (1, NULL, 2)"""
    qt_in27 """select 1 not in (2, NULL, 3)"""
    qt_in28 """select 1 not in (2, 3, 4)"""
    qt_in29 """select * from ${tableName2} where k1 in (1,2,3,4) and k1 in (1)"""
    qt_in30 """select * from (select 'jj' as kk1, sum(k2) from ${tableName2} where k10 = '2015-04-02' group by kk1)tt
            where kk1 in ('jj')"""
    qt_in31 """select * from (select 'jj' as kk1, sum(k2) from ${tableName2} where k10 = '2015-04-02' group by kk1)tt
            where kk1 = 'jj'"""
}
