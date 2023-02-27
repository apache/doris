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

suite("test_query_where", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"
    
    def tableName1 = "test"
    def tableName2 = "baseall"
    qt_where1"""select k1 from ${tableName1} where k1 < 4 order by k1, k2, k3, k4"""
    qt_where2"""select k2 from ${tableName1} where k2 < 1990 order by k1, k2, k3, k4"""
    qt_where3"""select k3 from ${tableName1} where k3 < 2000 order by k1, k2, k3, k4"""
    qt_where4"""select k4 from ${tableName1} where k4 < 0 order by k1, k2, k3, k4"""
    qt_where5"""select k5 from ${tableName1} where k5 < 1243.5 order by k1, k2, k3, k4"""
    qt_where6"""select lower(k6) from ${tableName2} where lower(k6) < 'false' order by lower(k6)"""
    qt_where7"""select lower(k7) from ${tableName2} where lower(k7) < 'g' order by lower(k7)"""
    qt_where8"""select k8 from ${tableName1} where k8 < 0 order by k1, k2, k3, k4"""
    qt_where9"""select k9 from ${tableName1} where k9 < 6.333 order by k1, k2, k3, k4"""
    qt_where10"""select k1 from ${tableName1} where k1 <> 4 order by k1, k2, k3, k4"""
    qt_where11"""select k2 from ${tableName1} where k2 <> 1989 order by k1, k2, k3, k4"""
    qt_where12"""select k3 from ${tableName1} where k3 <> 1001 order by k1, k2, k3, k4"""
    qt_where13"""select k4 from ${tableName1} where k4 <> -11011903 order by k1, k2, k3, k4"""
    qt_where14"""select k5 from ${tableName1} where k5 <> 1243.5 order by k1, k2, k3, k4"""
    qt_where15"""select k6 from ${tableName1} where k6 <> 'false' order by k1, k2, k3, k4"""
    qt_where16"""select k7 from ${tableName1} where k7 <> 'f' order by k1, k2, k3, k4"""
    qt_where17"""select k8 from ${tableName1} where k8 <> 0 order by k1, k2, k3, k4"""
    qt_where18"""select k8 from ${tableName1} where k8 <> 0.1 order by k1, k2, k3, k4"""
    qt_where19"""select k9 from ${tableName1} where k9 <> 6.333 order by k1, k2, k3, k4"""
    qt_where20"""select k9 from ${tableName1} where k9 <> -365 order by k1, k2, k3, k4"""
    qt_where21"""select k1 from ${tableName1} where k1 != 4 order by k1, k2, k3, k4"""
    qt_where22"""select k2 from ${tableName1} where k2 != 1989 order by k1, k2, k3, k4"""
    qt_where23"""select k3 from ${tableName1} where k3 != 1001 order by k1, k2, k3, k4"""
    qt_where24"""select k4 from ${tableName1} where k4 != -11011903 order by k1, k2, k3, k4"""
    qt_where25"""select k5 from ${tableName1} where k5 != 1243.5 order by k1, k2, k3, k4"""
    qt_where26"""select k6 from ${tableName1} where k6 != 'false' order by k1, k2, k3, k4"""
    qt_where27"""select k7 from ${tableName1} where k7 != 'f' order by k1, k2, k3, k4"""
    qt_where28"""select k8 from ${tableName1} where k8 != 0 order by k1, k2, k3, k4"""
    qt_where29"""select k8 from ${tableName1} where k8 != 0.1 order by k1, k2, k3, k4"""
    qt_where30"""select k9 from ${tableName1} where k9 != 6.333 order by k1, k2, k3, k4"""
    qt_where31"""select k9 from ${tableName1} where k9 != -365 order by k1, k2, k3, k4"""
    qt_where32"""select * from ${tableName1} where k1<10000000000000000000000000
		         order by k1, k2, k3, k4"""
    qt_where33"""select * from ${tableName1} where k5=123.123000001"""
    qt_where34"""select * from ${tableName1} where k1=1 or k1>=10 and k6="true" order by k1, k2, k3, k4"""
}
