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

suite("test_coalesce", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"

    def tableName1 = "test"
    def tableName2 = "baseall"

    for (k in range(1, 12)) {
        qt_coalesce1 "select k1, coalesce(k${k}) from ${tableName2} order by 1"
        qt_coalesce2 "select k1, coalesce(k${k}, k${k}) from ${tableName2} order by 1"
        qt_coalesce3 "select k1, coalesce(k${k}, null) from ${tableName2} order by 1"
        qt_coalesce4 "select k1, coalesce(null, k${k}) from ${tableName2} order by 1"
    }
    qt_coalesce5 "select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, null) from ${tableName1} order by 1"
    qt_coalesce6 "select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11) from ${tableName1} order by 1"
    qt_coalesce7 "select * from (select coalesce(\"string\", \"\")) a"
    qt_coalesce8 "select * from ${tableName2} where coalesce(k1, k2) in (1, null) order by 1, 2, 3, 4"
    qt_coalesce9 "select * from ${tableName1} where coalesce(k1, null) in (1, null) order by 1, 2, 3, 4, 5, 6"
    qt_coalesce10 "select  coalesce(1, null)"
}
