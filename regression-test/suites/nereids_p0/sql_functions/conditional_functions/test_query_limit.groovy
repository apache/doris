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

suite("test_query_limit", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db;"

    def tableName = "test"
    def tableName2 = "baseall"
    qt_limit1 "select * from ${tableName} order by k1, k2, k3, k4 limit 2"
    qt_limit2 "select * from ${tableName} order by k1, k2, k3, k4 limit 0"
    qt_limit3 "select * from ${tableName} where k6 = 'true' limit 0"
    qt_limit4 "select * from ${tableName} order by k1, k2, k3, k4 limit 100"
    qt_limit5 "select * from ${tableName} order by k1, k2, k3, k4 limit 2, 2"
    qt_limit6 "select * from ${tableName} order by k1, k2, k3, k4 limit 2, 20"
    qt_limit7 "select * from ${tableName} order by k1, k2, k3, k4 desc limit 2"
    qt_limit8 "select * from ${tableName} order by k1, k2, k3, k4 desc limit 0"
    qt_limit9 "select * from ${tableName} order by k1, k2, k3, k4 desc limit 100"
    qt_limit10 "select k3, sum(k9) from ${tableName} where k1<5 group by 1 order by 2 limit 3"
    qt_limit11 "select * from (select * from ${tableName} union all select * from ${tableName2}) b limit 0"
    // Nereids does't support this syntax
    // qt_limit12 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 1, 1"
    // Nereids does't support this syntax
    // qt_limit13 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 1, 2"
    // Nereids does't support this syntax
    // qt_limit14 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 1, 3"
    // Nereids does't support this syntax
    // qt_limit15 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 2, 1"
    // Nereids does't support this syntax
    // qt_limit16 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 2, 2"
    // Nereids does't support this syntax
    // qt_limit17 "select * from (select * from ${tableName} order by k1, k2, k3, k4 limit 1, 2) a limit 2, 3"
    // Nereids does't support this syntax
    // test {
    //     sql "select * from ${tableName} limit 1, 10"
    //     rowNum 2
    // }
    // Nereids does't support this syntax
    // test {
    //     sql "select * from ${tableName} limit 2, 10"
    //     rowNum 1
    // }
    // Nereids does't support this syntax
    // test {
    //     sql "select * from ${tableName} limit 3, 10"
    //     rowNum 0
    // }
}
