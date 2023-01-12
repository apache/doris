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

suite("test_logical_operators", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"
    
    def tableName = "test"
    for( i in range(1, 6)) {
        qt_logical_op1 "select k${i} %2 from ${tableName} order by k1, k2, k3, k4"
        qt_logical_op2 "select k${i} %-2 from ${tableName} order by k1, k2, k3, k4"
        if (i != 5) {
            qt_logical_op3 "select k${i} %0 from ${tableName} order by k1, k2, k3, k4"
        }
        qt_logical_op4 "select k${i} %2.1 from ${tableName} order by k1, k2, k3, k4"
        qt_logical_op5 "select k${i} %-2.1 from ${tableName} order by k1, k2, k3, k4"
    }
    for( i in range(1, 5)) {
        for( j in range(1, 5)) {
            qt_logical_op6 "select k${i}^k${j} from ${tableName} where k${i}>=0 and k${j} >=0 order by k1, k2, k3, k4"
            qt_logical_op7 "select k${i}|k${j}  from ${tableName} where k${i}>=0 and k${j} >=0 order by k1, k2, k3, k4"
            qt_logical_op8 "select k${i}&k${j}  from ${tableName} where k${i}>=0 and k${j} >=0 order by k1, k2, k3, k4"
        }
    }
    qt_logical_op9 "select k8, k9, k8%k9, k9%NULL, NULL%k9 from ${tableName} order by 1, 2"
    qt_logical_op10 'select * from baseall where (k1 = 1) or (k1 = 1 and k2 = 2) order by k1, k2, k3, k4'
    qt_logical_op11 'select * from baseall where k0 in (false,true) order by k1, k2, k3, k4'
    qt_logical_op12 'select * from baseall where k0 not in (false,true) order by k1, k2, k3, k4'
}
