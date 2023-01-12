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

suite("test_query_between", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql"use test_query_db"

    def tableName = "test"
    qt_between1 "select if(k1 between 1 and 2, 2, 0) as wj from ${tableName} order by wj"
    qt_between2 "select k1 from ${tableName} where k1 between 3 and 4 order by k1, k2, k3, k4"
    qt_between3 "select k2 from ${tableName} where k2 between 1980 and 1990 order by k1, k2, k3, k4"
    qt_between4 "select k3 from ${tableName} where k3 between 1000 and 2000 order by k1, k2, k3, k4"
    qt_between5 "select k4 from ${tableName} where k4 between -100000000 and 0 order by k1, k2, k3, k4"
    qt_between6 "select k6 from ${tableName} where lower(k6) between 'f' and 'false' order by k1, k2, k3, k4"
    qt_between7 "select k7 from ${tableName} where lower(k7) between 'a' and 'g' order by k1, k2, k3, k4"
    qt_between8 "select k8 from ${tableName} where k8 between -2 and 0 order by k1, k2, k3, k4"
    qt_between9 """select k10 from ${tableName} where k10 between \"2015-04-02 00:00:00\"
                and \"9999-12-31 12:12:12\" order by k1, k2, k3, k4"""
    qt_between10 """select k11 from ${tableName} where k11 between \"2015-04-02 00:00:00\"
                and \"9999-12-31 12:12:12\" order by k1, k2, k3, k4"""
    qt_between11 """select k10 from ${tableName} where k10 between \"2015-04-02\"
                and \"9999-12-31\" order by k1, k2, k3, k4"""
    qt_between12 "select k9 from ${tableName} where k9 between -1 and 6.333 order by k1, k2, k3, k4"
    qt_between13 "select k5 from ${tableName} where k5 between 0 and 1243.5 order by k1, k2, k3, k4"
}
