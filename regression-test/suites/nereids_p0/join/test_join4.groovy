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

suite("test_join4", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def DBname = "regression_test_join4"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    def tbName1 = "x"
    def tbName2 = "y"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"

    sql """create table if not exists ${tbName1} (x1 int, x2 int) DISTRIBUTED BY HASH(x1) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName2} (y1 int, y2 int) DISTRIBUTED BY HASH(y1) properties("replication_num" = "1");"""

    sql "insert into ${tbName1} values (1,11);"
    sql "insert into ${tbName1} values (2,22);"
    sql "insert into ${tbName1} values (3,null);"
    sql "insert into ${tbName1} values (4,44);"
    sql "insert into ${tbName1} values (5,null);"

    sql "insert into ${tbName2} values (1,111);"
    sql "insert into ${tbName2} values (2,222);"
    sql "insert into ${tbName2} values (3,333);"
    sql "insert into ${tbName2} values (4,null);"

    qt_join1 "select * from ${tbName1} left join ${tbName2} on (x1 = y1 and x2 is not null) order by 1,2,3,4;"
    qt_join2 "select * from ${tbName1} left join ${tbName2} on (x1 = y1 and y2 is not null) order by 1,2,3,4;"

    qt_join3 "select * from (select * from ${tbName1} left join y on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1) order by 1,2,3,4,5,6;"
    qt_join4 "select * from (select * from ${tbName1} left join y on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1 and a.x2 is not null) order by 1,2,3,4,5,6;"
    qt_join5 "select * from (select * from ${tbName1} left join y on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1 and a.y2 is not null) order by 1,2,3,4,5,6;"
    qt_join6 "select * from (select * from ${tbName1} left join y on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1 and a.x2 is not null) order by 1,2,3,4,5,6;"

    qt_join7 "select * from (select * from ${tbName1} left join ${tbName2} on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1) where (a.x2 is not null) order by 1,2,3,4,5,6;"
    qt_join8 "select * from (select * from ${tbName1} left join ${tbName2} on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1) where (a.y2 is not null) order by 1,2,3,4,5,6;"
    qt_join9 "select * from (select * from ${tbName1} left join ${tbName2} on (x1 = y1)) a left join ${tbName1} x on (a.x1 = x.x1) where (a.x2 is not null) order by 1,2,3,4,5,6;"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"
    sql "DROP DATABASE IF EXISTS ${DBname};"
}
