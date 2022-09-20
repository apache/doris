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

suite("test_left_join", "query,p0") {
    def DBname = "test_left_join"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    def tbName1 = "wftest1"
    def tbName2 = "wftest2"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"

    sql """CREATE TABLE ${tbName1} (`aa` varchar(200) NULL COMMENT "",`bb` int NULL COMMENT "") ENGINE = OLAP UNIQUE KEY (`aa`) COMMENT  "aa" DISTRIBUTED BY HASH (`aa`) BUCKETS 3 PROPERTIES ("replication_num" = "1");"""
    sql """CREATE TABLE ${tbName2} (`cc` varchar(200) NULL COMMENT "",`dd` int NULL COMMENT "") ENGINE = OLAP UNIQUE KEY (`cc`) COMMENT  "cc" DISTRIBUTED BY HASH (`cc`) BUCKETS 3 PROPERTIES ("replication_num" = "1");"""

    sql "INSERT INTO  ${tbName1} VALUES ('a', 1),('b', 1),('c', 1);"
    sql "INSERT INTO  ${tbName2} VALUES ('a', 1),('b', 1),('d', 1);"


    qt_join1 "select  * from ${tbName1} t1  left join ${tbName2} t2 on t1.aa=t2.cc order by t1.aa;"
    qt_join2 "select  t.* from ( select  * from ${tbName1} t1  left join ${tbName2} t2 on t1.aa=t2.cc  ) t  where   dayofweek(current_date())=2;"


    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"
    sql "DROP DATABASE IF EXISTS ${DBname};"
}
