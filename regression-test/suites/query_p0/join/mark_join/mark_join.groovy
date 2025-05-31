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

suite("mark_join") {
    String suiteName = "mark_join"
    String table_t1 = "${suiteName}_table_t1"
    String table_t2 = "${suiteName}_table_t2"
    String table_tbl1 = "${suiteName}_table_tbl1"
    String table_tbl2 = "${suiteName}_table_tbl2"
    String table_tbl3 = "${suiteName}_table_tbl3"
    
    sql "drop table if exists ${table_t1};"
    sql "drop table if exists ${table_t2};"
    sql """
        create table ${table_t1} (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql """
        create table ${table_t2} (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql "insert into ${table_t1} select 1,1,1,'a';"
    sql "insert into ${table_t1} select 2,2,2,'b';"
    sql "insert into ${table_t1} select 3,-3,null,'c';"
    sql "insert into ${table_t1} select 3,3,null,'c';"

    sql "insert into ${table_t2} select 1,1,1,'a';"
    sql "insert into ${table_t2} select 2,2,2,'b';"
    sql "insert into ${table_t2} select 3,-3,null,'c';"
    sql "insert into ${table_t2} select 3,3,null,'c';"

    qt_test """
    select * from ${table_t1} where exists (select ${table_t2}.k3 from ${table_t2} where ${table_t1}.k2 = ${table_t2}.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from ${table_t1} where not exists (select ${table_t2}.k3 from ${table_t2} where ${table_t1}.k2 = ${table_t2}.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from ${table_t1} where ${table_t1}.k1 not in (select ${table_t2}.k3 from ${table_t2} where ${table_t2}.k2 = ${table_t1}.k2) or k1 < 10 order by k1, k2;
    """
}
