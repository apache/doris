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
    sql "drop table if exists t1;"
    sql "drop table if exists t2;"
    sql """
        create table t1 (
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
        create table t2 (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql "insert into t1 select 1,1,1,'a';"
    sql "insert into t1 select 2,2,2,'b';"
    sql "insert into t1 select 3,-3,null,'c';"
    sql "insert into t1 select 3,3,null,'c';"

    sql "insert into t2 select 1,1,1,'a';"
    sql "insert into t2 select 2,2,2,'b';"
    sql "insert into t2 select 3,-3,null,'c';"
    sql "insert into t2 select 3,3,null,'c';"

    qt_test """
    select * from t1 where exists (select t2.k3 from t2 where t1.k2 = t2.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from t1 where not exists (select t2.k3 from t2 where t1.k2 = t2.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from t1 where t1.k1 not in (select t2.k3 from t2 where t2.k2 = t1.k2) or k1 < 10 order by k1, k2;
    """
}
