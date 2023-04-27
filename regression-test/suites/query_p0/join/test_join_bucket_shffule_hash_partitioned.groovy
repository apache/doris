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

suite("test_join_bucket_shffule_hash_partitioned", "query,p0") {
    sql """ drop table if exists t1; """
    sql """ create table t1 (
        k1 int not null,
        kdate datetime not null,
        k2 int not null,
        v1 int not null
    ) DISTRIBUTED BY HASH (k1, kdate) buckets 4
    PROPERTIES (
        "replication_num" = "1"
    ); """

    sql """ insert into t1 values (1,"2023-04-25 00:00:00", 1, 1),
                          (2,"2023-04-26 00:00:00", 2, 2),
                          (3,"2023-04-27 00:00:00", 3, 3);
    """

    sql """ drop table if exists t2; """
    sql """ create table t2 (
        k1 int not null,
        k2 int not null,
        kdate datetime not null,
        v1 int not null
    ) DISTRIBUTED BY HASH (k1, k2, kdate) buckets 4
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """ insert into t2 values (1,1,"2023-04-25 00:00:00", 1),
                          (2,2,"2023-04-26 00:00:00", 2);
    """

    qt_sql_join1 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/
        t1.k1,
        t1.k2,
        t1.kdate,
        t2.k1,
        t2.k2,
        t2.kdate
    from
        t1
        left join t2 on t1.k1 = t2.k1
        and t1.k2 = t2.k2
        and t1.kdate = t2.kdate
    order by
        t1.k1,
        t1.k2,
        t1.kdate,
        t2.k1,
        t2.k2,
        t2.kdate;
    """

     qt_sql_join1 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=4)*/
        t1.k1,
        t1.k2,
        t1.kdate,
        t2.k1,
        t2.k2,
        t2.kdate
    from
        t1
        left join t2 on t1.k1 = t2.k1
        and t1.k2 = t2.k2
        and t1.kdate = t2.kdate
    order by
        t1.k1,
        t1.k2,
        t1.kdate,
        t2.k1,
        t2.k2,
        t2.kdate;
    """
}
