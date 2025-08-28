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

suite("rfv2") {
    sql """
    drop table if exists a;
    create table a
    (
        a1 int,
        a2 int,
        a3 varchar
    ) engine=olap
    duplicate key (a1)
    distributed by hash(a1) buckets 3
    properties('replication_num'='1');

    insert into a values(1, 2, 3), (1, 2, 3), (4, 5, 6), (7,8,9),(10, 11, 12);

    drop table if exists b;
    create table b
    (
        b1 int,
        b2 int,
        b3 varchar
    ) engine=olap
    duplicate key (b1)
    distributed by hash(b1) buckets 3
    properties('replication_num'='1');

    insert into b values (1, 2, 3);

    drop table if exists c;
    create table c
    (
        c1 int,
        c2 int,
        c3 varchar
    ) engine=olap
    duplicate key (c1)
    distributed by hash(c1) buckets 3
    properties('replication_num'='1');

    insert into c values (7,8,9);

    set runtime_filter_type=6;
    set enable_parallel_result_sink=false;
    """

    qt_1 """
    explain shape plan
    select * from ((select a1, a2, a3 from a) intersect (select b1, b2, b3 from b) intersect (select c1, c2, c3 from c)) t;
    """
    
    qt_2 """
    explain shape plan
    select * from ((select a1+1 as x, a2, a3 from a) intersect (select b1, b2, b3 from b)) t;
    """

    qt_3 """
    explain shape plan
    select * from (
        (select a1+1 as x, a2, a3 from a) 
        intersect 
        (select b1, b2, b3 from b intersect select c1, c2, c3 from c)
        ) t;
    """

    qt_except """
    explain shape plan
    select * from ((select a1, a2, a3 from a) except (select b1, b2, b3 from b)) t;
    """

}
