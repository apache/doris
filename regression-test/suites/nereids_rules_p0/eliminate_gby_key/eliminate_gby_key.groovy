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

suite("eliminate_gby_key") {
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """DROP TABLE IF EXISTS t1;"""
    sql """DROP TABLE IF EXISTS t2;"""

    sql """
    CREATE TABLE `t1` (
      `c1` int(20) DEFAULT NULL,
      `c2` int(20) DEFAULT NULL,
      `c3` int(20) DEFAULT NULL
    )
    DUPLICATE KEY (`c1`)
    DISTRIBUTED BY HASH(`c1`) BUCKETS 3 PROPERTIES("replication_num"="1");
    """

    sql """
    CREATE TABLE `t2` (
      `c1` int(20)      DEFAULT NULL,
      `c2` varchar(20)  DEFAULT NULL,
      `c3` int(20)      DEFAULT NULL
    )
    DUPLICATE KEY (`c1`)
    DISTRIBUTED BY HASH(`c1`) BUCKETS 3 PROPERTIES("replication_num"="1");
    """

    sql """
    alter table t2 add constraint t2_c1_pk primary key (c1);
    """

    qt_1 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1               t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c1
        from   temp; 
    """

    qt_2 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1               t2_c1,
                        t1.c3,
                        count(*)            cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c2
        from   temp;
    """

    qt_3 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select c3
        from   temp; 
    """

    qt_4 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select cnt
        from   temp; 
    """

    qt_5 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c2, t2_c1
        from   temp; 
    """

    qt_6 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select c3, t2_c1
        from   temp; 
    """

    qt_7 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select c3, t2_c2
        from   temp; 
    """

    qt_8 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c1, cnt
        from   temp; 
    """

    qt_9 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select c3, cnt
        from   temp; 
    """

    qt_10 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c1, c3, cnt
        from   temp; 
    """

    qt_11 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c2, c3, t2_c1
        from   temp; 
    """

    qt_12 """
        explain physical plan
        with temp
             as (select substr(t2.c2, 1, 3) t2_c2,
                        t2.c1                t2_c1,
                        t1.c3,
                        count(*)             cnt
                 from   t2
                        join t1
                        on t2.c3 = t1.c2
                 group  by substr(t2.c2, 1, 3),
                           t2.c1,
                           t1.c3)
        select t2_c2, c3, t2_c1, cnt
        from   temp; 
    """
}
