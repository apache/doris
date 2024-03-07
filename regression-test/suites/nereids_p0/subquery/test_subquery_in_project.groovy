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

suite("test_subquery_in_project") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """drop table if exists test_sql;"""
    sql """
        CREATE TABLE `test_sql` (
        `user_id` varchar(10) NULL,
        `dt` date NULL,
        `city` varchar(20) NULL,
        `age` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """ insert into test_sql values (1,'2020-09-09',2,3);"""

    qt_sql1 """
        select (select age from test_sql) col from test_sql order by col; 
    """

    qt_sql2 """
        select (select sum(age) from test_sql) col from test_sql order by col;
    """

    qt_sql3 """
        select (select sum(age) from test_sql t2 where t2.dt = t1.dt ) col from test_sql t1 order by col;
    """

    qt_sql4 """
        select age in (select user_id from test_sql) col from test_sql order by col;
    """

    qt_sql5 """
        select age in (select user_id from test_sql t2 where t2.user_id = t1.age) col from test_sql t1 order by col;
    """

    qt_sql6 """
        select exists ( select user_id from test_sql ) col from test_sql order by col;
    """

    qt_sql7 """
        select case when age in (select user_id from test_sql) or age in (select user_id from test_sql t2 where t2.user_id = t1.age) or exists ( select user_id from test_sql ) or exists ( select t2.user_id from test_sql t2 where t2.age = t1.user_id) or age < (select sum(age) from test_sql t2 where t2.dt = t1.dt ) then 2 else 1 end col from test_sql t1 order by col;
    """

    sql """ insert into test_sql values (2,'2020-09-09',2,1);"""

    try { 
        sql """
                select (select age from test_sql) col from test_sql order by col; 
            """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Expected EQ 1 to be returned by expression"))
    }

    qt_sql8 """
        select (select sum(age) from test_sql) col from test_sql order by col;
    """

    qt_sql9 """
        select (select sum(age) from test_sql t2 where t2.dt = t1.dt ) col from test_sql t1 order by col;
    """

    qt_sql10 """
        select age in (select user_id from test_sql) col from test_sql order by col;
    """

    qt_sql11 """
        select age in (select user_id from test_sql t2 where t2.user_id = t1.age) col from test_sql t1 order by col;
    """

    qt_sql12 """
        select exists ( select user_id from test_sql ) col from test_sql order by col;
    """

    qt_sql13 """
        select case when age in (select user_id from test_sql) or age in (select user_id from test_sql t2 where t2.user_id = t1.age) or exists ( select user_id from test_sql ) or exists ( select t2.user_id from test_sql t2 where t2.age = t1.user_id) or age < (select sum(age) from test_sql t2 where t2.dt = t1.dt ) then 2 else 1 end col from test_sql t1 order by col;
    """

    qt_sql14 """
                select dt,case when 'med'='med' then ( 
                select sum(midean) from (
                        select sum(score) / count(*) as midean
                            from (
                                select age score,row_number() over (order by age desc) as desc_math,
                                row_number() over (order by age asc)   as asc_math from test_sql
                                ) as order_table
                        where asc_math in (desc_math, desc_math + 1, desc_math - 1)) m
                )
                end 'test'  from test_sql group by cube(dt) order by dt;
    """

    qt_sql15 """
        select sum(age + (select sum(age) from test_sql)) from test_sql order by 1;
    """

    qt_sql16 """
        select sum(distinct age + (select sum(age) from test_sql)) from test_sql order by 1;
    """

    qt_sql17 """
        select sum(age + (select sum(age) from test_sql)) over() from test_sql order by 1;
    """

    qt_sql18 """
        select sum(age + (select sum(age) from test_sql)) over() from test_sql group by dt, age order by 1;
    """

    qt_sql20 """
        select sum(age + (select sum(age) from test_sql)) from test_sql group by dt, age order by 1;
    """

    sql """drop table if exists test_sql;"""

    sql """drop table if exists markjoin_t1;"""
    sql """drop table if exists markjoin_t2;"""
    sql """drop table if exists markjoin_t3;"""

    sql """create table markjoin_t1
                    (a bigint, b bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(a, b)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""
    sql """create table markjoin_t2
                    (a int, b varchar(128), c bigint, v1 bigint, v2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(a, b)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""

    sql """insert into markjoin_t1 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);"""
    sql """insert into markjoin_t2 values (1,'abc',2,3,4), (1,'abcd',3,3,4), (2,'xyz',2,4,2), (2,'uvw',3,4,2), (2,'uvw',3,4,2), (3,'abc',4,5,3), (3,'abc',4,5,3), (null,null,null,null,null);"""
    
    qt_select_m1 """SELECT markjoin_t1.b IN
                    (SELECT markjoin_t2.c FROM markjoin_t2 WHERE markjoin_t1.a = markjoin_t2.a) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m2 """SELECT EXISTS 
                    (SELECT markjoin_t2.c FROM markjoin_t2 WHERE markjoin_t1.a = markjoin_t2.a) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m3 """SELECT  markjoin_t1.b NOT IN
                    (SELECT  markjoin_t2.c FROM markjoin_t2 WHERE markjoin_t1.a = markjoin_t2.a) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m4 """SELECT NOT EXISTS
                    (SELECT  markjoin_t2.c FROM markjoin_t2 WHERE markjoin_t1.a = markjoin_t2.a) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m5 """SELECT  markjoin_t1.b IN 
                    (SELECT  markjoin_t2.c FROM markjoin_t2) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m6 """SELECT EXISTS 
                    (SELECT markjoin_t2.c FROM markjoin_t2) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m7 """SELECT markjoin_t1.b NOT IN 
                    (SELECT markjoin_t2.c FROM markjoin_t2) 
                    FROM markjoin_t1 order by 1;"""
    qt_select_m8 """SELECT NOT EXISTS 
                    (SELECT markjoin_t2.c FROM markjoin_t2) 
                    FROM markjoin_t1 order by 1;"""
    sql """drop table if exists markjoin_t1;"""
    sql """drop table if exists markjoin_t2;"""
    sql """drop table if exists markjoin_t3;"""

}
