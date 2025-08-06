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

suite("test_view_dependency") {

    sql "DROP DATABASE IF EXISTS test_view_dependency_db"
    sql "CREATE DATABASE test_view_dependency_db"
    sql "USE test_view_dependency_db"

    sql """
        CREATE TABLE `stu` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """
        CREATE TABLE `grade` (
          `sid` int NULL,
          `cid` int NULL,
          `score` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """
        CREATE MATERIALIZED VIEW mv_a
        (sid,sname)
        BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT
        DUPLICATE KEY(`sid`, `sname`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        AS select  `stu`.`sid`,  `stu`.`sname` from  `stu` limit 1 
    """

    sql """
        CREATE MATERIALIZED VIEW mv_b
        (sid,sname)
        BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT
        DUPLICATE KEY(`sid`, `sname`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        AS select  `mv_a`.`sid`,  `mv_a`.`sname` from  `mv_a` 
    """

    sql """
        CREATE MATERIALIZED VIEW mv_c
        (sid,cid,score)
        BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT
        DUPLICATE KEY(`sid`, `cid`, `score`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        AS select  `stu`.`sid`, `grade`.`cid`, `grade`.`score` from  `stu` join  `grade` on  `stu`.`sid` =  `grade`.`sid`
    """

    sql "create view stu_view_cte as with a as (select sid from stu) select * from a"

    sql "create view stu_view as select * from stu"

    sql "create view stu_view_1 as select * from stu_view"

    sql "create view stu_view_2 as select * from mv_a join grade using(sid)"

    sql "create view stu_view_3 as select * from stu join grade using(sid);"

    sql "create view stu_view_4 as select * from stu_view_cte"

    sql "create view stu_view_5 as select * from (select sid from mv_b) a join grade using(sid);"

    qt_sql "select * from information_schema.view_dependency where view_schema = 'test_view_dependency_db' order by view_catalog,view_schema,view_name"

    // support eq
    def explain = sql """explain select * from information_schema.view_dependency where view_schema = 'test_view_dependency_db'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support in
    explain = sql """explain select * from information_schema.view_dependency where view_schema in ('test_view_dependency_db','test_view_dependency_db1')"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support not in
    explain = sql """explain select * from information_schema.view_dependency where view_schema not in ('test_view_dependency_db','test_view_dependency_db1')"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support or
    explain = sql """explain select * from information_schema.view_dependency where view_schema = 'test_view_dependency_db' or VIEW_NAME='stu_view_5'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support >
    explain = sql """explain select * from information_schema.view_dependency where view_schema > 'test_view_dependency_db'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support >=
    explain = sql """explain select * from information_schema.view_dependency where view_schema >= 'test_view_dependency_db'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support <
    explain = sql """explain select * from information_schema.view_dependency where view_schema < 'test_view_dependency_db'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // support <=
    explain = sql """explain select * from information_schema.view_dependency where view_schema <= 'test_view_dependency_db'"""
    assertTrue(explain.toString().contains("FRONTEND PREDICATES"))
    // not support like
    explain = sql """explain select * from information_schema.view_dependency where view_schema like '%test_view_dependency_db%'"""
    assertFalse(explain.toString().contains("FRONTEND PREDICATES"))
}

