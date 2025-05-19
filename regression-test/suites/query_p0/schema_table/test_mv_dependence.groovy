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

suite("test_mv_dependence") {

    sql "DROP DATABASE IF EXISTS test_mv_dependence_db"
    sql "CREATE DATABASE test_mv_dependence_db"
    sql "USE test_mv_dependence_db"

    sql "DROP TABLE IF EXISTS stu"
    sql "DROP TABLE IF EXISTS grade"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_a"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_b"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_c"
    sql "DROP TABLE IF EXISTS stu"
    sql "DROP TABLE IF EXISTS grade"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_a"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_b"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_c"

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

    qt_sql "select * from information_schema.mv_dependence where src_database = 'test_mv_dependence_db' order by src_catalog,src_database,src_table"
}

