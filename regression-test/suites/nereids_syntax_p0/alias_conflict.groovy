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

suite("alias_conflict") {

    sql """ DROP TABLE IF EXISTS `test_alias_conflict1` """
    sql """ DROP TABLE IF EXISTS `test_alias_conflict2` """
    sql """ DROP TABLE IF EXISTS `test_alias_conflict3` """
    sql """ DROP DATABASE IF EXISTS `alias_conflict1` """
    sql """ DROP DATABASE IF EXISTS `alias_conflict2` """
    sql """ DROP CATALOG IF EXISTS `jdbc_alias_conflict` """


    sql """ CREATE DATABASE IF NOT EXISTS `alias_conflict1` """
    sql """ CREATE DATABASE IF NOT EXISTS `alias_conflict2` """

    sql """
        CREATE TABLE `test_alias_conflict1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_alias_conflict2` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_alias_conflict3` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 6
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `alias_conflict1`.`test_alias_conflict1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `alias_conflict1`.`test_alias_conflict2` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `alias_conflict1`.`test_alias_conflict8` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `alias_conflict2`.`test_alias_conflict1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """insert into test_alias_conflict1 values('1','a',12);"""
    sql """insert into test_alias_conflict2 values('1','a',12);"""
    sql """insert into test_alias_conflict3 values('1','a',12);"""
    sql """insert into alias_conflict1.test_alias_conflict1 values('1','a',12);"""
    sql """insert into alias_conflict2.test_alias_conflict1 values('1','a',12);"""

    //create view
    sql """create view alias_conflict2.test_alias_conflict8 as select * from alias_conflict1.test_alias_conflict8;"""

    //create catalog
    sql """
        CREATE CATALOG jdbc_alias_conflict properties(
       'type'='jdbc',
       'user'='${context.config.jdbcUser}',
       'password'='${context.config.jdbcPassword}',
       'jdbc_url' = '${context.config.jdbcUrl}',
       'driver_url' = 'https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar',
       'driver_class' = 'com.mysql.cj.jdbc.Driver'
        );
    """


    // Valid query
    qt_select_normal """select t3.id from test_alias_conflict1 t1 inner join test_alias_conflict2 t2 on true inner join test_alias_conflict3 t3 on t3.id = t2.id;"""

    qt_catalog_normal """select * from internal.alias_conflict1.test_alias_conflict2, jdbc_alias_conflict.alias_conflict2.test_alias_conflict1;"""

    qt_view_normal """select * from alias_conflict2.test_alias_conflict8, alias_conflict1.test_alias_conflict8"""

    // Test for alias conflict
    test {
        sql "select * from test_alias_conflict1 t, test_alias_conflict1 t;"
        exception "Not unique table/alias: 't'"
    }

    // Test for table name conflict
    test {
        sql "select * from test_alias_conflict1 t1, test_alias_conflict2 t1;"
        exception "Not unique table/alias: 't1'"
    }



    // Test for view name conflict
    test {
        sql "select * from alias_conflict2.test_alias_conflict8 t1, alias_conflict2.test_alias_conflict1 t1;"
        exception "Not unique table/alias: 't1'"
    }

    // Test for view name conflict
    test {
        sql "select * from (select * from alias_conflict2.test_alias_conflict8) t1, (select 100 id) t1;"
        exception "Not unique table/alias: 't1'"
    }

    // Test for more table conflicts
    test {
        sql "select * from test_alias_conflict1, test_alias_conflict1 b, test_alias_conflict1 c, test_alias_conflict1"
        exception "Not unique table/alias: 'test_alias_conflict1'"
    }

    test {
        sql """select * from test_alias_conflict1
            join test_alias_conflict1 b on test_alias_conflict1.id = b.id
            join test_alias_conflict1 c on b.id = c.id
            join test_alias_conflict1 on true"""
        exception "Not unique table/alias: 'test_alias_conflict1'"
    }

    // Complex query with alias conflict
    test {
        sql "select * from (select * from test_alias_conflict1) a, (select * from test_alias_conflict1) a;"
        exception "Not unique table/alias: 'a'"
    }

    // Complex query with alias conflict
    test {
        sql "select 1 from (select 1 from test_alias_conflict1 where 1 = 1 group by 1 order by 1 limit 1 ) a, (select 1 from test_alias_conflict1 where 1 = 1 group by 1 order by 1 limit 1 ) a;"
        exception "Not unique table/alias: 'a'"
    }

    // Test for no conflict
    qt_select_no_conflict """select * from test_alias_conflict1 t1, test_alias_conflict2 t2 where t1.id = t2.id;"""

    qt_select_no_conflict "select * from alias_conflict1.test_alias_conflict1, alias_conflict2.test_alias_conflict1;"


    // Test case where alias are different
    qt_select_diff_alias """select * from test_alias_conflict2 a, test_alias_conflict2 b;"""

    // Test case where aliases conflict within subqueries should not raise error
    qt_select_nested_no_conflict """select * from 
    (
      select * from test_alias_conflict1 a
    ) b
    join
    (
      select * from test_alias_conflict1 a
    ) c
    on b.id = c.id;"""



    // Test case for cross database table names with no conflict
    qt_select_cross_db_no_conflict """select * from alias_conflict1.test_alias_conflict1 a, alias_conflict2.test_alias_conflict1 b where a.id = b.id;"""


    qt_child_query_no_conflict1 """select * from (select 1) as no_conflict1_tbl, alias_conflict1.test_alias_conflict1 as no_conflict1_tbl;"""


    qt_child_query_no_conflict2 """select * from (select * from alias_conflict1.test_alias_conflict1) as no_conflict1_tbl2, alias_conflict1.test_alias_conflict1 as no_conflict1_tbl2"""

    sql """ DROP TABLE IF EXISTS `test_alias_conflict1` """
    sql """ DROP TABLE IF EXISTS `test_alias_conflict2` """
    sql """ DROP TABLE IF EXISTS `test_alias_conflict3` """
    sql """ DROP TABLE IF EXISTS `alias_conflict1`.`test_alias_conflict1` """
    sql """ DROP TABLE IF EXISTS `alias_conflict2`.`test_alias_conflict1` """
    sql """ DROP DATABASE IF EXISTS `alias_conflict1` """
    sql """ DROP DATABASE IF EXISTS `alias_conflict2` """
    sql """ DROP CATALOG IF EXISTS `jdbc_alias_conflict` """
}

