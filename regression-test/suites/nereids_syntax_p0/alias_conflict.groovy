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

    sql """ DROP TABLE IF EXISTS `test1` """
    sql """ DROP TABLE IF EXISTS `test2` """
    sql """ DROP TABLE IF EXISTS `test3` """

    sql """
        CREATE TABLE `test1` (
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
        CREATE TABLE `test2` (
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
        CREATE TABLE `test3` (
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

    sql """insert into test1 values('1','a',12);"""
    sql """insert into test2 values('1','a',12);"""
    sql """insert into test3 values('1','a',12);"""

    // Valid query
    qt_select_normal """select t3.id from test1 t1 inner join test2 t2 on true inner join test3 t3 on t3.id = t2.id;"""

    // Test for alias conflict
    test {
        sql "select * from test1 t, test1 t;"
        exception "Not unique table/alias: 't'"
    }

    // Test for table name conflict
    test {
        sql "select * from test1 t1, test2 t1;"
        exception "Not unique table/alias: 't1'"
    }

    // Test for no conflict
    qt_select_no_conflict """select * from test1 t1, test2 t2 where t1.id = t2.id;"""

    // Complex query with alias conflict
    test {
        sql "select * from (select * from test1) a, (select * from test1) a;"
        exception "Not unique table/alias: 'a'"
    }


    sql """ DROP TABLE IF EXISTS `test1` """
    sql """ DROP TABLE IF EXISTS `test2` """
    sql """ DROP TABLE IF EXISTS `test3` """
}
