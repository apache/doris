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

suite("column_prune") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """ DROP TABLE IF EXISTS `test_prune1` """
    sql """ DROP TABLE IF EXISTS `test_prune2` """
    sql """ DROP TABLE IF EXISTS `test_prune3` """
    sql """
        CREATE TABLE `test_prune1` (
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
        CREATE TABLE `test_prune2` (
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
        CREATE TABLE `test_prune3` (
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

    sql """insert into test_prune1 values('1','a',12);"""
    sql """insert into test_prune2 values('1','a',12);"""
    sql """insert into test_prune3 values('1','a',12);"""

    qt_select """select t3.id from test_prune1 t1 inner join test_prune2 t2 on true inner join test_prune3 t3 on t3.id = t2.id;"""

    explain {
        sql("select count(*) from test_prune1 where id = '1';")
        notContains "age"
    }

    sql """ DROP TABLE IF EXISTS `test_prune1` """
    sql """ DROP TABLE IF EXISTS `test_prune2` """
    sql """ DROP TABLE IF EXISTS `test_prune3` """
}
