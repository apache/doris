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

suite("test_colocate_join") {
    sql """ DROP TABLE IF EXISTS `test_colo1` """
    sql """ DROP TABLE IF EXISTS `test_colo2` """
    sql """ DROP TABLE IF EXISTS `test_colo3` """
    sql """
        CREATE TABLE `test_colo1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "group",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """
        CREATE TABLE `test_colo2` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "group",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_colo3` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "group",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """insert into test_colo1 values('1','a',12);"""
    sql """insert into test_colo2 values('1','a',12);"""
    sql """insert into test_colo3 values('1','a',12);"""

    explain {
        sql("select a.id,a.name,b.id,b.name,c.id,c.name from test_colo1 a inner join test_colo2 b on a.id = b.id and a.name = b.name inner join test_colo3 c on a.id=c.id and a.name= c.name")
        contains "4:VHASH JOIN\n  |  join op: INNER JOIN(COLOCATE[])[]"
        contains "2:VHASH JOIN\n  |  join op: INNER JOIN(COLOCATE[])[]"
    }
}
