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

import org.junit.Assert;

suite("test_recreate_table_mtmv","mtmv") {
    String suiteName = "test_recreate_table_mtmv"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName1 = "${suiteName}_mv1"
    String mvName2 = "${suiteName}_mv2"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""

    sql """
        CREATE TABLE `${tableName1}` (
            `id` int NULL,
            `first_name` varchar(255) NULL,
            `last_name` varchar(255) NULL
        ) ENGINE=OLAP
         AGGREGATE KEY(`id`, `first_name`, `last_name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
         INSERT INTO `${tableName1}` (`id`, `first_name`, `last_name`) VALUES (21, 'mysql', 'test1');
        """
    sql """
        CREATE TABLE `${tableName2}` (
            `id` int NULL,
            `first_name` varchar(255) NULL,
            `last_name` varchar(255) NULL
        ) ENGINE=OLAP
         AGGREGATE KEY(`id`, `first_name`, `last_name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
         INSERT INTO `${tableName2}` (`id`, `first_name`, `last_name`) VALUES (21, 'doris', 'test1');
        """
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS ${mvName1} BUILD IMMEDIATE REFRESH AUTO ON COMMIT DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ) AS (SELECT * FROM ${tableName1} );
    """
    waitingMTMVTaskFinishedByMvName(mvName1);
    order_qt_mv1 "SELECT * FROM ${mvName1}"
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS ${mvName2} BUILD IMMEDIATE REFRESH AUTO ON COMMIT DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ) AS (SELECT first_name,count(last_name) FROM ${mvName1} GROUP BY first_name);
        """
    waitingMTMVTaskFinishedByMvName(mvName2);
    order_qt_mv2 "SELECT * FROM ${mvName2}"
    sql """drop materialized view if exists ${mvName1};"""
    sql """
            CREATE MATERIALIZED VIEW IF NOT EXISTS ${mvName1} BUILD IMMEDIATE REFRESH AUTO ON COMMIT DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            ) AS (SELECT * FROM ${tableName2} );
        """
    waitingMTMVTaskFinishedByMvName(mvName1);
    order_qt_mv1_recreate "SELECT * FROM ${mvName1}"
    waitingMTMVTaskFinishedByMvName(mvName2);
    order_qt_mv2_recreate "SELECT * FROM ${mvName2}"

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
}
