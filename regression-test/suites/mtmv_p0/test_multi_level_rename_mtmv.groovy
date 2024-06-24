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

suite("test_multi_level_rename_mtmv","mtmv") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_multi_level_rename_mtmv"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName1 = "${suiteName}_mv1"
    String mvName1_rename = "${suiteName}_mv1_rename"
    String mvName2 = "${suiteName}_mv2"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName1_rename};"""
    sql """drop materialized view if exists ${mvName2};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 TINYINT,
            k2 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE TABLE ${tableName2}
        (
            k3 TINYINT,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k4) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName1}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName1};
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName2}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${mvName1};
        """

    sql """
        alter MATERIALIZED VIEW ${mvName1} rename ${mvName1_rename};
        """

    order_qt_status "select Name,State  from mv_infos('database'='${dbName}') where Name='${mvName2}'"

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName1_rename};"""
    sql """drop materialized view if exists ${mvName2};"""
}
