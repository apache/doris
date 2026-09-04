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

suite("test_truncate_table_mtmv","mtmv") {
    String suiteName = "test_truncate_table_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists test_truncate_table_mtmv_dim"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        COMMENT "my first table"
        PARTITION BY LIST(`k3`)
        (
            PARTITION `p1` VALUES IN ('1'),
            PARTITION `p2` VALUES IN ('2'),
            PARTITION `p3` VALUES IN ('3')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE TABLE test_truncate_table_mtmv_dim (
            k2 TINYINT NOT NULL
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true'
        )
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`k3`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT fact.k2, fact.k3
        FROM ${tableName} fact
        INNER JOIN test_truncate_table_mtmv_dim dim ON fact.k2 = dim.k2;
        """

    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """
    sql """insert into test_truncate_table_mtmv_dim values(1),(2),(3)"""
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)

    // truncate partition
    order_qt_init "SELECT * FROM ${mvName}"
    sql """
    truncate table ${tableName} partition(p1);
    """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_truncate_partition "SELECT * FROM ${mvName}"

    // Save a non-PCT table snapshot at version 3 while the MV does not contain k2=2.
    sql """delete from test_truncate_table_mtmv_dim where k2=2"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)

    // Without monotonic table versions, reset to 1 plus two inserts collides with version 3.
    sql """truncate table test_truncate_table_mtmv_dim"""
    sql """insert into test_truncate_table_mtmv_dim values(1),(2)"""
    sql """insert into test_truncate_table_mtmv_dim values(3)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_non_pct_truncate "SELECT * FROM ${mvName}"

    // truncate table
    sql """
        truncate table ${tableName};
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_truncate_table "SELECT * FROM ${mvName}"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
