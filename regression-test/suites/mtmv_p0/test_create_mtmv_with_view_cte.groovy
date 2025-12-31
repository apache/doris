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

suite("test_create_mtmv_with_view_cte","mtmv") {
    String suiteName = "test_create_mtmv_with_view_cte"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName = "${suiteName}_mv"
    String viewName = "${suiteName}_view"
    String dbName = context.config.getDbNameByFile(context.file)
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop view if exists `${viewName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT,
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
            k3 INT,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k4) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql"""
        create view ${viewName} as select * from ${tableName1};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON COMMIT
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        with cte1 as (
            select * from ${tableName2}
        )
        SELECT * from ${viewName} a inner join cte1 b on a.k2=b.k3;
        """

    order_qt_is_sync_init "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    sql """
        insert into ${tableName1} values(1,2);
        """
    sql """
        insert into ${tableName2} values(2,3);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_after_refresh "SELECT * FROM ${mvName}"

    sql """drop view if exists `${viewName}`"""
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName};"""
}
