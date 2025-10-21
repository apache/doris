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

suite("test_create_mtmv_with_view","mtmv") {
    String suiteName = "test_create_mtmv_with_view"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String viewName = "${suiteName}_view"
    String dbName = context.config.getDbNameByFile(context.file)
    sql """drop table if exists `${tableName}`"""
    sql """drop view if exists `${viewName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql"""
        create view ${viewName} as select * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${viewName};
        """

    sql """
        insert into ${tableName} values(1,1);
        """
    order_qt_is_sync_init "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_is_sync_after_refresh "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    order_qt_after_refresh "SELECT * FROM ${mvName}"

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_not_need_refresh "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1;"

    sql """
        insert into ${tableName} values(2,2);
        """
    order_qt_is_sync_data_change "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    // test excluded_trigger_tables
    // view should not useful
     sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="internal.${dbName}.${viewName}");
        """

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
            """
        waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_trigger_view_need_refresh "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1;"
    order_qt_after_trigger_view "SELECT * FROM ${mvName}"

    // table should useful
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="internal.${dbName}.${tableName}");
        """
     sql """
        insert into ${tableName} values(3,3);
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
            """
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_trigger_table_not_need_refresh "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1;"
    order_qt_after_trigger_table "SELECT * FROM ${mvName}"


    sql """drop view if exists `${viewName}`"""
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
