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

suite("test_create_mtmv_with_view_pct","mtmv") {
    String suiteName = "test_create_mtmv_with_view_pct"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String viewName = "${suiteName}_view"
    String dbName = context.config.getDbNameByFile(context.file)
    sql """drop table if exists `${tableName}`"""
    sql """drop view if exists `${viewName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703 VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

     sql """
        insert into ${tableName} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql"""
        create view ${viewName} as select * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`date`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${viewName};
        """

    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_00000101_20170201"))
    assertTrue(showPartitionsResult.toString().contains("p_20170201_20170301"))
    assertTrue(showPartitionsResult.toString().contains("p_20170301_20170401"))

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
        insert into ${tableName} values(1,"2017-01-15",4);
        """
    order_qt_is_sync_data_change "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_pct_refresh "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1;"

    mv_rewrite_success_without_check_chosen("select * from ${viewName}", "${mvName}")
    mv_rewrite_success_without_check_chosen("select * from ${tableName}", "${mvName}")

    sql """drop view if exists `${viewName}`"""
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
