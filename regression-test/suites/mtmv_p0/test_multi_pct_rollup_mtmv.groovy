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

suite("test_multi_pct_rollup_mtmv","mtmv") {
    String suiteName = "test_multi_pct_rollup_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES [("2017-01-01"),  ("2017-02-01")),
            PARTITION `p203801` VALUES [("2038-01-01"), ("2038-02-01")),
            PARTITION `p203802` VALUES [("2038-02-01"), ("2038-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName1} values("2017-01-01",1);
        insert into ${tableName1} values("2038-01-01",2);
        insert into ${tableName1} values("2038-02-01",3);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201601` VALUES [("2016-01-01"),  ("2016-02-01")),
            PARTITION `p203802` VALUES [("2038-03-01"), ("2038-04-01")),
            PARTITION `p203803` VALUES [("2037-01-01"), ("2037-02-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName2} values("2016-01-01",4);
        insert into ${tableName2} values("2038-03-01",5);
        insert into ${tableName2} values("2037-01-01",6);
        """

    String mvSql = "SELECT * from ${tableName1} union all SELECT * from ${tableName2};";
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(date_trunc(`k1`, 'year'))
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'partition_sync_limit'='2',
        'partition_sync_time_unit'='YEAR',
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """

    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_20370101_20380101"))
    assertTrue(showPartitionsResult.toString().contains("p_20380101_20390101"))

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_1 "SELECT * FROM ${mvName}"

    sql """
        insert into ${tableName1} values("2038-01-01",7);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mode_t1 "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_2 "SELECT * FROM ${mvName}"

    sql """
        insert into ${tableName2} values("2037-01-01",8);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mode_t2 "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_3 "SELECT * FROM ${mvName}"
}
