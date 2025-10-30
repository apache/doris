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

suite("test_multi_pct_nested_mtmv","mtmv") {
    String suiteName = "test_multi_pct_nested_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String tableName3 = "${suiteName}_table3"
    String mvName1 = "${suiteName}_mv1"
    String mvName2 = "${suiteName}_mv2"
    String mvName3 = "${suiteName}_mv3"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
    sql """drop materialized view if exists ${mvName3};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES [("2017-01-01"),  ("2017-02-01")),
            PARTITION `p201702` VALUES [("2017-02-01"), ("2017-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName1} values("2017-01-01",1);
        insert into ${tableName1} values("2017-02-01",2);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201702` VALUES [("2017-02-01"),  ("2017-03-01")),
            PARTITION `p201703` VALUES [("2017-03-01"), ("2017-04-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName2} values("2017-02-01",3);
        insert into ${tableName2} values("2017-03-01",4);
        """

    sql """
        CREATE TABLE ${tableName3}
        (
            k3 INT not null,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName3} values(1,1);
        insert into ${tableName3} values(2,2);
        insert into ${tableName3} values(3,3);
        insert into ${tableName3} values(4,4);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName1}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        select * from ${tableName1};
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName1} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName1)

    sql """
        CREATE MATERIALIZED VIEW ${mvName2}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        select * from ${tableName2};
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName2)

    String mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${mvName1} t1 inner join ${mvName2} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;";
    sql """
        CREATE MATERIALIZED VIEW ${mvName3}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """

    def showPartitionsResult = sql """show partitions from ${mvName3}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_20170101_20170201"))
    assertTrue(showPartitionsResult.toString().contains("p_20170201_20170301"))
    assertTrue(showPartitionsResult.toString().contains("p_20170301_20170401"))

     sql """
        REFRESH MATERIALIZED VIEW ${mvName3} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName3)

    order_qt_1 "SELECT * FROM ${mvName3}"

    sql """
        insert into ${tableName1} values("2017-01-01",5);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName1} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName1)

    sql """
        REFRESH MATERIALIZED VIEW ${mvName3} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName3)

    order_qt_refresh_mode_t1 "select RefreshMode from tasks('type'='mv') where MvName='${mvName3}' order by CreateTime desc limit 1"
    order_qt_2 "SELECT * FROM ${mvName3}"

    sql """
        insert into ${tableName2} values("2017-02-01",6);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName2)

    sql """
        REFRESH MATERIALIZED VIEW ${mvName3} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName3)


    order_qt_refresh_mode_t2 "select RefreshMode from tasks('type'='mv') where MvName='${mvName3}' order by CreateTime desc limit 1"
    order_qt_3 "SELECT * FROM ${mvName3}"

    sql """
        insert into ${tableName3} values(5,5);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName3} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName3)
    order_qt_refresh_mode_t3 "select RefreshMode from tasks('type'='mv') where MvName='${mvName3}' order by CreateTime desc limit 1"
    order_qt_4 "SELECT * FROM ${mvName3}"

    mv_rewrite_success_without_check_chosen(mvSql, mvName3)
}
