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

suite("test_replace_mtmv","mtmv") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_replace_mtmv"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName1 = "${suiteName}_mv1"
    String mvName2 = "${suiteName}_mv2"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
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
        SELECT * from ${tableName2};
        """
    sql """
        insert into ${tableName1} values(1,1);
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName1} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName1)

    order_qt_mv1 "SELECT * FROM ${mvName1}"


    sql """
        insert into ${tableName2} values(2,2);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName2)

    order_qt_mv2 "SELECT * FROM ${mvName2}"

    test {
          sql """
              alter MATERIALIZED VIEW ${mvName1} replace with  MATERIALIZED VIEW ${tableName1};
          """
          exception "MATERIALIZED_VIEW"
    }

    test {
          sql """
              alter MATERIALIZED VIEW ${tableName1} replace with  MATERIALIZED VIEW ${mvName1};
          """
          exception "MATERIALIZED_VIEW"
    }

    sql """
        alter MATERIALIZED VIEW ${mvName1} replace with  MATERIALIZED VIEW ${mvName2};
        """
    order_qt_mv1_replace "SELECT * FROM ${mvName1}"
    order_qt_mv2_replace "SELECT * FROM ${mvName2}"

    sql """
        insert into ${tableName1} values(3,3);
        """
    sql """
        insert into ${tableName2} values(4,4);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName1} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName1)

    order_qt_mv1_refresh "SELECT * FROM ${mvName1}"

    sql """
        REFRESH MATERIALIZED VIEW ${mvName2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName2)

    order_qt_mv2_refresh "SELECT * FROM ${mvName2}"


    sql """
        alter MATERIALIZED VIEW ${mvName1} replace with  MATERIALIZED VIEW ${mvName2} PROPERTIES('swap' = 'false');
        """

    def mvResult = sql """select Name from mv_infos("database"="${dbName}");"""
    logger.info("mvResult: " + mvResult.toString())
    assertFalse(mvResult.toString().contains("${mvName2}"))

    def jobResult = sql """select MvName from jobs("type"="mv");"""
    logger.info("jobResult: " + jobResult.toString())
    assertFalse(jobResult.toString().contains("${mvName2}"))

    order_qt_mv1_replace_not_swap "SELECT * FROM ${mvName1}"

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
}
