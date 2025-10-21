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

suite("test_multi_pct_union_list_mtmv","mtmv") {
    String suiteName = "test_multi_pct_union_list_mtmv"
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
            k1 INT not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ("1"),
            PARTITION `p2` VALUES IN ("2")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName1} values(1,1);
        insert into ${tableName1} values(2,2);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 INT not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p2` VALUES IN ("2"),
            PARTITION `p3` VALUES IN ("3")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName2} values(2,3);
        insert into ${tableName2} values(3,4);
        """
    String mvSql = "SELECT * from ${tableName1} union all SELECT * from ${tableName2};";
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """

    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_1"))
    assertTrue(showPartitionsResult.toString().contains("p_2"))
    assertTrue(showPartitionsResult.toString().contains("p_3"))

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_1 "SELECT * FROM ${mvName}"

    sql """
        insert into ${tableName1} values(1,5);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mode_t1 "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_2 "SELECT * FROM ${mvName}"

    sql """
        insert into ${tableName2} values(2,6);
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mode_t2 "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_3 "SELECT * FROM ${mvName}"
}
