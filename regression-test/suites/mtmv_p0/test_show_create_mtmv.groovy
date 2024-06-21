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

suite("test_show_create_mtmv","mtmv") {
    String suiteName = "test_show_create_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` DATE NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY range(`k2`)
        (
        PARTITION p_20200101 VALUES [("2020-01-01"),("2020-01-02")),
        PARTITION p_20200102 VALUES [("2020-01-02"),("2020-01-03")),
        PARTITION p_20200201 VALUES [("2020-02-01"),("2020-02-02"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by (date_trunc(`k2`,'month'))
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        "grace_period"="333"
        )
        AS
        SELECT * FROM ${tableName};
    """

    def showCreateMTMVResult = sql """show CREATE MATERIALIZED VIEW ${mvName}"""
    logger.info("showCreateMTMVResult: " + showCreateMTMVResult.toString())
    assertTrue(showCreateMTMVResult.toString().contains("CREATE MATERIALIZED VIEW"))
    assertTrue(showCreateMTMVResult.toString().contains("BUILD DEFERRED REFRESH AUTO ON MANUAL"))
    assertTrue(showCreateMTMVResult.toString().contains("DUPLICATE KEY(`k1`, `k2`)"))
    assertTrue(showCreateMTMVResult.toString().contains("PARTITION BY (date_trunc(`k2`, 'month'))"))
    assertTrue(showCreateMTMVResult.toString().contains("DISTRIBUTED BY RANDOM BUCKETS 2"))
    assertTrue(showCreateMTMVResult.toString().contains("SELECT * FROM"))
    assertTrue(showCreateMTMVResult.toString().contains("grace_period"))

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 DAY
        partition by (`k2`)
        DISTRIBUTED BY hash(k1) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * FROM ${tableName};
    """
    showCreateMTMVResult = sql """show CREATE MATERIALIZED VIEW ${mvName}"""
    logger.info("showCreateMTMVResult: " + showCreateMTMVResult.toString())
    assertTrue(showCreateMTMVResult.toString().contains("BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 DAY"))
    assertTrue(showCreateMTMVResult.toString().contains("PARTITION BY (`k2`)"))
    assertTrue(showCreateMTMVResult.toString().contains("DISTRIBUTED BY HASH(`k1`) BUCKETS 2"))

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        (aa comment "aa_comment",bb)
        BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * FROM ${tableName};
    """
    showCreateMTMVResult = sql """show CREATE MATERIALIZED VIEW ${mvName}"""
    logger.info("showCreateMTMVResult: " + showCreateMTMVResult.toString())
    assertTrue(showCreateMTMVResult.toString().contains("aa comment 'aa_comment',bb"))
    assertTrue(showCreateMTMVResult.toString().contains("BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT"))
    assertTrue(showCreateMTMVResult.toString().contains("DISTRIBUTED BY RANDOM BUCKETS AUTO"))

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
