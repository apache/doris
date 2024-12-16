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

suite("test_limit_partition_mtmv") {
    def tableName = "t_test_limit_partition_mtmv_user"
    def mvName = "multi_mv_test_limit_partition_mtmv"
    def dbName = "regression_test_mtmv_p0"

    // list partition date type
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` DATE NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY list(`k2`)
        (
        PARTITION p_20380101 VALUES IN ("2038-01-01"),
        PARTITION p_20200101 VALUES IN ("2020-01-01")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2038-01-01"),(2,"2020-01-01");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`k2`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_sync_limit'='2',
            'partition_sync_time_unit'='YEAR'
            )
            AS
            SELECT * FROM ${tableName};
    """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_date_list "SELECT * FROM ${mvName} order by k1,k2"



    // list partition string type
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` VARCHAR(100) NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY list(`k2`)
        (
        PARTITION p_20380101 VALUES IN ("20380101"),
        PARTITION p_20200101 VALUES IN ("20200101")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"20380101"),(2,"20200101");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`k2`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_sync_limit'='2',
            'partition_sync_time_unit'='MONTH',
            'partition_date_format'='%Y%m%d'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_varchar_list "SELECT * FROM ${mvName} order by k1,k2"


    // list partition int type
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` int NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY list(`k2`)
        (
        PARTITION p_20380101 VALUES IN ("20380101"),
        PARTITION p_20200101 VALUES IN ("20200101")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,20380101),(2,20200101);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`k2`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_sync_limit'='2',
            'partition_sync_time_unit'='DAY',
            'partition_date_format'='%Y%m%d'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_varchar_list "SELECT * FROM ${mvName} order by k1,k2"


    // range partition date type
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
        PARTITION p2038 VALUES [("2038-01-01"),("2038-01-03")),
        PARTITION p2020 VALUES [("2020-01-01"),("2020-01-03"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2038-01-02"),(2,"2020-01-02");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`k2`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_sync_limit'='2',
            'partition_sync_time_unit'='YEAR'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101_20380103"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_date_range "SELECT * FROM ${mvName} order by k1,k2"


    // alter
    sql """
            alter Materialized View ${mvName} set("partition_sync_limit"="");
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101_20380103"))
    assertTrue(showPartitionsResult.toString().contains("p_20200101_20200103"))
    order_qt_date_range_all "SELECT * FROM ${mvName} order by k1,k2"
}
