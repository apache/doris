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

suite("test_rollup_partition_mtmv") {
    def tableName = "t_test_rollup_partition_mtmv_user"
    def mvName = "multi_mv_test_rollup_partition_mtmv"
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
        PARTITION p_20200101 VALUES IN ("2020-01-01"),
        PARTITION p_20200102 VALUES IN ("2020-01-02"),
        PARTITION p_20200201 VALUES IN ("2020-02-01")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2020-01-01"),(2,"2020-01-02"),(3,"2020-02-01");
        """

    // list date month
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`,'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_date_list_month "SELECT * FROM ${mvName} order by k1,k2"

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (month_alias)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
    """
    def date_list_month_partitions = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + date_list_month_partitions.toString())
    assertEquals(2, date_list_month_partitions.size())
    waitingMTMVTaskFinished(getJobName(dbName, mvName))
    order_qt_date_list_month_partition_by_column "SELECT * FROM ${mvName}"

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(month_alias, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as month_alias, * FROM ${tableName};
    """
    def date_list_month_partitions_level = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + date_list_month_partitions_level.toString())
    assertEquals(2, date_list_month_partitions_level.size())
    waitingMTMVTaskFinished(getJobName(dbName, mvName))
    order_qt_date_list_month_level "SELECT * FROM ${mvName}"

    // mv partition level should be higher or equal then query, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(month_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column time unit level should be greater than sql select column"))
    }

    // mv partition use a column not in mv sql select, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as month_alias FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column can not find from sql select column"))
    }

    sql """drop materialized view if exists ${mvName};"""
    // list date year
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`,'year'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())

    sql """drop materialized view if exists ${mvName};"""
    // list date year
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (year_alias)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'year') as year_alias, * FROM ${tableName};
    """
    def date_list_year_partitions = sql """show partitions from ${mvName}"""
    assertEquals(1, date_list_year_partitions.size())
    order_qt_date_list_year_partition_by_column "SELECT * FROM ${mvName}"

    // list string month
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` varchar(200) NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY list(`k2`)
        (
        PARTITION p_20200101 VALUES IN ("2020==01==01"),
        PARTITION p_20200102 VALUES IN ("2020==01==02"),
        PARTITION p_20200201 VALUES IN ("2020==02==01")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2020==01==01"),(2,"2020==01==02"),(3,"2020==02==01");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`,'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_date_format'='%Y==%m==%d'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_string_list_month "SELECT * FROM ${mvName} order by k1,k2"


    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(month_alias, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as month_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column use invalid implicit expression"))
    }

    // mv partition level should be higher or equal then query, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(month_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column use invalid implicit expression"))
    }

    // mv partition use a column not in mv sql select, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as month_alias FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column can not find from sql select column"))
    }

    // mv partition column type is date, base table is string, partition mapping fail
    // support later
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (month_alias)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'partition_date_format'='%Y==%m==%d'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // range date month
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
        insert into ${tableName} values(1,"2020-01-01"),(2,"2020-01-02"),(3,"2020-02-01");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`,'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_date_range_month "SELECT * FROM ${mvName} order by k1,k2"

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (month_alias)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
    """
    def date_range_month_partitions = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + date_range_month_partitions.toString())
    assertEquals(2, date_range_month_partitions.size())

    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_date_range_month_partition_by_column "SELECT * FROM ${mvName}"

    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(day_alias, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as day_alias FROM ${tableName};
    """
    def date_range_month_partitions_level = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + date_range_month_partitions_level.toString())
    assertEquals(2, date_range_month_partitions_level.size())
    waitingMTMVTaskFinished(getJobName(dbName, mvName))
    order_qt_date_range_month_level "SELECT * FROM ${mvName}"

    // mv partition level should be higher or equal then query, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(month_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column time unit level should be greater than sql select column"))
    }

    // mv partition use a column not in mv sql select, should fail
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`, 'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'day') as day_alias FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("partition column can not find from sql select column"))
    }


    // not support MAXVALUE
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
        PARTITION p_20200201 VALUES [("2020-02-01"),(MAXVALUE))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (date_trunc(`k2`,'month'))
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT * FROM ${tableName};
            """
             Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (month_alias)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }


    // range not support  other data type
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `k2` int NOT NULL COMMENT '\"数据灌入日期时间\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY range(`k2`)
        (
        PARTITION p_1 VALUES [(1),(2))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (date_trunc(`k2`,'month'))
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT * FROM ${tableName};
            """
             Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (month_alias)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT date_trunc(`k2`,'month') as month_alias, * FROM ${tableName};
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not support trunc hour
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

    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (date_trunc(`k2`,'hour'))
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT * FROM ${tableName};
            """
             Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (hour_alias)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                SELECT date_trunc(`k2`,'hour') as hour_alias, * FROM ${tableName};
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(miniute_alias, 'hour'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT date_trunc(`k2`,'miniute') as miniute_alias, * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }
}
