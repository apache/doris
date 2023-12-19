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

suite("test_partition_refresh_mtmv") {
    def tableName = "t_test_pr_mtmv_user"
    def mvName = "test_pr_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""


    // Inconsistent partition fields with baseTable
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`user_id`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // base table has two partition col
     sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`,`num`)
        (PARTITION p201701_1000 VALUES [('0000-01-01',1), ('2017-02-01',2)),
        PARTITION p201702_2000 VALUES [('2017-02-01',3), ('2017-03-01',4)),
        PARTITION p201703_all VALUES [('2017-03-01',5), ('2017-04-01',6)))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    try {
            sql """
                CREATE MATERIALIZED VIEW ${mvName}
                    BUILD DEFERRED REFRESH AUTO ON MANUAL
                    partition by(`date`)
                    DISTRIBUTED BY RANDOM BUCKETS 2
                    PROPERTIES ('replication_num' = '1')
                    AS
                    SELECT * FROM ${tableName};
            """
            Assert.fail();
        } catch (Exception e) {
            log.info(e.getMessage())
        }
        sql """drop table if exists `${tableName}`"""
        sql """drop materialized view if exists ${mvName};"""

    // range date partition
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_00000101_20170201"))
    assertTrue(showPartitionsResult.toString().contains("p_20170201_20170301"))
    assertTrue(showPartitionsResult.toString().contains("p_20170301_20170401"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName}
        """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_range_date_build "SELECT * FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // range int partition
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`num`)
        (PARTITION p1_2 VALUES [(1), (2)),
        PARTITION p2_3 VALUES [(2), (3)),
        PARTITION p3_4 VALUES [(3), (4)))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`num`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_1_2"))
    assertTrue(showPartitionsResult.toString().contains("p_2_3"))
    assertTrue(showPartitionsResult.toString().contains("p_3_4"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName}
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_range_int_build "SELECT * FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
