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

suite("test_null_partition_mtmv") {
    def tableName = "t_test_null_partition_mtmv_user"
    def mvName = "t_test_null_partition_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // list table
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `num` SMALLINT COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `num`)
        COMMENT 'OLAP'
        PARTITION BY list(`num`)
        (
        PARTITION p_1 VALUES IN (1),
        PARTITION p_null VALUES IN (null)
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,1),(2,null);
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
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_1"))
    assertTrue(showPartitionsResult.toString().contains("p_NULL"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)

    order_qt_list_null "SELECT * FROM ${mvName} partitions(p_NULL) order by user_id,num"
    order_qt_list_1 "SELECT * FROM ${mvName} partitions(p_1) order by user_id,num"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""


    // range table
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `num` SMALLINT COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `num`)
        COMMENT 'OLAP'
        PARTITION BY range(`num`)
        (
        PARTITION p_10 VALUES LESS THAN (10),
        PARTITION p_20 VALUES LESS THAN (20)
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,null),(2,15);
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
    assertTrue(showPartitionsResult.toString().contains("p_32768_10"))
    assertTrue(showPartitionsResult.toString().contains("p_10_20"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)

    order_qt_range_p_32768_10 "SELECT * FROM ${mvName} partitions(p_32768_10) order by user_id,num"
    order_qt_range_p_10_20 "SELECT * FROM ${mvName} partitions(p_10_20) order by user_id,num"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // range table
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `create_date` DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `create_date`)
        COMMENT 'OLAP'
        PARTITION BY range(`create_date`)
        (
        PARTITION p_10 VALUES LESS THAN ("2020-11-11"),
        PARTITION p_20 VALUES LESS THAN ("2021-11-11")
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,null),(2,"2021-01-01");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`create_date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_00000101_20201111"))
    assertTrue(showPartitionsResult.toString().contains("p_20201111_20211111"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)

    order_qt_range_p_00000101_20201111 "SELECT * FROM ${mvName} partitions(p_00000101_20201111) order by user_id,create_date"
    order_qt_range_p_20201111_20211111 "SELECT * FROM ${mvName} partitions(p_20201111_20211111) order by user_id,create_date"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

}
