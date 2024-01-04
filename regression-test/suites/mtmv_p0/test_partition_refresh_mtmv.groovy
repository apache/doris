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
    def tableNameNum = "t_test_pr_mtmv_user_num"
    def tableNameUser = "t_test_pr_mtmv_user"
    def mvName = "test_pr_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableNameNum}`"""
    sql """drop table if exists `${tableNameUser    }`"""
    sql """drop materialized view if exists ${mvName};"""


    // Inconsistent partition fields with baseTable
    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
                SELECT * FROM ${tableNameNum};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }
    sql """drop table if exists `${tableNameNum}`"""
    sql """drop materialized view if exists ${mvName};"""

    // base table has two partition col
     sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
                    SELECT * FROM ${tableNameNum};
            """
            Assert.fail();
        } catch (Exception e) {
            log.info(e.getMessage())
        }
        sql """drop table if exists `${tableNameNum}`"""
        sql """drop materialized view if exists ${mvName};"""

    // range date partition
    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
        insert into ${tableNameNum} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableNameNum};
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
    order_qt_range_date_build "SELECT * FROM ${mvName} order by user_id,date,num"

    sql """drop table if exists `${tableNameNum}`"""
    sql """drop materialized view if exists ${mvName};"""

    // range int partition
    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
        insert into ${tableNameNum} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`num`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableNameNum};
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
    order_qt_range_int_build "SELECT * FROM ${mvName} order by user_id,date,num"

    sql """drop table if exists `${tableNameNum}`"""
    sql """drop materialized view if exists ${mvName};"""

    // list int partition
    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY list(`num`)
        (
        PARTITION p_1 VALUES IN (1),
        PARTITION p_2_3 VALUES IN (2,3)
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableNameNum} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`num`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableNameNum};
    """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_1"))
    assertTrue(showPartitionsResult.toString().contains("p_2_3"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName}
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_list_int_build "SELECT * FROM ${mvName} order by user_id,date,num"

    sql """drop table if exists `${tableNameNum}`"""
    sql """drop materialized view if exists ${mvName};"""

    // refresh
    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
        insert into ${tableNameNum} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """
    sql """
        CREATE TABLE ${tableNameUser}
        (
            user_id LARGEINT,
            age INT
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(user_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableNameUser} values(1,10);
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select ${tableNameUser}.user_id,${tableNameUser}.age,${tableNameNum}.date,${tableNameNum}.num from ${tableNameUser} join ${tableNameNum} on ${tableNameUser}.user_id = ${tableNameNum}.user_id;
        """

    // refresh two partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_00000101_20170201,p_20170201_20170301);
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_two_partition "SELECT * FROM ${mvName} order by user_id,age,date,num"

    //refresh other partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName}
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_other_partition "SELECT * FROM ${mvName} order by user_id,age,date,num"

    // force refresh all partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_complete_partition "select RefreshMode from tasks('type'='mv') where JobName='${jobName}' order by CreateTime desc limit 1"

    // test related table data change
    sql """
        insert into ${tableNameNum} values(1,"2017-01-15",1);
        """
    // only refresh one partition ,data will be fresh
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partitions(p_00000101_20170201);
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_related_table_change "SELECT * FROM ${mvName} order by user_id,age,date,num"

    // test other table data change
    sql """
    insert into ${tableNameUser} values(1,9);
    """

    // only refresh one partition ,data will not be fresh
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partitions(p_00000101_20170201);
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_other_table_change_one "SELECT * FROM ${mvName} order by user_id,age,date,num"

    //refresh other partition ,data will be fresh
    sql """
        REFRESH MATERIALIZED VIEW ${mvName};
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_refresh_other_table_change_other "SELECT * FROM ${mvName} order by user_id,age,date,num"

    // test exclude table
    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableNameNum}`"""
    sql """drop table if exists `${tableNameUser}`"""

    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
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
        insert into ${tableNameNum} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """
    sql """
        CREATE TABLE ${tableNameUser}
        (
            user_id LARGEINT,
            age INT
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(user_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableNameUser} values(1,10);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1','excluded_trigger_tables'='${tableNameUser}')
            AS
            select ${tableNameUser}.user_id,${tableNameUser}.age,${tableNameNum}.date,${tableNameNum}.num from ${tableNameUser} join ${tableNameNum} on ${tableNameUser}.user_id = ${tableNameNum}.user_id;
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName};
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_exclude_init "SELECT * FROM ${mvName} order by user_id,age,date,num"

    // excluded table data change
    sql """
        insert into ${tableNameUser} values(1,9);
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName};
        """
     waitingMTMVTaskFinished(jobName)
     order_qt_exclude_will_not_change "SELECT * FROM ${mvName} order by user_id,age,date,num"
     order_qt_not_change_status "select SyncWithBaseTables  from mv_infos('database'='${dbName}') where Name='${mvName}'"
     sql """
          REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
         """
     waitingMTMVTaskFinished(jobName)
     order_qt_exclude_will_change "SELECT * FROM ${mvName} order by user_id,age,date,num"
     order_qt_change_status "select SyncWithBaseTables  from mv_infos('database'='${dbName}') where Name='${mvName}'"
}
