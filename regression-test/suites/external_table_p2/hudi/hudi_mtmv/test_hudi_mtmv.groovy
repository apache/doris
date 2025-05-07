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

suite("test_hudi_mtmv", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled hudi test")
        return
    }
    String suiteName = "test_hudi_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_mv"
    String dbName = context.config.getDbNameByFile(context.file)
    String otherDbName = "${suiteName}_otherdb"
    String tableName = "${suiteName}_table"

    sql """drop database if exists ${otherDbName}"""
    sql """create database ${otherDbName}"""
     sql """
        CREATE TABLE  ${otherDbName}.${tableName} (
          `user_id` INT,
          `num` INT
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
       """

    sql """
        insert into ${otherDbName}.${tableName} values(1,2);
        """

    String props = context.config.otherConfigs.get("hudiEmrCatalog")

    sql """drop catalog if exists ${catalogName}"""
    sql """CREATE CATALOG if not exists ${catalogName} PROPERTIES (
            ${props}
        );"""

    order_qt_base_table """ select * from ${catalogName}.hudi_mtmv_regression_test.hudi_table_1; """

    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`par`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1;
        """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_a"))
    assertTrue(showPartitionsResult.toString().contains("p_b"))

    // refresh one partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_a);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_one_partition "SELECT * FROM ${mvName} "

    //refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_auto "SELECT * FROM ${mvName} "
    order_qt_is_sync_before_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

   // rebuild catalog, should not Affects MTMV
    sql """drop catalog if exists ${catalogName}"""
    sql """CREATE CATALOG if not exists ${catalogName} PROPERTIES (
            ${props}
        );"""
    order_qt_is_sync_after_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    // should refresh normal after catalog rebuild
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_complete_rebuild "SELECT * FROM ${mvName} "

    sql """drop materialized view if exists ${mvName};"""

     // not have partition
     sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            KEY(`id`)
            COMMENT "comment1"
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ('replication_num' = '1',"grace_period"="333")
            AS
            SELECT id,age,par FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1;
        """
    order_qt_not_partition_before "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    //should can refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_not_partition "SELECT * FROM ${mvName} "
    order_qt_not_partition_after "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """drop materialized view if exists ${mvName};"""

    // refresh on schedule
    //  sql """
    //  CREATE MATERIALIZED VIEW ${mvName}
    //     BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "9999-12-13 21:07:09"
    //     KEY(`id`)
    //     COMMENT "comment1"
    //     DISTRIBUTED BY HASH(`id`) BUCKETS 2
    //     PROPERTIES ('replication_num' = '1',"grace_period"="333")
    //     AS
    //     SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1;
    // """
    //  waitingMTMVTaskFinishedByMvName(mvName)
    //  sql """drop materialized view if exists ${mvName};"""

    // refresh on schedule
     sql """
     CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON commit
        KEY(`id`)
        COMMENT "comment1"
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1',"grace_period"="333")
        AS
        SELECT id,age,par FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1;
    """
    waitingMTMVTaskFinishedByMvName(mvName)
     sql """drop materialized view if exists ${mvName};"""

    // cross db and join internal table
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`par`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 a left join internal.${otherDbName}.${tableName} b on a.id=b.user_id;
        """
    def showJoinPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showJoinPartitionsResult: " + showJoinPartitionsResult.toString())
    assertTrue(showJoinPartitionsResult.toString().contains("p_a"))
    assertTrue(showJoinPartitionsResult.toString().contains("p_b"))

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_a);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_join_one_partition "SELECT * FROM ${mvName} "
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`create_date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_two_partitions;
        """
    def showTwoPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showTwoPartitionsResult: " + showTwoPartitionsResult.toString())
    assertTrue(showTwoPartitionsResult.toString().contains("p_20200101"))
    assertTrue(showTwoPartitionsResult.toString().contains("p_20380101"))
    assertTrue(showTwoPartitionsResult.toString().contains("p_20380102"))
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_two_partition "SELECT * FROM ${mvName} "
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`create_date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1','partition_sync_limit'='2','partition_date_format'='%Y-%m-%d',
                        'partition_sync_time_unit'='MONTH')
            AS
            SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_two_partitions;
        """
    def showLimitPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showLimitPartitionsResult: " + showLimitPartitionsResult.toString())
    assertFalse(showLimitPartitionsResult.toString().contains("p_20200101"))
    assertTrue(showLimitPartitionsResult.toString().contains("p_20380101"))
    assertTrue(showLimitPartitionsResult.toString().contains("p_20380102"))
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_limit_partition "SELECT * FROM ${mvName} "
    sql """drop materialized view if exists ${mvName};"""

    // not allow date trunc
    test {
         sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by (date_trunc(`create_date`,'month'))
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1','partition_sync_limit'='2','partition_date_format'='%Y-%m-%d',
                            'partition_sync_time_unit'='MONTH')
                AS
                SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_two_partitions;
            """
          exception "only support"
      }

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`region`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_null_partition;
        """
    def showNullPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showNullPartitionsResult: " + showNullPartitionsResult.toString())
    // assertTrue(showNullPartitionsResult.toString().contains("p_null"))
    assertTrue(showNullPartitionsResult.toString().contains("p_NULL"))
    assertTrue(showNullPartitionsResult.toString().contains("p_bj"))
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    // Will lose null data
    order_qt_null_partition "SELECT * FROM ${mvName} "
    sql """drop materialized view if exists ${mvName};"""

    sql """drop catalog if exists ${catalogName}"""

}
