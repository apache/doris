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

suite("test_build_mtmv") {
    def tableName = "t_test_create_mtmv_user"
    def tableNamePv = "t_test_create_mtmv_user_pv"
    def mvName = "multi_mv_test_create_mtmv"
    def mvNameRenamed = "multi_mv_test_create_mtmv_renamed"

    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${tableNamePv}`"""

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        INSERT INTO ${tableName} VALUES("2022-10-26",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """
        create table IF NOT EXISTS ${tableNamePv}(
            event_day DATE,
            id BIGINT,
            pv BIGINT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO ${tableNamePv} VALUES("2022-10-26",1,200),("2022-10-28",2,200),("2022-10-28",3,300);
    """

    sql """drop materialized view if exists ${mvName};"""
    sql """drop materialized view if exists ${mvNameRenamed};"""

    // show create table
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        (aa comment "aaa",bb)
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        COMMENT "comment1"
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT id, username FROM ${tableName};
        """

    def showCreateTableResult = sql """show create table ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains("CREATE MATERIALIZED VIEW `multi_mv_test_create_mtmv` (\n  `aa` BIGINT NULL COMMENT 'aaa',\n  `bb` VARCHAR(20) NULL\n) ENGINE=MATERIALIZED_VIEW\nCOMMENT 'comment1'\nDISTRIBUTED BY RANDOM BUCKETS 10\nPROPERTIES"))
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // IMMEDIATE MANUAL
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    def jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // IMMEDIATE schedule interval
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // IMMEDIATE schedule interval and start
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // DEFERRED MANUAL
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName}
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // DEFERRED schedule interval
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // DEFERRED schedule interval and start
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    println jobName
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // random
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT random() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // repeat cols
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT ${tableName}.username, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // alter
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"

    // alter refreshMethod
    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH COMPLETE;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"

    // alter refreshTrigger
    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH ON MANUAL;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"

    // alter refreshMethod refreshTrigger
    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH COMPLETE ON MANUAL;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"

    // alter rename
    sql """
        alter Materialized View ${mvName} rename ${mvNameRenamed};
    """
    jobName = getJobName("regression_test_mtmv_p0", mvNameRenamed);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvNameRenamed}"

    sql """
        DROP MATERIALIZED VIEW ${mvNameRenamed}
    """

    // drop
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinished(jobName)
    order_qt_select "SELECT * FROM ${mvName}"

    // test use drop table
    try {
        sql """
            drop table ${mvName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // test use drop mv

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
    def jobs = sql """show mtmv job for ${jobName}"""
    println jobs
    assertEquals(jobs.size(), 0);
    def tasks = sql """show mtmv job tasks for ${jobName}"""
    println tasks
    assertEquals(tasks.size(), 0);

}
