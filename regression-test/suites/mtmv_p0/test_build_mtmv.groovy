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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Assert;

suite("test_build_mtmv") {
    def tableName = "t_test_create_mtmv_user"
    def tableNamePv = "t_test_create_mtmv_user_pv"
    def mvName = "multi_mv_test_create_mtmv"
    def viewName = "multi_mv_test_create_view"
    def mvNameRenamed = "multi_mv_test_create_mtmv_renamed"

    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${tableNamePv}`"""
    sql """drop view if exists `${viewName}`"""

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

    sql """
            create view if not exists ${viewName} as select * from ${tableName};
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
        PROPERTIES (
        'replication_num' = '1',
        "grace_period"="333"
        )
        AS
        SELECT id, username FROM ${tableName};
        """

    def showCreateTableResult = sql """show create table ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains("CREATE MATERIALIZED VIEW `multi_mv_test_create_mtmv` (\n  `aa` BIGINT NULL COMMENT 'aaa',\n  `bb` VARCHAR(20) NULL\n) ENGINE=MATERIALIZED_VIEW\nCOMMENT 'comment1'\nDISTRIBUTED BY RANDOM BUCKETS 2\nPROPERTIES"))

    def descTableAllResult = sql """desc ${mvName} all"""
    logger.info("descTableAllResult: " + descTableAllResult.toString())
    assertTrue(descTableAllResult.toString().contains("${mvName}"))

    // if not exist
    try {
        sql """
            CREATE MATERIALIZED VIEW IF NOT EXISTS ${mvName}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * from ${tableName};
        """
    } catch (Exception e) {
        log.info(e.getMessage())
        Assert.fail();
    }

    // not use `if not exist`
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * from ${mvName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow create mv use other mv
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvNameRenamed}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * from ${mvName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow create mv use view
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvNameRenamed}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * from ${viewName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    // use default value
    sql """
            CREATE MATERIALIZED VIEW ${mvName}
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """

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
        BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "9999-12-13 21:07:09"
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
        BUILD DEFERRED REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND
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
    def currentMs = System.currentTimeMillis() + 20000;
    def dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMs), ZoneId.systemDefault());
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    def startTime= dateTime.format(formatter);
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS '${startTime}'
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
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT random() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // now
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT now() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

     // uuid
     try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT uuid() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

     // unix_timestamp
     try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT unix_timestamp() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // utc_timestamp
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT utc_timestamp() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // CURDATE
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT CURDATE() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // uuid_numeric
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT uuid_numeric() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // current_time
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND STARTS "2023-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT current_time() as dd, ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // repeat cols
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 SECOND
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

    // alter mv property
    sql """
        alter Materialized View ${mvName} set("grace_period"="3333");
    """
    order_qt_select "select MvProperties from mv_infos('database'='regression_test_mtmv_p0') where Name = '${mvName}'"

    // use alter table
    // not allow rename
    try {
        sql """
            alter table ${mvName} rename ${mvNameRenamed}
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }


    // not allow modify `grace_period`
    try {
        sql """
            alter table ${mvName} set("grace_period"="3333");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // allow modify comment
    try {
        sql """
            alter table ${mvName} MODIFY COMMENT "new table comment";
            """
    } catch (Exception e) {
        log.info(e.getMessage())
        Assert.fail();
    }

    // not allow modify column
    try {
        sql """
            alter table ${mvName} DROP COLUMN pv;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow replace
    try {
        sql """
            alter table ${mvName} REPLACE WITH TABLE ${tableName};
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow use mv modify property of table
    try {
        sql """
            alter Materialized View ${mvName} set("replication_num" = "1");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

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
    def jobs = sql """select count(1) from jobs("type"="mv")  where name= '${jobName}'"""
    println jobs
    assertEquals(jobs.get(0).get(0), 0);
    def tasks = sql """select count(1) from tasks("type"="mv") where jobname = '${jobName}'"""
    println tasks
    assertEquals(tasks.get(0).get(0), 0);

    // test bitmap
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
                    id BIGINT,
                    user_id bitmap
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
    sql """
        insert into ${tableName} values(11,to_bitmap(111))
    """

     sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select id,BITMAP_UNION(user_id) as bb from ${tableName} group by id;
        """
     jobName = getJobName("regression_test_mtmv_p0", mvName);
     waitingMTMVTaskFinished(jobName)
     order_qt_select_union "SELECT id,bitmap_to_string(bb) FROM ${mvName}"

  sql """
      DROP MATERIALIZED VIEW ${mvName}
     """
}
