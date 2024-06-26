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

    // not support show create table
    test {
          sql """
              show create table ${mvName};
          """
          exception "not support"
      }

    // desc
    def descTableAllResult = sql """desc ${mvName} all"""
    logger.info("descTableAllResult: " + descTableAllResult.toString())
    assertTrue(descTableAllResult.toString().contains("${mvName}"))

    // show data
    def showDataResult = sql """show data"""
    logger.info("showDataResult: " + showDataResult.toString())
    assertTrue(showDataResult.toString().contains("${mvName}"))

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

    // check mvName
    try {
        sql """
            CREATE MATERIALIZED VIEW ` `
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * from ${tableName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

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
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
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

    // not allow use mv modify property of table
    if (!isCloudMode()) {
        try {
            sql """
                alter Materialized View ${mvName} set("replication_num" = "1");
                """
            Assert.fail();
        } catch (Exception e) {
            log.info(e.getMessage())
        }
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

    // test build mv which containing literal varchar field
    sql """
    drop table if exists lineitem
    """
    sql """
    CREATE TABLE IF NOT EXISTS lineitem (
      l_orderkey    INTEGER NOT NULL,
      l_partkey     INTEGER NOT NULL,
      l_suppkey     INTEGER NOT NULL,
      l_linenumber  INTEGER NOT NULL,
      l_quantity    DECIMALV3(15,2) NOT NULL,
      l_extendedprice  DECIMALV3(15,2) NOT NULL,
      l_discount    DECIMALV3(15,2) NOT NULL,
      l_tax         DECIMALV3(15,2) NOT NULL,
      l_returnflag  CHAR(1) NOT NULL,
      l_linestatus  CHAR(1) NOT NULL,
      l_shipdate    DATE NOT NULL,
      l_commitdate  DATE NOT NULL,
      l_receiptdate DATE NOT NULL,
      l_shipinstruct CHAR(25) NOT NULL,
      l_shipmode     CHAR(10) NOT NULL,
      l_comment      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    drop table if exists partsupp
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )"""

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """DROP MATERIALIZED VIEW IF EXISTS test_varchar_literal_mv;"""
    sql """
        CREATE MATERIALIZED VIEW test_varchar_literal_mv
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select case when l_orderkey > 1 then "一二三四" else "五六七八" end as field_1 from lineitem;
    """
    qt_desc_mv """desc test_varchar_literal_mv;"""

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_with_cte;"""
    sql """
        CREATE MATERIALIZED VIEW mv_with_cte
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            with `test_with` AS (
            select l_partkey, l_suppkey
            from lineitem
            union
            select
              ps_partkey, ps_suppkey
            from
            partsupp)
            select * from test_with;
    """
    waitingMTMVTaskFinished(getJobName("regression_test_mtmv_p0", "mv_with_cte"))
    order_qt_query_mv_with_cte """select * from mv_with_cte;"""
}
