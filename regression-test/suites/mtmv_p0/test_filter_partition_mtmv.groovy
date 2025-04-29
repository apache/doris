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


import org.apache.groovy.json.internal.Exceptions
import org.junit.Assert;

suite("test_filter_partition_mtmv") {
    def tableName = "t_test_filter_partition_mtmv_user"
    def mvName = "mv_test_filter_partition_mtmv"
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
        PARTITION p_20250101 VALUES IN ("2025-01-01"),
        PARTITION p_20250102 VALUES IN ("2025-01-02"),
        PARTITION p_20250103 VALUES IN ("2025-01-03"),
        PARTITION p_20250104 VALUES IN ("2025-01-04"),
        PARTITION p_20250105 VALUES IN ("2025-01-05")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2025-01-01"),(2,"2025-01-02");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`k2`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * FROM ${tableName} WHERE k2 > "2025-01-02" AND k2 <= "2025-01-04";
    """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    // When created use olap table, all partitions will be included.
    assertEquals(5, showPartitionsResult.size())

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())
    order_qt_date_list0 "SELECT * FROM ${mvName} order by k1,k2"

    sql """
        insert into ${tableName} values(3,"2025-01-03");
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
    assertTrue(showPartitionsResult.toString().contains("p_20250103"))
    order_qt_date_list1 "SELECT * FROM ${mvName} order by k1,k2"

    sql """
        insert into ${tableName} values(4,"2025-01-04"),(5,"2025-01-05");
        """

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} PARTITION p_20250104
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20250103"))
    assertTrue(showPartitionsResult.toString().contains("p_20250104"))
    order_qt_date_list2 "SELECT * FROM ${mvName} order by k1,k2"

    sql """
        insert into ${tableName} values(6,"2025-01-04");
        """

    sql """
            REFRESH MATERIALIZED VIEW ${mvName} COMPLETE
        """
    jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(2, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20250103"))
    assertTrue(showPartitionsResult.toString().contains("p_20250104"))
    order_qt_date_list3 "SELECT * FROM ${mvName} order by k1,k2"

    // partition p_20250105 is not included
    // because the query statement of the materialized view filters k2 > "2025-01-02" AND k2 <= "2025-01-04".
    try {
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} PARTITION p_20250105
        """
        Assert.fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("partition not exist: p_20250105"))
        log.info(e.getMessage())
    }
}
