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

suite("test_base_multi_partition_cols_mtmv") {
    def tableName = "t_multi_partition_cols_mtmv_user"
    def mvName = "test_base_multi_partition_cols_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE `${tableName}`
        (
        	k1 TINYINT,
            k2 TINYINT not null,
            k3 VARCHAR(20) not null
        )
        PARTITION BY LIST(`k2`, `k3`)
        (
            PARTITION `p1_bj` VALUES IN (("1", "bj")),
            PARTITION `p1_sh` VALUES IN (("1", "sh")),
            PARTITION `p2_bj` VALUES IN (("2", "bj")),
            PARTITION `p2_sh` VALUES IN (("2", "sh")),
            PARTITION `p3_bj` VALUES IN (("3", "bj")),
            PARTITION `p3_sh` VALUES IN (("3", "sh"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName} values (1,1,"bj"),(1,2,"bj"),(1,3,"bj"),(1,1,"sh"),(1,2,"sh"),(1,3,"sh");
        """
    
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k2)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """

    // should has 3 partitions
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_1"))
    assertTrue(showPartitionsResult.toString().contains("p_2"))
    assertTrue(showPartitionsResult.toString().contains("p_3"))

    // refresh one partition
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partitions(p_1);
    """
    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)
    order_qt_select_p1 "SELECT * FROM ${mvName}"

    // refresh other partition
    sql """
            REFRESH MATERIALIZED VIEW ${mvName};
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_task_other "select NeedRefreshPartitions from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_select_other "SELECT * FROM ${mvName}"

    // data change
    sql """
        insert into ${tableName} values(1,1,'bj');
        """
    waitingMTMVTaskFinished(jobName)
    order_qt_task_other "select NeedRefreshPartitions from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_select_other "SELECT * FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // partition change
    //add partition k3
    sql """
        alter table ${tableName} ADD PARTITION `p1_tj` VALUES IN (("1", "tj"))
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName};
        """
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(showPartitionsResult.size(),3)

    // add partition k2
    sql """
        alter table ${tableName} ADD PARTITION `p4_bj` VALUES IN (("4", "bj"))
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName};
        """
    waitingMTMVTaskFinished(jobName)
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(showPartitionsResult.size(),4)
    assertTrue(showPartitionsResult.toString().contains("p_4"))
}
