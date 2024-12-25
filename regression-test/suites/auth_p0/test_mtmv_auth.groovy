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

suite("test_mtmv_auth","p0,auth") {
    String suiteName = "test_mtmv_auth"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

   sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName};"""
   sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select * from ${tableName};
        """

    sql """refresh MATERIALIZED VIEW ${mvName} auto"""
    waitingMTMVTaskFinishedByMvName(mvName)

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def mvsRes = sql """select * from mv_infos("database"="${dbName}");"""
        logger.info("mvsRes: " + mvsRes.toString())
        assertFalse(mvsRes.toString().contains("${mvName}"))

        def jobsRes = sql """select * from jobs("type"="mv");"""
        logger.info("jobsRes: " + jobsRes.toString())
        assertFalse(jobsRes.toString().contains("${mvName}"))

        def tasksRes = sql """select * from tasks("type"="mv");"""
        logger.info("tasksRes: " + tasksRes.toString())
        assertFalse(tasksRes.toString().contains("${mvName}"))

    }

    sql """grant select_priv on ${dbName}.${mvName} to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
       def mvsRes = sql """select * from mv_infos("database"="${dbName}");"""
       logger.info("mvsRes: " + mvsRes.toString())
       assertTrue(mvsRes.toString().contains("${mvName}"))

       def jobsRes = sql """select * from jobs("type"="mv");"""
       logger.info("jobsRes: " + jobsRes.toString())
       assertTrue(jobsRes.toString().contains("${mvName}"))

       def tasksRes = sql """select * from tasks("type"="mv");"""
       logger.info("tasksRes: " + tasksRes.toString())
       assertTrue(tasksRes.toString().contains("${mvName}"))
    }

    try_sql("DROP USER ${user}")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName};"""
    sql """drop table if exists `${tableName}`"""
}
