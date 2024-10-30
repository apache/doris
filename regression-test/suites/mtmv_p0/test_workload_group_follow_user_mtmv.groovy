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

suite("test_workload_group_follow_user_mtmv") {
    String suiteName = "test_workload_group_follow_user_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String group1 = "${suiteName}_group1"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant admin_priv on *.*.* to ${user}"""

    sql """drop table if exists `${tableName}`"""

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
    sql """ insert into ${tableName} values('2020-10-01',1,"a");"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        set property 'default_workload_group' = '${group1}';
        sql """drop materialized view if exists ${mvName};"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
        """
        sql """
                refresh MATERIALIZED VIEW ${mvName} AUTO;
            """
        def jobName = getJobName(dbName, mvName);
        logger.info(jobName)
        waitingMTMVTaskFinishedNotNeedSuccess(jobName)
        def errors = sql """select ErrorMsg from tasks('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"""
        logger.info("errors: " + errors.toString())
        assertTrue(errors.toString().contains("${group1}"))
        sql """
                DROP MATERIALIZED VIEW ${mvName}
            """
    }



}
