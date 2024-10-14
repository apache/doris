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

suite("test_mtmv_sql_cache_and_profile", "mtmv") {

    sql """ADMIN SET FRONTEND CONFIG ('cache_enable_sql_mode' = 'true')"""

    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_mtmv_sql_cache_and_profile"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """use ${dbName};"""
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName} values (1,1),(1,2);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
    """

    def jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)

    sql """set enable_sql_cache=true;"""

    long startTime = System.currentTimeMillis()
    long timeoutTimestamp = startTime + 5 * 60 * 1000
    def explain_res = ""
    while (System.currentTimeMillis() < timeoutTimestamp) {
        sleep(5 * 1000)
        sql """select k2 from ${mvName} group by k2;"""
        try {
            explain_res = sql """explain plan select k2 from ${mvName} group by k2;"""
        } catch (Exception e) {
            logger.info(e.getMessage())
        }
        logger.info("explain_res: " + explain_res)
        if (explain_res.toString().indexOf("LogicalSqlCache") != -1 || explain_res.toString().indexOf("PhysicalSqlCache") != -1) {
            break
        }
    }
    assertTrue(explain_res.toString().indexOf("LogicalSqlCache") != -1 || explain_res.toString().indexOf("PhysicalSqlCache") != -1)

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
