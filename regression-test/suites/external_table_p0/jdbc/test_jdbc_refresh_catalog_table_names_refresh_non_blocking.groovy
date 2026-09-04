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

import java.util.UUID

// This case depends on JDBC regression configs, FE debug points, and a reachable
// external MySQL environment. Those prerequisites may be unavailable in the
// default Apache Doris CI pipeline, so the case can be skipped or may not run
// end-to-end there. That is expected. The case still serves as a valuable
// reference for manual validation of the table-names refresh non-blocking behavior.
suite("test_jdbc_refresh_catalog_table_names_refresh_non_blocking", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
    String s3Endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    String nameSuffix = UUID.randomUUID().toString().replace("-", "")
    String catalogName = "jdbc_table_names_refresh_" + nameSuffix
    String remoteDbName = "jdbc_table_names_refresh_db_" + nameSuffix.substring(0, 12)
    String slowTableName = "slow_probe_000000"
    int collisionTableCount = 80
    int slowSleepMs = 15000
    int refreshDelayMs = 5000

    def mysqlJdbcUrl = "jdbc:mysql://${externalEnvIp}:${mysqlPort}"
    def debugPointRows = sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_debug_points';"""
    // Skip the case when FE debug points are not enabled in the regression environment.
    if (!"true".equalsIgnoreCase(debugPointRows[0][1].toString())) {
        logger.info("skip ${catalogName} because enable_debug_points is not true")
        return
    }

    def executeOnRemoteMysql = { String statement ->
        connect("root", "123456", mysqlJdbcUrl) {
            sql statement
        }
    }

    // Recreate the remote table set so every run starts from the same metadata state.
    def recreateRemoteObjects = {
        executeOnRemoteMysql("""DROP DATABASE IF EXISTS ${remoteDbName}""")
        executeOnRemoteMysql("""CREATE DATABASE ${remoteDbName}""")
        executeOnRemoteMysql("""CREATE TABLE ${remoteDbName}.${slowTableName} (k INT)""")
        for (int i = 0; i < collisionTableCount; i++) {
            String tableName = String.format("collide_%06d", i)
            executeOnRemoteMysql("""CREATE TABLE ${remoteDbName}.${tableName} (k INT)""")
        }
    }

    // Run the migrated tableNames miss-load path once and assert refresh stays non-blocking.
    def runTableNamesRefreshRace = { long minRefreshMs, long maxRefreshMs ->
        try {
            GetDebugPoint().enableDebugPointForAllFEs(
                    "ExternalDatabase.listTableNames.sleep",
                    ["sleepMs": String.valueOf(slowSleepMs)])

            def showElapsedMs = -1L
            def showFailure = null
            def showThread = Thread.start {
                try {
                    long showStart = System.currentTimeMillis()
                    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                        sql """SHOW TABLES FROM ${catalogName}.${remoteDbName}"""
                    }
                    showElapsedMs = System.currentTimeMillis() - showStart
                } catch (Throwable t) {
                    showFailure = t
                }
            }

            // Delay the refresh long enough so SHOW TABLES enters the injected slow table-names load.
            Thread.sleep(refreshDelayMs)

            long refreshStart = System.currentTimeMillis()
            sql """REFRESH CATALOG ${catalogName}"""
            long refreshElapsedMs = System.currentTimeMillis() - refreshStart

            showThread.join(slowSleepMs + 15000)
            if (showThread.isAlive()) {
                throw new IllegalStateException("show tables thread does not finish in time")
            }
            if (showFailure != null) {
                throw new IllegalStateException("show tables thread failed", showFailure)
            }

            // Re-read while the debug point is still enabled so a stale write-back would surface as a fast cache hit.
            long secondShowStart = System.currentTimeMillis()
            def secondShowRows = sql """SHOW TABLES FROM ${catalogName}.${remoteDbName}"""
            long secondShowElapsedMs = System.currentTimeMillis() - secondShowStart

            logger.info("tableNames refreshElapsedMs=${refreshElapsedMs}, showElapsedMs=${showElapsedMs}, "
                    + "secondShowElapsedMs=${secondShowElapsedMs}")
            assertTrue(showElapsedMs >= slowSleepMs - 1000)
            assertEquals(collisionTableCount + 1, secondShowRows.size())
            assertTrue(refreshElapsedMs >= minRefreshMs)
            assertTrue(refreshElapsedMs <= maxRefreshMs)
            assertTrue(refreshElapsedMs < showElapsedMs)
            assertTrue(secondShowElapsedMs >= slowSleepMs - 1000)
            return [refreshElapsedMs, showElapsedMs, secondShowElapsedMs]
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("ExternalDatabase.listTableNames.sleep")
        }
    }

    try {
        executeOnRemoteMysql("""DROP DATABASE IF EXISTS ${remoteDbName}""")
        executeOnRemoteMysql("""CREATE DATABASE ${remoteDbName}""")
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """CREATE CATALOG ${catalogName} PROPERTIES(
                "type" = "jdbc",
                "user" = "root",
                "password" = "123456",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}/${remoteDbName}?useSSL=false",
                "driver_url" = "${driverUrl}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "only_specified_database" = "true",
                "include_database_list" = "${remoteDbName}"
            )"""

        recreateRemoteObjects()
        sql """REFRESH CATALOG ${catalogName}"""
        runTableNamesRefreshRace(0L, 5000L)
    } finally {
        try {
            GetDebugPoint().disableDebugPointForAllFEs("ExternalDatabase.listTableNames.sleep")
        } catch (Throwable t) {
            logger.warn("failed to disable table names debug point during cleanup", t)
        }
        try_sql("""DROP CATALOG IF EXISTS ${catalogName}""")
        try {
            executeOnRemoteMysql("""DROP DATABASE IF EXISTS ${remoteDbName}""")
        } catch (Throwable t) {
            logger.warn("failed to drop remote database during cleanup", t)
        }
    }
}
