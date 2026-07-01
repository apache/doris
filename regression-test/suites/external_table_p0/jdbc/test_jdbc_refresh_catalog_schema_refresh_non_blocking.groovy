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
// reference for manual validation of the schema refresh non-blocking behavior.
suite("test_jdbc_refresh_catalog_schema_refresh_non_blocking", "p0,external") {
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
    String catalogName = "jdbc_schema_refresh_" + nameSuffix
    String remoteDbName = "jdbc_schema_refresh_db_" + nameSuffix.substring(0, 12)
    String slowTableName = "slow_probe_000000"
    int collisionTableCount = 80
    int hashMod = 512
    int slowSleepMs = 15000
    int refreshDelayMs = 5000

    def mysqlJdbcUrl = "jdbc:mysql://${externalEnvIp}:${mysqlPort}"
    def debugPointRows = sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_debug_points';"""
    // Skip the case when FE debug points are not enabled in the regression environment.
    if (!"true".equalsIgnoreCase(debugPointRows[0][1].toString())) {
        logger.info("skip ${catalogName} because enable_debug_points is not true")
        return
    }

    def jint = { long value ->
        long result = value & 0xffffffffL
        if (result >= 0x80000000L) {
            result -= 0x100000000L
        }
        return (int) result
    }

    def javaStringHash = { String value ->
        int hash = 0
        for (int i = 0; i < value.length(); i++) {
            hash = jint(31L * hash + (int) value.charAt(i))
        }
        return hash
    }

    def javaLongHash = { long value ->
        return jint(value ^ (value >>> 32))
    }

    def objectsHash = { List<Object> values ->
        int hash = 1
        values.each { value ->
            int elementHash
            if (value instanceof Number) {
                elementHash = javaLongHash(((Number) value).longValue())
            } else {
                elementHash = javaStringHash(value.toString())
            }
            hash = jint(31L * hash + elementHash)
        }
        return hash
    }

    def spread = { int hash ->
        return jint(hash ^ (((long) hash & 0xffffffffL) >>> 16))
    }

    // Compute collision table names that land in the same Caffeine hash bin as the slow key.
    def findCollisionTableNames = { long catalogId ->
        def calcIndex = { String tableName ->
            int hash = objectsHash([catalogId, remoteDbName, tableName, remoteDbName, tableName])
            return spread(hash) & (hashMod - 1)
        }
        int targetIndex = calcIndex(slowTableName)
        def tableNames = []
        for (int i = 0; i < 300000; i++) {
            String candidate = String.format("collide_%06d", i)
            if (calcIndex(candidate) == targetIndex) {
                tableNames.add(candidate)
                if (tableNames.size() >= collisionTableCount) {
                    break
                }
            }
        }
        assertEquals(collisionTableCount, tableNames.size())
        return tableNames
    }

    def executeOnRemoteMysql = { String statement ->
        connect("root", "123456", mysqlJdbcUrl) {
            sql statement
        }
    }

    // Recreate the remote schema so every run starts from the same cache and metadata state.
    def recreateRemoteObjects = { List<String> collisionTableNames ->
        executeOnRemoteMysql("""DROP DATABASE IF EXISTS ${remoteDbName}""")
        executeOnRemoteMysql("""CREATE DATABASE ${remoteDbName}""")
        executeOnRemoteMysql("""CREATE TABLE ${remoteDbName}.${slowTableName} (k INT)""")
        collisionTableNames.each { tableName ->
            executeOnRemoteMysql("""CREATE TABLE ${remoteDbName}.${tableName} (k INT)""")
        }
    }

    def preheatSchemaCache = { List<String> collisionTableNames ->
        sql """REFRESH CATALOG ${catalogName}"""
        collisionTableNames.each { tableName ->
            sql """DESC ${catalogName}.${remoteDbName}.${tableName}"""
        }
        assertEquals(collisionTableCount, getSchemaCacheSize())
    }

    // Read the current schema cache size for this catalog from information_schema.
    def getSchemaCacheSize = {
        def statRows = sql """
                SELECT ESTIMATED_SIZE
                FROM information_schema.catalog_meta_cache_statistics
                WHERE CATALOG_NAME = '${catalogName}'
                  AND ENTRY_NAME = 'schema'
            """
        return ((Number) statRows[0][0]).intValue()
    }

    // Run the schema-entry manual miss load path once and assert refresh stays non-blocking.
    def runSchemaRefreshRace = { long minRefreshMs, long maxRefreshMs ->
        try {
            GetDebugPoint().enableDebugPointForAllFEs(
                    "PluginDrivenExternalTable.initSchema.sleep",
                    ["sleepMs": String.valueOf(slowSleepMs)])

            def descElapsedMs = -1L
            def descFailure = null
            def descThread = Thread.start {
                try {
                    long descStart = System.currentTimeMillis()
                    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                        sql """DESC ${catalogName}.${remoteDbName}.${slowTableName}"""
                    }
                    descElapsedMs = System.currentTimeMillis() - descStart
                } catch (Throwable t) {
                    descFailure = t
                }
            }

            // Delay the refresh long enough so the DESC path can enter the injected slow schema load.
            Thread.sleep(refreshDelayMs)

            long refreshStart = System.currentTimeMillis()
            sql """REFRESH CATALOG ${catalogName}"""
            long refreshElapsedMs = System.currentTimeMillis() - refreshStart

            descThread.join(slowSleepMs + 15000)
            if (descThread.isAlive()) {
                throw new IllegalStateException("desc thread does not finish in time")
            }
            if (descFailure != null) {
                throw new IllegalStateException("desc thread failed", descFailure)
            }

            logger.info("refreshElapsedMs=${refreshElapsedMs}, descElapsedMs=${descElapsedMs}")
            assertTrue(descElapsedMs >= slowSleepMs - 1000)
            assertTrue(refreshElapsedMs >= minRefreshMs)
            assertTrue(refreshElapsedMs <= maxRefreshMs)
            assertTrue(refreshElapsedMs < descElapsedMs)
            return [refreshElapsedMs, descElapsedMs]
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("PluginDrivenExternalTable.initSchema.sleep")
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

        def catalogRows = sql """SHOW CATALOGS LIKE '${catalogName}'"""
        long catalogId = ((Number) catalogRows[0][0]).longValue()
        def collisionTableNames = findCollisionTableNames(catalogId)

        recreateRemoteObjects(collisionTableNames)
        preheatSchemaCache(collisionTableNames)
        runSchemaRefreshRace(0L, 5000L)

        // Verify the invalidated slow load did not write back the stale key into schema cache.
        assertEquals(0, getSchemaCacheSize())
        sql """DESC ${catalogName}.${remoteDbName}.${slowTableName}"""
        assertEquals(1, getSchemaCacheSize())
    } finally {
        try {
            GetDebugPoint().disableDebugPointForAllFEs("PluginDrivenExternalTable.initSchema.sleep")
        } catch (Throwable t) {
            logger.warn("failed to disable debug point during cleanup", t)
        }
        try_sql("""DROP CATALOG IF EXISTS ${catalogName}""")
        try {
            executeOnRemoteMysql("""DROP DATABASE IF EXISTS ${remoteDbName}""")
        } catch (Throwable t) {
            logger.warn("failed to drop remote database during cleanup", t)
        }
    }
}
