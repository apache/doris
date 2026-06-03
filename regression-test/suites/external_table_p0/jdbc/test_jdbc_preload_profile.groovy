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

import java.util.regex.Pattern

// Run this suite in the external pipeline because it depends on external JDBC services.
suite("test_jdbc_preload_profile", "external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
    String s3Endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    String internalDbName = "regression_test_jdbc_preload_profile"
    String internalTableName = "jdbc_preload_profile_internal"
    String externalDbName = "doris_test"
    String externalTableName = "ex_tb0"
    String preloadOnCatalog = "jdbc_preload_profile_on"
    String preloadOffCatalog = "jdbc_preload_profile_off"
    long sleepMs = 10000L

    def getProfileText = { String queryId ->
        def dst = 'http://' + context.config.feHttpAddress
        int attempts = 0
        while (attempts < 15) {
            def conn = new URL(dst + "/api/profile/text/?query_id=$queryId").openConnection()
            conn.setRequestMethod("GET")
            def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":"
                    + (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword))
                    .getBytes("UTF-8"))
            conn.setRequestProperty("Authorization", "Basic ${encoding}")
            String profileText = conn.getInputStream().getText()
            if (profileText != null && !profileText.isEmpty()) {
                return profileText
            }
            Thread.sleep(500)
            attempts++
        }
        throw new IllegalStateException("Failed to fetch query profile for query id: ${queryId}")
    }

    def extractProfileValue = { String profileText, String keyName ->
        def matcher = profileText =~ /(?m)^\s*-\s*${Pattern.quote(keyName)}:\s*(.+)$/
        return matcher.find() ? matcher.group(1).trim() : null
    }

    def parsePrettyTimeMs = { String prettyTime ->
        if (prettyTime == null) {
            return -1L
        }
        if ("0".equals(prettyTime) || "0ms".equals(prettyTime)) {
            return 0L
        }
        def matcher = prettyTime =~ /^(?:(\d+)hour)?(?:(\d+)min)?(?:(\d+)sec)?(?:(\d+)ms)?$/
        assertTrue(matcher.matches(), "Unexpected time format: ${prettyTime}")
        long totalMs = 0L
        if (matcher.group(1) != null) {
            totalMs += Long.parseLong(matcher.group(1)) * 3600_000L
        }
        if (matcher.group(2) != null) {
            totalMs += Long.parseLong(matcher.group(2)) * 60_000L
        }
        if (matcher.group(3) != null) {
            totalMs += Long.parseLong(matcher.group(3)) * 1000L
        }
        if (matcher.group(4) != null) {
            totalMs += Long.parseLong(matcher.group(4))
        }
        return totalMs
    }

    // Point the catalog to the prebuilt JDBC source used by the existing external JDBC suites.
    def createJdbcCatalog = { String catalogName ->
        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}/${externalDbName}?useSSL=false",
            "driver_url" = "${driverUrl}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
    }

    def runProfiledQuery = { String catalogName, boolean enablePreload ->
        sql """set enable_nereids_planner=true"""
        sql """set enable_fallback_to_original_planner=false"""
        sql """set enable_profile=true"""
        sql """set profile_level=2"""
        sql """set enable_preload_external_metadata=${enablePreload}"""

        String sqlText = """select i.id
                from internal.${internalDbName}.${internalTableName} i
                join ${catalogName}.${externalDbName}.${externalTableName} j
                on i.id = j.id
                order by i.id
                limit 1"""
        sql sqlText
        def queryIdResult = sql """select last_query_id()"""
        String queryId = queryIdResult[0][0]
        String profileText = getProfileText(queryId)
        String preloadTime = extractProfileValue(profileText, "Nereids Preload External Metadata Time")
        String analysisTime = extractProfileValue(profileText, "Nereids Analysis Time")
        assertNotNull(preloadTime)
        assertNotNull(analysisTime)
        return [
                preloadTimeMs : parsePrettyTimeMs(preloadTime),
                analysisTimeMs: parsePrettyTimeMs(analysisTime),
                queryId       : queryId,
                profileText   : profileText
        ]
    }

    sql """create database if not exists ${internalDbName}"""
    sql """drop table if exists ${internalDbName}.${internalTableName}"""
    sql """
        create table ${internalDbName}.${internalTableName} (
            id int,
            name string
        ) distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """insert into ${internalDbName}.${internalTableName} values (111, 'doris')"""

    // Keep the JDBC schema cache cold by using a fresh catalog for each phase.
    GetDebugPoint().enableDebugPointForAllFEs("PluginDrivenExternalTable.initSchema.sleep",
            [sleepMs: "${sleepMs}"])
    try {
        createJdbcCatalog(preloadOnCatalog)
        def preloadOnResult = runProfiledQuery(preloadOnCatalog, true)
        sql """drop catalog if exists ${preloadOnCatalog}"""

        createJdbcCatalog(preloadOffCatalog)
        def preloadOffResult = runProfiledQuery(preloadOffCatalog, false)

        assertTrue(preloadOnResult.preloadTimeMs >= sleepMs - 500,
                "Expected preload time to include the injected JDBC schema delay")
        assertEquals(0L, preloadOffResult.preloadTimeMs,
                "Preload counter should stay at 0 when preload is disabled")
        assertTrue(preloadOffResult.analysisTimeMs >= sleepMs - 500,
                "Expected analysis time to include the injected JDBC schema delay when preload is disabled")
        assertTrue(preloadOffResult.analysisTimeMs > preloadOnResult.analysisTimeMs,
                "Analysis time should be larger when JDBC schema loading remains in the analysis phase")
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("PluginDrivenExternalTable.initSchema.sleep")
    }
}
