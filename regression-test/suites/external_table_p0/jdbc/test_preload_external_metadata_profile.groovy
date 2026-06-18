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

suite("test_preload_external_metadata_profile", "p0,external,doris,external_docker,external_docker_doris") {
    String dbName = "regression_test_preload_external_metadata_profile"
    String catalogOff = "preload_external_metadata_profile_off"
    String catalogOn = "preload_external_metadata_profile_on"
    String internalTable = "preload_profile_internal"
    String externalTablePrefix = "preload_profile_jdbc"
    int externalTableCount = 10
    int extraColumnCount = 60
    int randomSuffix = Math.random() * 2000000000

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3Endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    Map<String, Map<String, Integer>> profileMetrics = [:]

    def parseTimeMs = { String timeText ->
        if (timeText == "0") {
            return 0
        }
        int totalMs = 0
        def matcher = Pattern.compile("(\\d+)(hour|min|sec|ms)").matcher(timeText)
        while (matcher.find()) {
            int value = Integer.parseInt(matcher.group(1))
            String unit = matcher.group(2)
            if (unit == "hour") {
                totalMs += value * 60 * 60 * 1000
            } else if (unit == "min") {
                totalMs += value * 60 * 1000
            } else if (unit == "sec") {
                totalMs += value * 1000
            } else if (unit == "ms") {
                totalMs += value
            }
        }
        if (totalMs == 0) {
            fail("Could not parse time counter: ${timeText}")
        }
        return totalMs
    }

    def extractCounterMs = { String profileString, String counterName ->
        String flexibleCounterName = counterName.split(" ").collect { Pattern.quote(it) }.join("\\s+")
        Pattern pattern = Pattern.compile(flexibleCounterName + "\\s*:\\s*([0-9]+(?:hour|min|sec|ms)?(?:[0-9]+(?:min|sec|ms))*)")
        def matcher = pattern.matcher(profileString)
        if (!matcher.find()) {
            fail("Could not find profile counter: ${counterName}\n${profileString}")
        }
        return parseTimeMs(matcher.group(1))
    }

    def buildJdbcCatalog = { String catalogName ->
        sql """ DROP CATALOG IF EXISTS ${catalogName} """
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "user" = "${jdbcUser}",
                "type" = "jdbc",
                "password" = "${jdbcPassword}",
                "jdbc_url" = "${jdbcUrl}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "com.mysql.cj.jdbc.Driver"
            )
        """
    }

    def buildQuery = { String catalogName ->
        String selectColumns = (1..externalTableCount).collect { idx ->
            "j${idx}.c1 AS j${idx}_c1, j${idx}.c${extraColumnCount} AS j${idx}_c${extraColumnCount}"
        }.join(",\n")
        String joins = (1..externalTableCount).collect { idx ->
            "JOIN ${catalogName}.${dbName}.${externalTablePrefix}_${idx} j${idx} ON i.id = j${idx}.id"
        }.join("\n")
        return """
            SELECT i.id,
                   ${selectColumns}
            FROM ${internalTable} i
            ${joins}
            ORDER BY i.id
        """
    }

    def checkPreloadProfile = { String tag, String catalogName, boolean enablePreload, boolean expectPreload ->
        sql """ SET enable_preload_external_metadata = ${enablePreload} """
        profile(tag) {
            run {
                def result = sql """ /* ${tag} */ ${buildQuery(catalogName)} """
                assertEquals(2, result.size())
            }

            check { profileString, exception ->
                if (exception != null) {
                    throw exception
                }
                assertTrue(profileString.contains("PhysicalJdbcScan"))
                int preloadMs = extractCounterMs(profileString, "Nereids Preload External Metadata Time")
                int lockMs = extractCounterMs(profileString, "Nereids Lock Table Time")
                int analysisMs = extractCounterMs(profileString, "Nereids Analysis Time")
                profileMetrics[tag] = [
                        preloadMs: preloadMs,
                        lockMs: lockMs,
                        analysisMs: analysisMs
                ]
                log.info("preload external metadata profile: tag={}, preloadMs={}, lockMs={}, analysisMs={}",
                        tag, preloadMs, lockMs, analysisMs)
                if (expectPreload) {
                    assertTrue("preload metadata time should be recorded in preload counter", preloadMs > 0)
                } else {
                    assertEquals(0, preloadMs)
                }
                assertTrue("lock table counter should be non-negative", lockMs >= 0)
            }
        }
    }

    try {
        sql """ SET enable_nereids_planner = true """
        sql """ SET enable_fallback_to_original_planner = false """
        sql """ DROP CATALOG IF EXISTS ${catalogOff} """
        sql """ DROP CATALOG IF EXISTS ${catalogOn} """
        sql """ DROP DATABASE IF EXISTS ${dbName} FORCE """
        sql """ CREATE DATABASE ${dbName} """
        sql """ USE ${dbName} """

        sql """
            CREATE TABLE ${internalTable} (
                id INT,
                v INT
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """ INSERT INTO ${internalTable} VALUES (1, 10), (2, 20), (3, 30) """

        String extraColumns = (1..extraColumnCount).collect { idx -> "`c${idx}` VARCHAR(20)" }.join(",\n")
        String extraValues = (1..extraColumnCount).collect { idx -> "'v${idx}'" }.join(", ")
        for (int i = 1; i <= externalTableCount; i++) {
            sql """
                CREATE TABLE ${externalTablePrefix}_${i} (
                    id INT,
                    ${extraColumns}
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num" = "1")
            """
            sql """
                INSERT INTO ${externalTablePrefix}_${i}
                VALUES (1, ${extraValues}), (3, ${extraValues})
            """
        }

        buildJdbcCatalog(catalogOff)
        buildJdbcCatalog(catalogOn)

        String offTag = "preload_external_metadata_profile_off_${randomSuffix}"
        String onTag = "preload_external_metadata_profile_on_${randomSuffix}"
        checkPreloadProfile(offTag, catalogOff, false, false)
        checkPreloadProfile(onTag, catalogOn, true, true)
        Map<String, Integer> offMetrics = profileMetrics[offTag]
        Map<String, Integer> onMetrics = profileMetrics[onTag]
        assertTrue(offMetrics != null)
        assertTrue(onMetrics != null)
        int analysisDropMs = offMetrics.analysisMs - onMetrics.analysisMs
        int allowedNegativeJitterMs = Math.max(20, (int) (onMetrics.preloadMs * 0.1))
        log.info("preload external metadata profile comparison: analysisDropMs={}, preloadMs={}, "
                + "allowedNegativeJitterMs={}", analysisDropMs, onMetrics.preloadMs, allowedNegativeJitterMs)
        assertTrue("analysis time should not increase meaningfully when metadata is preloaded",
                analysisDropMs + allowedNegativeJitterMs >= 0)
    } finally {
        sql """ SET enable_preload_external_metadata = false """
        sql """ DROP CATALOG IF EXISTS ${catalogOff} """
        sql """ DROP CATALOG IF EXISTS ${catalogOn} """
        sql """ DROP DATABASE IF EXISTS ${dbName} FORCE """
    }
}
