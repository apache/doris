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

// ===================================================================================
// Query Cache instance planning tests
//
// Verify the instance planning fixes from PR #51202 and #60878:
//   1. When Query Cache is disabled, the instance count should be close to the BE count
//   2. After Query Cache is enabled, scan fragment instances increase (split by tablet granularity)
//   3. But non-scan fragment instances should not grow proportionally with the tablet count
//   4. The first cold run should have InsertCache=1, and the second warm run should have HitCache=1
//   5. Result sets should be identical before and after toggling cache
//
// Configuration: parallel_pipeline_task_num=1 and parallel_exchange_instance_num=1
// These settings amplify instance planning issues.
// ===================================================================================

import groovy.json.JsonSlurper

import java.util.regex.Pattern

suite("query_cache_instance_planning_behavior_test") {
    def tableName = "query_cache_instance_planning_table"
    int tabletCount = 12
    int aliveBeCount = sql_return_maparray("show backends")
            .stream()
            .filter { it["Alive"].toString().equalsIgnoreCase("true") }
            .count() as int

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_nereids_distribute_planner=true"
        sql "set enable_sql_cache=false"
        sql "set query_cache_force_refresh=false"
        sql "set parallel_pipeline_task_num=1"
        sql "set parallel_exchange_instance_num=1"
        sql "set enable_profile=true"
        sql "set profile_level=2"
    }

    def buildAuthorization = {
        def user = context.config.isCloudMode() ? context.config.feCloudHttpUser : context.config.feHttpUser
        def password = context.config.isCloudMode() ? context.config.feCloudHttpPassword : context.config.feHttpPassword
        return Base64.getEncoder().encodeToString((user + ":" + (password == null ? "" : password)).getBytes("UTF-8"))
    }

    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Authorization", "Basic ${buildAuthorization()}")
        return conn.getInputStream().getText()
    }

    def getProfileText = { String queryId ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/api/profile/text/?query_id=${queryId}").openConnection()
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Authorization", "Basic ${buildAuthorization()}")
        return conn.getInputStream().getText()
    }

    def waitProfileTextByTag = { String tag ->
        for (int retry = 0; retry < 30; retry++) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(tag)) {
                    return getProfileText(profileItem["Profile ID"].toString())
                }
            }
            sleep(1000)
        }
        throw new IllegalStateException("Missing profile with tag: ${tag}")
    }

    def runSqlWithProfile = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        def result = sql "/* ${tag} */ ${sqlStr}"
        def profileText = waitProfileTextByTag(tag)
        logger.info("profileText: " + profileText)
        return [result: result, profileText: profileText]
    }

    def extractProfileInt = { String profileText, String fieldName ->
        String regex = "(?m).*" + Pattern.quote(fieldName) + "\\s*:\\s*(\\d+).*"
        def matcher = profileText =~ regex
        assertTrue(matcher.find(), "Missing profile field ${fieldName}")
        return matcher.group(1).toInteger()
    }

    def containsProfileCounter = { String profileText, String counterName, String expectedValue ->
        String regex = "(?s).*" + Pattern.quote(counterName) + ":\\s*" + Pattern.quote(expectedValue) + ".*"
        return (profileText =~ regex).find()
    }

    def normalizeRows = { List<List<Object>> rows ->
        return rows
                .collect { row -> row.collect { value -> value == null ? "NULL" : value.toString() } }
                .sort { left, right -> left.join("|") <=> right.join("|") }
    }

    def assertQueryResultEqual = { List<List<Object>> expected, List<List<Object>> actual ->
        assertEquals(normalizeRows(expected), normalizeRows(actual))
    }

    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            id int,
            category int,
            value int
        ) ENGINE=OLAP
        DUPLICATE KEY(id, category)
        PARTITION BY RANGE(id) (
            PARTITION p1 VALUES [('1'), ('11')),
            PARTITION p2 VALUES [('11'), ('21')),
            PARTITION p3 VALUES [('21'), ('31')),
            PARTITION p4 VALUES [('31'), ('41')),
            PARTITION p5 VALUES [('41'), ('51')),
            PARTITION p6 VALUES [('51'), ('61'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 1, 10), (2, 2, 20),
        (11, 1, 30), (12, 3, 40),
        (21, 2, 50), (22, 3, 60),
        (31, 1, 70), (32, 2, 80),
        (41, 3, 90), (42, 1, 100),
        (51, 2, 110), (52, 3, 120)
    """
    sql "sync"

    setSessionVariables()

    def sqlStr = """
        select category, sum(value) as total_value
        from ${tableName}
        group by category
    """

    sql "set enable_query_cache=false"
    def withoutCache = runSqlWithProfile(sqlStr)
    int totalInstancesWithoutCache = extractProfileInt(withoutCache.profileText, "Total Instances Num")
    assertTrue(totalInstancesWithoutCache <= aliveBeCount + 2)

    sql "set enable_query_cache=true"
    def firstCacheRun = runSqlWithProfile(sqlStr)
    assertQueryResultEqual(withoutCache.result, firstCacheRun.result)
    assertTrue(containsProfileCounter(firstCacheRun.profileText, "HitCache", "0"))
    assertTrue(containsProfileCounter(firstCacheRun.profileText, "InsertCache", "1"))
    assertTrue(firstCacheRun.profileText.contains("CacheTabletId"))
    assertTrue(firstCacheRun.profileText.contains("Instances Num Per BE"))

    int totalInstancesWithCache = extractProfileInt(firstCacheRun.profileText, "Total Instances Num")
    assertTrue(totalInstancesWithCache > totalInstancesWithoutCache)
    assertTrue(totalInstancesWithCache <= tabletCount + 3)

    def secondCacheRun = runSqlWithProfile(sqlStr)
    assertQueryResultEqual(withoutCache.result, secondCacheRun.result)
    assertTrue(containsProfileCounter(secondCacheRun.profileText, "HitCache", "1"))

    int totalInstancesWithCacheHit = extractProfileInt(secondCacheRun.profileText, "Total Instances Num")
    assertTrue(totalInstancesWithCacheHit <= tabletCount + 3)
        })
    }
}
