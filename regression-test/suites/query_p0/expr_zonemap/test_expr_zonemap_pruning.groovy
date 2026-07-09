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

import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.doris.regression.action.ProfileAction

suite("test_expr_zonemap_pruning") {
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """

    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning """
    sql """
        CREATE TABLE test_expr_zonemap_pruning (
            id INT,
            v VARCHAR(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_expr_zonemap_pruning
        SELECT CAST(number AS INT), CONCAT('a', CAST(number AS STRING))
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    def profileAction = new ProfileAction(context)

    def getProfileByToken = { String token ->
        for (int retry = 0; retry < 10; ++retry) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    return profileAction.getProfile(profileItem["Profile ID"].toString())
                }
            }
            Thread.sleep(500)
        }
        throw new IllegalStateException("Missing profile for token: " + token)
    }

    def counterSum = { String profile, String counterName ->
        Pattern pattern = Pattern.compile(Pattern.quote(counterName) + ":\\s*([0-9,]+)")
        Matcher matcher = pattern.matcher(profile)
        long sum = 0
        while (matcher.find()) {
            sum += Long.parseLong(matcher.group(1).replace(",", ""))
        }
        return sum
    }

    def assertExprZonemapPruned = { String token ->
        String profile = ""
        long filteredSegments = 0
        long filteredPages = 0
        for (int retry = 0; retry < 20; ++retry) {
            profile = getProfileByToken(token).toString()
            filteredSegments = counterSum(profile, "ExprZoneMapFilteredSegments")
            filteredPages = counterSum(profile, "ExprZoneMapFilteredPages")
            if (filteredSegments + filteredPages > 0) {
                logger.info("${token} Profile Data: ${profile}")
                return
            }
            Thread.sleep(500)
        }
        logger.info("${token} Profile Data: ${profile}")
        assertTrue(filteredSegments + filteredPages > 0)
    }

    def assertProfileCounterPositive = { String token, String counterName ->
        String profile = ""
        long counterValue = 0
        for (int retry = 0; retry < 20; ++retry) {
            profile = getProfileByToken(token).toString()
            counterValue = counterSum(profile, counterName)
            if (counterValue > 0) {
                logger.info("${token} Profile Data: ${profile}")
                return
            }
            Thread.sleep(500)
        }
        logger.info("${token} Profile Data: ${profile}")
        assertTrue(counterValue > 0)
    }

    def matchedRows = sql """
        SELECT COUNT(*) FROM test_expr_zonemap_pruning WHERE starts_with(v, 'a')
    """
    assertEquals(4096L, matchedRows[0][0] as long)

    def startsWithToken = "expr_zonemap_pruning_starts_with_" + UUID.randomUUID().toString()
    def startsWithPrunedRows = sql """
        SELECT '${startsWithToken}', COUNT(*) FROM test_expr_zonemap_pruning WHERE starts_with(v, 'z')
    """
    assertEquals(0L, startsWithPrunedRows[0][1] as long)
    assertExprZonemapPruned(startsWithToken)

    def inToken = "expr_zonemap_pruning_in_" + UUID.randomUUID().toString()
    def inPrunedRows = sql """
        SELECT '${inToken}', COUNT(*) FROM test_expr_zonemap_pruning
        WHERE v IN ('z0', 'z1') OR starts_with(v, 'zz')
    """
    assertEquals(0L, inPrunedRows[0][1] as long)
    assertExprZonemapPruned(inToken)

    def nullToken = "expr_zonemap_pruning_is_null_" + UUID.randomUUID().toString()
    def nullPrunedRows = sql """
        SELECT '${nullToken}', COUNT(*) FROM test_expr_zonemap_pruning
        WHERE v IS NULL OR starts_with(v, 'zz')
    """
    assertEquals(0L, nullPrunedRows[0][1] as long)
    assertExprZonemapPruned(nullToken)

    def comparisonToken = "expr_zonemap_pruning_comparison_" + UUID.randomUUID().toString()
    def comparisonPrunedRows = sql """
        SELECT '${comparisonToken}', COUNT(*) FROM test_expr_zonemap_pruning
        WHERE id > 5000 OR id < 0
    """
    assertEquals(0L, comparisonPrunedRows[0][1] as long)
    assertExprZonemapPruned(comparisonToken)

    sql """ DROP TABLE IF EXISTS test_expr_zonemap_page_reachability """
    sql """
        CREATE TABLE test_expr_zonemap_page_reachability (
            id INT,
            v VARCHAR(512)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "storage_page_size" = "4096",
            "compression" = "no_compression"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_page_reachability
        SELECT CAST(number AS INT),
               IF(number < 4096,
                  CONCAT('a_', LPAD(CAST(number AS STRING), 5, '0'), '_', REPEAT('x', 256)),
                  CONCAT('m_', LPAD(CAST(number AS STRING), 5, '0'), '_', REPEAT('x', 256)))
        FROM numbers("number" = "8192")
    """
    sql """ sync """

    def pageReachabilityToken = "expr_zonemap_pruning_page_reachability_" + UUID.randomUUID().toString()
    def pageReachabilityRows = sql """
        SELECT '${pageReachabilityToken}', COUNT(*) FROM test_expr_zonemap_page_reachability
        WHERE starts_with(v, 'm_')
    """
    assertEquals(4096L, pageReachabilityRows[0][1] as long)
    assertProfileCounterPositive(pageReachabilityToken, "ExprZoneMapFilteredPages")

    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_char """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_char (
            id INT,
            c CHAR(10)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_char
        SELECT CAST(number AS INT), CONCAT('a', CAST(number AS STRING))
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    def charToken = "expr_zonemap_pruning_char_" + UUID.randomUUID().toString()
    def charPrunedRows = sql """
        SELECT '${charToken}', COUNT(*) FROM test_expr_zonemap_pruning_char
        WHERE starts_with(c, 'z') OR c IN ('z0', 'z1') OR c = 'z2'
    """
    assertEquals(0L, charPrunedRows[0][1] as long)
    assertExprZonemapPruned(charToken)

    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_nulls """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_nulls (
            id INT,
            v VARCHAR(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """ INSERT INTO test_expr_zonemap_pruning_nulls VALUES (1, NULL), (2, NULL) """
    sql """ sync """

    def isNotNullToken = "expr_zonemap_pruning_is_not_null_" + UUID.randomUUID().toString()
    def isNotNullPrunedRows = sql """
        SELECT '${isNotNullToken}', COUNT(*) FROM test_expr_zonemap_pruning_nulls
        WHERE v IS NOT NULL OR starts_with(v, 'zz')
    """
    assertEquals(0L, isNotNullPrunedRows[0][1] as long)
    assertExprZonemapPruned(isNotNullToken)
}
