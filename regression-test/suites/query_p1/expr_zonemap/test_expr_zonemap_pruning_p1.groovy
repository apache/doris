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

suite("test_expr_zonemap_pruning_p1") {
    sql """ set enable_expr_zonemap_filter = true """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """

    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_p1 """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_p1 (
            id INT,
            k BIGINT,
            v VARCHAR(32),
            nullable_v VARCHAR(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_expr_zonemap_pruning_p1
        SELECT CAST(number AS INT), CAST(number AS BIGINT),
               CONCAT('left_', CAST(number AS STRING)), NULL
        FROM numbers("number" = "2048")
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_p1
        SELECT CAST(number + 2048 AS INT), CAST(number + 2048 AS BIGINT),
               CONCAT('right_', CAST(number + 2048 AS STRING)),
               CONCAT('not_null_', CAST(number + 2048 AS STRING))
        FROM numbers("number" = "2048")
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

    def notNullToken = "expr_zonemap_pruning_p1_is_not_null_" + UUID.randomUUID().toString()
    def notNullRows = sql """
        SELECT '${notNullToken}', COUNT(*) FROM test_expr_zonemap_pruning_p1
        WHERE nullable_v IS NOT NULL OR starts_with(v, 'never_')
    """
    assertEquals(2048L, notNullRows[0][1] as long)
    assertExprZonemapPruned(notNullToken)

    def compoundToken = "expr_zonemap_pruning_p1_compound_" + UUID.randomUUID().toString()
    def compoundRows = sql """
        SELECT '${compoundToken}', COUNT(*) FROM test_expr_zonemap_pruning_p1
        WHERE (starts_with(v, 'never_') OR v IN ('missing_a', 'missing_b')) AND k >= 0
    """
    assertEquals(0L, compoundRows[0][1] as long)
    assertExprZonemapPruned(compoundToken)

    def largeInValues = (100000..100064).collect { "'left_" + it.toString() + "'" }.join(", ")
    def rangeOnlyToken = "expr_zonemap_pruning_p1_large_in_" + UUID.randomUUID().toString()
    def rangeOnlyRows = sql """
        SELECT '${rangeOnlyToken}', COUNT(*) FROM test_expr_zonemap_pruning_p1
        WHERE v IN (${largeInValues}) OR starts_with(v, 'never_')
    """
    assertEquals(0L, rangeOnlyRows[0][1] as long)
    assertExprZonemapPruned(rangeOnlyToken)
    assertProfileCounterPositive(rangeOnlyToken, "InZoneMapRangeOnlyCount")

    def pointCheckToken = "expr_zonemap_pruning_p1_small_in_" + UUID.randomUUID().toString()
    def pointCheckRows = sql """
        SELECT '${pointCheckToken}', COUNT(*) FROM test_expr_zonemap_pruning_p1
        WHERE v IN ('left_1', 'missing_left') OR starts_with(v, 'never_')
    """
    assertEquals(1L, pointCheckRows[0][1] as long)
    assertExprZonemapPruned(pointCheckToken)
    assertProfileCounterPositive(pointCheckToken, "InZoneMapPointCheckCount")

    def supportedMatchRows = sql """
        SELECT COUNT(*) FROM test_expr_zonemap_pruning_p1
        WHERE starts_with(v, 'right_409') AND nullable_v IS NOT NULL
    """
    assertEquals(6L, supportedMatchRows[0][0] as long)
}
