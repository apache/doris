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
import org.apache.doris.regression.action.ProfileAction

suite("test_variant_inverted_index_cast", "nonConcurrent") {
    def fetchProfileText = { sqlText ->
        def profileAction = new ProfileAction(context)
        def profiles = profileAction.getProfileList()
        assertTrue(profiles.size() > 0)

        def profileId = null
        for (def profile in profiles) {
            if (profile["Sql Statement"].contains(sqlText)) {
                profileId = profile["Profile ID"]
                break
            }
        }
        assertTrue(profileId != null)
        return profileAction.getProfile(profileId)
    }

    def maxCounter = { profileText, counterName ->
        def matcher = Pattern.compile(
                "${counterName}(?:_[^:\\s]+)?:\\s*(?:sum\\s+)?(\\d+)\\b").matcher(profileText)
        int maxVal = 0
        while (matcher.find()) {
            int val = matcher.group(1).toInteger()
            if (val > maxVal) {
                maxVal = val
            }
        }
        return maxVal
    }

    def assertIndexFilterUsed = { profileText ->
        assertTrue(maxCounter(profileText, "RowsInvertedIndexFiltered") > 0)
    }

    def assertScanRows = { profileText, expectedScanRows ->
        assertTrue(Pattern.compile("ScanRows:\\s*(?:sum\\s+)?${expectedScanRows}\\b").matcher(profileText).find())
    }

    def assertIndexFilterHit = { profileText ->
        assertIndexFilterUsed(profileText)
        assertScanRows(profileText, 1)
    }

    def assertRows = { expected, actual ->
        assertEquals(expected.size(), actual.size())
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected[i].size(), actual[i].size())
            for (int j = 0; j < expected[i].size(); j++) {
                assertEquals(expected[i][j] as long, actual[i][j] as long)
            }
        }
    }

    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true """
    sql """ set inverted_index_skip_threshold = 0 """

    sql """ DROP TABLE IF EXISTS test_variant_inverted_index_cast_auto_storage """
    sql """
        CREATE TABLE test_variant_inverted_index_cast_auto_storage (
            row_id BIGINT,
            v VARIANT COMMENT 'auto inferred variant',
            INDEX idx_v(v) USING INVERTED COMMENT ''
        )
        ENGINE=OLAP
        DUPLICATE KEY(row_id)
        DISTRIBUTED BY HASH(row_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "v2"
        )
    """

    sql """
        INSERT INTO test_variant_inverted_index_cast_auto_storage
        SELECT number, CONCAT(
                '{"int_key": ', number, ', "bigint_key": ', 1300000000000 + number, '}')
        FROM numbers("number" = "20")
    """
    sql """
        INSERT INTO test_variant_inverted_index_cast_auto_storage VALUES
            (269, '{"int_key": 269}'),
            (-243, '{"int_key": -243}'),
            (128, '{"int_key": 128}'),
            (-129, '{"int_key": -129}')
    """

    sql """ clean all profile """
    def autoBigintToIntSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as int) = 13"""
    def autoBigintToIntResult = sql """ ${autoBigintToIntSql} """
    assertRows([[13]], autoBigintToIntResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToIntSql))

    sql """ clean all profile """
    def autoBigintToTinyintSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) = 13
ORDER BY row_id"""
    def autoBigintToTinyintResult = sql """ ${autoBigintToTinyintSql} """
    assertRows([[13]], autoBigintToTinyintResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToTinyintSql))

    sql """ clean all profile """
    def autoBigintToTinyintInSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) IN (13)
ORDER BY row_id"""
    def autoBigintToTinyintInResult = sql """ ${autoBigintToTinyintInSql} """
    assertRows([[13]], autoBigintToTinyintInResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToTinyintInSql))

    def tinyintNotEqExpectedRows = [
            [0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12],
            [14], [15], [16], [17], [18], [19]
    ]

    sql """ clean all profile """
    def autoBigintToTinyintNotEqSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) != 13
ORDER BY row_id"""
    def autoBigintToTinyintNotEqResult = sql """ ${autoBigintToTinyintNotEqSql} """
    assertRows(tinyintNotEqExpectedRows, autoBigintToTinyintNotEqResult)

    sql """ clean all profile """
    def autoBigintToTinyintNotInSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) NOT IN (13)
ORDER BY row_id"""
    def autoBigintToTinyintNotInResult = sql """ ${autoBigintToTinyintNotInSql} """
    assertRows(tinyintNotEqExpectedRows, autoBigintToTinyintNotInResult)

    sql """ clean all profile """
    def autoBigintToTinyintPositiveOverflowSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) = 128
ORDER BY row_id"""
    def autoBigintToTinyintPositiveOverflowResult =
            sql """ ${autoBigintToTinyintPositiveOverflowSql} """
    assertRows([], autoBigintToTinyintPositiveOverflowResult)

    sql """ clean all profile """
    def autoBigintToTinyintNegativeOverflowSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as tinyint) = -129
ORDER BY row_id"""
    def autoBigintToTinyintNegativeOverflowResult =
            sql """ ${autoBigintToTinyintNegativeOverflowSql} """
    assertRows([], autoBigintToTinyintNegativeOverflowResult)

    sql """ clean all profile """
    def autoBigintToSmallintSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as smallint) = 13"""
    def autoBigintToSmallintResult = sql """ ${autoBigintToSmallintSql} """
    assertRows([[13]], autoBigintToSmallintResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToSmallintSql))

    sql """ clean all profile """
    def autoBigintToBigintSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as bigint) = 13"""
    def autoBigintToBigintResult = sql """ ${autoBigintToBigintSql} """
    assertRows([[13]], autoBigintToBigintResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToBigintSql))

    sql """ clean all profile """
    // BIGINT -> BIGINT cast pushdown was already supported on old master. Keep this
    // case as a regression guard so the existing same-type large-value index path
    // is not broken while fixing cross-width cast pushdown.
    def autoBigintLargeValueSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["bigint_key"] as bigint) = 1300000000013"""
    def autoBigintLargeValueResult = sql """ ${autoBigintLargeValueSql} """
    assertRows([[13]], autoBigintLargeValueResult)
    assertIndexFilterUsed(fetchProfileText(autoBigintLargeValueSql))

    sql """ clean all profile """
    def autoBigintToLargeintSql = """SELECT row_id
FROM test_variant_inverted_index_cast_auto_storage
WHERE cast(v["int_key"] as largeint) = 13"""
    def autoBigintToLargeintResult = sql """ ${autoBigintToLargeintSql} """
    assertRows([[13]], autoBigintToLargeintResult)
    assertIndexFilterHit(fetchProfileText(autoBigintToLargeintSql))

    sql """ DROP TABLE IF EXISTS test_variant_inverted_index_cast_bool_storage """
    sql """
        CREATE TABLE test_variant_inverted_index_cast_bool_storage (
            row_id BIGINT,
            v VARIANT COMMENT 'auto inferred variant',
            INDEX idx_v(v) USING INVERTED COMMENT ''
        )
        ENGINE=OLAP
        DUPLICATE KEY(row_id)
        DISTRIBUTED BY HASH(row_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "v2"
        )
    """

    sql """
        INSERT INTO test_variant_inverted_index_cast_bool_storage VALUES
            (1, '{"bool_key": true}'),
            (2, '{"bool_key": false}')
    """

    sql """ clean all profile """
    def boolCastSql = """SELECT row_id
FROM test_variant_inverted_index_cast_bool_storage
WHERE cast(v["bool_key"] as boolean)"""
    def boolCastResult = sql """ ${boolCastSql} """
    assertRows([[1]], boolCastResult)
    assertIndexFilterHit(fetchProfileText(boolCastSql))

    sql """ set enable_profile = false """
}
