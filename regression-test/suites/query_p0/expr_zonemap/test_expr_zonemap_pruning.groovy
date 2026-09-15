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
    sql """ set enable_expr_zonemap_filter = true """
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

    sql """ DROP TABLE IF EXISTS test_monotonic_zonemap """
    sql """
        CREATE TABLE test_monotonic_zonemap (
            id INT,
            v VARCHAR(512),
            dt DATETIMEV2,
            payload VARCHAR(512),
            INDEX idx_id (id) USING INVERTED
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
        INSERT INTO test_monotonic_zonemap
        SELECT CAST(number AS INT),
               CONCAT('a_', LPAD(CAST(number AS STRING), 5, '0')),
               seconds_add(CAST('2026-07-28 00:00:00' AS DATETIMEV2), number),
               REPEAT('x', 256)
        FROM numbers("number" = "4096")
    """
    sql """ sync """
    sql """
        INSERT INTO test_monotonic_zonemap
        SELECT CAST(number + 4096 AS INT),
               CONCAT('m_', LPAD(CAST(number AS STRING), 5, '0')),
               seconds_add(CAST('2026-08-28 00:00:00' AS DATETIMEV2), number),
               REPEAT('x', 256)
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    sql """ set enable_expr_zonemap_filter = false """
    sql """ set enable_count_on_index_pushdown = true """
    explain {
        sql """
            SELECT COUNT(*) FROM test_monotonic_zonemap
            WHERE date_trunc(dt, 'day') = '2026-07-28 00:00:00'
        """
        contains "pushAggOp=COUNT_ON_INDEX"
    }

    def inferredPrefixRangeToken = "expr_zonemap_pruning_inferred_prefix_range_" + UUID.randomUUID().toString()
    def inferredPrefixRangeRows = sql """
        SELECT '${inferredPrefixRangeToken}', COUNT(*) FROM test_monotonic_zonemap
        WHERE substring(v, 1, 1) = 'a'
    """
    assertEquals(4096L, inferredPrefixRangeRows[0][1] as long)
    assertProfileCounterPositive(inferredPrefixRangeToken, "RowsStatsFiltered")

    def inferredDateRangeToken = "expr_zonemap_pruning_inferred_date_range_" + UUID.randomUUID().toString()
    def inferredDateRangeRows = sql """
        SELECT '${inferredDateRangeToken}', COUNT(*) FROM test_monotonic_zonemap
        WHERE date_trunc(dt, 'day') = '2026-07-28 00:00:00'
    """
    assertEquals(4096L, inferredDateRangeRows[0][1] as long)
    assertProfileCounterPositive(inferredDateRangeToken, "RowsStatsFiltered")

    def inferredDateFormatToken = "expr_zonemap_pruning_inferred_date_format_" + UUID.randomUUID().toString()
    def inferredDateFormatRows = sql """
        SELECT '${inferredDateFormatToken}', COUNT(*) FROM test_monotonic_zonemap
        WHERE date_format(dt, '%Y-%m-%d') >= '2026-08-01'
    """
    assertEquals(4096L, inferredDateFormatRows[0][1] as long)
    assertProfileCounterPositive(inferredDateFormatToken, "RowsStatsFiltered")

    def inferredDateFormatEqualityToken =
            "expr_zonemap_pruning_inferred_date_format_equality_" + UUID.randomUUID().toString()
    def inferredDateFormatEqualityRows = sql """
        SELECT '${inferredDateFormatEqualityToken}', COUNT(*) FROM test_monotonic_zonemap
        WHERE date_format(dt, '%Y-%m-%d') = '2026-07-28'
    """
    assertEquals(4096L, inferredDateFormatEqualityRows[0][1] as long)
    assertProfileCounterPositive(inferredDateFormatEqualityToken, "RowsStatsFiltered")

    sql """ set enable_count_on_index_pushdown = false """
    explain {
        sql """
            SELECT COUNT(*) FROM test_monotonic_zonemap
            WHERE date_trunc(dt, 'day') = '2026-07-28 00:00:00'
        """
        contains "pushAggOp=NONE"
    }
    def inferredDirectScanToken =
            "expr_zonemap_pruning_inferred_direct_scan_" + UUID.randomUUID().toString()
    def inferredDirectScanRows = sql """
        SELECT '${inferredDirectScanToken}', COUNT(*) FROM test_monotonic_zonemap
        WHERE date_trunc(dt, 'day') = '2026-07-28 00:00:00'
    """
    assertEquals(4096L, inferredDirectScanRows[0][1] as long)
    assertProfileCounterPositive(inferredDirectScanToken, "RowsStatsFiltered")

    def largeQuarterPeriodRows = sql """
        SELECT COUNT(*) FROM test_monotonic_zonemap
        WHERE quarter_floor(dt, 1431655766) = '0001-01-01 00:00:00'
    """
    assertEquals(8192L, largeQuarterPeriodRows[0][0] as long)
    sql """ set enable_count_on_index_pushdown = true """
    sql """ set enable_expr_zonemap_filter = true """

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

    // Column-vs-column comparisons. A predicate over two columns of the same table never became a
    // ColumnPredicate, so it reaches the scanner as a common expression and is evaluated against the
    // segment zone map of both slots at once.
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_two_columns """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_two_columns (
            id INT,
            lo INT,
            hi INT,
            alt INT,
            expected INT,
            actual INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    // lo lands in [0, 4095] and hi in [10000, 14095], so the two ranges are fully separated. alt
    // lands in [0, 7095] and equals lo on even rows and lo + 3000 on odd ones, so lo vs alt cannot
    // be decided from the bounds and every row has to be evaluated. expected and actual are both
    // the constant 7.
    sql """
        INSERT INTO test_expr_zonemap_pruning_two_columns
        SELECT CAST(number AS INT),
               CAST(number AS INT),
               CAST(number + 10000 AS INT),
               IF(number % 2 = 0, CAST(number AS INT), CAST(number + 3000 AS INT)),
               7,
               7
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    // Runs the same query with expr zonemap pruning on and off and asserts the two agree. The
    // counter only shows that pruning fired; this is what shows it fired correctly.
    def assertSameWithAndWithoutPruning = { String predicate ->
        sql """ set enable_expr_zonemap_filter = false """
        def withoutPruning = sql """
            SELECT COUNT(*) FROM test_expr_zonemap_pruning_two_columns WHERE ${predicate}
        """
        sql """ set enable_expr_zonemap_filter = true """
        def withPruning = sql """
            SELECT COUNT(*) FROM test_expr_zonemap_pruning_two_columns WHERE ${predicate}
        """
        assertEquals(withoutPruning[0][0] as long, withPruning[0][0] as long)
        return withPruning[0][0] as long
    }

    def assertTwoColumnPruned = { String predicate, String label ->
        def token = "expr_zonemap_pruning_two_columns_" + label + "_" + UUID.randomUUID().toString()
        def rows = sql """
            SELECT '${token}', COUNT(*) FROM test_expr_zonemap_pruning_two_columns
            WHERE ${predicate}
        """
        assertEquals(0L, rows[0][1] as long)
        assertExprZonemapPruned(token)
        assertEquals(0L, assertSameWithAndWithoutPruning(predicate))
    }

    // lo > hi and lo >= hi: rejected because min(hi) is already above max(lo).
    assertTwoColumnPruned("lo > hi", "gt")
    assertTwoColumnPruned("lo >= hi", "ge")
    // hi < lo and hi <= lo: the mirrored rules.
    assertTwoColumnPruned("hi < lo", "lt")
    assertTwoColumnPruned("hi <= lo", "le")
    // lo = hi: the ranges are disjoint, so no row can be equal.
    assertTwoColumnPruned("lo = hi", "eq")
    // expected != actual: both columns collapse to the single value 7, which is the only shape that
    // lets != prune.
    assertTwoColumnPruned("expected != actual", "ne")

    // Overlapping ranges must survive, and the row counts must be exact. These are the cases that
    // catch a rule reading the wrong end of a range: lo in [0, 4095] against alt in [0, 7095] cannot
    // be separated by the bounds, so all of these have to fall through to per-row evaluation.
    assertEquals(2048L, assertSameWithAndWithoutPruning("lo < alt"))
    assertEquals(2048L, assertSameWithAndWithoutPruning("lo != alt"))
    assertEquals(2048L, assertSameWithAndWithoutPruning("lo = alt"))
    assertEquals(4096L, assertSameWithAndWithoutPruning("lo <= alt"))
    assertEquals(0L, assertSameWithAndWithoutPruning("lo > alt"))
    assertEquals(4096L, assertSameWithAndWithoutPruning("lo >= alt - 3000"))
    // A cast on either side is rejected by the capability gate, so this must still return the right
    // answer rather than being pruned on raw bounds.
    assertEquals(4096L, assertSameWithAndWithoutPruning("lo < CAST(hi AS BIGINT)"))

    // One side partially NULL. min/max summarize the non-null values only, and a NULL row makes the
    // comparison NULL, which never satisfies the conjunct, so the separated ranges still prune.
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_two_columns_nulls """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_two_columns_nulls (
            id INT,
            lo INT,
            hi INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_two_columns_nulls
        SELECT CAST(number AS INT),
               IF(number % 8 = 0, NULL, CAST(number AS INT)),
               CAST(number + 10000 AS INT)
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    def twoColumnNullToken =
            "expr_zonemap_pruning_two_columns_null_" + UUID.randomUUID().toString()
    def twoColumnNullRows = sql """
        SELECT '${twoColumnNullToken}', COUNT(*) FROM test_expr_zonemap_pruning_two_columns_nulls
        WHERE lo > hi
    """
    assertEquals(0L, twoColumnNullRows[0][1] as long)
    assertExprZonemapPruned(twoColumnNullToken)

    // A column with no non-null value at all makes the comparison NULL on every row.
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_two_columns_all_null """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_two_columns_all_null (
            id INT,
            lo INT,
            hi INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_two_columns_all_null
        SELECT CAST(number AS INT), NULL, CAST(number AS INT)
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    def allNullToken =
            "expr_zonemap_pruning_two_columns_all_null_" + UUID.randomUUID().toString()
    def allNullRows = sql """
        SELECT '${allNullToken}', COUNT(*) FROM test_expr_zonemap_pruning_two_columns_all_null
        WHERE lo < hi
    """
    assertEquals(0L, allNullRows[0][1] as long)
    assertExprZonemapPruned(allNullToken)

    // Same as assertExprZonemapPruned but returns the count, so a case can pin the exact number of
    // segments that had to be dropped instead of only that pruning happened at all.
    def filteredSegmentsOf = { String token ->
        long filteredSegments = 0
        for (int retry = 0; retry < 20; ++retry) {
            String profile = getProfileByToken(token).toString()
            filteredSegments = counterSum(profile, "ExprZoneMapFilteredSegments")
            if (filteredSegments > 0) {
                return filteredSegments
            }
            Thread.sleep(500)
        }
        return filteredSegments
    }

    // Three loads, one segment each, each segment holding a constant in both columns. This is the
    // shape where a cross-column rule has to decide per segment rather than per table, so the exact
    // number of dropped segments is the assertion that matters.
    //
    //   batch | a | b | a != b        | a = b
    //   1     | a | a | both collapse | overlap, keep
    //   2     | a | b | disjoint,keep | disjoint, drop
    //   3     | b | b | both collapse | overlap, keep
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_per_segment """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_per_segment (
            id INT,
            a VARCHAR(32),
            b VARCHAR(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_per_segment
        SELECT CAST(number AS INT), 'a', 'a' FROM numbers("number" = "1024")
    """
    sql """ sync """
    sql """
        INSERT INTO test_expr_zonemap_pruning_per_segment
        SELECT CAST(number AS INT), 'a', 'b' FROM numbers("number" = "1024")
    """
    sql """ sync """
    sql """
        INSERT INTO test_expr_zonemap_pruning_per_segment
        SELECT CAST(number AS INT), 'b', 'b' FROM numbers("number" = "1024")
    """
    sql """ sync """

    def perSegmentNeToken = "expr_zonemap_pruning_per_segment_ne_" + UUID.randomUUID().toString()
    def perSegmentNeRows = sql """
        SELECT '${perSegmentNeToken}', COUNT(*) FROM test_expr_zonemap_pruning_per_segment
        WHERE a != b
    """
    assertEquals(1024L, perSegmentNeRows[0][1] as long)
    assertEquals(2L, filteredSegmentsOf(perSegmentNeToken))

    def perSegmentEqToken = "expr_zonemap_pruning_per_segment_eq_" + UUID.randomUUID().toString()
    def perSegmentEqRows = sql """
        SELECT '${perSegmentEqToken}', COUNT(*) FROM test_expr_zonemap_pruning_per_segment
        WHERE a = b
    """
    assertEquals(2048L, perSegmentEqRows[0][1] as long)
    assertEquals(1L, filteredSegmentsOf(perSegmentEqToken))

    sql """ set enable_expr_zonemap_filter = false """
    def perSegmentNeWithout = sql """
        SELECT COUNT(*) FROM test_expr_zonemap_pruning_per_segment WHERE a != b
    """
    def perSegmentEqWithout = sql """
        SELECT COUNT(*) FROM test_expr_zonemap_pruning_per_segment WHERE a = b
    """
    sql """ set enable_expr_zonemap_filter = true """
    assertEquals(1024L, perSegmentNeWithout[0][0] as long)
    assertEquals(2048L, perSegmentEqWithout[0][0] as long)

    // The single-slot path on the same table, to show that widening the zonemap capability gate for
    // two-slot shapes did not disturb the one-slot one it shares a gate with. It has to be a
    // predicate that expression zone maps actually see: `a = 'zzz'` is turned into a ColumnPredicate
    // and pruned by the ordinary olap path instead, which leaves ExprZoneMapFilteredSegments at 0.
    def literalStillPrunesToken =
            "expr_zonemap_pruning_per_segment_literal_" + UUID.randomUUID().toString()
    def literalStillPrunesRows = sql """
        SELECT '${literalStillPrunesToken}', COUNT(*) FROM test_expr_zonemap_pruning_per_segment
        WHERE starts_with(a, 'z')
    """
    assertEquals(0L, literalStillPrunesRows[0][1] as long)
    assertEquals(3L, filteredSegmentsOf(literalStillPrunesToken))

    // And the ColumnPredicate shape still returns the right answer.
    def columnPredicateRows = sql """
        SELECT COUNT(*) FROM test_expr_zonemap_pruning_per_segment WHERE a = 'zzz'
    """
    assertEquals(0L, columnPredicateRows[0][0] as long)

    // Two DOUBLE columns on a native table. Unlike Parquet, the segment writer records NaN presence
    // in the zone map, so a float column with no NaN in it prunes normally.
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_two_columns_double """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_two_columns_double (
            id INT,
            lo DOUBLE,
            hi DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_two_columns_double
        SELECT CAST(number AS INT), CAST(number AS DOUBLE), CAST(number + 10000 AS DOUBLE)
        FROM numbers("number" = "4096")
    """
    sql """ sync """

    def doubleToken = "expr_zonemap_pruning_two_columns_double_" + UUID.randomUUID().toString()
    def doubleRows = sql """
        SELECT '${doubleToken}', COUNT(*) FROM test_expr_zonemap_pruning_two_columns_double
        WHERE lo > hi
    """
    assertEquals(0L, doubleRows[0][1] as long)
    assertExprZonemapPruned(doubleToken)

    // NaN and infinities make the bounds unusable for range pruning, so these cases only have to
    // return the right rows. has_nan / has_positive_inf / has_negative_inf are what stops them.
    sql """ DROP TABLE IF EXISTS test_expr_zonemap_pruning_two_columns_nan """
    sql """
        CREATE TABLE test_expr_zonemap_pruning_two_columns_nan (
            id INT,
            lo DOUBLE,
            hi DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_expr_zonemap_pruning_two_columns_nan VALUES
            (1, CAST('NaN' AS DOUBLE), 1.0),
            (2, 1.0, CAST('NaN' AS DOUBLE)),
            (3, CAST('infinity' AS DOUBLE), 1.0),
            (4, CAST('-infinity' AS DOUBLE), 1.0),
            (5, 1.0, 2.0),
            (6, 2.0, 1.0)
    """
    sql """ sync """

    def assertSameOnNanTable = { String predicate ->
        sql """ set enable_expr_zonemap_filter = false """
        def without = sql """
            SELECT COUNT(*) FROM test_expr_zonemap_pruning_two_columns_nan WHERE ${predicate}
        """
        sql """ set enable_expr_zonemap_filter = true """
        def with_ = sql """
            SELECT COUNT(*) FROM test_expr_zonemap_pruning_two_columns_nan WHERE ${predicate}
        """
        assertEquals(without[0][0] as long, with_[0][0] as long)
        return with_[0][0] as long
    }

    // Doris orders NaN above every other value and treats NaN = NaN as true. So lo > hi holds for
    // rows 1, 3 and 6; lo < hi for rows 2, 4 and 5; and no row has the two columns equal.
    assertEquals(3L, assertSameOnNanTable("lo > hi"))
    assertEquals(6L, assertSameOnNanTable("lo != hi"))
    assertEquals(0L, assertSameOnNanTable("lo = hi"))
    assertEquals(3L, assertSameOnNanTable("lo < hi"))
}
