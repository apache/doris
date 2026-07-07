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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorTableStatistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveConnectorMetadata#getTableStatistics} (§4.2 read-side SPI, layers 1+2).
 *
 * <p>WHY: without this override the connector inherits {@code ConnectorStatisticsOps}'s
 * {@code Optional.empty()}, so every flipped hive table reports row count -1 (UNKNOWN) and the Nereids
 * cost model collapses cardinality to 1 (join-reorder disabled). The connector surfaces two RAW metastore
 * facts and does NO Doris-type math: the exact {@code numRows} row count, and the on-disk {@code totalSize}
 * data size (fe-core turns a size-without-count into an estimated row count). These assertions pin the
 * legacy {@code StatisticsUtil.getHiveRowCount} / {@code getRowCountFromParameters} / {@code getTotalSizeFromHMS}
 * behaviour, including two deliberate asymmetries: the spark {@code numRows} key is consulted ONLY when the
 * standard {@code numRows} is present-but-non-positive, whereas the spark {@code totalSize} key is consulted
 * when the standard {@code totalSize} is ABSENT.</p>
 */
public class HiveConnectorMetadataStatisticsTest {

    // getTableStatistics never touches the HmsClient (it reads the handle's captured parameters), so a null
    // client is sufficient and keeps the test focused on the parameter interpretation.
    private static Optional<ConnectorTableStatistics> statsOf(Map<String, String> tableParameters) {
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                null, Collections.emptyMap(), new FakeConnectorContext());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .tableParameters(tableParameters)
                .build();
        return metadata.getTableStatistics(null, handle);
    }

    private static Map<String, String> params(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void numRowsSurfacedAsExactRowCount() {
        // A positive numRows is the exact cardinality; it MUST reach the FE cost model. MUTATION:
        // inheriting the default empty -> not present -> red.
        Optional<ConnectorTableStatistics> stats = statsOf(params("numRows", "1234"));
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(1234L, stats.get().getRowCount());
    }

    @Test
    public void zeroNumRowsMapsToUnknown() {
        // Legacy gated on rows > 0; a 0 count means UNKNOWN, not a real 0 cardinality (which would corrupt
        // cost estimates). With no size either, the whole result is empty. MUTATION: dropping the >0 gate
        // (reporting rowCount 0) -> present with rowCount 0 -> red.
        Assertions.assertFalse(statsOf(params("numRows", "0")).isPresent(),
                "a 0 numRows with no size must map to UNKNOWN (empty)");
    }

    @Test
    public void sparkNumRowsUsedOnlyWhenNumRowsPresentButNonPositive() {
        // Legacy consults spark.sql.statistics.numRows as a fallback ONLY inside the "numRows key present"
        // branch, when numRows <= 0. Here numRows=0 present -> spark 500 is used.
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("numRows", "0", "spark.sql.statistics.numRows", "500"));
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(500L, stats.get().getRowCount());
    }

    @Test
    public void sparkNumRowsIgnoredWhenNumRowsKeyAbsent() {
        // WHY (legacy quirk, faithfully preserved): getRowCountFromParameters only enters the spark
        // fallback if the STANDARD numRows key is present. A table carrying ONLY the spark row-count key
        // does not surface it as a row count. MUTATION: checking spark unconditionally would return 500
        // here -> rowCount 500 -> red.
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("spark.sql.statistics.numRows", "500"));
        Assertions.assertFalse(stats.isPresent(),
                "spark numRows must be ignored when the standard numRows key is absent (legacy parity)");
    }

    @Test
    public void totalSizeSurfacedAsDataSizeWithUnknownRowCount() {
        // A table with totalSize but no numRows reports rowCount UNKNOWN(-1) + the raw dataSize; fe-core
        // performs the totalSize/rowWidth estimation (the connector must not import fe-type). MUTATION:
        // estimating in the connector, or dropping dataSize, breaks the fe-core estimate.
        Optional<ConnectorTableStatistics> stats = statsOf(params("totalSize", "4000000"));
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(-1L, stats.get().getRowCount());
        Assertions.assertEquals(4000000L, stats.get().getDataSize());
    }

    @Test
    public void sparkTotalSizeUsedWhenStandardTotalSizeAbsent() {
        // The size branch's asymmetry: the spark totalSize IS consulted when the standard totalSize key is
        // absent (contrast the numRows fallback). MUTATION: requiring the standard key present -> dataSize
        // -1 here -> red.
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("spark.sql.statistics.totalSize", "9999"));
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(9999L, stats.get().getDataSize());
    }

    @Test
    public void standardTotalSizePreferredOverSpark() {
        // When both size keys exist the standard totalSize wins (legacy: containsKey(TOTAL_SIZE) ? ... : spark).
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("totalSize", "100", "spark.sql.statistics.totalSize", "200"));
        Assertions.assertEquals(100L, stats.get().getDataSize());
    }

    @Test
    public void exactCountAndSizeBothSurfaced() {
        // numRows present AND totalSize present: both facts surface (rowCount wins downstream, dataSize is
        // still reported for callers that consume it). Legacy returned only the count, but reporting the
        // size too is harmless and lets fe-core keep both.
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("numRows", "10", "totalSize", "4096"));
        Assertions.assertEquals(10L, stats.get().getRowCount());
        Assertions.assertEquals(4096L, stats.get().getDataSize());
    }

    @Test
    public void bothAbsentReturnsEmpty() {
        Assertions.assertFalse(statsOf(params("comment", "hi")).isPresent(),
                "a table with neither a row count nor a size must report UNKNOWN (empty)");
    }

    @Test
    public void nullParametersReturnsEmpty() {
        Assertions.assertFalse(statsOf(null).isPresent());
    }

    @Test
    public void malformedNumRowsDoesNotThrowAndRecoversSparkCount() {
        // DELIBERATE DEVIATION from legacy (documented): legacy's bare Long.parseLong on a malformed numRows
        // threw, aborting the whole metastore-stat path so the query fell through to the file-list estimate.
        // The connector instead parses defensively (-1) and, because the numRows KEY is present, recovers the
        // valid spark count — strictly more useful on corrupt metadata, and it must never throw. MUTATION:
        // letting the parse propagate -> exception -> red.
        Optional<ConnectorTableStatistics> stats = Assertions.assertDoesNotThrow(() ->
                statsOf(params("numRows", "not-a-number", "spark.sql.statistics.numRows", "7")));
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(7L, stats.get().getRowCount());
    }

    @Test
    public void malformedTotalSizeDegradesToEmpty() {
        // A malformed size parses to -1; with no count either, the result is empty (not an exception).
        Optional<ConnectorTableStatistics> stats = Assertions.assertDoesNotThrow(() ->
                statsOf(params("totalSize", "garbage")));
        Assertions.assertFalse(stats.isPresent());
    }

    @Test
    public void malformedStandardTotalSizeShortCircuitsSparkKey() {
        // The size branch is an if/else-if on the STANDARD key: a present-but-malformed totalSize parses to -1
        // and must NOT fall through to the spark key (the standard key's presence wins). With no row count the
        // result is empty. MUTATION: restructuring the else-if so a malformed standard key falls to spark would
        // surface 9999 -> present -> red.
        Optional<ConnectorTableStatistics> stats = statsOf(
                params("totalSize", "garbage", "spark.sql.statistics.totalSize", "9999"));
        Assertions.assertFalse(stats.isPresent(),
                "a present (even malformed) standard totalSize must short-circuit the spark size key");
    }
}
