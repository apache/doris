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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FIX-E (explain gap) — guards {@link PluginDrivenScanNode#countNativeReadRanges} and
 * {@link PluginDrivenScanNode#resolvePushDownRowCount}, the per-scan accounting that feeds the two
 * EXPLAIN lines the legacy {@code PaimonScanNode} emitted but the SPI scan path dropped:
 * {@code paimonNativeReadSplits=<native>/<total>} and {@code pushdown agg=COUNT (n)}.
 *
 * <p><b>Why this matters (Rule 9 — tests encode WHY):</b></p>
 * <ul>
 *   <li><b>native/total accounting:</b> under {@code force_jni_scanner=true} every paimon range goes
 *       JNI ({@code isNativeReadRange()==false}), so the native numerator MUST be 0 over N total
 *       ({@code paimonNativeReadSplits=0/1} is the exact assertion in
 *       {@code test_paimon_catalog_varbinary}/{@code _timestamp_tz}). A mutation that counts all ranges
 *       as native, or that ignores {@code isNativeReadRange()}, is killed.</li>
 *   <li><b>the {@code -1} sentinel must survive:</b> a deletion-vector table emits NO precomputed count
 *       range, so the pushdown count must stay {@code -1} and render as {@code (-1)}
 *       ({@code test_paimon_deletion_vector} asserts {@code pushdown agg=COUNT (-1)}). Append/merge
 *       tables DO emit a count range carrying the merged sum (12 / 8), which must be picked up. A
 *       mutation that defaults the sentinel to 0, or that reads a count even when pushdown is inactive,
 *       is killed.</li>
 * </ul>
 */
public class PluginDrivenScanNodeExplainStatsTest {

    /** Minimal fake range: native flag + optional precomputed count, the only two getters under test. */
    private static ConnectorScanRange range(boolean nativeRead, long pushDownRowCount) {
        return new ConnectorScanRange() {
            @Override
            public ConnectorScanRangeType getRangeType() {
                return ConnectorScanRangeType.FILE_SCAN;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public boolean isNativeReadRange() {
                return nativeRead;
            }

            @Override
            public long getPushDownRowCount() {
                return pushDownRowCount;
            }
        };
    }

    private static List<ConnectorScanRange> ranges(ConnectorScanRange... rs) {
        List<ConnectorScanRange> list = new ArrayList<>();
        Collections.addAll(list, rs);
        return list;
    }

    // ==================== native/total accounting ====================

    @Test
    public void allJniRangesCountZeroNative() {
        // force_jni_scanner=true: every range is JNI -> native count 0 (the 0 in 0/1). Total is the
        // caller's ranges.size(); here the single JNI split gives 0/1, exactly the failing assertion.
        List<ConnectorScanRange> rs = ranges(range(false, -1));
        Assertions.assertEquals(0, PluginDrivenScanNode.countNativeReadRanges(rs));
        Assertions.assertEquals(1, rs.size());
    }

    @Test
    public void mixedNativeAndJniCountsOnlyNative() {
        // Native router on: a mix of native ORC/Parquet sub-splits and JNI splits -> numerator counts
        // ONLY the native ones. Kills a "count all ranges" or "count JNI" mutation.
        List<ConnectorScanRange> rs = ranges(
                range(true, -1), range(false, -1), range(true, -1), range(false, -1));
        Assertions.assertEquals(2, PluginDrivenScanNode.countNativeReadRanges(rs));
        Assertions.assertEquals(4, rs.size());
    }

    @Test
    public void emptyRangesCountZeroNative() {
        Assertions.assertEquals(0,
                PluginDrivenScanNode.countNativeReadRanges(Collections.emptyList()));
    }

    // ==================== pushdown COUNT(*) sentinel ====================

    @Test
    public void countPushdownPicksUpPrecomputedSum() {
        // Append/merge tables: the collapsed count range carries the merged sum (e.g. 12). With count
        // pushdown active it is surfaced so EXPLAIN prints "pushdown agg=COUNT (12)".
        List<ConnectorScanRange> rs = ranges(range(false, 12));
        Assertions.assertEquals(12, PluginDrivenScanNode.resolvePushDownRowCount(true, rs));
    }

    @Test
    public void countPushdownWithNoCountRangeKeepsMinusOneSentinel() {
        // THE sentinel guard: a deletion-vector table emits no precomputed-count range (every range
        // returns -1). Even with count pushdown active the result must stay -1 -> "pushdown agg=COUNT
        // (-1)" (test_paimon_deletion_vector). A mutation defaulting to 0 makes this red.
        List<ConnectorScanRange> rs = ranges(range(true, -1), range(false, -1));
        Assertions.assertEquals(-1, PluginDrivenScanNode.resolvePushDownRowCount(true, rs));
    }

    @Test
    public void noCountPushdownNeverReadsACount() {
        // A non-COUNT scan must NOT pick up a stray precomputed count even if a range happens to carry
        // one. Pins that the count is gated on countPushdown, not just on the range value.
        List<ConnectorScanRange> rs = ranges(range(false, 99));
        Assertions.assertEquals(-1, PluginDrivenScanNode.resolvePushDownRowCount(false, rs));
    }

    @Test
    public void countPushdownReturnsFirstPrecomputedCount() {
        // Only ONE collapsed count range is emitted, but guard the "first non-negative wins" contract
        // so a leading data range (-1) does not mask the trailing count range's value.
        List<ConnectorScanRange> rs = ranges(range(true, -1), range(false, 8));
        Assertions.assertEquals(8, PluginDrivenScanNode.resolvePushDownRowCount(true, rs));
    }
}
