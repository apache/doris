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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.stream.BaseTableStream.StreamScanType;
import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamRowCountEstimatorTest {
    private static final long UNKNOWN = StreamRowCountEstimator.UNKNOWN_ROW_COUNT;

    private long records(long c0, long b0, long c1, long b1, long fallback) {
        return records(c0, b0, c1, b1, false, StreamScanType.APPEND_ONLY, fallback);
    }

    private long records(long c0, long b0, long c1, long b1,
            boolean mergeOnWrite, StreamScanType scanType, long fallback) {
        return StreamRowCountEstimator.estimateChangeRecords(
                100L, 200L, c0, b0, c1, b1, mergeOnWrite, scanType, () -> fallback);
    }

    @Test
    public void testBothDeltasAndTtlCompensation() {
        Assertions.assertEquals(700, records(1000, 1000, 1200, 1700, 9999));
        Assertions.assertEquals(900, records(1000, 1000, 1900, 1700, 9999));
        Assertions.assertEquals(1200, records(UNKNOWN, 1000, 1200, 1700, 9999));
        Assertions.assertEquals(200, records(1000, UNKNOWN, 1200, 1700, 9999));
        Assertions.assertEquals(700, records(1000, 1000, 500, 1700, 9999));
    }

    @Test
    public void testChangelogStockCompetesWithBaseGrowthWhenBaselineIsMissing() {
        for (StreamScanType type : StreamScanType.values()) {
            Assertions.assertEquals(1000, records(UNKNOWN, 1000, 1000, 1005, true, type, 9999));
            Assertions.assertEquals(1200, records(UNKNOWN, 1000, 1000, 2200, true, type, 9999));
            Assertions.assertEquals(20, records(UNKNOWN, 1000, 20, 900, true, type, 9999));
        }
    }

    @Test
    public void testBaseDeclineRequiresMowDeleteOutputAndUnavailableChangelogEstimate() {
        for (StreamScanType type : StreamScanType.values()) {
            boolean canUseDecline = type == StreamScanType.DETAIL || type == StreamScanType.MIN_DELTA;
            Assertions.assertEquals(canUseDecline ? 100 : 400, records(500, 1000, 400, 900, true, type, 9999));
            Assertions.assertEquals(canUseDecline ? 100 : 900, records(500, 1000, UNKNOWN, 900, true, type, 9999));
            Assertions.assertEquals(canUseDecline ? 100 : 900, records(UNKNOWN, 1000, 0, 900, true, type, 9999));
            Assertions.assertEquals(500, records(500, 1000, 500, 900, true, type, 9999));
            Assertions.assertEquals(20, records(500, 1000, 520, 900, true, type, 9999));
            Assertions.assertEquals(900, records(UNKNOWN, UNKNOWN, UNKNOWN, 900, true, type, 9999));
            Assertions.assertEquals(400, records(500, 1000, 400, 1000, true, type, 9999));
            Assertions.assertEquals(400, records(500, 1000, 400, 900, false, type, 9999));
        }
    }

    @Test
    public void testFallbackOrderWithoutAbsoluteDifferences() {
        Assertions.assertEquals(400, records(1000, 1000, 400, 800, 9999));
        Assertions.assertEquals(400, records(400, 800, 400, 800, 9999));
        Assertions.assertEquals(400, records(UNKNOWN, UNKNOWN, 400, 800, 9999));
        Assertions.assertEquals(800, records(UNKNOWN, UNKNOWN, UNKNOWN, 800, 9999));
        Assertions.assertEquals(9999, records(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, 9999));
        Assertions.assertEquals(UNKNOWN, records(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN));
        Assertions.assertEquals(UNKNOWN, records(0, 0, 0, 0, 0));
    }

    @Test
    public void testOnlyResolvedOffsetsProveEmpty() {
        Assertions.assertEquals(0,
                StreamRowCountEstimator.estimateChangeRecords(100L, 100L, UNKNOWN, 1000, 800, 800,
                        true, StreamScanType.DETAIL, () -> 9999));
        Assertions.assertEquals(0,
                StreamRowCountEstimator.estimateChangeRecords(110L, 100L, 0, 0, 800, 800,
                        false, StreamScanType.APPEND_ONLY, () -> 9999));
        Assertions.assertEquals(800,
                StreamRowCountEstimator.estimateChangeRecords(null, 200L, 100, 100, 800, 900,
                        false, StreamScanType.APPEND_ONLY, () -> 9999));
        Assertions.assertEquals(800,
                StreamRowCountEstimator.estimateChangeRecords(100L, null, 1000, 1000, 800, 900,
                        true, StreamScanType.DETAIL, () -> 9999));
    }

    @Test
    public void testOutputExpansionUsesActualScanTypeOnce() {
        for (BaseTableStream.StreamScanType type : BaseTableStream.StreamScanType.values()) {
            Assertions.assertEquals(300, StreamRowCountEstimator.outputRows(300, false, type));
            Assertions.assertEquals(type == BaseTableStream.StreamScanType.APPEND_ONLY ? 300 : 600,
                    StreamRowCountEstimator.outputRows(300, true, type));
            Assertions.assertEquals(0, StreamRowCountEstimator.outputRows(0, true, type));
            Assertions.assertEquals(UNKNOWN, StreamRowCountEstimator.outputRows(UNKNOWN, true, type));
        }
    }

    @Test
    public void testSnapshotPrefersConsumptionBaselineIncludingZero() {
        Assertions.assertEquals(500, StreamRowCountEstimator.estimateSnapshotRead(true, 500, 700, 300, () -> 9999));
        Assertions.assertEquals(800, StreamRowCountEstimator.estimateSnapshotRead(false, 800, 700, 300, () -> 9999));
        Assertions.assertEquals(0, StreamRowCountEstimator.estimateSnapshotRead(true, 0, 700, 300, () -> 9999));
        Assertions.assertEquals(700, StreamRowCountEstimator.estimateSnapshotRead(true, UNKNOWN, 700, 300, () -> 9999));
        Assertions.assertEquals(1000,
                StreamRowCountEstimator.estimateSnapshotRead(false, UNKNOWN, 700, 300, () -> 9999));
        Assertions.assertEquals(700,
                StreamRowCountEstimator.estimateSnapshotRead(false, UNKNOWN, 700, UNKNOWN, () -> 9999));
        Assertions.assertEquals(9999,
                StreamRowCountEstimator.estimateSnapshotRead(false, UNKNOWN, UNKNOWN, 300, () -> 9999));
        Assertions.assertEquals(UNKNOWN,
                StreamRowCountEstimator.estimateSnapshotRead(false, UNKNOWN, UNKNOWN, 300, () -> UNKNOWN));
    }

    @Test
    public void testRawCountAndReportAvailability() {
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        Assertions.assertEquals(UNKNOWN, StreamRowCountEstimator.rowCount(index));
        index.setRowCount(0);
        Assertions.assertEquals(UNKNOWN, StreamRowCountEstimator.rowCount(index));
        index.setRowCount(50);
        Assertions.assertEquals(50, StreamRowCountEstimator.rowCount(index));
        Assertions.assertFalse(index.getRowCountReported());
        index.setRowCountReported(true);
        index.setRowCount(0);
        Assertions.assertEquals(0, StreamRowCountEstimator.rowCount(index));
        index.setRowCount(UNKNOWN);
        Assertions.assertEquals(UNKNOWN, StreamRowCountEstimator.rowCount(index));
        Assertions.assertEquals(UNKNOWN, StreamRowCountEstimator.rowCount(null));
    }

    @Test
    public void testBaselineMatchesConsumedOrHistoricalOffset() {
        PartitionStreamState consumed = PartitionStreamState.restored(100L, 1L, null, 50L, 20L);
        Assertions.assertTrue(consumed.matchesOffset(100L));
        Assertions.assertFalse(consumed.matchesOffset(200L));
        Assertions.assertFalse(consumed.matchesOffset(null));

        PartitionStreamState initialSnapshot = consumed.atHistoricalTso(200L);
        Assertions.assertTrue(initialSnapshot.matchesOffset(200L));
        Assertions.assertFalse(initialSnapshot.matchesOffset(100L));
        Assertions.assertFalse(initialSnapshot.matchesOffset(null));
    }
}
