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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PaimonCacheSizeBenchmarkTest {
    @Test
    void weightedFixturePreservesProductionProjectionAndPreparedEstimate() {
        PaimonCacheSizeBenchmark.Fixture fixture = new PaimonCacheSizeBenchmark.Fixture(
                10, 32, PaimonCacheSizeBenchmark.Distribution.UNIFORM);
        List<ConnectorPartitionInfo> baseline = fixture.load(false);
        PaimonPartitionView weighted = (PaimonPartitionView) fixture.load(true);
        Assertions.assertEquals(32, baseline.size());
        Assertions.assertEquals(baseline.size(), weighted.size());
        for (int index = 0; index < baseline.size(); index++) {
            Assertions.assertEquals(baseline.get(index).getPartitionName(), weighted.get(index).getPartitionName());
            Assertions.assertEquals(baseline.get(index).getPartitionValues(), weighted.get(index).getPartitionValues());
        }
        Assertions.assertEquals("region=region-0", weighted.get(0).getPartitionName());
        Assertions.assertTrue(weighted.getSizeEstimate().isComplete());
        Assertions.assertTrue(weighted.getSizeEstimate().getBytes() > 0);
        Assertions.assertSame(weighted.getSizeEstimate(),
                PaimonPartitionViewSizeEstimator.estimateEntry(fixture.key, weighted));
        Assertions.assertNotSame(baseline, fixture.load(false), "baseline must rebuild rather than hit a cache");
    }

    @Test
    void longTailFixturesCoverSampledAndUnsampledPositions() {
        for (PaimonCacheSizeBenchmark.Distribution distribution : new PaimonCacheSizeBenchmark.Distribution[] {
                PaimonCacheSizeBenchmark.Distribution.TAIL_END,
                PaimonCacheSizeBenchmark.Distribution.TAIL_INTERIOR}) {
            List<ConnectorPartitionInfo> partitions = new PaimonCacheSizeBenchmark.Fixture(
                    100, 1_000, distribution).load(false);
            int largeIndex = distribution == PaimonCacheSizeBenchmark.Distribution.TAIL_END ? 999 : 998;
            Assertions.assertTrue(partitions.get(largeIndex).getPartitionValues().get("region").length()
                    >= PaimonCacheSizeBenchmark.Fixture.LARGE_VALUE_CHARS);
            for (int samples : new int[] {5, 16}) {
                boolean sampled = false;
                for (int sample = 0; sample < samples; sample++) {
                    sampled |= sample * (partitions.size() - 1) / (samples - 1) == largeIndex;
                }
                Assertions.assertEquals(distribution == PaimonCacheSizeBenchmark.Distribution.TAIL_END, sampled);
            }
        }
    }

    @Test
    void longTailWeightIsIndependentOfPositionAndNotExtrapolated() {
        for (int count : new int[] {1_000, 10_000}) {
            PaimonCacheSizeBenchmark.Fixture fixture = new PaimonCacheSizeBenchmark.Fixture(
                    10, count, PaimonCacheSizeBenchmark.Distribution.UNIFORM);
            PaimonPartitionView uniform = (PaimonPartitionView) fixture.load(true);
            PaimonPartitionView interior = (PaimonPartitionView) new PaimonCacheSizeBenchmark.Fixture(
                    10, count, PaimonCacheSizeBenchmark.Distribution.TAIL_INTERIOR).load(true);
            PaimonPartitionView end = (PaimonPartitionView) new PaimonCacheSizeBenchmark.Fixture(
                    10, count, PaimonCacheSizeBenchmark.Distribution.TAIL_END).load(true);
            Assertions.assertTrue(interior.getSizeEstimate().isComplete());
            Assertions.assertTrue(end.getSizeEstimate().isComplete());
            // One value String shared by the list/map, plus a separately materialized partition-name String.
            ConnectorPartitionInfo small = uniform.get(count - 2);
            ConnectorPartitionInfo large = interior.get(count - 2);
            long expectedGrowth = JvmSizeUtils.stringSize(large.getPartitionName())
                    - JvmSizeUtils.stringSize(small.getPartitionName())
                    + JvmSizeUtils.stringSize(large.getOrderedPartitionValues().get(0))
                    - JvmSizeUtils.stringSize(small.getOrderedPartitionValues().get(0));
            Assertions.assertTrue(expectedGrowth >= 2L * PaimonCacheSizeBenchmark.Fixture.LARGE_VALUE_CHARS);
            Assertions.assertEquals(expectedGrowth,
                    interior.getSizeEstimate().getBytes() - uniform.getSizeEstimate().getBytes());
            Assertions.assertEquals(interior.getSizeEstimate().getBytes(), end.getSizeEstimate().getBytes());
            List<ConnectorPartitionInfo> reordered = new ArrayList<>(interior);
            Collections.swap(reordered, 0, count - 2);
            Assertions.assertEquals(interior.getSizeEstimate().getBytes(),
                    new PaimonPartitionView(fixture.key, reordered).getSizeEstimate().getBytes());
        }
    }

    @Test
    void unicodeAndMultipleColumnsAreCountedWithoutDoubleChargingMapValues() {
        PaimonCacheSizeBenchmark.Fixture fixture = new PaimonCacheSizeBenchmark.Fixture(
                10, 0, PaimonCacheSizeBenchmark.Distribution.UNIFORM);
        for (int columns : new int[] {1, 4, 16}) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int column = 0; column < columns; column++) {
                values.put("column-" + column, "small-" + column);
            }
            ConnectorPartitionInfo small = partition(values);
            Map<String, String> largeValues = new LinkedHashMap<>(values);
            largeValues.put("column-0", "中".repeat(65_536));
            ConnectorPartitionInfo large = partition(largeValues);
            long before = new PaimonPartitionView(fixture.key, List.of(small)).getSizeEstimate().getBytes();
            long after = new PaimonPartitionView(fixture.key, List.of(large)).getSizeEstimate().getBytes();
            Assertions.assertEquals(JvmSizeUtils.stringSize(largeValues.get("column-0"))
                    - JvmSizeUtils.stringSize(values.get("column-0")), after - before);
        }
    }

    @Test
    void projectionRemainsImmutableWithAPreparedWeight() {
        for (int count : new int[] {0, 1, 32, 1_000}) {
            PaimonCacheSizeBenchmark.Fixture fixture = new PaimonCacheSizeBenchmark.Fixture(
                    10, count, PaimonCacheSizeBenchmark.Distribution.UNIFORM);
            List<ConnectorPartitionInfo> input = fixture.load(false);
            PaimonPartitionView view = new PaimonPartitionView(fixture.key, input);
            Assertions.assertTrue(view.getSizeEstimate().isComplete());
            Assertions.assertTrue(view.getSizeEstimate().getBytes() > 0);
            Assertions.assertSame(view.getSizeEstimate(),
                    PaimonPartitionViewSizeEstimator.estimateEntry(fixture.key, view));
            input.clear();
            Assertions.assertEquals(count, view.size());
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> view.add(partition(Collections.emptyMap())));
        }
    }

    private static ConnectorPartitionInfo partition(Map<String, String> values) {
        return new ConnectorPartitionInfo("fixed-name", values, Collections.emptyMap(),
                new ArrayList<>(values.values()), new ArrayList<>(Collections.nCopies(values.size(), false)));
    }
}
