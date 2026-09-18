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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.spi.Split;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * FIX-M1 — guards {@link PluginDrivenScanNode#sampleSplits}, the pure, connector-agnostic TABLESAMPLE
 * split selector that restores the sampling lost when Hive flipped onto the plugin SPI.
 *
 * <p><b>Why this matters:</b> post-cutover, TABLESAMPLE was silently dropped for plugin-driven external
 * tables — {@code SELECT ... TABLESAMPLE(N)} returned the FULL table. The fix re-applies sampling in
 * {@code getSplits}, but ONLY for connectors that declare {@code supportsTableSample()} (their ranges carry
 * byte lengths); this test pins the size-weighted selection itself. Getting the accumulation wrong
 * re-introduces the silent full-table bug (returning every split) or over-truncates (dropping rows the
 * sample should keep). The helper is invoked only with positive-length splits (the capability gate lives in
 * getSplits and is covered by live e2e — connectors whose ranges report -1 must NOT opt in).</p>
 */
public class PluginDrivenScanNodeTableSampleTest {

    /** A split whose byte length is {@code len} (the only property sampleSplits reads). */
    private static Split split(long len) {
        Split s = Mockito.mock(Split.class);
        Mockito.when(s.getLength()).thenReturn(len);
        return s;
    }

    /** {@code count} splits, each {@code len} bytes. */
    private static List<Split> uniform(int count, long len) {
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(split(len));
        }
        return splits;
    }

    private static long totalLen(List<Split> splits) {
        long t = 0;
        for (Split s : splits) {
            t += s.getLength();
        }
        return t;
    }

    @Test
    public void testPercentFullSelectsAll() {
        // 100 PERCENT -> sampleSize == totalSize -> every split is selected (no reduction).
        List<Split> splits = uniform(10, 10L);
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(splits, new TableSample(true, 100L, 0L), 0L);
        Assertions.assertEquals(10, sampled.size());
    }

    @Test
    public void testPercentHalfIsMinimalPrefixOverTarget() {
        // 50 PERCENT of 10x10-byte splits: sampleSize = 50 -> exactly 5 splits (5*10 >= 50, 4*10 < 50).
        // Pins the accumulate-until-(>=sampleSize) boundary: an off-by-one (> vs >=, or pre/post-increment)
        // changes the count. Split sizes are uniform, so the count is shuffle-independent.
        List<Split> splits = uniform(10, 10L);
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(splits, new TableSample(true, 50L, 0L), 0L);
        Assertions.assertEquals(5, sampled.size());
        // The selection is a strict reduction of the full set (this is the property the silent-drop bug violated).
        Assertions.assertTrue(sampled.size() < 10);
        // Minimal prefix: selected size >= target, and dropping the last selected split would fall under it.
        long selected = totalLen(sampled);
        Assertions.assertTrue(selected >= 50L);
        Assertions.assertTrue(selected - 10L < 50L);
    }

    @Test
    public void testRowsModeUsesEstimatedRowSize() {
        // ROWS sampling: sampleSize = estimatedRowSize * rows = 8 * 3 = 24 -> 3 splits of 10 bytes
        // (10,20,30; 30 >= 24 at index 3). Pins that ROWS mode consults estimatedRowSize (not totalSize).
        List<Split> splits = uniform(10, 10L);
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(splits, new TableSample(false, 3L, 0L), 8L);
        Assertions.assertEquals(3, sampled.size());
    }

    @Test
    public void testSampleLargerThanTableSelectsAll() {
        // A sample target exceeding the whole table returns every split (never NPEs / over-reads).
        List<Split> splits = uniform(5, 10L);
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(splits, new TableSample(false, 1_000_000L, 0L), 1000L);
        Assertions.assertEquals(5, sampled.size());
    }

    @Test
    public void testEmptySplitsReturnEmpty() {
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(new ArrayList<>(), new TableSample(true, 10L, 0L), 8L);
        Assertions.assertTrue(sampled.isEmpty());
    }

    @Test
    public void testRepeatableSeekIsDeterministic() {
        // REPEATABLE <seek> seeds the shuffle: two runs of the SAME query (same seek) select the SAME subset.
        // Distinct split sizes make the selection shuffle-sensitive, so this actually exercises seed determinism
        // (uniform sizes would pass trivially). MUTATION: an unseeded / time-seeded shuffle -> flaky subset -> red.
        List<Split> base = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            base.add(split(i)); // sizes 1..10, total 55
        }
        // Copies preserve element identity + initial order, so same-seed shuffles produce the same permutation.
        List<Split> run1 = PluginDrivenScanNode.sampleSplits(new ArrayList<>(base), new TableSample(true, 30L, 7L), 0L);
        List<Split> run2 = PluginDrivenScanNode.sampleSplits(new ArrayList<>(base), new TableSample(true, 30L, 7L), 0L);
        Assertions.assertEquals(run1, run2);
        // And it is a real sample, not the whole table (30% target).
        Assertions.assertTrue(run1.size() < base.size());
    }
}
