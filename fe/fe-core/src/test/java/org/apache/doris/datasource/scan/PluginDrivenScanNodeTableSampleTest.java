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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.spi.Split;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

    @Test
    public void testPaimonJniDataSplitsUseSelfSplitWeightWhileSystemSplitsBypassSampling() {
        List<Split> jniDataSplits = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ConnectorScanRange range = Mockito.mock(ConnectorScanRange.class);
            Mockito.when(range.getPath()).thenReturn(Optional.empty());
            Mockito.when(range.getStart()).thenReturn(0L);
            Mockito.when(range.getLength()).thenReturn(-1L);
            Mockito.when(range.getFileSize()).thenReturn(-1L);
            Mockito.when(range.getModificationTime()).thenReturn(0L);
            Mockito.when(range.getHosts()).thenReturn(Collections.emptyList());
            Mockito.when(range.getPartitionValues()).thenReturn(Collections.emptyMap());
            Mockito.when(range.getPushDownRowCount()).thenReturn(-1L);
            Mockito.when(range.getSelfSplitWeight()).thenReturn(100L);
            Mockito.when(range.getTargetSplitSize()).thenReturn(1024L);
            Mockito.when(range.getProperties()).thenReturn(Collections.singletonMap("paimon.bucket", String.valueOf(i)));
            Mockito.when(range.getSplitIdentity()).thenReturn("split-" + i);
            jniDataSplits.add(new PluginDrivenSplit(range));
        }
        List<Split> sampled = PluginDrivenScanNode.sampleSplits(
                jniDataSplits, new TableSample(true, 40L, 3L), 16L);
        Assertions.assertEquals(4, sampled.size());

        // Non-DataSplit JNI system splits carry rowCount in selfSplitWeight without paimon.bucket or fileSize;
        // sampleSplits must return them intact rather than treating rowCount as bytes or summing -1.
        List<Split> sysSplits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ConnectorScanRange sysRange = Mockito.mock(ConnectorScanRange.class);
            Mockito.when(sysRange.getPath()).thenReturn(Optional.of("/dummyPath"));
            Mockito.when(sysRange.getStart()).thenReturn(0L);
            Mockito.when(sysRange.getLength()).thenReturn(-1L);
            Mockito.when(sysRange.getFileSize()).thenReturn(-1L);
            Mockito.when(sysRange.getModificationTime()).thenReturn(0L);
            Mockito.when(sysRange.getHosts()).thenReturn(Collections.emptyList());
            Mockito.when(sysRange.getPartitionValues()).thenReturn(Collections.emptyMap());
            Mockito.when(sysRange.getPushDownRowCount()).thenReturn(-1L);
            Mockito.when(sysRange.getSelfSplitWeight()).thenReturn(10L);
            Mockito.when(sysRange.getTargetSplitSize()).thenReturn(1024L);
            Mockito.when(sysRange.getProperties()).thenReturn(Collections.emptyMap());
            Mockito.when(sysRange.getSplitIdentity()).thenReturn("sys-" + i);
            sysSplits.add(new PluginDrivenSplit(sysRange));
        }
        List<Split> sysResult = PluginDrivenScanNode.sampleSplits(
                sysSplits, new TableSample(true, 25L, 3L), 16L);
        Assertions.assertEquals(4, sysResult.size());
    }
}
