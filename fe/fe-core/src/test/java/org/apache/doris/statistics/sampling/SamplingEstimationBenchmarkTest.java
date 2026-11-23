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

package org.apache.doris.statistics.sampling;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacyTabletScanClause;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

class SamplingEstimationBenchmarkTest {
    enum Distribution { UNIFORM, ZIPF, SORTED_KEY, CLUSTERED_NULLS, UNEQUAL_TABLETS }

    private static final int UNIT = 8;
    private static final int TARGET = 2048;
    private static final int TRIALS = 600;

    @ParameterizedTest
    @EnumSource(Distribution.class)
    void compareWithCensusAndOriginalTabletPicker(Distribution distribution) {
        Random values = new Random(12345);
        double[] zipf = new double[32];
        double normalizer = 0;
        for (int rank = 0; rank < zipf.length; rank++) {
            normalizer += 1.0 / (rank + 1);
            zipf[rank] = normalizer;
        }
        List<PartitionSampleMeta> partitions = new ArrayList<>();
        Map<Long, int[]> data = new HashMap<>();
        OlapTable table = Mockito.mock(OlapTable.class);
        List<Partition> catalog = new ArrayList<>();
        Moments truth = new Moments();
        for (int p = 0; p < 4; p++) {
            Partition part = Mockito.mock(Partition.class);
            MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
            List<TabletSampleMeta> tablets = new ArrayList<>();
            List<Long> ids = new ArrayList<>();
            long rows = 0;
            for (int t = 0; t < 8; t++) {
                long id = 1000L * p + t + 1;
                int size = distribution == Distribution.UNEQUAL_TABLETS ? (64 << (t % 5)) : 512;
                int[] column = new int[size];
                for (int i = 0; i < size; i++) {
                    int rank = distribution == Distribution.ZIPF
                            ? zipfRank(zipf, values.nextDouble() * normalizer) : values.nextInt(32);
                    column[i] = distribution == Distribution.SORTED_KEY ? truth.n : rank;
                    if (distribution == Distribution.CLUSTERED_NULLS ? i < size / 4 : values.nextDouble() < 0.12) {
                        column[i] = -1;
                    }
                    truth.add(column[i]);
                }
                data.put(id, column);
                tablets.add(new TabletSampleMeta(id, size, false));
                ids.add(id);
                LocalTablet tablet = Mockito.mock(LocalTablet.class);
                Mockito.when(tablet.getMinReplicaRowCount(Mockito.anyLong())).thenReturn((long) size);
                Mockito.when(index.getTablet(id)).thenReturn(tablet);
                rows += size;
            }
            partitions.add(new PartitionSampleMeta(p, tablets));
            Mockito.when(index.getTabletIdsInOrder()).thenReturn(ids);
            Mockito.when(index.getRowCount()).thenReturn(rows);
            Mockito.when(part.getBaseIndex()).thenReturn(index);
            Mockito.when(part.getName()).thenReturn("p" + p);
            catalog.add(part);
        }
        Mockito.doReturn(catalog).when(table).getPartitions();
        boolean key = distribution == Distribution.SORTED_KEY;
        var budget = new SamplingBudget(TARGET, UNIT, 32, 4, 32000);
        var layout = new ColumnPhysicalLayout(key, false, false, false);
        var scope = TabletSamplingScope.table(partitions);
        Errors fixed = new Errors();
        Errors legacy = new Errors();
        for (int seed = 0; seed < TRIALS; seed++) {
            var request = new TabletSamplingRequest(scope, layout, budget, truth.n, seed);
            var spec = new HierarchicalFixedBudgetTabletSamplingMethod().build(request);
            var sample = ((FixedBlockScanClause) spec.scanClause()).blocks();
            Moments actual = drawBlocks(data, sample, seed);
            double q = ((TabletSampleSpec.EqualProbabilitySamplingSemantics) spec.semantics()).probability();
            fixed.record(actual, truth, actual.n / q, (double) truth.n / actual.n, true);
            if (distribution != Distribution.UNEQUAL_TABLETS) {
                Assertions.assertEquals(key ? 16 : 8, sample.tabletIds().size());
            }
            var old = new LegacyTabletSamplingMethod(table, -1, -1).build(request);
            if (old.scanClause() instanceof LegacyTabletScanClause picked) {
                var semantics = (LegacySamplingSemantics) old.semantics();
                Moments observed = new Moments();
                for (long id : picked.tablets()) {
                    for (int value : data.get(id)) {
                        if (semantics.limit() > 0 && observed.n >= semantics.limit()) {
                            break;
                        }
                        observed.add(value);
                    }
                }
                legacy.record(observed, truth, observed.n * semantics.scaleFactor(), semantics.scaleFactor(), false);
            } else {
                legacy.record(truth, truth, truth.n, 1, false);
            }
        }
        // Check the Monte Carlo mean of the linear row estimate, and per-trial absolute relative
        // NDV / NULL error (averaging signed errors alone would hide a high-variance sampler).
        Assertions.assertEquals(1, fixed.rows / TRIALS, 0.025, distribution.name());
        Assertions.assertEquals(1, fixed.bytes / TRIALS, 0.04, distribution.name());
        LogManager.getLogger(SamplingEstimationBenchmarkTest.class).info(
                "{} mean row estimate/truth {}, mean relative NDV error fixed {} legacy {}, "
                        + "mean relative NULL error fixed {} legacy {}", distribution, fixed.rows / TRIALS,
                fixed.ndvError / TRIALS, legacy.ndvError / TRIALS, fixed.nullError / TRIALS, legacy.nullError / TRIALS);
        Assertions.assertEquals(1, fixed.nulls / TRIALS, 0.04, distribution.name());
        Assertions.assertTrue(fixed.ndvError / TRIALS < 0.08, distribution.name());
        Assertions.assertTrue(fixed.nullError / TRIALS < 0.16, distribution.name());
        if (distribution == Distribution.SORTED_KEY) {
            // Legacy includes NULLs in its DUJ1 sample size, which biases this nullable all-singleton case.
            Assertions.assertTrue(fixed.ndvError < legacy.ndvError, distribution.name());
        } else {
            Assertions.assertTrue(legacy.ndvError / TRIALS < 0.12, distribution.name());
        }
        Assertions.assertTrue(legacy.nullError / TRIALS < 0.35, distribution.name());
    }

    private static int zipfRank(double[] cumulative, double draw) {
        int rank = Arrays.binarySearch(cumulative, draw);
        return rank >= 0 ? rank : -rank - 1;
    }

    // Simulate one segment per tablet. Choose a randomized integral count with expectation B*p,
    // then sample that many distinct blocks; every selected block contributes all its visible rows.
    private static Moments drawBlocks(Map<Long, int[]> data, FixedBlockSampleSpec spec, long seed) {
        Moments result = new Moments();
        Random random = new Random(seed ^ 0x78d9320faL);
        for (var group : spec.groups) {
            for (long id : group.tablets) {
                int[] values = data.get(id);
                List<Integer> starts = new ArrayList<>();
                for (int i = 0; i < values.length; i += UNIT) {
                    starts.add(i);
                }
                int count = (int) (starts.size() * group.unitRate + random.nextDouble());
                Collections.shuffle(starts, random);
                for (int start : starts.subList(0, count)) {
                    Arrays.stream(values, start, Math.min(start + UNIT, values.length)).forEach(result::add);
                }
            }
        }
        return result;
    }

    private static final class Moments {
        int n;
        int z;
        long bytes;
        final Map<Integer, Integer> frequencies = new HashMap<>();

        void add(int value) {
            n++;
            if (value == -1) {
                z++;
            } else {
                bytes += 1 + value % 13;
                frequencies.merge(value, 1, Integer::sum);
            }
        }
    }

    private static final class Errors {
        double rows;
        double bytes;
        double nulls;
        double ndvError;
        double nullError;

        void record(Moments sample, Moments truth, double estimatedRows, double scale, boolean fixed) {
            int dujRows = fixed ? sample.n - sample.z : sample.n;
            double nullEstimate = fixed ? Math.round(sample.z * scale) : (long) (sample.z * scale);
            long f1 = sample.frequencies.values().stream().filter(f -> f == 1).count();
            double ndv = dujRows * (double) sample.frequencies.size()
                    / (dujRows - f1 + f1 * (double) dujRows / truth.n);
            ndv = fixed ? Math.min(truth.n - nullEstimate, Math.round(ndv)) : (long) ndv;
            rows += estimatedRows / truth.n;
            bytes += Math.round(sample.bytes * scale) / (double) truth.bytes;
            nulls += nullEstimate / truth.z;
            nullError += Math.abs(nullEstimate / truth.z - 1);
            ndvError += Math.abs(ndv / truth.frequencies.size() - 1);
        }
    }
}
