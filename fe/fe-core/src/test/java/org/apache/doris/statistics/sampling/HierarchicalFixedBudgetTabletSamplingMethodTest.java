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
import org.apache.doris.statistics.analysis.AnalyzeSkipException;
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod.SystematicRounding;
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HierarchicalFixedBudgetTabletSamplingMethodTest {
    static TabletSampleSpec plan(List<PartitionSampleMeta> parts, long target, int cap, long seed, boolean key) {
        return new HierarchicalFixedBudgetTabletSamplingMethod().build(new TabletSamplingRequest(
                TabletSamplingScope.table(parts), new ColumnPhysicalLayout(key, false, false, false),
                new SamplingBudget(target, 8, cap, 4, 4096), 100000, seed));
    }

    static PartitionSampleMeta partition(int id, long... rows) {
        List<TabletSampleMeta> tablets = new ArrayList<>();
        for (int i = 0; i < rows.length; i++) {
            tablets.add(new TabletSampleMeta(id * 1000L + i + 1, rows[i], rows[i] <= 0));
        }
        return new PartitionSampleMeta(id, tablets);
    }

    @Test
    void partitionGranularityCannotExecuteATableSample() {
        var scope = new TabletSamplingScope(TabletSamplingScope.SamplingGranularity.PARTITION, List.of(partition(1, 10)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TabletSamplingRequest(scope,
                new ColumnPhysicalLayout(false, false, false, false), new SamplingBudget(8, 8, 8, 4, 4096), 10, 0));
    }

    @Test
    void imputationUsesPartitionThenIndexMeanAndNeverSelectsTheSmallTableShortcut() {
        var frame = IndexSamplingSnapshot.impute(List.of(partition(1, 2, 3, 0), partition(2, 8), partition(3, 0)));
        Assertions.assertEquals(new TabletSampleMeta(1003, 3, true), frame.partitions().get(0).tablets().get(2));
        Assertions.assertEquals(new TabletSampleMeta(3001, 5, true), frame.partitions().get(2).tablets().get(0));
        TabletSampleSpec spec = plan(frame.partitions(), 1000, 10, 0, false);
        Assertions.assertInstanceOf(FixedBlockScanClause.class, spec.scanClause());
        Assertions.assertThrows(AnalyzeSkipException.class,
                () -> IndexSamplingSnapshot.impute(List.of(partition(1, 0, -1))));
        Assertions.assertEquals(TabletSampleSpec.fullScan(), plan(List.of(partition(1, 10)), 10, 10, 0, false));
        Assertions.assertEquals(TabletSampleSpec.fullScan(), plan(List.of(partition(1, 8)), 7, 10, 0, false));
    }

    @Test
    void everyAllowedSeatCanBeUsedIncludingASingleTabletBudget() {
        var parts = List.of(partition(1, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192));
        for (int cap : List.of(1, 2)) {
            for (int seed = 0; seed < 100; seed++) {
                TabletSampleSpec spec = plan(parts, 512, cap, seed, false);
                var blocks = ((FixedBlockScanClause) spec.scanClause()).blocks();
                Assertions.assertEquals(cap, blocks.tabletIds().size());
                var group = blocks.groups.get(0);
                Assertions.assertEquals(cap / 8.0, group.tabletRate);
                Assertions.assertEquals(((EqualProbabilitySamplingSemantics) spec.semantics()).probability(),
                        group.tabletRate * group.unitRate);
            }
        }
    }

    @Test
    void singleSeatScalesTheMostExpensivePartitionBeforeDrawing() {
        var parts = List.of(partition(1, 4096, 4096), partition(2, 4096, 4096, 4096, 4096, 4096, 4096));
        Map<Long, Integer> hits = new HashMap<>();
        for (int seed = 0; seed < 512; seed++) {
            TabletSampleSpec spec;
            try {
                spec = plan(parts, 512, 1, seed, false);
            } catch (AnalyzeSkipException empty) {
                Assertions.assertTrue(empty.getMessage().contains("no tablets"));
                continue;
            }
            var blocks = ((FixedBlockScanClause) spec.scanClause()).blocks();
            Assertions.assertEquals(1, blocks.tabletIds().size());
            var group = blocks.groups.get(0);
            Assertions.assertEquals(1.0 / 12, group.tabletRate, 1e-15);
            Assertions.assertEquals(((EqualProbabilitySamplingSemantics) spec.semantics()).probability(),
                    group.unitRate * group.tabletRate, 1e-15);
            hits.merge(group.tablets.get(0), 1, Integer::sum);
        }
        Assertions.assertEquals(8, hits.size());
    }

    @Test
    void systematicRoundingPreservesCellBoundsAcrossUlpBoundaries() {
        // After the first cell the exact prefix is nextDown(32). In binary64, adding one rounds to 33.
        var boundary = new SystematicRounding(-65536L);
        Assertions.assertEquals(31, boundary.take(31, SystematicRounding.units(1)));
        Assertions.assertEquals(1, boundary.take(1, SystematicRounding.units(1)));

        var rounding = new SystematicRounding(Long.MIN_VALUE);
        BigDecimal prefix = new BigDecimal("0.5");
        for (int count : new int[] {0, 1, 31, 32, 1000, Integer.MAX_VALUE}) {
            for (double beta : new double[] {Double.MIN_VALUE, 0x1.0p-53, 0.1, 0.5, Math.nextDown(1.0), 1.0}) {
                BigInteger before = prefix.toBigInteger();
                prefix = prefix.add(new BigDecimal(beta).multiply(BigDecimal.valueOf(count)));
                int take = rounding.take(count, SystematicRounding.units(beta));
                Assertions.assertTrue(take >= 0 && take <= count);
                Assertions.assertEquals(prefix.toBigInteger().subtract(before).intValueExact(), take);
            }
        }
    }

    @Test
    void narrowSqlBudgetCanFitOneOfSeveralCohorts() {
        var scope = TabletSamplingScope.table(List.of(partition(1, 80, 800, 8000)));
        var layout = new ColumnPhysicalLayout(false, false, false, false);
        var method = new HierarchicalFixedBudgetTabletSamplingMethod();
        var small = new SamplingBudget(64, 8, 512, 4, 400);
        var spec = method.build(new TabletSamplingRequest(scope, layout, small, 8880, 0));
        var blocks = ((FixedBlockScanClause) spec.scanClause()).blocks();
        Assertions.assertEquals(1, blocks.tabletIds().size());
        SampledScanSqlRenderer.render(spec.scanClause(), small.maxInternalSqlBytes());

        var insufficient = new SamplingBudget(64, 8, 512, 4, 100);
        var skipped = Assertions.assertThrows(AnalyzeSkipException.class,
                () -> method.build(new TabletSamplingRequest(scope, layout, insufficient, 8880, 0)));
        Assertions.assertTrue(skipped.getMessage().contains("cannot fit one tablet"));
    }

    @Test
    void preflightEnforcesSqlBytesEvenWithNineteenDigitTabletIds() {
        List<TabletSampleMeta> tablets = new ArrayList<>();
        for (long i = 0; i < 150; i++) {
            tablets.add(new TabletSampleMeta(Long.MAX_VALUE - i, 8000, false));
        }
        var scope = TabletSamplingScope.table(List.of(new PartitionSampleMeta(1, tablets)));
        var budget = new SamplingBudget(600000, 8, 512, 4, 400);
        for (int seed = 0; seed < 100; seed++) {
            var request = new TabletSamplingRequest(scope, new ColumnPhysicalLayout(false, false, false, false),
                    budget, 1200000, seed);
            var spec = new HierarchicalFixedBudgetTabletSamplingMethod().build(request);
            double q = ((EqualProbabilitySamplingSemantics) spec.semantics()).probability();
            Assertions.assertTrue(q > 0 && q < 0.5);
            String sql = SampledScanSqlRenderer.render(spec.scanClause(), budget.maxInternalSqlBytes());
            Assertions.assertTrue(sql.getBytes(StandardCharsets.UTF_8).length <= budget.maxInternalSqlBytes());
        }
    }

    @Test
    void deterministicAcrossTraversalOrder() {
        var a = partition(1, 100, 500, 9000);
        var b = partition(2, 300, 200, 1000);
        ArrayList<TabletSampleMeta> reverse = new ArrayList<>(a.tablets());
        Collections.reverse(reverse);
        Assertions.assertEquals(plan(List.of(a, b), 1024, 7, 142, false),
                plan(List.of(b, new PartitionSampleMeta(1, reverse)), 1024, 7, 142, false));
    }

    @Test
    void unevenPartitionCostsAndJointRoundingPreserveEveryTabletMarginal() {
        var parts = List.of(partition(1, 128, 1024, 4096), partition(2, 128), partition(3, 128, 1024),
                partition(4, 128, 128, 128, 128, 128, 1024, 1024, 4096, 4096, 4096));
        Map<Long, Double> probabilities = new HashMap<>();
        Map<Long, Integer> hits = new HashMap<>();
        int trials = 16000;
        double common = -1;
        for (int seed = 0; seed < trials; seed++) {
            TabletSampleSpec spec;
            try {
                spec = plan(parts, 768, 4, seed, false);
            } catch (AnalyzeSkipException empty) {
                Assertions.assertTrue(empty.getMessage().contains("no tablets"));
                continue;
            }
            double q = ((EqualProbabilitySamplingSemantics) spec.semantics()).probability();
            if (common < 0) {
                common = q;
            }
            Assertions.assertEquals(common, q);
            FixedBlockSampleSpec blocks = ((FixedBlockScanClause) spec.scanClause()).blocks();
            Assertions.assertTrue(blocks.tabletIds().size() <= 4);
            Assertions.assertTrue(blocks.groups.size() <= 4);
            for (var group : blocks.groups) {
                Assertions.assertEquals(q, group.tabletRate * group.unitRate, 1e-14);
                for (long id : group.tablets) {
                    probabilities.put(id, group.tabletRate);
                    hits.merge(id, 1, Integer::sum);
                }
            }
        }
        Assertions.assertEquals(parts.stream().mapToInt(p -> p.tablets().size()).sum(), hits.size());
        probabilities.forEach((id, pi) -> Assertions.assertEquals(pi, hits.get(id) / (double) trials,
                5 * Math.sqrt(pi * (1 - pi) / trials) + 1.0 / trials, "tablet " + id));
    }
}
