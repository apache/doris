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
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Three-stage sample with a common unconditional inclusion probability for every physical block. */
public final class HierarchicalFixedBudgetTabletSamplingMethod implements TabletSamplingMethod {
    private final FixedBlockSampleSpec.Mode mode;

    public HierarchicalFixedBudgetTabletSamplingMethod() {
        this(FixedBlockSampleSpec.Mode.FIXED_BLOCK);
    }

    public HierarchicalFixedBudgetTabletSamplingMethod(FixedBlockSampleSpec.Mode mode) {
        this.mode = mode;
    }

    private static final long PARTITION = 0x706172746974696fL;
    private static final long ROUNDING = 0x726f756e64696e67L;
    private static final long TABLET = 0x7461626c6574L;

    @Override
    public TabletSampleSpec build(TabletSamplingRequest request) {
        SamplingBudget budget = request.budget();
        List<PartitionSampleMeta> partitions = request.scope().partitions();
        long k = 1 + (budget.targetRows() - 1) / budget.unitRows();
        Population population = Population.of(partitions, budget.unitRows());
        if (population.isCensus(budget.targetRows(), budget.unitRows())) {
            return TabletSampleSpec.fullScan();
        }
        long min = population.min();
        long max = population.max();
        int tiers = max <= 4.0 * min ? 1 : Math.min(3, budget.maxSampleGroups());
        double step = Math.pow((double) max / min, 1.0 / tiers);
        double[] boundaries = new double[tiers - 1];
        for (int h = 0; h < boundaries.length; h++) {
            boundaries[h] = min * Math.pow(step, h + 1);
        }
        int[] counts = new int[tiers];
        double[] weights = new double[tiers];
        long[] largest = new long[tiers];
        long[] largestRows = new long[tiers];
        List<Cell[]> cells = new ArrayList<>();
        for (PartitionSampleMeta part : partitions) {
            Cell[] strata = new Cell[tiers];
            Arrays.setAll(strata, i -> new Cell());
            for (TabletSampleMeta t : part.tablets()) {
                long b = t.blocks(budget.unitRows());
                int h = 0;
                while (h < boundaries.length && b >= boundaries[h]) {
                    h++;
                }
                strata[h].ids.add(t.id());
                counts[h]++;
                weights[h] += b;
                largest[h] = Math.max(largest[h], b);
                largestRows[h] = Math.max(largestRows[h], t.estimatedRows());
            }
            cells.add(strata);
        }
        int cap = Math.min(budget.maxSelectedTablets(),
                SampledScanSqlRenderer.tabletCapacity(budget.maxInternalSqlBytes(), tiers, mode));
        if (cap < 1) {
            throw new AnalyzeSkipException("Fixed-block SQL budget cannot fit one tablet");
        }
        int divisor = request.layout().key() || request.layout().partition() || request.layout().distribution()
                ? 16 : 32;
        int seats = (int) Math.min(cap, Math.max(1, k / divisor));
        int m = Math.min(partitions.size(), seats);
        double alpha = (double) m / partitions.size();
        // Even if every selected tablet reads all its blocks, equal marginals need at least
        // q * tabletCount seats. Reduce q before water-filling if the nominal target is infeasible.
        long tabletCount = Arrays.stream(counts).asLongStream().sum();
        double q = Math.min(Math.min(alpha, k / population.blocks()), (double) seats / tabletCount);
        double[] beta = allocate(counts, weights, seats / alpha, q / alpha);

        // Any common offset u < 1 gives floor(worst + u) <= cap when worst <= cap.
        // Evaluate the bound in the same exact units used to draw, before consulting the seed.
        BigInteger[] rates = Arrays.stream(beta).mapToObj(SystematicRounding::units).toArray(BigInteger[]::new);
        BigInteger limit = SystematicRounding.units(1.0).multiply(BigInteger.valueOf(cap));
        BigInteger worst = worstCost(cells, m, rates);
        if (worst.compareTo(limit) > 0) {
            q = SystematicRounding.scaleDown(SystematicRounding.units(q), limit, worst);
            for (int h = 0; h < tiers; h++) {
                beta[h] = SystematicRounding.scaleDown(rates[h], limit, worst);
                rates[h] = SystematicRounding.units(beta[h]);
            }
        }
        // Division when computing the water-filling floor can round down, even without budget scaling.
        for (int h = 0; h < tiers; h++) {
            if (counts[h] > 0) {
                q = Math.min(q, alpha * beta[h]);
            }
        }
        return draw(request, partitions, cells, m, alpha, beta, weights, rates, q, largest, largestRows,
                population.rows());
    }

    public record Population(long min, long max, double rows, double blocks, boolean imputed) {
        public static Population of(List<PartitionSampleMeta> partitions, long unitRows) {
            long min = Long.MAX_VALUE;
            long max = 0;
            double blocks = 0;
            double rows = 0;
            boolean imputed = false;
            for (PartitionSampleMeta part : partitions) {
                for (TabletSampleMeta t : part.tablets()) {
                    long b = t.blocks(unitRows);
                    min = Math.min(min, b);
                    max = Math.max(max, b);
                    blocks += b;
                    rows += t.estimatedRows();
                    imputed |= t.imputed();
                }
            }
            return new Population(min, max, rows, blocks, imputed);
        }

        public boolean isCensus(long targetRows, long unitRows) {
            return !imputed && (rows <= targetRows || 1 + (targetRows - 1) / unitRows >= blocks);
        }
    }

    private static BigInteger worstCost(List<Cell[]> cells, int m, BigInteger[] rates) {
        return cells.stream().map(strata -> {
            BigInteger cost = BigInteger.ZERO;
            for (int h = 0; h < strata.length; h++) {
                cost = cost.add(rates[h].multiply(BigInteger.valueOf(strata[h].ids.size())));
            }
            return cost;
        }).sorted(Comparator.reverseOrder()).limit(m).reduce(BigInteger.ZERO, BigInteger::add);
    }

    // Allocate in proportion to block mass, capping saturated cohorts and redistributing their surplus.
    // The lower bound makes the subsequent conditional block probability realizable.
    private static double[] allocate(int[] counts, double[] weights, double seats, double floor) {
        double[] rates = new double[counts.length];
        double hi = 0;
        for (int h = 0; h < counts.length; h++) {
            if (counts[h] > 0) {
                hi = Math.max(hi, counts[h] / weights[h]);
            }
        }
        double lo = 0;
        for (int i = 0; i < 64; i++) {
            double lambda = (lo + hi) / 2;
            double total = 0;
            for (int h = 0; h < counts.length; h++) {
                rates[h] = counts[h] == 0 ? 0 : Math.max(floor, Math.min(1, lambda * weights[h] / counts[h]));
                total += counts[h] * rates[h];
            }
            if (total > seats) {
                hi = lambda;
            } else {
                lo = lambda;
            }
        }
        return rates;
    }

    private TabletSampleSpec draw(TabletSamplingRequest request, List<PartitionSampleMeta> partitions,
            List<Cell[]> cells, int m, double alpha, double[] beta, double[] weights, BigInteger[] rates,
            double q, long[] largest, long[] largestRows, double populationRows) {
        long seed = request.seed();
        List<Integer> order = new ArrayList<>();
        for (int j = 0; j < partitions.size(); j++) {
            order.add(j);
        }
        order.sort(Comparator.comparingLong(j -> hash(seed, PARTITION, partitions.get(j).id())));
        order = order.subList(0, m);
        order.sort(Comparator.comparingLong(j -> partitions.get(j).id()));
        List<List<Long>> selected = new ArrayList<>();
        for (int h = 0; h < beta.length; h++) {
            selected.add(new ArrayList<>());
        }
        SystematicRounding rounding = new SystematicRounding(hash(seed, ROUNDING, 0));
        for (int j : order) {
            for (int h = 0; h < beta.length; h++) {
                List<Long> ids = cells.get(j)[h].ids;
                int take = rounding.take(ids.size(), rates[h]);
                if (take > 0) {
                    ids.sort(Comparator.comparingLong(id -> hash(seed, TABLET, id)));
                    selected.get(h).addAll(ids.subList(0, take));
                }
            }
        }
        int selectedTablets = selected.stream().mapToInt(List::size).sum();
        long effectiveUnitRows = request.budget().unitRows();
        if (mode == FixedBlockSampleSpec.Mode.KEY_RANGE_BLOCK && selectedTablets > 0) {
            // Coalesce narrow key units into at most ~32 stratified contiguous spans per selected
            // tablet so overlapping segments do not decompress separate 64 KB pages for hundreds of cuts.
            double expectedRowsPerTablet = (q * populationRows) / selectedTablets;
            long minUnitByTarget = Math.max(1L, (long) Math.ceil(expectedRowsPerTablet / 32.0));
            effectiveUnitRows = Math.max(effectiveUnitRows, minUnitByTarget);
        }
        Map<GroupRates, List<Long>> combined = new LinkedHashMap<>();
        for (int h = 0; h < beta.length; h++) {
            if (!selected.get(h).isEmpty()) {
                double pi = alpha * beta[h];
                double p = q / pi;
                long maxBlocks = effectiveUnitRows == request.budget().unitRows()
                        ? largest[h]
                        : (1L + (Math.max(1L, largestRows[h]) - 1L) / effectiveUnitRows);
                // BE rounds per segment. Allow for segment tails and delayed tablet reports.
                long safetyCap = (long) Math.ceil(2 * p * maxBlocks + 64);
                combined.computeIfAbsent(new GroupRates(pi, p, safetyCap), ignored -> new ArrayList<>())
                        .addAll(selected.get(h));
            }
        }
        if (combined.isEmpty()) {
            // Redrawing until nonempty would change the marginal inclusion probabilities.
            throw new AnalyzeSkipException("Fixed-block sample selected no tablets; preserving column statistics");
        }
        List<FixedBlockSampleSpec.Group> groups = combined.entrySet().stream().map(e -> new FixedBlockSampleSpec.Group(
                e.getKey().tabletRate(), e.getKey().unitRate(), e.getKey().maxUnits(), e.getValue())).toList();
        double totalWeight = Arrays.stream(weights).sum();
        double bucketRate = 1.0;
        if (totalWeight > 0) {
            bucketRate = 0.0;
            for (int h = 0; h < beta.length; h++) {
                bucketRate += (weights[h] / totalWeight) * beta[h];
            }
            bucketRate = Math.max(Math.min(bucketRate, 1.0), q / Math.max(alpha, Double.MIN_NORMAL));
        }
        return new TabletSampleSpec(new FixedBlockScanClause(new FixedBlockSampleSpec(
                mode, effectiveUnitRows, seed, groups)),
                new EqualProbabilitySamplingSemantics(q, alpha, bucketRate));
    }

    /** Every nonnegative binary64 probability is an integer multiple of 2^-1074. */
    static final class SystematicRounding {
        private static final int FRACTION_BITS = 1074;
        private static final int MANTISSA_BITS = 52;
        private static final long HIDDEN_BIT = 1L << MANTISSA_BITS;
        private BigInteger remainder;

        SystematicRounding(long randomBits) {
            BigInteger unsigned = BigInteger.valueOf(randomBits >>> 1).shiftLeft(1)
                    .add(BigInteger.valueOf(randomBits & 1));
            remainder = unsigned.shiftLeft(FRACTION_BITS - Long.SIZE);
        }

        static BigInteger units(double probability) {
            long bits = Double.doubleToRawLongBits(probability);
            int exponent = (int) (bits >>> MANTISSA_BITS);
            long fraction = bits & (HIDDEN_BIT - 1);
            return exponent == 0 ? BigInteger.valueOf(fraction)
                    : BigInteger.valueOf(fraction | HIDDEN_BIT).shiftLeft(exponent - 1);
        }

        // Round toward zero in both divisions. Every new partition cost is at most its
        // original exact cost times limit/worst, including after conversion back to binary64.
        static double scaleDown(BigInteger value, BigInteger limit, BigInteger worst) {
            BigInteger scaled = value.multiply(limit).divide(worst);
            int discard = Math.max(0, scaled.bitLength() - (MANTISSA_BITS + 1));
            return Math.scalb(scaled.shiftRight(discard).doubleValue(), discard - FRACTION_BITS);
        }

        int take(int count, BigInteger rate) {
            BigInteger next = remainder.add(rate.multiply(BigInteger.valueOf(count)));
            BigInteger whole = next.shiftRight(FRACTION_BITS);
            remainder = next.subtract(whole.shiftLeft(FRACTION_BITS));
            return whole.intValueExact();
        }
    }

    private record GroupRates(double tabletRate, double unitRate, long maxUnits) {
    }

    private static final class Cell {
        private final List<Long> ids = new ArrayList<>();
    }

    static long hashPartition(long seed, long partitionId) {
        return hash(seed, PARTITION, partitionId);
    }

    private static long hash(long seed, long domain, long id) {
        return mix(mix(seed ^ domain) + id);
    }

    // SplitMix64 finalizer. Domain separation keeps rounding independent of both rankings.
    private static long mix(long value) {
        long z = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
