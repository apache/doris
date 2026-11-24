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
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod.Population;
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Samples RANDOM-distributed AGG_KEYS indexes. When candidate backends support shared key-range
 * block sampling and at least one whole partition fits within the tablet budget, coordinates
 * partition-wide storage key intervals across all tablets of each sampled partition; otherwise
 * falls back to whole-partition block pruning plus a deterministic key-tuple hash filter.
 */
public final class RandomAggTabletSamplingMethod implements TabletSamplingMethod {
    private static final int MAX_SHARED_KEY_RANGE_PARTITIONS = 32;

    private final boolean backendSupportsFixedBlock;
    private final boolean backendSupportsSharedKeyRange;

    public RandomAggTabletSamplingMethod(boolean backendSupportsFixedBlock) {
        this(backendSupportsFixedBlock, false);
    }

    public RandomAggTabletSamplingMethod(boolean backendSupportsFixedBlock, boolean backendSupportsSharedKeyRange) {
        this.backendSupportsFixedBlock = backendSupportsFixedBlock;
        this.backendSupportsSharedKeyRange = backendSupportsSharedKeyRange;
    }

    public boolean usesSharedKeyRange() {
        return backendSupportsSharedKeyRange;
    }

    @Override
    public TabletSampleSpec build(TabletSamplingRequest request) {
        SamplingBudget budget = request.budget();
        List<PartitionSampleMeta> partitions = request.scope().partitions();
        Population population = Population.of(partitions, budget.unitRows());
        if (population.isCensus(budget.targetRows(), budget.unitRows())) {
            return TabletSampleSpec.fullScan();
        }
        long seed = request.seed();
        int maxPartTablets = partitions.stream().mapToInt(p -> p.tablets().size()).max().orElse(0);
        int partitionCount = partitions.size();
        int keyRangeCap = Math.min(budget.maxSelectedTablets(),
                SampledScanSqlRenderer.tabletCapacity(budget.maxInternalSqlBytes(), 1,
                        FixedBlockSampleSpec.Mode.KEY_RANGE_BLOCK));
        if (backendSupportsSharedKeyRange && maxPartTablets > 0 && keyRangeCap >= maxPartTablets) {
            int maxPartitionsByCap = keyRangeCap / maxPartTablets;
            int targetPartitions = (int) Math.ceil(partitionCount * (budget.targetRows() / population.rows()));
            long k = 1L + (budget.targetRows() - 1L) / budget.unitRows();
            int desiredPartitions = Math.max(targetPartitions,
                    (int) Math.min(MAX_SHARED_KEY_RANGE_PARTITIONS, Math.max(1L, k / 4L)));
            int m = Math.min(partitionCount, Math.max(1, Math.min(maxPartitionsByCap, desiredPartitions)));
            double alpha = (double) m / partitionCount;
            List<Integer> order = selectPartitions(partitions, seed, m);
            List<Long> tabletIds = new ArrayList<>();
            long maxPartRows = 1L;
            long maxFixedBlocks = 1L;
            for (int j : order) {
                long partRows = 0L;
                for (TabletSampleMeta t : partitions.get(j).tablets()) {
                    tabletIds.add(t.id());
                    partRows += t.estimatedRows();
                    maxFixedBlocks = Math.max(maxFixedBlocks, t.blocks(budget.unitRows()));
                }
                maxPartRows = Math.max(maxPartRows, partRows);
            }
            double remainingRows = alpha * population.rows();
            if (remainingRows <= budget.targetRows()) {
                if (m < partitionCount && backendSupportsFixedBlock) {
                    long safetyCap = Math.max(64L, 2L * maxFixedBlocks + 64L);
                    return new TabletSampleSpec(
                            new FixedBlockScanClause(new FixedBlockSampleSpec(FixedBlockSampleSpec.Mode.FIXED_BLOCK,
                                    budget.unitRows(), seed,
                                    List.of(new FixedBlockSampleSpec.Group(alpha, 1.0, safetyCap, tabletIds)))),
                            new EqualProbabilitySamplingSemantics(alpha, alpha, 1.0));
                }
                if (m == partitionCount && !population.imputed()) {
                    return TabletSampleSpec.fullScan();
                }
            }
            double gamma = remainingRows > budget.targetRows()
                    ? Math.min(1.0, Math.max(Double.MIN_NORMAL, budget.targetRows() / remainingRows))
                    : 1.0;
            long minUnitByCap = Math.max(1L, Math.round(
                    (budget.targetRows() / (double) Math.max(1, m)) / 512.0));
            long partitionUnitRows = Math.max(64L, Math.max(budget.unitRows(), minUnitByCap));
            long maxBlocks = 1L + (maxPartRows - 1L) / partitionUnitRows;
            long safetyCap = Math.max(128L, (long) Math.ceil(2.0 * gamma * maxBlocks + 128.0));
            double q = alpha * gamma;
            return new TabletSampleSpec(
                    new FixedBlockScanClause(new FixedBlockSampleSpec(FixedBlockSampleSpec.Mode.KEY_RANGE_BLOCK,
                            partitionUnitRows, seed,
                            List.of(new FixedBlockSampleSpec.Group(alpha, gamma, safetyCap, tabletIds)))),
                    new EqualProbabilitySamplingSemantics(q, alpha, 1.0));
        }

        int cap = Math.min(budget.maxSelectedTablets(),
                SampledScanSqlRenderer.tabletCapacity(budget.maxInternalSqlBytes(), 1,
                        FixedBlockSampleSpec.Mode.FIXED_BLOCK));
        double alpha = 1.0;
        Optional<FixedBlockSampleSpec> partitionBlocks = Optional.empty();
        if (backendSupportsFixedBlock && partitionCount > 1 && maxPartTablets > 0 && cap >= maxPartTablets) {
            int maxPartitionsByCap = cap / maxPartTablets;
            int targetPartitions = (int) Math.ceil(partitionCount * (budget.targetRows() / population.rows()));
            int m = Math.min(partitionCount, Math.max(1, Math.min(maxPartitionsByCap, Math.max(1, targetPartitions))));
            if (m < partitionCount) {
                alpha = (double) m / partitionCount;
                List<Integer> order = selectPartitions(partitions, seed, m);
                List<Long> tabletIds = new ArrayList<>();
                long maxBlocks = 1;
                for (int j : order) {
                    for (TabletSampleMeta t : partitions.get(j).tablets()) {
                        tabletIds.add(t.id());
                        maxBlocks = Math.max(maxBlocks, t.blocks(budget.unitRows()));
                    }
                }
                long safetyCap = Math.max(64L, 2 * maxBlocks + 64);
                partitionBlocks = Optional.of(new FixedBlockSampleSpec(FixedBlockSampleSpec.Mode.FIXED_BLOCK,
                        budget.unitRows(), seed,
                        List.of(new FixedBlockSampleSpec.Group(alpha, 1.0, safetyCap, tabletIds))));
            }
        }
        double remainingRows = alpha * population.rows();
        long cutoff = KeyHashScanClause.HASH_MODULUS;
        if (remainingRows > budget.targetRows()) {
            cutoff = Math.max(1L, Math.min(KeyHashScanClause.HASH_MODULUS,
                    Math.round(KeyHashScanClause.HASH_MODULUS * (budget.targetRows() / remainingRows))));
        }
        double gamma = (double) cutoff / KeyHashScanClause.HASH_MODULUS;
        double q = alpha * gamma;
        return new TabletSampleSpec(new KeyHashScanClause(partitionBlocks, seed, cutoff),
                new EqualProbabilitySamplingSemantics(q, alpha, 1.0));
    }

    private static List<Integer> selectPartitions(List<PartitionSampleMeta> partitions, long seed, int m) {
        List<Integer> order = new ArrayList<>(partitions.size());
        for (int j = 0; j < partitions.size(); j++) {
            order.add(j);
        }
        if (m < partitions.size()) {
            order.sort(Comparator.comparingLong(j ->
                    HierarchicalFixedBudgetTabletSamplingMethod.hashPartition(seed, partitions.get(j).id())));
            order = new ArrayList<>(order.subList(0, m));
        }
        order.sort(Comparator.comparingLong(j -> partitions.get(j).id()));
        return order;
    }
}
