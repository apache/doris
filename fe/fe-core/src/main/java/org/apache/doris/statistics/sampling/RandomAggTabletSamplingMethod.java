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
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Samples RANDOM-distributed AGG_KEYS indexes by combining whole-partition selection (when multiple
 * partitions fit within the tablet capacity) with a deterministic key-tuple hash filter so every
 * sampled key retains all partial rows across every tablet of its partition.
 */
public final class RandomAggTabletSamplingMethod implements TabletSamplingMethod {
    private final boolean backendSupportsFixedBlock;

    public RandomAggTabletSamplingMethod(boolean backendSupportsFixedBlock) {
        this.backendSupportsFixedBlock = backendSupportsFixedBlock;
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
        int cap = Math.min(budget.maxSelectedTablets(),
                SampledScanSqlRenderer.tabletCapacity(budget.maxInternalSqlBytes(), 1,
                        FixedBlockSampleSpec.Mode.FIXED_BLOCK));
        int maxPartTablets = partitions.stream().mapToInt(p -> p.tablets().size()).max().orElse(0);
        int partitionCount = partitions.size();
        double alpha = 1.0;
        Optional<FixedBlockSampleSpec> partitionBlocks = Optional.empty();
        if (backendSupportsFixedBlock && partitionCount > 1 && maxPartTablets > 0 && cap >= maxPartTablets) {
            int maxPartitionsByCap = cap / maxPartTablets;
            int targetPartitions = (int) Math.ceil(partitionCount * (budget.targetRows() / population.rows()));
            int m = Math.min(partitionCount, Math.max(1, Math.min(maxPartitionsByCap, Math.max(1, targetPartitions))));
            if (m < partitionCount) {
                alpha = (double) m / partitionCount;
                List<Integer> order = new ArrayList<>();
                for (int j = 0; j < partitionCount; j++) {
                    order.add(j);
                }
                order.sort(Comparator.comparingLong(j ->
                        HierarchicalFixedBudgetTabletSamplingMethod.hashPartition(seed, partitions.get(j).id())));
                order = order.subList(0, m);
                order.sort(Comparator.comparingLong(j -> partitions.get(j).id()));
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
}
