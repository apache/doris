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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExternalWriterParallelismPolicyTest {
    @Test
    void testWriterCountIsLimitedByCapacity() {
        Slot partition = new SlotReference("partition", StringType.INSTANCE);
        Alias route = new Alias(partition, "route");
        Statistics statistics = statistics(partition, 1_000, 100);

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.statelessHash(ImmutableList.of(route)),
                statistics, 64);

        Assertions.assertEquals(64, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(100, parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testConnectorCardinalityCapLimitsWriterCount() {
        Slot bucketKey = new SlotReference("bucket_key", StringType.INSTANCE);
        Alias route = new Alias(bucketKey, "bucket");
        Statistics statistics = statistics(bucketKey, 10_000, 1_000);
        ExternalWriteDistributionPlan distribution = ExternalWriteDistributionPlan.statelessHash(
                ImmutableList.of(route), ImmutableMap.of(route.getExprId(), 4L));

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                distribution, statistics, 64);

        Assertions.assertEquals(4, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(4, parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testSmallInputDoesNotChangeWriterCount() {
        Slot partition = new SlotReference("partition", StringType.INSTANCE);
        Alias route = new Alias(partition, "route");

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.statelessHash(ImmutableList.of(route)),
                statistics(partition, 10, 10), 64);

        Assertions.assertEquals(10, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(10, parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testUnknownStatisticsKeepNormalWriterCapacity() {
        Slot partition = new SlotReference("partition", StringType.INSTANCE);
        Alias route = new Alias(partition, "route");

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.statelessHash(ImmutableList.of(route)),
                null, 16);

        Assertions.assertEquals(16, parallelism.getPlannedWriterCount());
        Assertions.assertFalse(parallelism.getEstimatedOwnershipCount().isPresent());
    }

    @Test
    void testKnownRouteCapLimitsWriterCountWithoutStatistics() {
        Slot bucketKey = new SlotReference("bucket_key", StringType.INSTANCE);
        Alias route = new Alias(bucketKey, "bucket");
        ExternalWriteDistributionPlan distribution = ExternalWriteDistributionPlan.statelessHash(
                ImmutableList.of(route), ImmutableMap.of(route.getExprId(), 4L));

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                distribution, null, 16);

        Assertions.assertEquals(4, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(4, parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testUnknownInputInCompositeRouteKeepsWriterCapacity() {
        Slot known = new SlotReference("known", IntegerType.INSTANCE);
        Slot unknown = new SlotReference("unknown", IntegerType.INSTANCE);
        Alias route = new Alias(new Add(known, unknown), "route");
        Statistics statistics = new Statistics(10_000, ImmutableMap.of(
                known, new ColumnStatisticBuilder(10_000).setNdv(1).build()));

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.statelessHash(ImmutableList.of(route)),
                statistics, 64);

        Assertions.assertEquals(64, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(10_000,
                parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testAdaptiveHashKeepsCapacityForHotPartitionFanout() {
        Slot partition = new SlotReference("partition", StringType.INSTANCE);
        Alias route = new Alias(partition, "route");

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.adaptiveHash(
                        ImmutableList.of(route), ImmutableMap.of()),
                statistics(partition, 10_000, 1), 64);

        Assertions.assertEquals(64, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(1, parallelism.getEstimatedOwnershipCount().getAsLong());
    }

    @Test
    void testRandomRouteKeepsWriterCapacity() {
        Slot value = new SlotReference("value", StringType.INSTANCE);

        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.random(),
                statistics(value, 10_000, 10_000), 32);

        Assertions.assertEquals(32, parallelism.getPlannedWriterCount());
        Assertions.assertFalse(parallelism.getEstimatedOwnershipCount().isPresent());
    }

    @Test
    void testSingleWriterFallbackIsPreservedAndObservable() {
        Slot value = new SlotReference("value", StringType.INSTANCE);
        ExternalWriterParallelism parallelism = ExternalWriterParallelismPolicy.plan(
                ExternalWriteDistributionPlan.singleWriter("stateful assigner is unavailable"),
                statistics(value, 10_000, 10_000), 64);

        Assertions.assertEquals(1, parallelism.getPlannedWriterCount());
        Assertions.assertEquals(1, parallelism.getEstimatedOwnershipCount().getAsLong());
        Assertions.assertEquals("stateful assigner is unavailable",
                parallelism.getFallbackReason().orElseThrow(AssertionError::new));
    }

    private static Statistics statistics(Slot slot, double rowCount, double ndv) {
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(rowCount)
                .setNdv(ndv)
                .build();
        return new Statistics(rowCount, ImmutableMap.of(slot, columnStatistic));
    }
}
