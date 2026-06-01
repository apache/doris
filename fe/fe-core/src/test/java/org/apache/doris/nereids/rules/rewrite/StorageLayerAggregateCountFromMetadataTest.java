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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Test for STORAGE_LAYER_AGGREGATE_COUNT_FROM_METADATA_FOR_FILE_SCAN rule.
 * This rule detects sum0(__count_from_metadata__) produced by RewriteCountAggToFileScanRule
 * and wraps PhysicalFileScan in PhysicalStorageLayerAggregate with COUNT_FROM_METADATA.
 */
class StorageLayerAggregateCountFromMetadataTest extends TestWithFeService
        implements MemoPatternMatchSupported {

    private final SlotReference countFromMetadataSlot =
            new SlotReference("__count_from_metadata__", BigIntType.INSTANCE, true, ImmutableList.of());
    private final SlotReference partCol =
            new SlotReference("dt", VarcharType.SYSTEM_DEFAULT, true, ImmutableList.of());
    private final SlotReference col1 =
            new SlotReference("col1", BigIntType.INSTANCE, true, ImmutableList.of());

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    private void setupConstructorMocks(HMSExternalTable mockTable) {
        new Expectations() {
            {
                mockTable.getName();
                result = "test_table";
                minTimes = 0;
                mockTable.getBaseSchema();
                result = Collections.emptyList();
                minTimes = 0;
                mockTable.initSelectedPartitions(Optional.empty());
                result = SelectedPartitions.NOT_PRUNED;
                minTimes = 0;
                mockTable.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
                minTimes = 0;
                mockTable.isHoodieCowTable();
                result = false;
                minTimes = 0;
            }
        };
    }

    private LogicalFileScan createFileScan(HMSExternalTable mockTable) {
        return new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("catalog", "db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private Rule countFromMetadataRule() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType()
                        == RuleType.STORAGE_LAYER_AGGREGATE_COUNT_FROM_METADATA_FOR_FILE_SCAN)
                .findFirst()
                .get();
    }

    /**
     * Test sum0(__count_from_metadata__) without GROUP BY
     * produces PhysicalStorageLayerAggregate with COUNT_FROM_METADATA.
     */
    @Test
    void testCountFromMetadataNoGroupBy(@Injectable HMSExternalTable mockTable) {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        // Simulate the result of RewriteCountAggToFileScanRule: add __count_from_metadata__ to file scan output
        LogicalFileScan fileScanWithMetadata = ((LogicalFileScan) fileScan
                .withOperativeSlots(ImmutableList.of(countFromMetadataSlot)))
                .withCachedOutput(ImmutableList.of(countFromMetadataSlot));

        LogicalProject<LogicalFileScan> project = new LogicalProject<>(
                ImmutableList.of(countFromMetadataSlot), fileScanWithMetadata);

        Sum0 sum0 = new Sum0(countFromMetadataSlot);
        Alias sum0Alias = new Alias(sum0, "sum0(__count_from_metadata__)");
        LogicalAggregate<LogicalProject<LogicalFileScan>> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(sum0Alias), project);

        PlanChecker.from(MemoTestUtils.createCascadesContext(connectContext, agg))
                .applyImplementation(countFromMetadataRule())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            physicalStorageLayerAggregate()
                                .when(storageAgg -> storageAgg.getAggOp()
                                        == PushDownAggOp.COUNT_FROM_METADATA)
                                .when(storageAgg -> storageAgg.getRelation() instanceof PhysicalFileScan)
                        )
                    )
                );
    }

    /**
     * Test sum0(__count_from_metadata__) with GROUP BY partition column
     * produces PhysicalStorageLayerAggregate with COUNT_FROM_METADATA.
     */
    @Test
    void testCountFromMetadataGroupByPartitionColumn(@Injectable HMSExternalTable mockTable) {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        // Simulate the result of RewriteCountAggToFileScanRule: add partition col and
        // __count_from_metadata__ to file scan output
        LogicalFileScan fileScanWithMetadata = ((LogicalFileScan) fileScan
                .withOperativeSlots(ImmutableList.of(partCol, countFromMetadataSlot)))
                .withCachedOutput(ImmutableList.of(partCol, countFromMetadataSlot));

        LogicalProject<LogicalFileScan> project = new LogicalProject<>(
                ImmutableList.of(partCol, countFromMetadataSlot), fileScanWithMetadata);

        Sum0 sum0 = new Sum0(countFromMetadataSlot);
        Alias sum0Alias = new Alias(sum0, "sum0(__count_from_metadata__)");
        LogicalAggregate<LogicalProject<LogicalFileScan>> agg = new LogicalAggregate<>(
                ImmutableList.of(partCol), ImmutableList.of(partCol, sum0Alias), project);

        PlanChecker.from(MemoTestUtils.createCascadesContext(connectContext, agg))
                .applyImplementation(countFromMetadataRule())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            physicalStorageLayerAggregate()
                                .when(storageAgg -> storageAgg.getAggOp()
                                        == PushDownAggOp.COUNT_FROM_METADATA)
                                .when(storageAgg -> storageAgg.getRelation() instanceof PhysicalFileScan)
                        )
                    )
                );
    }

    /**
     * Test that sum(col1) (not sum0(__count_from_metadata__)) does NOT match
     * the COUNT_FROM_METADATA rule.
     */
    @Test
    void testSumNotMatchingCountFromMetadataRule(@Injectable HMSExternalTable mockTable) {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        // Add col1 to file scan output
        LogicalFileScan fileScanWithCol = ((LogicalFileScan) fileScan
                .withOperativeSlots(ImmutableList.of(col1)))
                .withCachedOutput(ImmutableList.of(col1));

        LogicalProject<LogicalFileScan> project = new LogicalProject<>(
                ImmutableList.of(col1), fileScanWithCol);

        Sum sum = new Sum(col1);
        Alias sumAlias = new Alias(sum, "sum(col1)");
        LogicalAggregate<LogicalProject<LogicalFileScan>> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(sumAlias), project);

        // Apply the COUNT_FROM_METADATA rule; it should not transform the plan
        Plan result = countFromMetadataRule().transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg)).get(0);

        // The plan should remain unchanged (same structure)
        Assertions.assertTrue(result instanceof LogicalAggregate,
                "Plan should remain LogicalAggregate when rule doesn't match");
        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        Assertions.assertTrue(resultAgg.child() instanceof LogicalProject,
                "Child should remain LogicalProject when rule doesn't match");
    }
}
