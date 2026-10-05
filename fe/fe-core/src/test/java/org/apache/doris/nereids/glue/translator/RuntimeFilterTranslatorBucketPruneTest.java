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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

class RuntimeFilterTranslatorBucketPruneTest {
    private static final int SCAN_NODE_ID = 7;

    private ConnectContext previousContext;
    private SessionVariable sessionVariable;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        sessionVariable = new SessionVariable();
        sessionVariable.setEnableRuntimeFilterPartitionPrune(false);
        sessionVariable.setEnableRuntimeFilterBucketPrune(true);
        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(sessionVariable);
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void testGroupedSameTargetSerializesOneExpressionAndBucketTarget() {
        TranslatorHarness harness = new TranslatorHarness();
        SlotReference target = harness.addTargetSlot("dist_col", harness.distributionColumn,
                IntegerType.INSTANCE);

        TRuntimeFilterDesc desc = harness.translate(ImmutableList.of(
                harness.newFilter(target, target), harness.newFilter(target, target)));

        Assertions.assertEquals(1, desc.planId_to_target_expr.size());
        Assertions.assertEquals(firstLegacySlotId(harness, target),
                desc.planId_to_target_expr.get(SCAN_NODE_ID).nodes.get(0).slot_ref.slot_id);
        Assertions.assertTrue(desc.isSetBucketPruningTargetIds());
        Assertions.assertEquals(ImmutableList.of(SCAN_NODE_ID),
                desc.bucket_pruning_target_ids.stream().sorted().collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void testGroupedDifferentTargetsKeepThriftMapButSuppressBucketTarget() {
        TranslatorHarness harness = new TranslatorHarness();
        SlotReference distributionTarget = harness.addTargetSlot("dist_col", harness.distributionColumn,
                IntegerType.INSTANCE);
        Column valueColumn = new Column("value_col", PrimitiveType.INT);
        SlotReference valueTarget = harness.addTargetSlot("value_col", valueColumn, IntegerType.INSTANCE);

        TRuntimeFilterDesc desc = harness.translate(ImmutableList.of(
                harness.newFilter(distributionTarget, distributionTarget),
                harness.newFilter(valueTarget, valueTarget)));

        Assertions.assertEquals(1, desc.planId_to_target_expr.size());
        Assertions.assertEquals(firstLegacySlotId(harness, valueTarget),
                desc.planId_to_target_expr.get(SCAN_NODE_ID).nodes.get(0).slot_ref.slot_id);
        Assertions.assertFalse(desc.isSetBucketPruningTargetIds());
    }

    @Test
    void testCastAfterNonIdentityTargetSuppressesBucketTarget() {
        TranslatorHarness harness = new TranslatorHarness(PrimitiveType.BIGINT);
        SlotReference target = harness.addTargetSlot("dist_col", harness.distributionColumn,
                IntegerType.INSTANCE);

        TRuntimeFilterDesc desc = harness.translate(ImmutableList.of(
                harness.newFilter(target, new Add(target, new IntegerLiteral(1)))));

        Assertions.assertEquals(TExprNodeType.CAST_EXPR,
                desc.planId_to_target_expr.get(SCAN_NODE_ID).nodes.get(0).node_type);
        Assertions.assertFalse(desc.isSetBucketPruningTargetIds());
    }

    @Test
    void testDisabledFeatureSkipsClassificationAndSerialization() {
        sessionVariable.setEnableRuntimeFilterBucketPrune(false);
        TranslatorHarness harness = new TranslatorHarness();
        SlotReference target = harness.addTargetSlot("dist_col", harness.distributionColumn,
                IntegerType.INSTANCE);
        Mockito.clearInvocations(harness.targetRelation);

        TRuntimeFilterDesc desc = harness.translate(ImmutableList.of(harness.newFilter(target, target)));

        Mockito.verify(harness.targetRelation, Mockito.never()).getTable();
        Mockito.verify(harness.targetRelation, Mockito.never()).getSelectedPartitionIds();
        Assertions.assertEquals(1, desc.planId_to_target_expr.size());
        Assertions.assertFalse(desc.isSetBucketPruningTargetIds());
    }

    private static int firstLegacySlotId(TranslatorHarness harness, SlotReference target) {
        return harness.translatorContext.findSlotRef(target.getExprId()).getSlotId().asInt();
    }

    private class TranslatorHarness {
        private final Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        private final OlapTable table = Mockito.mock(OlapTable.class);
        private final Partition partition = Mockito.mock(Partition.class);
        private final OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        private final PlanNode builderNode = Mockito.mock(PlanNode.class);
        private final AbstractPhysicalPlan nereidsBuilder = Mockito.mock(AbstractPhysicalPlan.class);
        private final PhysicalOlapScan targetRelation = Mockito.mock(PhysicalOlapScan.class);
        private final PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        private final RuntimeFilterContext runtimeFilterContext;
        private final RuntimeFilterTranslator translator;
        private final IdGenerator<RuntimeFilterId> filterIdGenerator = RuntimeFilterId.createGenerator();
        private final SlotReference source;
        private final TupleDescriptor targetTuple = translatorContext.generateTupleDesc();
        private int nextExprId = 1;

        TranslatorHarness() {
            this(PrimitiveType.INT);
        }

        TranslatorHarness(PrimitiveType sourceType) {
            runtimeFilterContext = new RuntimeFilterContext(sessionVariable, filterIdGenerator);
            translator = new RuntimeFilterTranslator(runtimeFilterContext);

            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(partitionInfo.getType()).thenReturn(PartitionType.UNPARTITIONED);
            Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(table.getPartition(1L)).thenReturn(partition);
            Mockito.when(partition.getDistributionInfo()).thenReturn(
                    new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));
            Mockito.when(scanNode.getOlapTable()).thenReturn(table);
            Mockito.when(scanNode.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L));
            Mockito.when(scanNode.getId()).thenReturn(new PlanNodeId(SCAN_NODE_ID));
            Mockito.when(targetRelation.getTable()).thenReturn(table);
            Mockito.when(targetRelation.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L));

            PlanFragment fragment = Mockito.mock(PlanFragment.class);
            PlanFragmentId fragmentId = new PlanFragmentId(3);
            Mockito.when(fragment.getFragmentId()).thenReturn(fragmentId);
            Mockito.doCallRealMethod().when(builderNode).setFragment(fragment);
            Mockito.doCallRealMethod().when(scanNode).setFragment(fragment);
            builderNode.setFragment(fragment);
            scanNode.setFragment(fragment);
            Mockito.when(builderNode.getFragment()).thenReturn(fragment);
            Mockito.when(scanNode.getFragment()).thenReturn(fragment);
            Mockito.when(builderNode.getFragmentId()).thenReturn(fragmentId);
            Mockito.when(scanNode.getFragmentId()).thenReturn(fragmentId);

            Column sourceColumn = new Column("src", sourceType);
            source = addSlot("src", sourceColumn, DataType.fromCatalogType(sourceColumn.getType()),
                    translatorContext.generateTupleDesc());
        }

        SlotReference addTargetSlot(String name, Column column, DataType dataType) {
            SlotReference target = addSlot(name, column, dataType, targetTuple);
            SlotRef targetSlotRef = translatorContext.findSlotRef(target.getExprId());
            runtimeFilterContext.getExprIdToOlapScanNodeSlotRef().put(target.getExprId(), targetSlotRef);
            runtimeFilterContext.getScanNodeOfLegacyRuntimeFilterTarget().put(target, scanNode);
            return target;
        }

        RuntimeFilter newFilter(SlotReference target, Expression targetExpression) {
            RuntimeFilter filter = new RuntimeFilter(filterIdGenerator.getNextId(), source, target, targetExpression,
                    TRuntimeFilterType.IN, 0, nereidsBuilder, 10, false,
                    TMinMaxRuntimeFilterType.MIN_MAX, targetRelation);
            runtimeFilterContext.generateRuntimeFilterPruneMetadata(filter);
            return filter;
        }

        TRuntimeFilterDesc translate(List<RuntimeFilter> filters) {
            translator.createLegacyRuntimeFilters(filters, builderNode, translatorContext);
            Assertions.assertEquals(1, runtimeFilterContext.getLegacyFilters().size());
            return runtimeFilterContext.getLegacyFilters().get(0).toThrift();
        }

        private SlotReference addSlot(String name, Column column, DataType dataType, TupleDescriptor tuple) {
            SlotReference slot = new SlotReference(new ExprId(nextExprId++), name, dataType,
                    false, ImmutableList.of("t"), table, column, table, column);
            translatorContext.createSlotDesc(tuple, slot);
            return slot;
        }
    }
}
