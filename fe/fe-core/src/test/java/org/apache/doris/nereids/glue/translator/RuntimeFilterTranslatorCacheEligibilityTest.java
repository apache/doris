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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.runtimefilterv2.RuntimeFilterV2;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class RuntimeFilterTranslatorCacheEligibilityTest {
    private static final int SCAN_NODE_ID = 7;

    private ConnectContext previousContext;
    private SessionVariable sessionVariable;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        sessionVariable = new SessionVariable();
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
    void testVolatileProbeDisablesCacheForSingleAndGroupedFilters() {
        for (int mode = 0; mode < 4; mode++) {
            boolean grouped = mode % 2 == 1;
            for (boolean implicitCast : ImmutableList.of(false, true)) {
                TranslatorHarness harness = new TranslatorHarness(PrimitiveType.DATETIMEV2, mode >= 2);
                SlotReference target = harness.addTargetSlot("s", new Column("s", PrimitiveType.STRING),
                        StringType.INSTANCE);
                Expression time = new Cast(target, TimeV2Type.SYSTEM_DEFAULT);
                Expression probe = implicitCast ? time : new Cast(time, DateTimeV2Type.SYSTEM_DEFAULT);
                RuntimeFilter filter = harness.newFilter(target, probe);
                List<RuntimeFilter> filters = grouped
                        ? ImmutableList.of(harness.newFilter(target, new Cast(target, DateTimeV2Type.SYSTEM_DEFAULT)),
                                filter)
                        : ImmutableList.of(filter);
                TRuntimeFilterDesc desc = harness.translate(filters);
                Assertions.assertTrue(desc.planId_to_target_expr.containsKey(SCAN_NODE_ID));
                Mockito.verify(harness.scanNode).setEnableConditionCache(false);
                Mockito.verify(harness.scanNode, Mockito.never()).setEnableConditionCache(true);
            }
        }
    }

    @Test
    void testDeterministicProbeDoesNotChangeCacheEligibility() {
        for (int mode = 0; mode < 4; mode++) {
            boolean grouped = mode % 2 == 1;
            TranslatorHarness harness = new TranslatorHarness(PrimitiveType.BIGINT, mode >= 2);
            SlotReference target = harness.addTargetSlot("k", harness.distributionColumn, IntegerType.INSTANCE);
            RuntimeFilter filter = harness.newFilter(target, new Add(target, new IntegerLiteral(1)));
            harness.translate(grouped ? ImmutableList.of(filter, harness.newFilter(target, target))
                    : ImmutableList.of(filter));
            Mockito.verify(harness.scanNode, Mockito.never()).setEnableConditionCache(Mockito.anyBoolean());
        }
    }

    private class TranslatorHarness {
        private final Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        private final OlapTable table = Mockito.mock(OlapTable.class);
        private final OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        private final HashJoinNode builderNode = Mockito.mock(HashJoinNode.class);
        private final AbstractPhysicalJoin nereidsBuilder = Mockito.mock(AbstractPhysicalJoin.class);
        private final PhysicalOlapScan targetRelation = Mockito.mock(PhysicalOlapScan.class);
        private final PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        private final RuntimeFilterContext runtimeFilterContext;
        private final RuntimeFilterTranslator translator;
        private final IdGenerator<RuntimeFilterId> filterIdGenerator = RuntimeFilterId.createGenerator();
        private final SlotReference source;
        private final TupleDescriptor targetTuple = translatorContext.generateTupleDesc();
        private int nextExprId = 1;

        private final boolean v2;

        TranslatorHarness(PrimitiveType sourceType, boolean v2) {
            this.v2 = v2;
            runtimeFilterContext = new RuntimeFilterContext(sessionVariable, filterIdGenerator);
            translator = new RuntimeFilterTranslator(runtimeFilterContext);

            BinaryPredicate joinCondition = Mockito.mock(BinaryPredicate.class);
            Mockito.when(joinCondition.getOp()).thenReturn(BinaryPredicate.Operator.EQ);
            Mockito.when(builderNode.getEqJoinConjuncts()).thenReturn(ImmutableList.of(joinCondition));
            Mockito.when(scanNode.getId()).thenReturn(new PlanNodeId(SCAN_NODE_ID));
            Mockito.when(scanNode.getTupleIds()).thenReturn(new ArrayList<>(ImmutableList.of(targetTuple.getId())));
            Mockito.when(targetRelation.getTable()).thenReturn(table);

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
            return new RuntimeFilter(filterIdGenerator.getNextId(), source, ImmutableList.of(target),
                    ImmutableList.of(targetExpression), TRuntimeFilterType.IN, 0, nereidsBuilder, 10, false,
                    TMinMaxRuntimeFilterType.MIN_MAX, targetRelation);
        }

        TRuntimeFilterDesc translate(List<RuntimeFilter> filters) {
            if (v2) {
                List<RuntimeFilterV2> v2Filters = new ArrayList<>();
                for (RuntimeFilter filter : filters) {
                    Expression probe = filter.getTargetExpressions().get(0);
                    RuntimeFilterV2 v2Filter = new RuntimeFilterV2(filter.getId(), nereidsBuilder, source,
                            10, 0, targetRelation, probe, TRuntimeFilterType.IN);
                    v2Filter.setMinMaxSubType(TMinMaxRuntimeFilterType.MIN_MAX);
                    v2Filter.setLegacyTargetNode(scanNode);
                    v2Filter.setLegacyTargetExpr(ExpressionTranslator.translate(probe, translatorContext));
                    v2Filters.add(v2Filter);
                }
                RunTimeFilterTranslatorV2.INSTANCE.createLegacyRuntimeFilters(builderNode, v2Filters,
                        translatorContext);
                Assertions.assertEquals(1, translatorContext.getRuntimeFilterV2Context().getLegacyFilters().size());
                return translatorContext.getRuntimeFilterV2Context().getLegacyFilters().get(0).toThrift();
            }
            RuntimeFilter head = filters.get(0);
            for (int i = 1; i < filters.size(); i++) {
                RuntimeFilter filter = filters.get(i);
                head.addTargetSlot(filter.getTargetSlots().get(0), filter.getTargetExpressions().get(0), targetRelation);
            }
            translator.createLegacyRuntimeFilter(head, builderNode, translatorContext);
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
