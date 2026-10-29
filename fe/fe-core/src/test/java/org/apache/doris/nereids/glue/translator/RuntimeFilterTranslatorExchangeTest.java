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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Runtime filters are also attached to the exchanges between the target scan and the builder join. */
class RuntimeFilterTranslatorExchangeTest {
    private static final int SCAN_NODE_ID = 7;
    private static final int EXCHANGE_NODE_ID = 11;

    private ConnectContext previousContext;
    private SessionVariable sessionVariable;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        sessionVariable = new SessionVariable();
        sessionVariable.setEnableRuntimeFilterPartitionPrune(false);
        sessionVariable.setEnableRuntimeFilterBucketPrune(false);
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
    void testExchangeOnScanToBuilderPathGetsTarget() {
        TranslatorHarness harness = new TranslatorHarness();
        TranslatorHarness.Target target = harness.addTargetSlot("k");

        TRuntimeFilterDesc desc = harness.translate(target, /*intermediateChildren=*/1);

        // The scan target and the exchange target are both serialized, each with the slot that is
        // valid in the corresponding node's output tuple.
        Assertions.assertEquals(2, desc.planId_to_target_expr.size());
        Assertions.assertEquals(target.scanSlotId,
                desc.planId_to_target_expr.get(SCAN_NODE_ID).nodes.get(0).slot_ref.slot_id);
        Assertions.assertEquals(target.exchangeSlotId,
                desc.planId_to_target_expr.get(EXCHANGE_NODE_ID).nodes.get(0).slot_ref.slot_id);
    }

    @Test
    void testExchangeBelowBranchingNodeIsSkipped() {
        TranslatorHarness harness = new TranslatorHarness();
        TranslatorHarness.Target target = harness.addTargetSlot("k");

        // A join/union between the exchange and the scan means rows crossing the exchange are not
        // all rows of the target scan, so filtering there could drop valid rows.
        TRuntimeFilterDesc desc = harness.translate(target, /*intermediateChildren=*/2);

        Assertions.assertEquals(1, desc.planId_to_target_expr.size());
        Assertions.assertTrue(desc.planId_to_target_expr.containsKey(SCAN_NODE_ID));
    }

    @Test
    void testExchangeWithoutTargetSlotInOutputIsSkipped() {
        TranslatorHarness harness = new TranslatorHarness();
        TranslatorHarness.Target target = harness.addTargetSlot("k");

        // The exchange output tuple does not carry the target slot any more (an intermediate
        // operator consumed or renamed it), so the filter cannot be evaluated there.
        TRuntimeFilterDesc desc = harness.translate(target, /*intermediateChildren=*/1,
                /*exchangeCarriesTargetSlot=*/false);

        Assertions.assertEquals(1, desc.planId_to_target_expr.size());
        Assertions.assertTrue(desc.planId_to_target_expr.containsKey(SCAN_NODE_ID));
    }

    private class TranslatorHarness {
        private final OlapTable table = Mockito.mock(OlapTable.class);
        private final OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        private final ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        private final PlanNode intermediateNode = Mockito.mock(PlanNode.class);
        private final PlanNode otherBranchNode = Mockito.mock(PlanNode.class);
        private final PlanNode builderNode = Mockito.mock(PlanNode.class);
        private final AbstractPhysicalPlan nereidsBuilder = Mockito.mock(AbstractPhysicalPlan.class);
        private final PhysicalOlapScan targetRelation = Mockito.mock(PhysicalOlapScan.class);
        private final PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        private final RuntimeFilterContext runtimeFilterContext;
        private final RuntimeFilterTranslator translator;
        private final IdGenerator<RuntimeFilterId> filterIdGenerator = RuntimeFilterId.createGenerator();
        private final TupleDescriptor targetTuple = translatorContext.generateTupleDesc();
        private final TupleDescriptor exchangeTuple = translatorContext.generateTupleDesc();
        private final SlotReference source;
        private int nextExprId = 1;

        TranslatorHarness() {
            runtimeFilterContext = new RuntimeFilterContext(sessionVariable, filterIdGenerator);
            translator = new RuntimeFilterTranslator(runtimeFilterContext);

            PlanFragment fragment = Mockito.mock(PlanFragment.class);
            PlanFragmentId fragmentId = new PlanFragmentId(3);
            Mockito.when(fragment.getFragmentId()).thenReturn(fragmentId);
            for (PlanNode node : ImmutableList.of(builderNode, scanNode, exchangeNode, intermediateNode)) {
                Mockito.doCallRealMethod().when(node).setFragment(fragment);
                node.setFragment(fragment);
                Mockito.when(node.getFragment()).thenReturn(fragment);
                Mockito.when(node.getFragmentId()).thenReturn(fragmentId);
            }
            Mockito.when(scanNode.getId()).thenReturn(new PlanNodeId(SCAN_NODE_ID));
            Mockito.when(exchangeNode.getId()).thenReturn(new PlanNodeId(EXCHANGE_NODE_ID));
            Mockito.when(scanNode.getChildren()).thenReturn(Lists.newArrayList());

            Column sourceColumn = new Column("src", PrimitiveType.INT);
            source = addSlot("src", sourceColumn, DataType.fromCatalogType(sourceColumn.getType()),
                    translatorContext.generateTupleDesc());
        }

        Target addTargetSlot(String name) {
            Column column = new Column(name, PrimitiveType.INT);
            SlotReference targetSlot = addSlot(name, column, IntegerType.INSTANCE, targetTuple);
            SlotRef scanSlotRef = translatorContext.findSlotRef(targetSlot.getExprId());
            runtimeFilterContext.getExprIdToOlapScanNodeSlotRef().put(targetSlot.getExprId(), scanSlotRef);
            runtimeFilterContext.getScanNodeOfLegacyRuntimeFilterTarget().put(targetSlot, scanNode);
            return new Target(targetSlot, scanSlotRef.getSlotId().asInt());
        }

        TRuntimeFilterDesc translate(Target target, int intermediateChildren) {
            return translate(target, intermediateChildren, true);
        }

        TRuntimeFilterDesc translate(Target target, int intermediateChildren,
                boolean exchangeCarriesTargetSlot) {
            if (exchangeCarriesTargetSlot) {
                SlotDescriptor exchangeSlot =
                        translatorContext.createSlotDesc(exchangeTuple, target.slot);
                target.exchangeSlotId = exchangeSlot.getId().asInt();
            }
            Mockito.when(exchangeNode.getOutputTupleIds()).thenReturn(
                    ImmutableList.of(exchangeTuple.getId()));
            Mockito.when(intermediateNode.getOutputTupleIds()).thenReturn(
                    ImmutableList.of(exchangeTuple.getId()));
            Mockito.when(exchangeNode.getChildren()).thenReturn(Lists.newArrayList(intermediateNode));
            Mockito.when(intermediateNode.getChildren()).thenReturn(intermediateChildren == 1
                    ? Lists.newArrayList(scanNode)
                    : Lists.newArrayList(scanNode, otherBranchNode));
            Mockito.when(builderNode.getChildren()).thenReturn(Lists.newArrayList(exchangeNode));

            RuntimeFilter filter = new RuntimeFilter(filterIdGenerator.getNextId(), source,
                    target.slot, target.slot, TRuntimeFilterType.IN, 0, nereidsBuilder, 10, false,
                    TMinMaxRuntimeFilterType.MIN_MAX, targetRelation);
            translator.createLegacyRuntimeFilters(ImmutableList.of(filter), builderNode,
                    translatorContext);
            Assertions.assertEquals(1, runtimeFilterContext.getLegacyFilters().size());
            return runtimeFilterContext.getLegacyFilters().get(0).toThrift();
        }

        private SlotReference addSlot(String name, Column column, DataType dataType,
                TupleDescriptor tuple) {
            SlotReference slot = new SlotReference(new ExprId(nextExprId++), name, dataType,
                    false, ImmutableList.of("t"), table, column, table, column);
            translatorContext.createSlotDesc(tuple, slot);
            return slot;
        }

        private static class Target {
            private final SlotReference slot;
            private final int scanSlotId;
            private int exchangeSlotId;

            Target(SlotReference slot, int scanSlotId) {
                this.slot = slot;
                this.scanSlotId = scanSlotId;
            }
        }
    }
}
