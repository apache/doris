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
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/** Exercises RF generation, real exchange translation, slot binding and fragment registration together. */
public class RuntimeFilterExchangeTest extends SSBTestBase {
    private static final String SHUFFLE_JOIN = "select lo_custkey from lineorder"
            + " join [shuffle] customer on lo_custkey = c_custkey";

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setRuntimeFilterMode("GLOBAL");
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN.getValue());
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @BeforeEach
    void enableExchangeFilters() {
        connectContext.getSessionVariable().enableRuntimeFilterOnExchange = true;
    }

    @AfterEach
    void resetExchangeFilters() {
        connectContext.getSessionVariable().enableRuntimeFilterOnExchange = false;
    }

    @Test
    void testShuffleRegistersRemoteExchangeAndScanTargets() {
        TranslatedPlan plan = translate(SHUFFLE_JOIN);
        Assertions.assertEquals(1, plan.filters.getLegacyFilters().size());
        org.apache.doris.planner.RuntimeFilter filter = plan.filters.getLegacyFilters().get(0);
        TRuntimeFilterDesc desc = filter.toThrift();
        Assertions.assertTrue(desc.has_remote_targets);
        Assertions.assertFalse(desc.has_local_targets);
        Assertions.assertTrue(plan.exchangeTargets().size() > 0);
        Assertions.assertTrue(desc.planId_to_target_expr.size() > plan.exchangeTargets().size());
        Assertions.assertTrue(filter.getBuilderNode().getFragment().getTargetRuntimeFilterIds()
                .contains(filter.getFilterId()));
        plan.checkExchangeBindings();
    }

    @Test
    void testDisabledByDefaultAndKeepsScanFilters() {
        Assertions.assertFalse(new SessionVariable().enableRuntimeFilterOnExchange);
        connectContext.getSessionVariable().enableRuntimeFilterOnExchange = false;
        TranslatedPlan plan = translate(SHUFFLE_JOIN);
        Assertions.assertEquals(1, plan.filters.getLegacyFilters().size());
        Assertions.assertTrue(plan.exchangeTargets().isEmpty());
        Assertions.assertTrue(plan.filters.getNereidsRuntimeFilter().stream()
                .allMatch(filter -> filter.getExchangeTargets().isEmpty()));
    }

    @Test
    void testAliasBelowExchangeUsesBoundaryExpression() {
        TranslatedPlan plan = translate("select x from (select lo_custkey + 1 as x from lineorder) t"
                + " join [shuffle] customer on x = c_custkey");
        Assertions.assertFalse(plan.exchangeTargets().isEmpty());
        Assertions.assertTrue(plan.filters.getNereidsRuntimeFilter().stream()
                .anyMatch(filter -> !(filter.getTargetExpression() instanceof Slot)
                        && filter.getExchangeTargets().values().stream().anyMatch(Slot.class::isInstance)));
        plan.checkExchangeBindings();
    }

    @Test
    void testProjectionOnExchangeBindsInputTuple() {
        TranslatedPlan plan = translate(SHUFFLE_JOIN, input -> (PhysicalPlan) input.rewriteDownShortCircuit(node -> {
            if (!(node instanceof PhysicalDistribute)) {
                return node;
            }
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) node;
            return new PhysicalProject<>(ImmutableList.<NamedExpression>copyOf(distribute.getOutput()),
                    distribute.getLogicalProperties(), distribute)
                    .withPhysicalPropertiesAndStats(distribute.getPhysicalProperties(), distribute.getStats());
        }));
        Assertions.assertTrue(plan.exchangeTargets().stream()
                .anyMatch(target -> target.exchange.getOutputTupleDesc() != null));
        plan.checkExchangeBindings();
    }

    @Test
    void testMultipleExchangesReceiveSameFilter() {
        TranslatedPlan plan = translate(SHUFFLE_JOIN, input -> (PhysicalPlan) input.rewriteDownShortCircuit(node -> {
            if (!(node instanceof PhysicalDistribute)) {
                return node;
            }
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) node;
            return new PhysicalDistribute<>(distribute.getDistributionSpec(), distribute)
                    .withPhysicalPropertiesAndStats(distribute.getPhysicalProperties(), distribute.getStats());
        }));
        Assertions.assertEquals(1, plan.filters.getLegacyFilters().size());
        Assertions.assertTrue(plan.exchangeTargets().size() >= 2);
        plan.checkExchangeBindings();
    }

    @Test
    void testGroupedUnionTargetsDeduplicateSharedExchange() {
        TranslatedPlan plan = translate("select u.k from (select lo_custkey as k from lineorder"
                + " union all select p_partkey as k from part) u"
                + " join [shuffle] customer on u.k = c_custkey");
        Assertions.assertEquals(2, plan.filters.getNereidsRuntimeFilter().size());
        Assertions.assertEquals(1, plan.filters.getLegacyFilters().size());
        Assertions.assertFalse(plan.exchangeTargets().isEmpty());
        for (ExchangeTarget target : plan.exchangeTargets()) {
            Assertions.assertEquals(1, target.exchange.treeToThrift().nodes.get(0).runtime_filters.size());
        }
        plan.checkExchangeBindings();
    }

    @Test
    void testExchangeAboveJoinKeepsCorrectProbeExpression() {
        TranslatedPlan plan = translate("select lo_custkey from lineorder"
                + " join [broadcast] part on lo_partkey = p_partkey"
                + " join [shuffle] customer on lo_custkey = c_custkey");
        Assertions.assertTrue(plan.exchangeTargets().stream()
                .anyMatch(target -> target.exchange.getChild(0).getChildren().size() > 1));
        plan.checkExchangeBindings();
    }

    @Test
    void testEquivalentReceiverTargetsKeepFirstPredicate() {
        TranslatedPlan plan = translate(SHUFFLE_JOIN, UnaryOperator.identity(), filters -> {
            RuntimeFilter filter = filters.getNereidsRuntimeFilter().get(0);
            Map.Entry<Integer, Expression> target = filter.getExchangeTargets().entrySet().iterator().next();
            // Equivalence expansion may revisit one scan/receiver through another equality.
            // x and x + 0 are both valid targets; the protocol needs only one predicate per node.
            filter.addExchangeTargets(ImmutableMap.of(target.getKey(),
                    new Add(target.getValue(), new IntegerLiteral(0))));
            Assertions.assertSame(target.getValue(), filter.getExchangeTargets().get(target.getKey()));
        });
        Assertions.assertEquals(1, plan.exchangeTargets().size());
        plan.checkExchangeBindings();
    }

    @Test
    void testGroupedNonBlockingTargetKeepsZeroWait() {
        TranslatedPlan plan = translate("select u.k from (select lo_custkey as k from lineorder"
                + " union all select p_partkey as k from part) u"
                + " join [shuffle] customer on u.k = c_custkey", UnaryOperator.identity(),
                filters -> filters.getNereidsRuntimeFilter().get(0).setNonBlocking(true));
        Assertions.assertEquals(1, plan.filters.getLegacyFilters().size());
        Assertions.assertEquals(0, plan.filters.getLegacyFilters().get(0).toThrift().wait_time_ms);
        Assertions.assertFalse(plan.exchangeTargets().isEmpty());
        plan.checkExchangeBindings();
    }

    @Test
    void testCteSinkExpressionIsNotOverwrittenByReceiverTarget() {
        boolean oldMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        try {
            TranslatedPlan plan = translate("with t as (select lo_custkey as k from lineorder)"
                    + " select c1.k from t c1 join [shuffle] customer on c1.k = c_custkey"
                    + " union all select c2.k from t c2");
            Assertions.assertTrue(plan.filters.getNereidsRuntimeFilter().stream()
                    .anyMatch(filter -> filter.getTargetScan() instanceof PhysicalCTEConsumer));
            Assertions.assertFalse(plan.filters.getLegacyFilters().isEmpty());
            Assertions.assertTrue(plan.exchangeTargets().isEmpty());
        } finally {
            connectContext.getSessionVariable().enableCTEMaterialize = oldMaterialize;
        }
    }

    @Test
    void testGroupingSetsKeepExistingPushdownBarrier() {
        TranslatedPlan plan = translate("select t.k from (select lo_custkey as k from lineorder"
                + " group by grouping sets ((lo_custkey), ())) t"
                + " join [shuffle] customer on t.k = c_custkey");
        Assertions.assertTrue(plan.filters.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertTrue(plan.exchangeTargets().isEmpty());
    }

    @Test
    void testTopNRemainsPushdownBarrier() {
        TranslatedPlan plan = translate("select t.k from (select lo_custkey as k from lineorder"
                + " order by lo_orderkey limit 10) t join [shuffle] customer on t.k = c_custkey");
        Assertions.assertTrue(plan.filters.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertTrue(plan.exchangeTargets().isEmpty());
    }

    private TranslatedPlan translate(String sql) {
        return translate(sql, UnaryOperator.identity());
    }

    private TranslatedPlan translate(String sql, UnaryOperator<PhysicalPlan> beforeGeneration) {
        return translate(sql, beforeGeneration, filters -> { });
    }

    private TranslatedPlan translate(String sql, UnaryOperator<PhysicalPlan> beforeGeneration,
            Consumer<RuntimeFilterContext> beforeTranslation) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().optimize();
        PhysicalPlan physical = checker.getBestPlanTree();
        for (PlanPostProcessor processor : new PlanPostProcessors(checker.getCascadesContext()).getProcessors()) {
            if (processor instanceof RuntimeFilterGenerator) {
                // Inject legal physical shapes after normal project cleanup, before RF generation.
                physical = beforeGeneration.apply(physical);
            }
            physical = (PhysicalPlan) processor.processRoot(physical, checker.getCascadesContext());
        }
        beforeTranslation.accept(checker.getCascadesContext().getRuntimeFilterContext());
        PlanTranslatorContext context = new PlanTranslatorContext(checker.getCascadesContext());
        PlanFragment root = new PhysicalPlanTranslator(context).translatePlan(physical);
        return new TranslatedPlan(context, root, checker.getCascadesContext().getRuntimeFilterContext());
    }

    private static class ExchangeTarget {
        final ExchangeNode exchange;
        final TRuntimeFilterDesc filter;

        ExchangeTarget(ExchangeNode exchange, TRuntimeFilterDesc filter) {
            this.exchange = exchange;
            this.filter = filter;
        }
    }

    private static class TranslatedPlan {
        final PlanTranslatorContext context;
        final RuntimeFilterContext filters;
        final Map<Integer, PlanNode> nodes = new HashMap<>();

        TranslatedPlan(PlanTranslatorContext context, PlanFragment root, RuntimeFilterContext filters) {
            this.context = context;
            this.filters = filters;
            collectNodes(root.getPlanRoot());
        }

        void collectNodes(PlanNode node) {
            if (nodes.putIfAbsent(node.getId().asInt(), node) != null) {
                return;
            }
            node.getChildren().forEach(this::collectNodes);
        }

        List<ExchangeTarget> exchangeTargets() {
            List<ExchangeTarget> targets = new ArrayList<>();
            for (PlanNode node : nodes.values()) {
                if (!(node instanceof ExchangeNode)) {
                    continue;
                }
                List<TRuntimeFilterDesc> descs = node.treeToThrift().nodes.get(0).runtime_filters;
                if (descs != null) {
                    descs.forEach(desc -> targets.add(new ExchangeTarget((ExchangeNode) node, desc)));
                }
            }
            return targets;
        }

        void checkExchangeBindings() {
            for (ExchangeTarget target : exchangeTargets()) {
                Map<Integer, SlotDescriptor> inputSlots = target.exchange.getTupleIds().stream()
                        .flatMap(tuple -> context.getTupleDesc(tuple).getSlots().stream())
                        .collect(Collectors.toMap(slot -> slot.getId().asInt(), slot -> slot));
                for (TExprNode expr : target.filter.planId_to_target_expr.get(target.exchange.getId().asInt()).nodes) {
                    if (expr.isSetSlotRef()) {
                        Assertions.assertTrue(inputSlots.containsKey(expr.slot_ref.slot_id),
                                "exchange RF must use an input slot, before receiver projection");
                    }
                }
                Assertions.assertTrue(target.exchange.getFragment().getTargetRuntimeFilterIds().stream()
                        .anyMatch(id -> id.asInt() == target.filter.filter_id));
            }
        }
    }
}
