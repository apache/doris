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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class TopNRuntimeFilterTest extends SSBTestBase implements MemoPatternMatchSupported {
    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
    }

    @Test
    public void testUseTopNRf() {
        String sql = "select * from customer order by c_custkey limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        Plan rfSource = plan.child(0).child(0).child(0).child(0);
        Assertions.assertInstanceOf(PhysicalTopN.class, rfSource);
        PhysicalTopN<? extends Plan> localTopN
                = (PhysicalTopN<? extends Plan>) rfSource;
        Assertions.assertTrue(checker.getCascadesContext().getTopnFilterContext().isTopnFilterSource(localTopN));
    }

    @Test
    public void testTranslateTopNFilterTargetThroughLazyMaterializeOlapScan() {
        String sql = "select * from customer order by c_custkey limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        TopnFilter filter = checker.getCascadesContext().getTopnFilterContext().getTopnFilters().stream()
                .filter(f -> f.targets.keySet().stream().anyMatch(PhysicalLazyMaterializeOlapScan.class::isInstance))
                .findFirst()
                .orElseThrow(() -> new AssertionError("topn filter target is not lazy materialize olap scan"));
        PhysicalLazyMaterializeOlapScan lazyScan = filter.targets.keySet().stream()
                .filter(PhysicalLazyMaterializeOlapScan.class::isInstance)
                .map(PhysicalLazyMaterializeOlapScan.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("lazy materialize olap scan not found"));
        Expression probeExpr = filter.targets.get(lazyScan);

        TopnFilterContext topnFilterContext = new TopnFilterContext();
        topnFilterContext.addTopnFilter(filter.topn, filter.source, lazyScan.getScan(), probeExpr);
        PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        probeExpr.getInputSlots().forEach(slot -> translatorContext
                .createSlotDesc(translatorContext.generateTupleDesc(), (SlotReference) slot));
        ScanNode legacyScan = Mockito.mock(ScanNode.class);
        topnFilterContext.translateTarget(lazyScan, legacyScan, translatorContext);

        Assertions.assertTrue(topnFilterContext.getTopnFilter(filter.source).legacyTargets.containsKey(legacyScan));
    }

    @Test
    public void testUseTopNRfForComplexCase() {
        String sql = "select * from (select 1) tl join (select * from customer order by c_custkey limit 5) tb";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        Plan rfSource = plan.child(0).child(1).child(0).child(0).child(0);
        Assertions.assertEquals(SortPhase.LOCAL_SORT, ((PhysicalTopN<? extends Plan>) rfSource).getSortPhase());
        PhysicalTopN<? extends Plan> localTopN = (PhysicalTopN<? extends Plan>) rfSource;
        Assertions.assertTrue(checker.getCascadesContext().getTopnFilterContext().isTopnFilterSource(localTopN));
    }

    @Test
    public void testUseLowestAggregateAsTopNRuntimeFilterSource() {
        String sql = "select c_nation, count(*) from customer group by c_nation limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        TopnFilterContext filterContext = checker.getCascadesContext().getTopnFilterContext();
        TopnFilter filter = filterContext.getTopnFilters().stream()
                .filter(f -> f.source instanceof PhysicalHashAggregate)
                .findFirst()
                .orElseThrow(() -> new AssertionError("aggregate topn filter source not found"));
        PhysicalHashAggregate<?> aggregateSource = (PhysicalHashAggregate<?>) filter.source;
        List<PhysicalHashAggregate<? extends Plan>> pushedAggregates = plan.collectToList(
                node -> node instanceof PhysicalHashAggregate
                        && ((PhysicalHashAggregate<?>) node).getTopnPushInfo() != null);

        Assertions.assertTrue(pushedAggregates.size() >= 2, plan.treeString());
        int lowestDepth = pushedAggregates.stream().mapToInt(Plan::depth).min().orElseThrow();
        Assertions.assertEquals(lowestDepth, aggregateSource.depth());
        Assertions.assertTrue(filterContext.isTopnFilterSource(aggregateSource));
    }

    @Test
    public void testTranslateAggregateTopNRuntimeFilterSource() {
        String sql = "select c_nation, count(*) from customer "
                + "group by c_nation order by c_nation limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        TopnFilter filter = checker.getCascadesContext().getTopnFilterContext().getTopnFilters().stream()
                .filter(f -> f.source instanceof PhysicalHashAggregate)
                .findFirst()
                .orElseThrow(() -> new AssertionError("aggregate topn filter source not found"));
        PlanFragment fragment = new PhysicalPlanTranslator(
                new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);

        Assertions.assertNotNull(fragment);
        Assertions.assertInstanceOf(AggregationNode.class, filter.legacySourceNode);
        Assertions.assertEquals(filter.legacySourceNode.getId().asInt(),
                filter.toThrift().getSourceNodeId());
        Assertions.assertFalse(filter.legacyTargets.isEmpty());
        for (ScanNode scanNode : filter.legacyTargets.keySet()) {
            Assertions.assertTrue(scanNode.getTopnFilterSourceNodes().contains(filter.legacySourceNode));
        }
    }

    @Test
    public void testFallbackToSortSourceWhenPushTopNToAggregateDisabled() {
        String sql = "select c_nation, count(*) from customer "
                + "group by c_nation order by c_nation limit 5";
        boolean originalPushTopnToAgg = connectContext.getSessionVariable().pushTopnToAgg;
        long originalTopnOptLimitThreshold = connectContext.getSessionVariable().topnOptLimitThreshold;
        connectContext.getSessionVariable().pushTopnToAgg = false;
        connectContext.getSessionVariable().topnOptLimitThreshold = 0;
        try {
            PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                    .rewrite()
                    .implement();
            PhysicalPlan plan = checker.getPhysicalPlan();
            new PlanPostProcessors(checker.getCascadesContext()).process(plan);

            Assertions.assertFalse(checker.getCascadesContext().getTopnFilterContext()
                    .getTopnFilters().isEmpty());
            checker.getCascadesContext().getTopnFilterContext().getTopnFilters()
                    .forEach(filter -> Assertions.assertInstanceOf(PhysicalTopN.class, filter.source));
        } finally {
            connectContext.getSessionVariable().pushTopnToAgg = originalPushTopnToAgg;
            connectContext.getSessionVariable().topnOptLimitThreshold = originalTopnOptLimitThreshold;
        }
    }

    @Test
    public void testNotUseTopNRfOnWindow() {
        String sql = "select rank() over (partition by c_nation order by c_custkey) "
                + "from customer order by c_custkey limit 3";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite().implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        System.out.println(plan.treeString());
        PhysicalTopN<? extends Plan> localTopN =
                (PhysicalTopN<? extends Plan>) plan.child(0).child(0).child(0);
        Assertions.assertTrue(localTopN.getSortPhase().isLocal());
        Assertions.assertFalse(checker.getCascadesContext().getTopnFilterContext().isTopnFilterSource(localTopN));
    }

    @Test
    public void testNotUseTopNRfForUnsupportedComplexOrderKey() {
        String sql = "select c_custkey from customer order by array(c_custkey) limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        List<PhysicalTopN<? extends Plan>> localTopNs = plan.collectToList(
                node -> node instanceof PhysicalTopN
                        && ((PhysicalTopN<?>) node).getSortPhase().isLocal());
        Assertions.assertFalse(localTopNs.isEmpty(), plan.treeString());
        for (PhysicalTopN<? extends Plan> localTopN : localTopNs) {
            Assertions.assertFalse(
                    checker.getCascadesContext().getTopnFilterContext().isTopnFilterSource(localTopN));
        }
    }

    @Test
    public void testProbeExprNullableThroughRightOuterJoin() {
        // topn node push down filter value to scan node.
        // the filter value is nullable.
        // but c_name in scan node is not nullable. so we use
        // substring(nullable(c_name), 1, 5) as probe expr on scan node
        String sql = "select substring(c_name, 1, 5) "
                + "from customer c right outer join lineorder l "
                + "on c.c_custkey = l.lo_custkey "
                + "order by substring(c_name, 1, 5) nulls last limit 3";
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        TopnFilterContext ctx = checker.getCascadesContext().getTopnFilterContext();
        Assertions.assertFalse(ctx.getTopnFilters().isEmpty(), "topn filter should be created");

        TopnFilter filter = ctx.getTopnFilters().stream()
                .filter(f -> f.targets.values().stream().anyMatch(expr -> expr instanceof Substring))
                .findFirst()
                .orElseThrow(() -> new AssertionError("topn filter with substring probe not found"));

        Map.Entry<PhysicalRelation, Expression> target = filter.targets.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof Substring)
                .findFirst()
                .orElseThrow(() -> new AssertionError("substring probe target not found"));
        PhysicalRelation relation = target.getKey();
        Expression probeExpr = target.getValue();

        Slot cNameSlot = relation.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase("c_name"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("c_name slot not found"));

        Expression expectedProbeExpr = new Substring(new Nullable(cNameSlot), new IntegerLiteral(1),
                new IntegerLiteral(5));
        Assertions.assertEquals(expectedProbeExpr, probeExpr);
    }
}
