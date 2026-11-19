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
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExplainLevel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopNRuntimeFilterTest extends SSBTestBase implements MemoPatternMatchSupported {
    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        createTable("CREATE TABLE topn_safety (id INT, k INT, d DATE, ts BIGINT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE uuid_topn (id INT, u UUID) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE topn_nested_fields (id INT, "
                + "ts STRUCT<a: TIMESTAMPTZ(0), b: TIMESTAMPTZ(0)>, "
                + "si STRUCT<a: INT, b: INT>, sd STRUCT<a: DATE, b: DATE>) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
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
    public void testTopNFilterDoesNotRequireTotalSecondaryExpressions() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        try {
            for (String expression : List.of("weeks_add(d, 1)", "from_microsecond(ts)",
                    "cast(ts AS TINYINT)", "assert_true(k < 3, 'out of range')")) {
                // A row excluded by the leading key need not evaluate its secondary expression.
                PlanChecker checker = PlanChecker.from(connectContext)
                        .analyze("SELECT k, " + expression + " AS v FROM topn_safety "
                                + "ORDER BY k, v LIMIT 3")
                        .rewrite().implement();
                PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                        .process(checker.getPhysicalPlan());
                Assertions.assertFalse(checker.getCascadesContext().getTopnFilterContext()
                        .getTopnFilters().isEmpty(), plan.treeString());
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
        }
    }

    @Test
    public void testTopNFilterCrossesGroupedKeyAggregation() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        boolean previousPushTopn = connectContext.getSessionVariable().pushTopnToAgg;
        boolean previousJoinReorder = connectContext.getSessionVariable().isDisableJoinReorder();
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().pushTopnToAgg = false;
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        // Empty unit-test tables must retain scans to exercise the final translated plan.
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules + ",PRUNE_EMPTY_PARTITION");
        try {
            for (String sql : List.of(
                    "SELECT k, count(1) FROM topn_safety GROUP BY k ORDER BY k LIMIT 1",
                    "SELECT t.k, t.total FROM (SELECT k, count(1) AS total FROM topn_safety "
                            + "GROUP BY k HAVING total > 10) t JOIN customer c ON t.k = c.c_custkey "
                            + "ORDER BY t.k LIMIT 1",
                    "SELECT k, sum(ts) FROM topn_safety GROUP BY GROUPING SETS((k)) ORDER BY k LIMIT 2")) {
                // Exercise the runtime filter across aggregation, independently of TopN-to-agg pushdown.
                PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
                    String explain = planner.getFragments().stream()
                            .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                            .collect(Collectors.joining("\n"));
                    Assertions.assertTrue(explain.contains("TOPN OPT:"), explain);
                });
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().pushTopnToAgg = previousPushTopn;
            connectContext.getSessionVariable().setDisableJoinReorder(previousJoinReorder);
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testTopNFilterThroughRepeat() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        boolean previousPushTopn = connectContext.getSessionVariable().pushTopnToAgg;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().pushTopnToAgg = false;
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION");
        try {
            for (String groupingSets : List.of("((k), ())", "((k), (id))", "((k, id), (k))")) {
                for (String orderKey : List.of("k", "k + 1", "coalesce(k, 0)")) {
                    for (String direction : List.of("ASC", "DESC")) {
                        for (String nullOrder : List.of("FIRST", "LAST")) {
                            String sql = "SELECT k, c FROM (SELECT k, count(*) AS c FROM topn_safety "
                                    + "GROUP BY GROUPING SETS " + groupingSets
                                    + " UNION ALL SELECT c_custkey, CAST(0 AS BIGINT) FROM customer) u "
                                    + "ORDER BY " + orderKey + " " + direction + " NULLS " + nullOrder + " LIMIT 1";
                            PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                            PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                                    .process(checker.getPhysicalPlan());
                            Assertions.assertTrue(plan.containsType(PhysicalRepeat.class), plan.treeString());
                            List<TopnFilter> filters = checker.getCascadesContext()
                                    .getTopnFilterContext().getTopnFilters();
                            Assertions.assertFalse(filters.isEmpty(), plan.treeString());
                            long targetCount = filters.stream().flatMap(filter -> filter.targets.keySet().stream())
                                    .distinct().count();
                            // Only the common grouping key keeps its value through every Repeat expansion.
                            Assertions.assertEquals(groupingSets.equals("((k, id), (k))") ? 2 : 1,
                                    targetCount, sql + "\n" + filters);
                        }
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().pushTopnToAgg = previousPushTopn;
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testTopNFilterDoesNotCrossAssertNumRows() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION");
        try {
            for (String direction : List.of("ASC", "DESC")) {
                for (boolean nullsFirst : List.of(true, false)) {
                    String sql = "SELECT k FROM (SELECT (SELECT k FROM topn_safety) AS k "
                            + "UNION ALL SELECT c_custkey FROM customer) u ORDER BY k " + direction
                            + " NULLS " + (nullsFirst ? "FIRST" : "LAST") + " LIMIT 1";
                    PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                    PhysicalPlan plan = checker.getPhysicalPlan();
                    List<PhysicalAssertNumRows<? extends Plan>> assertions = plan.collectToList(
                            node -> node instanceof PhysicalAssertNumRows);
                    Assertions.assertEquals(1, assertions.size(), plan.treeString());
                    PhysicalAssertNumRows<? extends Plan> assertion = assertions.get(0);
                    List<PhysicalTopN<? extends Plan>> topns = plan.collectToList(node -> node instanceof PhysicalTopN);
                    Assertions.assertFalse(topns.isEmpty(), plan.treeString());
                    Slot slot = assertion.getOutput().get(0);
                    for (Expression probe : List.of(slot, new Coalesce(slot, new IntegerLiteral(-1)))) {
                        PushDownContext context = new PushDownContext(topns.get(0), probe, nullsFirst);
                        TopnFilterContext filters = new TopnFilterContext();
                        TopnFilterPushDownVisitor visitor = new TopnFilterPushDownVisitor(filters);
                        // Visit Assert directly so the test does not depend on the scalar join's probe side.
                        Assertions.assertFalse(assertion.accept(visitor, context), sql);
                        Assertions.assertTrue(filters.getTopnFilters().isEmpty(), sql);
                        // The child accepts this filter; Assert must protect its row count and empty-row behavior.
                        Assertions.assertTrue(assertion.child().accept(visitor, context), sql);
                        Assertions.assertFalse(filters.getTopnFilters().isEmpty(), sql);
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testTopNFilterSupportsNestedOrderKeyWithSiblingPredicate() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        boolean previousPrune = connectContext.getSessionVariable().enablePruneNestedColumns;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().enablePruneNestedColumns = true;
        // Empty unit-test tables must retain scans to exercise nested access-path translation.
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules + ",PRUNE_EMPTY_PARTITION");
        try {
            for (String column : List.of("ts", "si", "sd")) {
                for (String direction : List.of("ASC", "DESC")) {
                    String sql = "SELECT struct_element(" + column + ", 'a') FROM topn_nested_fields "
                            + "WHERE struct_element(" + column + ", 'b') IS NOT NULL "
                            + "ORDER BY 1 " + direction + " NULLS LAST LIMIT 1";
                    PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
                        String explain = planner.getFragments().stream()
                                .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                                .collect(Collectors.joining("\n"));
                        // TopN must retain its order field when the predicate reads only a sibling's nulls.
                        Assertions.assertTrue(explain.contains("TOPN OPT:"), explain);
                        Assertions.assertTrue(explain.contains("all access paths: ["
                                + column + ".a, " + column + ".b.NULL]"), explain);
                        Assertions.assertTrue(explain.contains("predicate access paths: ["
                                + column + ".b.NULL]"), explain);
                    });
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().enablePruneNestedColumns = previousPrune;
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testUuidTopNFilterPreservesOrderingAndNullPlacement() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        try {
            for (String direction : List.of("ASC", "DESC")) {
                for (String nullPlacement : List.of("FIRST", "LAST")) {
                    PlanChecker checker = PlanChecker.from(connectContext)
                            .analyze("SELECT id,u FROM uuid_topn ORDER BY u " + direction
                                    + " NULLS " + nullPlacement + ",id LIMIT 5")
                            .rewrite().implement();
                    new PlanPostProcessors(checker.getCascadesContext()).process(checker.getPhysicalPlan());
                    List<TopnFilter> filters = checker.getCascadesContext().getTopnFilterContext().getTopnFilters();
                    Assertions.assertFalse(filters.isEmpty());
                    for (TopnFilter filter : filters) {
                        Assertions.assertTrue(filter.topn.getOrderKeys().get(0).getExpr().getDataType().isUuidType());
                        Assertions.assertEquals(direction.equals("ASC"), filter.topn.getOrderKeys().get(0).isAsc());
                        Assertions.assertEquals(nullPlacement.equals("FIRST"),
                                filter.topn.getOrderKeys().get(0).isNullFirst());
                        Assertions.assertFalse(filter.targets.isEmpty());
                        Assertions.assertTrue(filter.targets.values().stream()
                                .allMatch(expr -> expr.getDataType().isUuidType()));
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
        }
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
        topnFilterContext.addTopnFilter(filter.topn, lazyScan.getScan(), probeExpr);
        PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        probeExpr.getInputSlots().forEach(slot -> translatorContext
                .createSlotDesc(translatorContext.generateTupleDesc(), (SlotReference) slot));
        ScanNode legacyScan = Mockito.mock(ScanNode.class);
        topnFilterContext.translateTarget(lazyScan, legacyScan, translatorContext);

        Assertions.assertTrue(topnFilterContext.getTopnFilter(filter.topn).legacyTargets.containsKey(legacyScan));
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
