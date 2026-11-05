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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TSortAlgorithm;
import org.apache.doris.thrift.TSortNode;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTopnFilterDesc;
import org.apache.doris.tso.TSOTimestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopNRuntimeFilterTest extends SSBTestBase implements MemoPatternMatchSupported {
    private boolean previousEnableChangelog;

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        previousEnableChangelog = Config.enable_feature_changelog;
        Config.enable_feature_changelog = true;
        createTable("CREATE TABLE topn_safety (id INT, k INT, d DATE, ts BIGINT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE topn_storage_agg (k INT, v BIGINT SUM) AGGREGATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE topn_storage_dup (k INT, v BIGINT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE topn_storage_mor (k INT, v BIGINT) UNIQUE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='false')");
        createTable("CREATE TABLE topn_storage_mow (k INT, v BIGINT) UNIQUE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'changelog.enable'='true', "
                + "'changelog.need_historical_value'='true')");
        createTable("CREATE TABLE uuid_topn (id INT, u UUID) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE topn_nested_fields (id INT, "
                + "ts STRUCT<a: TIMESTAMPTZ(0), b: TIMESTAMPTZ(0)>, "
                + "si STRUCT<a: INT, b: INT>, sd STRUCT<a: DATE, b: DATE>) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Override
    protected void runAfterAll() throws Exception {
        Config.enable_feature_changelog = previousEnableChangelog;
        super.runAfterAll();
    }

    @Test
    public void testTopNFilterDoesNotCrossStorageMerge() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        String previousRules = String.join(",", connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().forceSortAlgorithm = "heap";
        connectContext.getSessionVariable().setDisableNereidsRules(previousRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION");
        try {
            for (String source : List.of("topn_storage_agg", "topn_storage_mor", "topn_storage_mow",
                    "topn_storage_mow@incr('incrementType'='MIN_DELTA')",
                    "topn_storage_mow@incr('incrementType'='DETAIL')")) {
                for (String key : List.of("k", "abs(k)")) {
                    for (String direction : List.of("ASC", "DESC")) {
                        String sql = "SELECT k,v FROM (SELECT k,v FROM " + source
                                + " UNION ALL SELECT k,v FROM topn_storage_dup) u ORDER BY "
                                + key + " " + direction + " NULLS LAST LIMIT 1";
                        PlanChecker checker = PlanChecker.from(connectContext).parse(sql);
                        // Direct analyzer tests must supply the planner's changelog reference timestamp.
                        checker.getCascadesContext().getStatementContext().getOrRegisterChangelogReferenceTso(
                                () -> TSOTimestamp.composePhysicalTimestamp(System.currentTimeMillis()));
                        checker.analyze().rewrite().implement();
                        PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                                .process(checker.getPhysicalPlan());
                        // The unsafe aggregation happens inside the reader, not in a SQL aggregate node.
                        Assertions.assertFalse(plan.anyMatch(PhysicalHashAggregate.class::isInstance),
                                plan.treeString());
                        List<TopnFilter> filters = checker.getCascadesContext().getTopnFilterContext().getTopnFilters();
                        Assertions.assertEquals(1, filters.size(), plan.treeString());
                        TopnFilter filter = filters.get(0);
                        boolean safeSource = source.equals("topn_storage_mow");
                        Assertions.assertEquals(safeSource ? 2 : 1, filter.targets.size(), plan.treeString());
                        Assertions.assertTrue(filter.targets.keySet().stream().anyMatch(target ->
                                ((PhysicalOlapScan) target).getTable().getName().equals("topn_storage_dup")));
                        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext()))
                                .translatePlan(plan);
                        Assertions.assertEquals(safeSource ? 2 : 1,
                                filter.toThrift().getTargetNodeIdToTargetExprSize());
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().setDisableNereidsRules(previousRules);
        }
    }

    @Test
    public void testTopNFilterChecksSelectedIndexThroughLazyScan() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousRules = String.join(",", connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().setDisableNereidsRules(previousRules + ",PRUNE_EMPTY_PARTITION");
        try {
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze("SELECT k,v FROM topn_storage_dup ORDER BY k LIMIT 1").rewrite().implement();
            PhysicalPlan plan = checker.getPhysicalPlan();
            PhysicalOlapScan scan = plan.<PhysicalOlapScan>collectToList(PhysicalOlapScan.class::isInstance).get(0);
            PhysicalTopN<?> topn = plan.<PhysicalTopN<?>>collectToList(PhysicalTopN.class::isInstance).get(0);
            OlapTable table = PlanConstructor.newOlapTable(10001, "topn_index_model", 0, KeysType.DUP_KEYS);
            table.setIndexMeta(10002, "aggregate_index", table.getFullSchema(), 0, 0, (short) 0,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);
            for (long indexId : List.of(table.getBaseIndexId(), 10002L)) {
                PhysicalOlapScan selectedScan = Mockito.spy(scan);
                Mockito.doReturn(table).when(selectedScan).getTable();
                Mockito.doReturn(indexId).when(selectedScan).getSelectedIndexId();
                for (PhysicalOlapScan target : List.of(selectedScan, new PhysicalLazyMaterializeOlapScan(
                        selectedScan, Mockito.mock(SlotReference.class), List.of()))) {
                    TopnFilterContext context = new TopnFilterContext();
                    boolean accepted = target.accept(new TopnFilterPushDownVisitor(context),
                            new PushDownContext(topn, scan.getOutput().get(0), false));
                    Assertions.assertEquals(indexId == table.getBaseIndexId(), accepted);
                    Assertions.assertEquals(indexId == table.getBaseIndexId() ? 1 : 0, context.getTopnFilters().size());
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().setDisableNereidsRules(previousRules);
        }
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
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        TTopnFilterDesc desc = checker.getCascadesContext().getTopnFilterContext()
                .getTopnFilter(localTopN).toThrift();
        Assertions.assertFalse(desc.getTargetNodeIdToTargetExpr().isEmpty());
    }

    @Test
    public void testSerializedSortAlgorithmAndOrderKeys() {
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        try {
            for (String algorithm : List.of("heap", "topn", "full")) {
                connectContext.getSessionVariable().forceSortAlgorithm = algorithm;
                TSortAlgorithm expectedAlgorithm = algorithm.equals("heap") ? TSortAlgorithm.HEAP_SORT
                        : algorithm.equals("topn") ? TSortAlgorithm.TOPN_SORT : TSortAlgorithm.FULL_SORT;
                assertSerializedSortMetadata("SELECT id,k FROM topn_safety ORDER BY k LIMIT 3 OFFSET 2", expectedAlgorithm, 1);
                assertSerializedSortMetadata("SELECT id,k FROM topn_safety ORDER BY k,id LIMIT 3 OFFSET 2", expectedAlgorithm, 2);
                assertSerializedSortMetadata("SELECT id,k FROM topn_safety ORDER BY k,k LIMIT 3 OFFSET 2", expectedAlgorithm, 1);
                assertSerializedSortMetadata("SELECT id,k FROM topn_safety ORDER BY k,id,k LIMIT 3 OFFSET 2", expectedAlgorithm, 2);
            }
        } finally {
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
        }
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
    public void testTopNFilterDoesNotCrossAggregation() {
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
                    "SELECT k, sum(ts + 1) FROM topn_safety GROUP BY k ORDER BY abs(k) LIMIT 1",
                    "SELECT k, count(DISTINCT ts) FROM topn_safety GROUP BY k ORDER BY k LIMIT 1",
                    "SELECT t.k, t.total FROM (SELECT k, count(1) AS total FROM topn_safety "
                            + "GROUP BY k HAVING total > 10) t JOIN customer c ON t.k = c.c_custkey "
                            + "ORDER BY t.k LIMIT 1",
                    "SELECT k, sum(ts) FROM topn_safety GROUP BY GROUPING SETS((k)) ORDER BY k LIMIT 2")) {
                // A bound arriving after a group's first input must not truncate its remaining inputs.
                PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
                    String explain = planner.getFragments().stream()
                            .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                            .collect(Collectors.joining("\n"));
                    Assertions.assertFalse(explain.contains("TOPN OPT:"), explain);
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
    public void testTopNFilterDoesNotCrossAggregateUnionBranch() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        boolean previousPushTopn = connectContext.getSessionVariable().pushTopnToAgg;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().forceSortAlgorithm = "heap";
        connectContext.getSessionVariable().pushTopnToAgg = false;
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION");
        try {
            for (boolean grouped : List.of(false, true)) {
                for (String innerOrder : List.of("", " ORDER BY k LIMIT 2")) {
                    for (String order : List.of("ASC NULLS FIRST", "DESC NULLS FIRST",
                            "ASC NULLS LAST", "DESC NULLS LAST")) {
                        String sql = "SELECT k,s FROM (SELECT " + (grouped ? "k" : "CAST(1 AS INT)")
                                + " AS k, sum(ts + 1) AS s FROM (SELECT k,ts FROM topn_safety"
                                + innerOrder + ") a" + (grouped ? " GROUP BY k" : "")
                                + " UNION ALL SELECT k,ts AS s FROM topn_safety) u ORDER BY abs(k) "
                                + order + " LIMIT 1";
                        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                        PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                                .process(checker.getPhysicalPlan());
                        List<PhysicalHashAggregate<?>> aggregates = plan.collectToList(
                                PhysicalHashAggregate.class::isInstance);
                        Assertions.assertFalse(aggregates.isEmpty(), plan.treeString());
                        List<PhysicalRelation> aggregateScans = aggregates.stream()
                                .flatMap(aggregate -> aggregate.<PhysicalRelation>collectToList(
                                        PhysicalRelation.class::isInstance).stream())
                                .distinct().collect(Collectors.toList());
                        Assertions.assertEquals(1, aggregateScans.size(), plan.treeString());
                        List<TopnFilter> filters = checker.getCascadesContext().getTopnFilterContext().getTopnFilters();
                        List<TopnFilter> outerFilters = filters.stream()
                                .filter(filter -> aggregates.stream().noneMatch(aggregate ->
                                        aggregate.anyMatch(node -> node == filter.topn)))
                                .collect(Collectors.toList());
                        Assertions.assertEquals(1, outerFilters.size(), plan.treeString());
                        // The sibling can publish an early bound, but cannot change the aggregate's inputs.
                        Assertions.assertEquals(1, outerFilters.get(0).targets.size(), plan.treeString());
                        Assertions.assertTrue(outerFilters.get(0).targets.keySet().stream()
                                .noneMatch(aggregateScans::contains), plan.treeString());
                        // The independent TopN inside the aggregate can still filter its own scan.
                        long innerFilters = filters.stream()
                                .filter(filter -> aggregates.stream().anyMatch(aggregate ->
                                        aggregate.anyMatch(node -> node == filter.topn)))
                                .filter(filter -> filter.targets.keySet().stream().anyMatch(aggregateScans::contains))
                                .count();
                        Assertions.assertEquals(innerOrder.isEmpty() ? 0 : 1, innerFilters, plan.treeString());
                        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext()))
                                .translatePlan(plan);
                        Assertions.assertEquals(1, outerFilters.get(0).legacyTargets.size());
                        Assertions.assertFalse(outerFilters.get(0).toThrift().getTargetNodeIdToTargetExpr().isEmpty());
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().pushTopnToAgg = previousPushTopn;
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

    private void assertSerializedSortMetadata(String sql, TSortAlgorithm expectedAlgorithm, int expectedKeyCount) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
        PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                .process(checker.getPhysicalPlan());
        TopnFilterContext context = checker.getCascadesContext().getTopnFilterContext();
        Assertions.assertFalse(context.getTopnFilters().isEmpty(), plan.treeString());
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        for (TopnFilter filter : context.getTopnFilters()) {
            TSortNode sort = filter.legacySortNode.treeToThrift().getNodes().get(0).getSortNode();
            Assertions.assertTrue(sort.isSetAlgorithm());
            Assertions.assertEquals(expectedAlgorithm, sort.getAlgorithm(), plan.treeString());
            Assertions.assertEquals(expectedKeyCount, sort.getSortInfo().getOrderingExprsSize());
        }
    }

    @Test
    public void testTopNFilterPreservesRightJoinRows() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        boolean previousJoinReorder = connectContext.getSessionVariable().isDisableJoinReorder();
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().forceSortAlgorithm = "heap";
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION,PUSH_DOWN_TOP_N_THROUGH_JOIN");
        try {
            for (JoinType joinType : List.of(JoinType.RIGHT_OUTER_JOIN, JoinType.FULL_OUTER_JOIN,
                    JoinType.RIGHT_ANTI_JOIN, JoinType.RIGHT_SEMI_JOIN, JoinType.INNER_JOIN,
                    JoinType.LEFT_OUTER_JOIN)) {
                String joinSql = joinType.name().replace('_', ' ');
                String payload = joinType.isRightAntiJoin() || joinType.isRightSemiJoin() ? "b.ts" : "a.ts";
                for (String condition : List.of("=", "<")) {
                    for (String rank : List.of("b.k", "CAST(1 AS INT)", "CAST(sleep(0) AS INT)")) {
                        for (String order : List.of("ASC NULLS FIRST", "DESC NULLS FIRST",
                                "ASC NULLS LAST", "DESC NULLS LAST")) {
                            String sql = "SELECT k,payload FROM (SELECT /*+ leading(a b) */ " + rank
                                    + " AS k," + payload + " AS payload FROM topn_safety a " + joinSql
                                    + " topn_safety b ON a.k " + condition + " b.k UNION ALL "
                                    + "SELECT k,ts AS payload FROM topn_safety) u ORDER BY k " + order + " LIMIT 1";
                            PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                            PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                                    .process(checker.getPhysicalPlan());
                            AbstractPhysicalJoin<?, ?> join = getOnlyJoin(plan);
                            Assertions.assertEquals(joinType, join.getJoinType(), plan.treeString());
                            Assertions.assertEquals(condition.equals("="), join instanceof PhysicalHashJoin,
                                    plan.treeString());
                            boolean safeJoin = joinType.isInnerJoin() || joinType.isLeftOuterJoin()
                                    || joinType.isRightSemiJoin();
                            boolean safeNullOrder = !joinType.isLeftOuterJoin() || order.contains("LAST");
                            boolean canResolveProbe = condition.equals("=") || rank.startsWith("CAST");
                            assertSubtreeTopNTargets(checker, plan, join, safeJoin && safeNullOrder && canResolveProbe);
                        }
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().setDisableJoinReorder(previousJoinReorder);
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testTopNFilterPreservesRightSemiMark() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().forceSortAlgorithm = "heap";
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION,PUSH_DOWN_TOP_N_THROUGH_JOIN");
        try {
            for (String order : List.of("ASC NULLS FIRST", "DESC NULLS FIRST",
                    "ASC NULLS LAST", "DESC NULLS LAST")) {
                String sql = "SELECT k,m FROM (SELECT CAST(1 AS INT) AS k, "
                        + "EXISTS(SELECT 1 FROM topn_safety a WHERE a.k = b.k) AS m FROM topn_safety b "
                        + "UNION ALL SELECT k,k IS NOT NULL AS m FROM topn_safety) u ORDER BY k " + order + " LIMIT 1";
                PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                // JoinCommute permits this hash mark alternative. Exercise it independently of costing.
                PhysicalPlan plan = (PhysicalPlan) checker.getPhysicalPlan().rewriteUp(node -> {
                    if (node instanceof PhysicalHashJoin && ((PhysicalHashJoin<?, ?>) node).isMarkJoin()) {
                        PhysicalHashJoin<?, ?> join = (PhysicalHashJoin<?, ?>) node;
                        Assertions.assertFalse(join.getHashJoinConjuncts().isEmpty());
                        Assertions.assertTrue(join.getMarkJoinConjuncts().isEmpty());
                        if (join.getJoinType().isLeftSemiJoin()) {
                            PhysicalHashJoin<Plan, Plan> swapped = new PhysicalHashJoin<>(JoinType.RIGHT_SEMI_JOIN,
                                    join.getHashJoinConjuncts(), join.getOtherJoinConjuncts(),
                                    join.getMarkJoinConjuncts(), join.getDistributeHint(),
                                    join.getMarkJoinSlotReference(), join.getLogicalProperties(),
                                    join.right(), join.left());
                            swapped = swapped.withPhysicalPropertiesAndStats(join.getPhysicalProperties(), join.getStats());
                            swapped.addFilterConjuncts(join.getFilterConjuncts());
                            swapped.setShouldTranslateOutput(join.isShouldTranslateOutput());
                            return swapped;
                        }
                    }
                    return node;
                });
                plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
                AbstractPhysicalJoin<?, ?> join = getOnlyJoin(plan);
                Assertions.assertEquals(JoinType.RIGHT_SEMI_JOIN, join.getJoinType(), plan.treeString());
                Assertions.assertTrue(join.isMarkJoin(), plan.treeString());
                assertSubtreeTopNTargets(checker, plan, join, false);
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    @Test
    public void testTopNFilterDoesNotReevaluateVolatileProbe() {
        double previousRatio = connectContext.getSessionVariable().topnFilterRatio;
        String previousAlgorithm = connectContext.getSessionVariable().forceSortAlgorithm;
        String previousDisabledRules = String.join(",",
                connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().topnFilterRatio = 1000000;
        connectContext.getSessionVariable().forceSortAlgorithm = "heap";
        connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules
                + ",PRUNE_EMPTY_PARTITION,PUSH_DOWN_TOP_N_THROUGH_UNION");
        try {
            Map<String, Boolean> ranks = Map.of(
                    "if(rand(3) < 0.5, 0, 1)", false,
                    "if(rand(3) < 0.5, 0, 1) + k", false,
                    "cast(if(rand(3) < 0.5, 0, 1) + k AS BIGINT)", false,
                    "length(uuid())", false,
                    "abs(k)", true,
                    "CAST(1 AS INT)", true);
            for (Map.Entry<String, Boolean> rank : ranks.entrySet()) {
                for (String order : List.of("ASC NULLS FIRST", "DESC NULLS FIRST",
                        "ASC NULLS LAST", "DESC NULLS LAST")) {
                    String sql = "SELECT k,payload FROM (SELECT " + rank.getKey()
                            + " AS k,ts AS payload FROM topn_safety UNION ALL "
                            + "SELECT k,ts AS payload FROM topn_safety) u ORDER BY k " + order + " LIMIT 1";
                    PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
                    PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                            .process(checker.getPhysicalPlan());
                    List<PhysicalUnion> unions = plan.collectToList(PhysicalUnion.class::isInstance);
                    Assertions.assertEquals(1, unions.size(), plan.treeString());
                    Plan branch = unions.get(0).child(0);
                    Assertions.assertEquals(1, branch.collectToList(PhysicalRelation.class::isInstance).size());
                    Assertions.assertEquals(!rank.getValue(), branch.anyMatch(node -> ((Plan) node).getExpressions()
                            .stream().anyMatch(Expression::containsVolatileExpression)), plan.treeString());
                    // Alias expansion must reject the volatile branch while retaining the deterministic sibling.
                    assertSubtreeTopNTargets(checker, plan, branch, rank.getValue());
                    if (!rank.getValue()) {
                        // Without a sibling, the source must not register any scan filter at all.
                        assertTopNFilterPresence("SELECT " + rank.getKey() + " AS rk,ts FROM topn_safety "
                                + "ORDER BY rk " + order + " LIMIT 1", false);
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().topnFilterRatio = previousRatio;
            connectContext.getSessionVariable().forceSortAlgorithm = previousAlgorithm;
            connectContext.getSessionVariable().setDisableNereidsRules(previousDisabledRules);
        }
    }

    private AbstractPhysicalJoin<?, ?> getOnlyJoin(PhysicalPlan plan) {
        List<AbstractPhysicalJoin<?, ?>> joins = plan.collectToList(AbstractPhysicalJoin.class::isInstance);
        Assertions.assertEquals(1, joins.size(), plan.treeString());
        return joins.get(0);
    }

    private void assertSubtreeTopNTargets(PlanChecker checker, PhysicalPlan plan, Plan subtree,
            boolean allowSubtreeTarget) {
        List<TopnFilter> filters = checker.getCascadesContext().getTopnFilterContext().getTopnFilters();
        List<TopnFilter> outerFilters = filters.stream()
                .filter(filter -> !subtree.anyMatch(node -> node == filter.topn)).collect(Collectors.toList());
        Assertions.assertEquals(1, outerFilters.size(), plan.treeString());
        TopnFilter filter = outerFilters.get(0);
        List<PhysicalRelation> subtreeScans = subtree.collectToList(PhysicalRelation.class::isInstance);
        // The safe UNION sibling still supplies a bound; check the other branch independently.
        Assertions.assertEquals(allowSubtreeTarget ? 2 : 1, filter.targets.size(), plan.treeString());
        Assertions.assertEquals(allowSubtreeTarget,
                filter.targets.keySet().stream().anyMatch(subtreeScans::contains), plan.treeString());
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        Assertions.assertEquals(allowSubtreeTarget ? 2 : 1, filter.legacyTargets.size());
        Assertions.assertEquals(allowSubtreeTarget ? 2 : 1,
                filter.toThrift().getTargetNodeIdToTargetExprSize());
    }

    private void assertTopNFilterPresence(String sql, boolean expected) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().implement();
        PhysicalPlan plan = new PlanPostProcessors(checker.getCascadesContext())
                .process(checker.getPhysicalPlan());
        Assertions.assertEquals(expected,
                !checker.getCascadesContext().getTopnFilterContext().getTopnFilters().isEmpty(),
                plan.treeString());
        if (expected) {
            new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
            for (TopnFilter filter : checker.getCascadesContext().getTopnFilterContext().getTopnFilters()) {
                TTopnFilterDesc desc = filter.toThrift();
                Assertions.assertEquals(filter.legacyTargets.size(), desc.getTargetNodeIdToTargetExprSize());
            }
        }
    }
}
