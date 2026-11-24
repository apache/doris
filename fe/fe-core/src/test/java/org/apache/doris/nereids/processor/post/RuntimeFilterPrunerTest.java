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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWorkTableReference;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

class RuntimeFilterPrunerTest extends TestWithFeService {
    private boolean originalEnableFeatureChangelog;

    @Override
    protected void runBeforeAll() throws Exception {
        originalEnableFeatureChangelog = Config.enable_feature_changelog;
        Config.enable_feature_changelog = true;
        createDatabase("rf_pruner_test");
        useDatabase("rf_pruner_test");
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(true);
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.BLOOM.getValue());

        createTable("CREATE TABLE rf_prune_fact (id INT, agent_code INT, username VARCHAR(32), k BIGINT) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        // Merge-on-read keeps a hidden delete-sign Filter above the agent scan.
        createTable("CREATE TABLE rf_prune_agent (agent_code INT, bloc_id INT) "
                + "UNIQUE KEY(agent_code) DISTRIBUTED BY HASH(agent_code) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='false')");
        createTable("CREATE TABLE rf_prune_bloc (id INT, bloc_code VARCHAR(32)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE rf_prune_stream_base (id INT, agent_code INT) "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true', "
                + "'changelog.enable'='true', 'changelog.need_historical_value'='true')");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_feature_changelog = originalEnableFeatureChangelog;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPreserveRuntimeFilterThroughHiddenFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code "
                        + "JOIN [broadcast] rf_prune_bloc b ON e.bloc_id = b.id "
                        + "WHERE b.bloc_code = 'keep' AND f.username LIKE '%user%'", knownStats);

        // b.id -> e.bloc_id must keep e.agent_code -> f.agent_code effective, even though
        // e has no visible predicate and its static join-column statistics are non-selective.
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertEquals(2, filters.size());
        Assertions.assertTrue(filters.stream().anyMatch(filter ->
                filter.getTargetSlot().getName().equals("agent_code")));
        Assertions.assertTrue(filters.stream().anyMatch(filter ->
                filter.getTargetSlot().getName().equals("bloc_id")));
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHiddenFilterAloneDoesNotKeepIneffectiveRuntimeFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code", knownStats);

        Assertions.assertTrue(context.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertEquals(1, context.prunedRF.size());
    }

    @Test
    void testVisibleFilterKeepsRuntimeFilter() {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code "
                        + "WHERE e.bloc_id = 1", true);

        Assertions.assertEquals(1, context.getNereidsRuntimeFilter().size());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"true, MIN_DELTA", "false, MIN_DELTA", "true, APPEND_ONLY", "false, APPEND_ONLY"})
    void testStreamSourceKeepsRuntimeFilter(boolean knownStats, String streamType) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_stream_base@incr('incrementType'='" + streamType + "') s "
                        + "ON f.agent_code = s.agent_code", knownStats);

        Assertions.assertEquals(1, context.getNereidsRuntimeFilter().size());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStreamSourceKeepsRuntimeFilterThroughAggregate(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] "
                        + "(SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') "
                        + "GROUP BY agent_code) s ON f.agent_code = s.agent_code", knownStats);

        Assertions.assertEquals(1, context.getNereidsRuntimeFilter().size());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @Test
    void testStorageAggregateKeepsStreamRuntimeFilter() {
        assertBuildKeepsRuntimeFilter("SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                + "SELECT COUNT(*) AS n FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA')"
                + ") b ON f.k = b.n", false, PhysicalStorageLayerAggregate.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMixedUnionKeepsRuntimeFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                        + "SELECT agent_code FROM rf_prune_agent "
                        + "UNION ALL SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') "
                        + "WHERE id > 0) s ON f.agent_code = s.agent_code", knownStats);

        Assertions.assertEquals(1, context.getNereidsRuntimeFilter().size());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMaterializedCteKeepsStreamRuntimeFilter(boolean knownStats) {
        assertBuildKeepsRuntimeFilter("WITH s AS ("
                + "SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA')) "
                + "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] s ON f.agent_code = s.agent_code "
                + "UNION ALL SELECT agent_code FROM s", knownStats, PhysicalCTEConsumer.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptKeepsStreamRuntimeFilter(boolean knownStats) {
        assertBuildKeepsRuntimeFilter("SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                + "SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') "
                + "EXCEPT SELECT agent_code FROM rf_prune_agent) s ON f.agent_code = s.agent_code",
                knownStats, PhysicalExcept.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNestedLoopBuildKeepsStreamRuntimeFilter(boolean knownStats) {
        assertBuildKeepsRuntimeFilter("SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                + "SELECT s.agent_code FROM rf_prune_agent a "
                + "JOIN rf_prune_stream_base@incr('incrementType'='MIN_DELTA') s "
                + "ON COALESCE(a.agent_code, 0) < s.agent_code"
                + ") b ON f.agent_code = b.agent_code", knownStats, PhysicalNestedLoopJoin.class);
    }

    @Test
    void testNestedLoopLeftSemiBuildDoesNotPropagateRightChangelog() {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                        + "SELECT a.agent_code FROM rf_prune_agent a LEFT SEMI JOIN "
                        + "rf_prune_stream_base@incr('incrementType'='MIN_DELTA') s ON a.agent_code < s.agent_code"
                        + ") b ON f.agent_code = b.agent_code", true);

        Assertions.assertTrue(context.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertEquals(1, context.prunedRF.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRecursiveUnionKeepsStreamRuntimeFilter(boolean knownStats) {
        assertBuildKeepsRuntimeFilter("WITH RECURSIVE s(agent_code) AS ("
                + "SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') "
                + "UNION DISTINCT SELECT CAST(0 AS INT) FROM s) "
                + "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] s ON f.agent_code = s.agent_code",
                knownStats, PhysicalRecursiveUnion.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRecursiveTermKeepsWorkTableRuntimeFilter(boolean knownStats) {
        assertBuildKeepsRuntimeFilter("WITH RECURSIVE walk(agent_code) AS ("
                + "SELECT agent_code FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') "
                + "UNION ALL SELECT e.agent_code FROM rf_prune_fact e "
                + "JOIN [broadcast] walk w ON e.agent_code = w.agent_code) "
                + "SELECT agent_code FROM walk", knownStats, PhysicalWorkTableReference.class);
    }

    @Test
    void testOrdinaryUnionDoesNotKeepIneffectiveRuntimeFilter() {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f JOIN [broadcast] ("
                        + "SELECT agent_code FROM rf_prune_agent WHERE bloc_id = 1 "
                        + "UNION ALL SELECT agent_code FROM rf_prune_fact) s ON f.agent_code = s.agent_code", true);

        Assertions.assertTrue(context.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertEquals(1, context.prunedRF.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStreamProbeDoesNotKeepIneffectiveRuntimeFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT s.id FROM rf_prune_stream_base@incr('incrementType'='MIN_DELTA') s "
                        + "JOIN [broadcast] rf_prune_fact f ON s.agent_code = f.agent_code", knownStats);

        Assertions.assertTrue(context.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertEquals(1, context.prunedRF.size());
    }

    private void assertBuildKeepsRuntimeFilter(String sql, boolean knownStats, Class<? extends Plan> buildType) {
        RuntimeFilterContext context = getRuntimeFilterContext(sql, knownStats);
        List<RuntimeFilter> hashJoinFilters = context.getNereidsRuntimeFilter().stream()
                .filter(filter -> filter.getBuilderNode() instanceof PhysicalHashJoin)
                .toList();
        Assertions.assertEquals(1, hashJoinFilters.size());
        PhysicalHashJoin<?, ?> join = (PhysicalHashJoin<?, ?>) hashJoinFilters.get(0).getBuilderNode();
        Assertions.assertTrue(join.right().anyMatch(buildType::isInstance), () -> join.right().treeString());
        Assertions.assertEquals(RuntimeFilterContext.EffectiveSrcType.CHANGELOG,
                context.getEffectiveSrcType(join.right()), () -> join.right().treeString());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql, boolean knownStats) {
        PlanChecker checker = PlanChecker.from(connectContext).parse(sql);
        CascadesContext cascadesContext = checker.getCascadesContext();
        cascadesContext.newTableCollector(true).collect();
        cascadesContext.getStatementContext().getOrRegisterChangelogReferenceTso(() -> 2000L);
        // Match the planner's reset before analysis, so recursive references bind to the work table.
        cascadesContext.setCteContext(new CTEContext());
        cascadesContext.newAnalyzer().analyze();
        MemoTestUtils.initMemoAndValidState(cascadesContext);
        checker.rewrite().optimize();
        PhysicalPlan plan = checker.getBestPlanTree();
        ColumnStatistic nonSelectiveStats = new ColumnStatisticBuilder(100)
                .setNdv(100)
                .setNumNulls(0)
                .setAvgSizeByte(4)
                .setMinValue(1)
                .setMaxValue(100)
                .build();
        if (knownStats) {
            Set<PhysicalHashJoin<? extends Plan, ? extends Plan>> joins =
                    plan.collect(PhysicalHashJoin.class::isInstance);
            for (PhysicalHashJoin<? extends Plan, ? extends Plan> join : joins) {
                EqualTo equalTo = (EqualTo) join.getEqualToConjuncts().get(0);
                Slot probeSlot = equalTo.child(0).getInputSlots().iterator().next();
                Slot buildSlot = equalTo.child(1).getInputSlots().iterator().next();
                join.left().getStats().addColumnStats(probeSlot, nonSelectiveStats);
                join.right().getStats().addColumnStats(buildSlot, nonSelectiveStats);
            }
        }
        plan = new PlanPostProcessors(cascadesContext).process(plan);
        new PhysicalPlanTranslator(new PlanTranslatorContext(cascadesContext)).translatePlan(plan);
        return cascadesContext.getRuntimeFilterContext();
    }
}
