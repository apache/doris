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

package org.apache.doris.nereids.mv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewPredicateCollector;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter.PreRewriteStrategy;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests derivation and use of predicates guaranteed by materialized view outputs.
 */
public class RelationImpliedPredicateTest extends SqlTestBase {

    @Test
    void testOutputPredicatesBindToGeneratedDirectAndCopiedScans() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        try {
            createMvByNereids("create materialized view mv_relation_implied_fact "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select T1.id, T1.score, T1.score as score_copy from T1 where T1.score = 1;");
            mockRefreshedMtmv("mv_relation_implied_fact");

            CascadesContext context = createCascadesContext(
                    "select T1.id, T1.score, T1.score as score_copy from T1 where T1.score = 1",
                    connectContext);
            PlanChecker.from(context)
                    .setIsQuery()
                    .analyze()
                    .rewrite();

            Assertions.assertEquals(1, context.getMaterializationContexts().size());
            MaterializationContext materializationContext = context.getMaterializationContexts().get(0);
            Plan scanPlan = materializationContext.getScanPlan(null, context);
            Assertions.assertNotNull(scanPlan);
            LogicalOlapScan generatedScan = scanPlan.<LogicalOlapScan>collectFirst(LogicalOlapScan.class::isInstance)
                    .orElseThrow(() -> new AssertionError(scanPlan.treeString()));

            CascadesContext directContext = createCascadesContext("select * from mv_relation_implied_fact");
            PlanChecker.from(directContext).setIsQuery().analyze();
            LogicalOlapScan directScan = directContext.getRewritePlan()
                    .<LogicalOlapScan>collectFirst(LogicalOlapScan.class::isInstance).orElseThrow(AssertionError::new);
            LogicalOlapScan copiedScan = (LogicalOlapScan) directScan.accept(
                    LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
            Assertions.assertTrue(directScan.getOutputSet().stream().noneMatch(copiedScan.getOutputSet()::contains));

            // Duplicate definition expressions need one output representative, not a product of slot mappings.
            for (LogicalOlapScan scan : ImmutableList.of(generatedScan, directScan, copiedScan)) {
                Set<Expression> predicates = new MaterializedViewPredicateCollector(directContext).collect(scan);
                Assertions.assertTrue(predicates.stream()
                        .allMatch(predicate -> scan.getOutputSet().containsAll(predicate.getInputSlots())),
                        predicates::toString);
                Slot score = scan.getOutput().stream().filter(slot -> slot.getName().equals("score"))
                        .findFirst().orElseThrow(AssertionError::new);
                Assertions.assertTrue(predicates.stream().anyMatch(predicate -> predicate instanceof EqualTo
                                && predicate.child(0).equals(score) && predicate.child(1) instanceof Literal
                                && ((Literal) predicate.child(1)).getStringValue().equals("1")),
                        predicates::toString);
            }
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        }
    }

    @Test
    void testNestedMvRewriteByOutputPredicate() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_explicit_join");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_fact_with_score");
        try {
            createMvByNereids("create materialized view mv_relation_implied_fact_with_score "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select T1.id, T1.score from T1 where T1.score = 1;");
            mockRefreshedMtmv("mv_relation_implied_fact_with_score");

            createMvByNereids("create materialized view mv_relation_implied_explicit_join "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select f.id, f.score as fact_score, T2.score as dim_score "
                    + "from mv_relation_implied_fact_with_score f "
                    + "inner join T2 on f.id = T2.id "
                    + "where f.score = 1;");
            mockRefreshedMtmv("mv_relation_implied_explicit_join");

            CascadesContext context = createCascadesContext(
                    "select T1.id, T1.score as fact_score, T2.score as dim_score "
                            + "from T1 inner join T2 on T1.id = T2.id "
                            + "where T1.score = 1",
                    connectContext);
            PhysicalPlan bestPlan = PlanChecker.from(context)
                    .setIsQuery()
                    .analyze()
                    .rewrite()
                    .preMvRewrite()
                    .optimize()
                    .getBestPlanTree();
            String plan = bestPlan.treeString();
            Assertions.assertTrue(plan.contains("mv_relation_implied_explicit_join"),
                    () -> plan + MaterializationContext.toSummaryString(context, bestPlan));
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_explicit_join");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_fact_with_score");
        }
    }

    @Test
    void testNestedMvRewriteAfterPredicateColumnIsProjectedOut() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_projected_upper");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_projected_lower");
        try {
            createMvByNereids("create materialized view mv_relation_implied_projected_lower "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id, score from T1 where score = 1;");
            mockRefreshedMtmv("mv_relation_implied_projected_lower");
            createMvByNereids("create materialized view mv_relation_implied_projected_upper "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select f.id, T2.score as dim_score "
                    + "from mv_relation_implied_projected_lower f inner join T2 on f.id = T2.id "
                    + "where f.score > 0;");
            MTMV upper = mockRefreshedMtmv("mv_relation_implied_projected_upper");
            StructInfo cachedDefinition = upper.getOrGenerateCache(connectContext)
                    .getAllRulesRewrittenPlanAndStructInfo().value();
            Assertions.assertTrue(cachedDefinition.getTopPlan().anyMatch(LogicalFilter.class::isInstance));

            CascadesContext context = createCascadesContext(
                    "select T1.id, T2.score as dim_score from T1 inner join T2 on T1.id = T2.id "
                            + "where T1.score = 1");
            PhysicalPlan bestPlan = PlanChecker.from(context).setIsQuery().analyze().rewrite()
                    .preMvRewrite().optimize().getBestPlanTree();
            Assertions.assertTrue(bestPlan.anyMatch(plan -> plan instanceof PhysicalOlapScan
                            && ((PhysicalOlapScan) plan).getTable().getName()
                                    .equals("mv_relation_implied_projected_upper")),
                    () -> bestPlan.treeString() + MaterializationContext.toSummaryString(context, bestPlan));
            // The successful candidate uses a query-local proof; the stored definition keeps its filter.
            Assertions.assertTrue(cachedDefinition.getTopPlan().anyMatch(LogicalFilter.class::isInstance));
            List<PhysicalFilter<? extends Plan>> filters = bestPlan.collectToList(PhysicalFilter.class::isInstance);
            Assertions.assertTrue(filters.stream().allMatch(filter -> filter.getConjuncts().stream()
                            .allMatch(predicate -> filter.child().getOutputSet()
                                    .containsAll(predicate.getInputSlots()))),
                    bestPlan::treeString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_projected_upper");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_projected_lower");
        }
    }

    @Test
    void testStructInfoRetainsFilterAboveNullExtendedMvScan() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_nullable_input");
        try {
            createMvByNereids("create materialized view mv_relation_implied_nullable_input "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id, score from T1 where score = 1;");
            mockRefreshedMtmv("mv_relation_implied_nullable_input");
            CascadesContext context = createCascadesContext(
                    "select f.id from T2 left join mv_relation_implied_nullable_input f on T2.id = f.id "
                            + "where f.score = 1");
            // Analyze only: rewriting would turn this null-rejecting LEFT JOIN into an INNER JOIN first.
            PlanChecker.from(context).setIsQuery().analyze();
            Plan analyzedPlan = context.getRewritePlan();
            LogicalFilter<? extends Plan> filter = analyzedPlan
                    .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                    .orElseThrow(AssertionError::new);
            StructInfo structInfo = StructInfo.of(analyzedPlan, context).withoutRedundantMvFilters(context);
            Assertions.assertTrue(structInfo.getPredicates().getPulledUpPredicates()
                    .containsAll(filter.getConjuncts()), structInfo.getTopPlan()::treeString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_nullable_input");
        }
    }

    @Test
    void testNestedMvRewriteWithNonOutputDefinitionPredicate() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_join");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        try {
            createMvByNereids("create materialized view mv_relation_implied_fact "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select T1.id from T1 where T1.score = 1;");
            mockRefreshedMtmv("mv_relation_implied_fact");

            createMvByNereids("create materialized view mv_relation_implied_join "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select f.id, T2.score as dim_score "
                    + "from mv_relation_implied_fact f "
                    + "inner join T2 on f.id = T2.id;");
            mockRefreshedMtmv("mv_relation_implied_join");

            // Both sides scan the same lower MV. Its identity guarantees score = 1 even though score is not output.
            CascadesContext context = createCascadesContext(
                    "select T1.id, T2.score as dim_score "
                            + "from T1 inner join T2 on T1.id = T2.id "
                            + "where T1.score = 1",
                    connectContext);
            PhysicalPlan bestPlan = PlanChecker.from(context)
                    .setIsQuery()
                    .analyze()
                    .rewrite()
                    .preMvRewrite()
                    .optimize()
                    .getBestPlanTree();
            String plan = bestPlan.treeString();
            Assertions.assertTrue(plan.contains("mv_relation_implied_join"),
                    () -> plan + MaterializationContext.toSummaryString(context, bestPlan));
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_join");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        }
    }

    @Test
    void testNestedInnerJoinRewriteRetainsNullRejectFilter() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_left_join");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_positive_id");
        try {
            createMvByNereids("create materialized view mv_relation_implied_positive_id "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from T1 where id > 0;");
            mockRefreshedMtmv("mv_relation_implied_positive_id");
            createMvByNereids("create materialized view mv_relation_implied_left_join "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select T2.id as dim_id, f.id as fact_id "
                    + "from T2 left join mv_relation_implied_positive_id f on T2.id = f.id;");
            mockRefreshedMtmv("mv_relation_implied_left_join");

            CascadesContext context = createCascadesContext(
                    "select T2.id as dim_id, T1.id as fact_id "
                            + "from T2 inner join T1 on T2.id = T1.id where T1.id > 0");
            PhysicalPlan bestPlan = PlanChecker.from(context).setIsQuery().analyze().rewrite()
                    .preMvRewrite().optimize().getBestPlanTree();
            Optional<PhysicalOlapScan> mvScan = bestPlan.collectFirst(plan -> plan instanceof PhysicalOlapScan
                    && ((PhysicalOlapScan) plan).getTable().getName().equals("mv_relation_implied_left_join"));
            Assertions.assertTrue(mvScan.isPresent(),
                    () -> bestPlan.treeString() + MaterializationContext.toSummaryString(context, bestPlan));
            Slot factId = mvScan.get().getOutput().stream().filter(slot -> slot.getName().equals("fact_id"))
                    .findFirst().orElseThrow(AssertionError::new);
            List<PhysicalFilter<? extends Plan>> filters = bestPlan.collectToList(PhysicalFilter.class::isInstance);
            Set<Expression> compensation = filters.stream().flatMap(filter -> filter.getConjuncts().stream())
                    .collect(Collectors.toSet());
            // A LEFT JOIN MV can contain (dim_id = 2, fact_id = NULL); its scan must not be used unfiltered.
            Assertions.assertTrue(ExpressionUtils.inferNotNullSlots(compensation, context).contains(factId),
                    bestPlan::treeString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_left_join");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_positive_id");
        }
    }

    @Test
    void testNestedInnerJoinRewriteDoesNotRequireUnprojectedJoinKey() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_unprojected_join_key");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_join_key");
        try {
            createMvByNereids("create materialized view mv_relation_implied_join_key "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from T1 where id > 0;");
            mockRefreshedMtmv("mv_relation_implied_join_key");
            createMvByNereids("create materialized view mv_relation_implied_unprojected_join_key "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select f.id as fact_id "
                    + "from T2 left join mv_relation_implied_join_key f on T2.id = f.id;");
            mockRefreshedMtmv("mv_relation_implied_unprojected_join_key");

            CascadesContext context = createCascadesContext(
                    "select T1.id as fact_id from T2 inner join T1 on T2.id = T1.id where T1.id > 0");
            List<Plan> alternatives = PlanChecker.from(context).setIsQuery().analyze().rewrite()
                    .preMvRewrite().optimize().getAllPlan();
            List<Plan> rewrittenPlans = alternatives.stream().filter(plan -> plan.anyMatch(node ->
                            node instanceof LogicalOlapScan && ((LogicalOlapScan) node).getTable().getName()
                                    .equals("mv_relation_implied_unprojected_join_key")))
                    .collect(Collectors.toList());
            Assertions.assertFalse(rewrittenPlans.isEmpty(),
                    () -> alternatives.stream().map(Plan::treeString).collect(Collectors.joining("\n")));
            // HyperGraph already matched T2.id = f.id. Requiring it as a filter would need the absent T2.id output.
            for (Plan rewrittenPlan : rewrittenPlans) {
                LogicalOlapScan scan = rewrittenPlan.<LogicalOlapScan>collectFirst(node ->
                        node instanceof LogicalOlapScan && ((LogicalOlapScan) node).getTable().getName()
                                .equals("mv_relation_implied_unprojected_join_key")).orElseThrow(AssertionError::new);
                Slot factId = scan.getOutput().stream().filter(slot -> slot.getName().equals("fact_id"))
                        .findFirst().orElseThrow(AssertionError::new);
                List<LogicalFilter<? extends Plan>> filters =
                        rewrittenPlan.collectToList(LogicalFilter.class::isInstance);
                Set<Expression> compensation = filters.stream().flatMap(filter -> filter.getConjuncts().stream())
                        .collect(Collectors.toSet());
                Assertions.assertTrue(ExpressionUtils.inferNotNullSlots(compensation, context).contains(factId),
                        rewrittenPlan::treeString);
            }
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_unprojected_join_key");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_join_key");
        }
    }

    @Test
    void testDirectLowerMvQueryUsesUpperMv() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_direct_upper");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_direct_lower");
        try {
            createMvByNereids("create materialized view mv_relation_implied_direct_lower "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from T1 where score = 1;");
            mockRefreshedMtmv("mv_relation_implied_direct_lower");
            createMvByNereids("create materialized view mv_relation_implied_direct_upper "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from mv_relation_implied_direct_lower;");
            mockRefreshedMtmv("mv_relation_implied_direct_upper");

            CascadesContext context = createCascadesContext("select id from mv_relation_implied_direct_lower");
            List<Plan> alternatives = PlanChecker.from(context).setIsQuery().analyze().rewrite()
                    .preMvRewrite().optimize().getAllPlan();
            // Both one-table plans can have the same estimated cost; assert rewrite availability.
            Assertions.assertTrue(alternatives.stream().anyMatch(plan -> plan.anyMatch(node ->
                            node instanceof LogicalOlapScan && ((LogicalOlapScan) node).getTable().getName()
                                    .equals("mv_relation_implied_direct_upper"))),
                    () -> alternatives.stream().map(Plan::treeString).collect(Collectors.joining("\n")));
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_direct_upper");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_direct_lower");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIvmOutputPredicatesSkipHiddenColumns(boolean showHiddenColumns) throws Exception {
        initMvRewriteSession();
        boolean originalEnableTableStream = Config.enable_table_stream;
        boolean originalShowHiddenColumns = connectContext.getSessionVariable().showHiddenColumns();
        Config.enable_table_stream = true;
        try {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_ivm");
            dropTableWithSql("drop table if exists relation_implied_ivm_base");
            createTable("create table relation_implied_ivm_base (id bigint, score bigint) "
                    + "duplicate key(id) distributed by hash(id) buckets 1 "
                    + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
            createMvByNereids("create materialized view mv_relation_implied_ivm "
                    + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id, score from relation_implied_ivm_base where score = 1;");
            mockRefreshedMtmv("mv_relation_implied_ivm");
            CascadesContext context = createCascadesContext("select id, score from mv_relation_implied_ivm");
            PlanChecker.from(context).setIsQuery().analyze();
            LogicalOlapScan scan = context.getRewritePlan()
                    .<LogicalOlapScan>collectFirst(LogicalOlapScan.class::isInstance).orElseThrow(AssertionError::new);
            Assertions.assertTrue(scan.getOutput().stream().anyMatch(slot -> slot instanceof SlotReference
                    && slot.getName().equals(Column.IVM_ROW_ID_COL) && !((SlotReference) slot).isVisible()));
            Set<Slot> visibleOutput = scan.getOutput().stream()
                    .filter(slot -> ((SlotReference) slot).isVisible()).collect(Collectors.toSet());
            MTMVCache cache = ((MTMV) scan.getTable()).getOrGenerateCache(connectContext);
            Assertions.assertFalse(cache.getOutputPredicates().isEmpty(),
                    () -> cache.getOriginalFinalPlan().treeString() + "\n"
                            + cache.getAllRulesRewrittenPlanAndStructInfo().key().treeString());
            // Definition output binding always uses visible physical columns, regardless of
            // whether this query's session asks to display hidden columns.
            connectContext.getSessionVariable().setShowHiddenColumns(showHiddenColumns);
            Set<Expression> predicates = new MaterializedViewPredicateCollector(context).collect(scan);
            Assertions.assertFalse(predicates.isEmpty());
            Assertions.assertTrue(predicates.stream()
                    .allMatch(predicate -> visibleOutput.containsAll(predicate.getInputSlots())), predicates::toString);
            Slot score = visibleOutput.stream().filter(slot -> slot.getName().equals("score"))
                    .findFirst().orElseThrow(AssertionError::new);
            Assertions.assertTrue(predicates.stream().anyMatch(predicate -> predicate instanceof EqualTo
                            && predicate.child(0).equals(score) && predicate.child(1) instanceof Literal
                            && ((Literal) predicate.child(1)).getStringValue().equals("1")), predicates::toString);
        } finally {
            connectContext.getSessionVariable().setShowHiddenColumns(originalShowHiddenColumns);
            Config.enable_table_stream = originalEnableTableStream;
            dropMvByNereids("drop materialized view if exists mv_relation_implied_ivm");
            dropTableWithSql("drop table if exists relation_implied_ivm_base");
        }
    }

    @Test
    void testSelfJoinNonOutputPredicatesAreNotExported() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_self_join");
        try {
            createMvByNereids("create materialized view mv_relation_implied_self_join "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select a.id from T1 a cross join T1 b where a.score = 1 and b.score = 2;");
            mockRefreshedMtmv("mv_relation_implied_self_join");
            CascadesContext context = createCascadesContext("select id from mv_relation_implied_self_join");
            PlanChecker.from(context).setIsQuery().analyze();
            LogicalOlapScan scan = context.getRewritePlan()
                    .<LogicalOlapScan>collectFirst(LogicalOlapScan.class::isInstance).orElseThrow(AssertionError::new);
            // The predicates constrain different rows of T1 and say nothing about the MV's only output, id.
            Set<Expression> predicates = ((MTMV) scan.getTable()).getOrGenerateCache(connectContext)
                    .getOutputPredicates();
            Assertions.assertTrue(predicates.isEmpty(), predicates::toString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_self_join");
        }
    }

    @Test
    void testTimeDependentMvDefinitionDoesNotRemoveQueryFilter() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_current_date");
        try {
            createMvByNereids("create materialized view mv_relation_implied_current_date "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'enable_nondeterministic_function' = 'true')\n"
                    + "as select id, score from T1 where score > dayofmonth(current_date());");
            mockRefreshedMtmv("mv_relation_implied_current_date");
            CascadesContext context = createCascadesContext(
                    "select id from mv_relation_implied_current_date where score > dayofmonth(current_date())");
            PlanChecker.from(context).setIsQuery().analyze().rewrite();
            Plan queryPlan = context.getRewritePlan();
            LogicalFilter<? extends Plan> filter = queryPlan
                    .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                    .orElseThrow(AssertionError::new);
            StructInfo structInfo = StructInfo.of(queryPlan, context).withoutRedundantMvFilters(context);
            // Stored rows were selected at refresh time. Re-evaluating the date while planning
            // cannot prove that today's query filter is redundant on those rows.
            Assertions.assertTrue(structInfo.getPredicates().getPulledUpPredicates()
                    .containsAll(filter.getConjuncts()), structInfo.getTopPlan()::treeString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_current_date");
        }
    }

    @Test
    void testChangedBaseViewDoesNotSupplyOutputGuarantees() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_changed_view");
        dropView("drop view if exists relation_implied_base_view");
        try {
            createView("create view relation_implied_base_view as select id from T1 where id > 0");
            createMvByNereids("create materialized view mv_relation_implied_changed_view "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from relation_implied_base_view;");
            MTMV mtmv = mockRefreshedMtmv("mv_relation_implied_changed_view");
            MTMVCache originalCache = mtmv.getOrGenerateCache(connectContext);
            Assertions.assertFalse(originalCache.getOutputPredicates().isEmpty());

            executeNereidsSql("alter view relation_implied_base_view as select id from T1 where id > 10");
            Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
            MTMVCache changedCache = mtmv.getOrGenerateCache(connectContext);
            Assertions.assertNotSame(originalCache, changedCache);
            Assertions.assertTrue(changedCache.getOutputPredicates().isEmpty());
            Assertions.assertFalse(originalCache.getOutputPredicates().isEmpty());
            Assertions.assertNotNull(changedCache.getOriginalFinalPlan());

            CascadesContext context = createCascadesContext(
                    "select id from mv_relation_implied_changed_view where id > 10");
            PlanChecker.from(context).setIsQuery().analyze().rewrite();
            Plan queryPlan = context.getRewritePlan();
            LogicalFilter<? extends Plan> filter = queryPlan
                    .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                    .orElseThrow(AssertionError::new);
            StructInfo structInfo = StructInfo.of(queryPlan, context).withoutRedundantMvFilters(context);
            Assertions.assertTrue(structInfo.getPredicates().getPulledUpPredicates()
                    .containsAll(filter.getConjuncts()), structInfo.getTopPlan()::treeString);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_changed_view");
            dropView("drop view if exists relation_implied_base_view");
        }
    }

    @Test
    void testRefreshedLowerMvDoesNotRemoveCompensationForStaleUpper() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_stored_upper");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_stored_lower");
        dropView("drop view if exists relation_implied_refresh_view");
        try {
            createView("create view relation_implied_refresh_view as select id from T1 where id > 0");
            createMvByNereids("create materialized view mv_relation_implied_stored_lower "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "as select id from relation_implied_refresh_view;");
            MTMV lower = mockRefreshedMtmv("mv_relation_implied_stored_lower");
            createMvByNereids("create materialized view mv_relation_implied_stored_upper "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'grace_period' = '31536000')\n"
                    + "as select id from mv_relation_implied_stored_lower;");
            MTMV upper = mockRefreshedMtmv("mv_relation_implied_stored_upper");

            executeNereidsSql("alter view relation_implied_refresh_view as select id from T1 where id > 10");
            Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, lower.getStatus().getState());
            // Only the lower MV is refreshed. The upper still contains rows from before the view changed.
            lower.getStatus().setState(MTMVState.NORMAL);
            mockRefreshedPartitions(lower);
            Assertions.assertFalse(lower.getOrGenerateCache(connectContext).getOutputPredicates().isEmpty());
            Assertions.assertEquals(MTMVState.NORMAL, upper.getStatus().getState());
            upper.invalidateRewriteCache();
            Assertions.assertTrue(upper.getOrGenerateCache(connectContext).getOutputPredicates().isEmpty());

            CascadesContext context = createCascadesContext(
                    "select id from mv_relation_implied_stored_upper where id > 10");
            PlanChecker.from(context).setIsQuery().analyze().rewrite();
            LogicalFilter<? extends Plan> filter = context.getRewritePlan()
                    .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                    .orElseThrow(AssertionError::new);
            StructInfo structInfo = StructInfo.of(context.getRewritePlan(), context)
                    .withoutRedundantMvFilters(context);
            Assertions.assertTrue(structInfo.getPredicates().getPulledUpPredicates()
                    .containsAll(filter.getConjuncts()), structInfo.getTopPlan()::treeString);

            // Let the real grace-period check admit the old upper MV, instead of the fixture's
            // unconditional partition-valid mock. Its old rows must still satisfy the query filter.
            MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
            Mockito.doCallRealMethod().when(relationManager).isMVPartitionValid(Mockito.eq(upper),
                    Mockito.any(ConnectContext.class), Mockito.anyBoolean(), Mockito.anyMap());
            CascadesContext lowerQueryContext = createCascadesContext(
                    "select id from mv_relation_implied_stored_lower where id > 10");
            List<Plan> alternatives = PlanChecker.from(lowerQueryContext).setIsQuery().analyze().rewrite()
                    .preMvRewrite().optimize().getAllPlan();
            List<Plan> upperPlans = alternatives.stream().filter(plan -> plan.anyMatch(node ->
                            node instanceof LogicalOlapScan && ((LogicalOlapScan) node).getTable().getName()
                                    .equals("mv_relation_implied_stored_upper")))
                    .collect(Collectors.toList());
            Assertions.assertFalse(upperPlans.isEmpty(),
                    () -> alternatives.stream().map(Plan::treeString).collect(Collectors.joining("\n")));
            for (Plan upperPlan : upperPlans) {
                List<LogicalFilter<? extends Plan>> filters = upperPlan.collectToList(LogicalFilter.class::isInstance);
                Assertions.assertTrue(filters.stream().anyMatch(candidateFilter -> candidateFilter.getConjuncts().stream()
                                .anyMatch(predicate -> predicate instanceof GreaterThan
                                        && candidateFilter.child().getOutputSet()
                                                .containsAll(predicate.getInputSlots())
                                        && predicate.child(1) instanceof Literal
                                        && ((Literal) predicate.child(1)).getStringValue().equals("10"))),
                        upperPlan::treeString);
            }
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_stored_upper");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_stored_lower");
            dropView("drop view if exists relation_implied_refresh_view");
        }
    }

    private MTMV mockRefreshedMtmv(String name) {
        MTMV mtmv = mockCandidateMtmv(name);
        mockRefreshedPartitions(mtmv);
        return mtmv;
    }

    private void mockRefreshedPartitions(MTMV mtmv) {
        // Candidate validity alone does not model a completed refresh. Populate definition coverage
        // for every fixture partition, as addTaskResult does after successful refreshes.
        Map<String, MTMVRefreshPartitionSnapshot> snapshots = new HashMap<>();
        for (String partitionName : mtmv.getPartitionNames()) {
            snapshots.put(partitionName, new MTMVRefreshPartitionSnapshot());
        }
        mtmv.getRefreshSnapshot().updateSnapshots(snapshots, mtmv.getPartitionNames());
        mtmv.invalidateRewriteCache();
    }

    private void initMvRewriteSession() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        installValidRelationManager();
        connectContext.getState().setIsQuery(true);
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(PreRewriteStrategy.NOT_IN_RBO.name());
        connectContext.getSessionVariable().materializedViewRewriteDurationThresholdMs = 1000000;
    }
}
