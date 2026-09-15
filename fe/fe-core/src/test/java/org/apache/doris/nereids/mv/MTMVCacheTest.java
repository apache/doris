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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.rules.exploration.mv.AsyncMaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter.PreRewriteStrategy;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Relevant test case about mtmv cache.
 */
public class MTMVCacheTest extends SqlTestBase {

    @Test
    void testMTMVCacheIsCorrect() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        installValidRelationManager();

        connectContext.getState().setIsQuery(true);

        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id, sum(score) from T1 group by T1.id;");
        mockCandidateMtmv("mv1");
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(score) from T1 group by T1.id;",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();
        List<MaterializationContext> normalMaterializationContexts = c1.getMaterializationContexts();
        Assertions.assertEquals(1, normalMaterializationContexts.size());

        MTMV mtmv = ((AsyncMaterializationContext) normalMaterializationContexts.get(0)).getMtmv();
        MTMVCache cacheWithoutGuard = mtmv.getOrGenerateCache(connectContext);

        Plan cachePlan = cacheWithoutGuard.getAllRulesRewrittenPlanAndStructInfo().key();
        Optional<LogicalAggregate<? extends Plan>> aggregate = cachePlan
                .collectFirst(LogicalAggregate.class::isInstance);
        Assertions.assertTrue(aggregate.isPresent(),
                "Expected LogicalAggregate in cache plan but got: " + cachePlan.treeString()
                + "\nmtmv class=" + mtmv.getClass().getName()
                + "\nmtmv querySql=" + mtmv.getQuerySql());
        Assertions.assertTrue(aggregate.get().getOutputExpressions().stream()
                .noneMatch(expr -> expr.containsType(SessionVarGuardExpr.class)));

        mtmv.invalidateRewriteCache();
        MTMVCache cacheAfterInvalidate = mtmv.getOrGenerateCache(connectContext);
        Assertions.assertNotSame(cacheWithoutGuard, cacheAfterInvalidate);
        Optional<LogicalAggregate<? extends Plan>> aggregateAfterInvalidate =
                cacheAfterInvalidate.getAllRulesRewrittenPlanAndStructInfo().key()
                        .collectFirst(LogicalAggregate.class::isInstance);
        Assertions.assertTrue(aggregateAfterInvalidate.isPresent());

        // set guard check session var
        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_UNSIGNED_SUBTRACTION);
        CascadesContext c2 = createCascadesContext(
                "select T1.id, sum(score) from T1 group by T1.id;",
                connectContext
        );
        connectContext.getState().setIsQuery(true);
        PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();

        List<MaterializationContext> sessionChangedMaterializationContexts = c2.getMaterializationContexts();
        Assertions.assertEquals(1, sessionChangedMaterializationContexts.size());

        MTMV mvWithGuard = ((AsyncMaterializationContext) sessionChangedMaterializationContexts.get(0)).getMtmv();
        MTMVCache cacheWithGuard = mvWithGuard.getOrGenerateCache(connectContext);

        aggregate = cacheWithGuard.getAllRulesRewrittenPlanAndStructInfo().key()
                .collectFirst(LogicalAggregate.class::isInstance);
        Assertions.assertTrue(aggregate.isPresent());
        Assertions.assertTrue(aggregate.get().getOutputExpressions().stream()
                .anyMatch(expr -> expr.containsType(SessionVarGuardExpr.class)));
        dropMvByNereids("drop materialized view mv1");
    }

    /**
     * A query in +08:00 going through a view created in UTC must not CONSUME the UTC-materialized day column
     * of an MTMV created in UTC when pre_materialized_view_rewrite_strategy = FORCE_IN_RBO. Expanding the
     * view adds a query-side guard (the view's UTC creation vars) around the time-zone sensitive expression,
     * and the MTMV cache built for the +08:00 session carries the identical-looking cache-mismatch guard
     * (same child, same session vars). The cache guard must be structurally distinct from the nested
     * persisted-object guard so that the materialized day column is never substituted (for 2024-01-01
     * 20:30Z the UTC day is January 1 while the +08:00 day is January 2).
     * Recomputing the day from the MTMV's raw ts column is a different, valid rewrite: it is evaluated in
     * the current query session and returns exactly what the direct query computes, so it stays allowed.
     */
    @Test
    void testUtcViewNotRewrittenByUtcMtmvForCrossZoneQueryInRbo() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String originTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            installValidRelationManager();

            connectContext.getState().setIsQuery(true);
            connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
            connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(
                    PreRewriteStrategy.FORCE_IN_RBO.name());
            // the view and the MTMV are both created in UTC
            connectContext.getSessionVariable().setTimeZone("+00:00");
            createTables("CREATE TABLE IF NOT EXISTS tz_cross_zone_table (\n"
                    + "    ts TIMESTAMPTZ NOT NULL,\n"
                    + "    val bigint\n"
                    + ")\n"
                    + "DUPLICATE KEY(ts)\n"
                    + "DISTRIBUTED BY HASH(ts) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "  \"replication_num\" = \"1\"\n"
                    + ")\n");
            createView("CREATE VIEW tz_cross_zone_view AS "
                    + "SELECT ts, date_trunc(ts, 'day') AS d FROM tz_cross_zone_table");
            createMvByNereids("CREATE MATERIALIZED VIEW tz_cross_zone_mv BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "        PROPERTIES ('replication_num' = '1') \n"
                    + "        AS SELECT ts, date_trunc(ts, 'day') AS d FROM tz_cross_zone_table");
            mockCandidateMtmv("tz_cross_zone_mv");
            // the query runs in +08:00, a different zone than the creation zone
            connectContext.getSessionVariable().setTimeZone("+08:00");

            CascadesContext c1 = createCascadesContext("SELECT d FROM tz_cross_zone_view", connectContext);
            PlanChecker checker = PlanChecker.from(c1)
                    .setIsQuery()
                    .analyze()
                    .rewrite();

            // the pre-RBO snapshot of the query still carries the query-side (nested persisted-object) guard
            // that BindRelation added while expanding the UTC view into the +08:00 query
            List<Plan> tmpPlans = c1.getStatementContext().getTmpPlanForMvRewrite();
            Assertions.assertFalse(tmpPlans.isEmpty(),
                    "FORCE_IN_RBO should record the query plan for pre rewrite");
            Optional<SessionVarGuardExpr> queryGuard = findGuardExpr(tmpPlans.get(0));
            Assertions.assertTrue(queryGuard.isPresent(),
                    "expanding the UTC view in the +08:00 query must add a query-side guard");
            Assertions.assertFalse(queryGuard.get().isCacheGuard(),
                    "the query-side nested-object guard must not be a cache guard");

            // the MTMV cache built for the +08:00 session carries the cache-mismatch guard around the same
            // expression with the same session vars
            List<MaterializationContext> contexts = c1.getMaterializationContexts();
            Assertions.assertFalse(contexts.isEmpty(),
                    "the UTC MTMV should be a rewrite candidate for the query");
            MTMV mtmv = ((AsyncMaterializationContext) contexts.get(0)).getMtmv();
            MTMVCache cache = mtmv.getOrGenerateCache(connectContext);
            Plan cachePlan = cache.getAllRulesRewrittenPlanAndStructInfo().key();
            Optional<SessionVarGuardExpr> cacheGuard = findGuardExpr(cachePlan);
            Assertions.assertTrue(cacheGuard.isPresent(),
                    "the MTMV cache built for a different zone must carry a cache-mismatch guard");
            Assertions.assertTrue(cacheGuard.get().isCacheGuard(),
                    "the cache-mismatch guard must be structurally distinct from a query-side guard");
            Assertions.assertEquals(queryGuard.get().getSessionVars(), cacheGuard.get().getSessionVars(),
                    "the two guards carry the same session vars, so only the cache-guard distinction "
                            + "prevents the pre-RBO match");

            // the pre rewrite must not read the UTC-materialized day column: the day is recomputed from the
            // raw ts scan column in the +08:00 query session, which is what the direct query computes
            checker.preMvRewrite();
            List<Plan> rewrittenPlans = c1.getStatementContext().getRewrittenPlansByMv();
            Assertions.assertFalse(rewrittenPlans.isEmpty(),
                    "recomputing the day from the raw instant must keep rewriting across zones");
            for (Plan rewrittenPlan : rewrittenPlans) {
                List<LogicalOlapScan> scans = rewrittenPlan.collectToList(LogicalOlapScan.class::isInstance);
                Optional<LogicalOlapScan> mvScan = scans.stream()
                        .filter(scan -> "tz_cross_zone_mv".equals(scan.getTable().getName()))
                        .findFirst();
                Assertions.assertTrue(mvScan.isPresent(),
                        "the rewritten plan must scan the materialized view");
                Set<Slot> materializedDaySlots = mvScan.get().getOutput().stream()
                        .filter(slot -> "d".equals(slot.getName()))
                        .collect(Collectors.toSet());
                List<Plan> planNodes = rewrittenPlan.collectToList(plan -> true);
                for (Plan node : planNodes) {
                    for (Expression expr : node.getExpressions()) {
                        Set<Slot> exprSlots = expr.collectToSet(Slot.class::isInstance);
                        for (Slot slot : exprSlots) {
                            Assertions.assertFalse(materializedDaySlots.contains(slot),
                                    "the cross-zone rewrite must not read the creation-zone materialized day "
                                            + "column " + slot + ", plan is " + rewrittenPlan.treeString());
                        }
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originTimeZone);
            dropView("DROP VIEW IF EXISTS tz_cross_zone_view");
            dropMvByNereids("DROP MATERIALIZED VIEW IF EXISTS tz_cross_zone_mv");
            dropTable("tz_cross_zone_table", false);
        }
    }

    /**
     * Finds the first {@link SessionVarGuardExpr} anywhere in the plan's expression trees.
     */
    private static Optional<SessionVarGuardExpr> findGuardExpr(Plan plan) {
        for (Plan node : plan.<Plan>collectToList(p -> true)) {
            for (Expression expr : node.getExpressions()) {
                Optional<SessionVarGuardExpr> guard = expr.collectFirst(SessionVarGuardExpr.class::isInstance);
                if (guard.isPresent()) {
                    return guard;
                }
            }
        }
        return Optional.empty();
    }

    @Test
    void testInvalidateShouldNotPublishInFlightRewriteCache() throws Exception {
        ControlledCacheMTMV mtmv = new ControlledCacheMTMV();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<MTMVCache> cacheFuture = executor.submit(() -> mtmv.getOrGenerateCache(connectContext));
            Assertions.assertTrue(mtmv.firstBuildStarted.await(5, TimeUnit.SECONDS));

            mtmv.invalidateRewriteCache();
            mtmv.releaseFirstBuild.countDown();

            MTMVCache generatedCache = cacheFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertNotSame(mtmv.firstCache, generatedCache);
            Assertions.assertSame(mtmv.secondCache, generatedCache);
            Assertions.assertSame(generatedCache, mtmv.getOrGenerateCache(connectContext));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testNondeterministicDefinitionDoesNotExportFoldedPredicates() throws Exception {
        createFunction("create alias function mv_cache_day(int) with parameter(n) "
                + "as dayofmonth(days_add(current_date(), n))");
        try {
            for (String expression : ImmutableList.of(
                    "dayofmonth(current_date())", "dayofmonth(CURRENT_DATE)", "mv_cache_day(0)")) {
                for (LocalDate date : ImmutableList.of(LocalDate.of(2026, 9, 9), LocalDate.of(2026, 9, 10))) {
                    ConnectContext cacheContext = Mockito.spy(MTMVPlanUtil.createBasicMvContext(connectContext,
                            MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE,
                            connectContext.getSessionVariable().getAffectQueryResultInPlanVariables()));
                    cacheContext.getSessionVariable().setTimeZone("UTC");
                    Mockito.doReturn(date.atStartOfDay(ZoneOffset.UTC).toInstant())
                            .when(cacheContext).getStartTimeInstant();
                    cacheContext.setThreadLocalInfo();
                    MTMVCache cache = MTMVCache.from("select id, score from T1 where score > " + expression,
                            cacheContext, false, true, connectContext, SessionVarGuardRewriter.GUARD_NONE);
                    LogicalFilter<? extends Plan> filter = cache.getOriginalFinalPlan()
                            .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                            .orElseThrow(AssertionError::new);
                    // The same definition folds to a different bound when its cache is rebuilt tomorrow.
                    Assertions.assertTrue(filter.getConjuncts().stream().anyMatch(predicate ->
                                    predicate instanceof GreaterThan && predicate.child(1) instanceof Literal
                                            && ((Literal) predicate.child(1)).getStringValue()
                                                    .equals(Integer.toString(date.getDayOfMonth()))),
                            cache.getOriginalFinalPlan()::treeString);
                    Assertions.assertTrue(cache.getOutputPredicates().isEmpty(), cache.getOutputPredicates()::toString);
                }
            }
        } finally {
            connectContext.setThreadLocalInfo();
            executeNereidsSql("drop function mv_cache_day(int)");
        }
    }

    /**
     * A successful refresh ({@link MTMV#addTaskResult}) replaces the cache set and advances the rewrite
     * generation, so an in-flight rewrite-cache builder that captured the previous generation (e.g. it
     * expanded a base-view definition that was ALTERed in the meantime) must discard its stale result
     * instead of publishing it over the newly refreshed data.
     */
    @Test
    void testAddTaskResultShouldNotPublishInFlightRewriteCache() throws Exception {
        ControlledCacheMTMV mtmv = new ControlledCacheMTMV();
        // persisted creation zone +00:00; the query session below uses +08:00 so the in-flight builder
        // needs the time-zone guarded cache (mask 1), which the refresh (mask 0 only) does not cover
        setPrivateField(mtmv, "sessionVariables", ImmutableMap.of(SessionVariable.TIME_ZONE, "+00:00"));
        setPrivateField(mtmv, "jobInfo", new MTMVJobInfo(MTMVJobManager.MTMV_JOB_PREFIX + "test"));
        setPrivateField(mtmv, "mvPartitionInfo", new MTMVPartitionInfo(MTMVPartitionType.SELF_MANAGE));
        setPrivateField(mtmv, "refreshSnapshot", new MTMVRefreshSnapshot());
        setPrivateField(mtmv, "status", new MTMVStatus());
        // the no-arg MTMV constructor leaves ivmInfo null, but addTaskResult reads ivmInfo.isEnableIvm()
        setPrivateField(mtmv, "ivmInfo", new IvmInfo());
        connectContext.getSessionVariable().setTimeZone("+08:00");
        // addTaskResult's refreshComplete hook builds a BaseTableInfo from the (uninitialized) test MTMV,
        // which requires a resolved database; deregister the hook for the duration of the test
        MTMVService mtmvService = Env.getCurrentEnv().getMtmvService();
        mtmvService.deregisterHook("MTMVRelationManager");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<MTMVCache> cacheFuture = executor.submit(() -> mtmv.getOrGenerateCache(connectContext));
            Assertions.assertTrue(mtmv.firstBuildStarted.await(5, TimeUnit.SECONDS));

            MTMVTask task = new MTMVTask();
            setPrivateField(task, "status", TaskStatus.SUCCESS);
            AlterMTMV alterMTMV = new AlterMTMV(
                    new TableNameInfo("db", "test_mv"), MTMVAlterOpType.ADD_TASK);
            alterMTMV.setTask(task);
            alterMTMV.setRelation(new MTMVRelation(Collections.emptySet(), Collections.emptySet(),
                    Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
            alterMTMV.setPartitionSnapshots(Collections.emptyMap());
            mtmv.addTaskResult(alterMTMV, false);
            mtmv.releaseFirstBuild.countDown();

            MTMVCache generatedCache = cacheFuture.get(5, TimeUnit.SECONDS);
            // the in-flight builder captured the pre-refresh generation; it must NOT publish its stale plan
            Assertions.assertNotSame(mtmv.firstCache, generatedCache);
            Assertions.assertSame(mtmv.secondCache, generatedCache);
        } finally {
            mtmvService.registerHook("MTMVRelationManager", mtmvService.getRelationManager());
            executor.shutdownNow();
        }
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = null;
        Class<?> clazz = target.getClass();
        while (clazz != null && field == null) {
            try {
                field = clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        Assertions.assertNotNull(field, "field not found: " + fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @ParameterizedTest(name = "throughView={0}")
    @ValueSource(booleans = {false, true})
    void testReplacedAliasDefinitionDoesNotExportFoldedPredicates(boolean throughView) throws Exception {
        createFunction("create alias function mv_cache_bound(int) with parameter(n) as abs(n)");
        try {
            String definitionSql = "select id from T1 where id > mv_cache_bound(0)";
            if (throughView) {
                // View analysis must retain the alias dependency in its parent's statement.
                createView("create view mv_cache_alias_view as " + definitionSql);
                definitionSql = "select id from mv_cache_alias_view";
            }
            for (int bound : ImmutableList.of(0, 10)) {
                if (bound == 10) {
                    executeNereidsSql("drop function mv_cache_bound(int)");
                    createFunction("create alias function mv_cache_bound(int) with parameter(n) as abs(n + 10)");
                }
                ConnectContext cacheContext = MTMVPlanUtil.createBasicMvContext(connectContext,
                        MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE,
                        connectContext.getSessionVariable().getAffectQueryResultInPlanVariables());
                cacheContext.setThreadLocalInfo();
                MTMVCache cache = MTMVCache.from(definitionSql, cacheContext, false, true, connectContext,
                        SessionVarGuardRewriter.GUARD_NONE);
                LogicalFilter<? extends Plan> filter = cache.getOriginalFinalPlan()
                        .<LogicalFilter<? extends Plan>>collectFirst(LogicalFilter.class::isInstance)
                        .orElseThrow(AssertionError::new);
                // A rebuild binds the new deterministic body, although stored MV rows have not been refreshed.
                Assertions.assertTrue(filter.getConjuncts().stream().anyMatch(predicate ->
                                predicate instanceof GreaterThan && predicate.child(1) instanceof Literal
                                        && ((Literal) predicate.child(1)).getStringValue()
                                                .equals(Integer.toString(bound))),
                        cache.getOriginalFinalPlan()::treeString);
                Assertions.assertTrue(cache.getOutputPredicates().isEmpty(), cache.getOutputPredicates()::toString);
            }
        } finally {
            connectContext.setThreadLocalInfo();
            if (throughView) {
                dropView("drop view if exists mv_cache_alias_view");
            }
            executeNereidsSql("drop function if exists mv_cache_bound(int)");
        }
    }

    private static class ControlledCacheMTMV extends MTMV {
        private static final AtomicLong NEXT_MTMV_ID = new AtomicLong(2_000_000_000L);
        private final CountDownLatch firstBuildStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstBuild = new CountDownLatch(1);
        private final AtomicInteger buildCount = new AtomicInteger();
        private final MTMVCache firstCache = new MTMVCache(
                null, null, null, Collections.emptyList(), Collections.emptySet());
        private final MTMVCache secondCache = new MTMVCache(
                null, null, null, Collections.emptyList(), Collections.emptySet());

        private ControlledCacheMTMV() {
            setId(NEXT_MTMV_ID.incrementAndGet());
            setStatus(new MTMVStatus(MTMVState.NORMAL, null));
            setRefreshSnapshot(new MTMVRefreshSnapshot());
        }

        @Override
        protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock, int guardMask) {
            if (buildCount.incrementAndGet() == 1) {
                firstBuildStarted.countDown();
                try {
                    Assertions.assertTrue(releaseFirstBuild.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return firstCache;
            }
            return secondCache;
        }
    }
}
