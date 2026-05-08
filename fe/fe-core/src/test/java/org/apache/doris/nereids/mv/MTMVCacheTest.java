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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.AsyncMaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static class ControlledCacheMTMV extends MTMV {
        private final CountDownLatch firstBuildStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstBuild = new CountDownLatch(1);
        private final AtomicInteger buildCount = new AtomicInteger();
        private final MTMVCache firstCache = new MTMVCache(null, null, null, Collections.emptyList());
        private final MTMVCache secondCache = new MTMVCache(null, null, null, Collections.emptyList());

        @Override
        protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock,
                boolean addSessionVarGuard) {
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
