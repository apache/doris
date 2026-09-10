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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MTMVOutputPredicatesTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDefinitionChangeInvalidatesPublishedGuarantees(boolean baseViewChange) {
        CacheMTMV mtmv = new CacheMTMV();
        MTMVCache beforeChange = mtmv.getOrGenerateCache(mtmv.context);
        Assertions.assertEquals(Set.of(mtmv.predicate), beforeChange.getOutputPredicates());

        if (baseViewChange) {
            mtmv.processBaseViewChange("base view definition changed");
        } else {
            mtmv.alterStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "base table changed"));
        }

        MTMVCache afterChange = mtmv.getOrGenerateCache(mtmv.context);
        Assertions.assertNotSame(beforeChange, afterChange);
        Assertions.assertTrue(afterChange.getOutputPredicates().isEmpty());
        Assertions.assertTrue(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
        // Return the normalized cache that was published, not the builder's unqualified guarantees.
        Assertions.assertSame(afterChange, mtmv.getOrGenerateCache(mtmv.context));
        Assertions.assertSame(beforeChange.getOriginalFinalPlan(), afterChange.getOriginalFinalPlan());
    }

    @Test
    void testViewChangeDiscardsInFlightGuarantees() throws Exception {
        BlockingCacheMTMV mtmv = new BlockingCacheMTMV();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<MTMVCache> cacheFuture = executor.submit(() -> mtmv.getOrGenerateCache(mtmv.context));
            Assertions.assertTrue(mtmv.firstBuildStarted.await(5, TimeUnit.SECONDS));
            mtmv.processBaseViewChange("base view changed while its cache was building");
            mtmv.releaseFirstBuild.countDown();

            MTMVCache cache = cacheFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(2, mtmv.buildCount.get());
            Assertions.assertTrue(cache.getOutputPredicates().isEmpty());
            Assertions.assertSame(cache, mtmv.getOrGenerateCache(mtmv.context));
        } finally {
            mtmv.releaseFirstBuild.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void testGuaranteesRecoverOnlyAfterEveryPartitionUsesCurrentDefinition() {
        CacheMTMV mtmv = new CacheMTMV();
        mtmv.processBaseViewChange("base view predicate changed");
        MTMVCache changedCache = mtmv.getOrGenerateCache(mtmv.context);
        Assertions.assertTrue(changedCache.getOutputPredicates().isEmpty());

        Env env = Mockito.mock(Env.class);
        MTMVService service = Mockito.mock(MTMVService.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        Mockito.when(env.getMtmvService()).thenReturn(service);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class)))
                .thenReturn(editLogItem);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // Replay restores NORMAL after refreshing p1, but p2 still contains the old definition's rows.
            Assertions.assertTrue(mtmv.addTaskResult(successfulRefresh(mtmv, "p1"), true));
            Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
            Assertions.assertEquals(Set.of("p1"), mtmv.getRefreshSnapshot().getPartitionSnapshots().keySet());
            MTMVCache partiallyRefreshedCache = mtmv.getOrGenerateCache(mtmv.context);
            Assertions.assertNotSame(changedCache, partiallyRefreshedCache);
            Assertions.assertTrue(partiallyRefreshedCache.getOutputPredicates().isEmpty());

            // A live completion builds both caches before adding p2's snapshot. Publication must use
            // the merged snapshot coverage and replace the previous cache without rebuilding on read.
            Assertions.assertTrue(mtmv.addTaskResult(successfulRefresh(mtmv, "p2"), false));
            int buildsAtRefreshCompletion = mtmv.buildCount.get();
            MTMVCache fullyRefreshedCache = mtmv.getOrGenerateCache(mtmv.context);
            Assertions.assertNotSame(partiallyRefreshedCache, fullyRefreshedCache);
            Assertions.assertEquals(Set.of("p1", "p2"),
                    mtmv.getRefreshSnapshot().getPartitionSnapshots().keySet());
            Assertions.assertEquals(Set.of(mtmv.predicate), fullyRefreshedCache.getOutputPredicates());
            Assertions.assertEquals(buildsAtRefreshCompletion, mtmv.buildCount.get());
        }
        Mockito.verify(editLogItem).await();
    }

    private static AlterMTMV successfulRefresh(CacheMTMV mtmv, String partition) {
        MTMVTask task = new MTMVTask(mtmv, mtmv.getRelation(),
                new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        task.setTaskId(1L);
        task.setStatus(TaskStatus.SUCCESS);
        Deencapsulation.setField(task, "mtmvSchemaChangeVersion", mtmv.getSchemaChangeVersion());
        AlterMTMV result = new AlterMTMV(new TableNameInfo("db", "mv"), MTMVAlterOpType.ADD_TASK);
        result.setTask(task);
        result.setRelation(mtmv.getRelation());
        result.setPartitionSnapshots(Map.of(partition, new MTMVRefreshPartitionSnapshot()));
        return result;
    }

    private static class CacheMTMV extends MTMV {
        final ConnectContext context = Mockito.mock(ConnectContext.class);
        final AtomicInteger buildCount = new AtomicInteger();
        private final Plan plan = new LogicalOneRowRelation(new RelationId(1),
                List.of(new Alias(new ExprId(1), new IntegerLiteral(20), "id")));
        private final Expression predicate = new GreaterThan(plan.getOutput().get(0), new IntegerLiteral(10));

        CacheMTMV() {
            Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
            setId(1L);
            Deencapsulation.setField(this, "name", "mv");
            setQualifiedDbName("db");
            setQuerySql("select id from base_view");
            MTMVStatus status = new MTMVStatus(MTMVState.NORMAL, null);
            status.setRefreshState(MTMVRefreshState.SUCCESS);
            setStatus(status);
            setRefreshInfo(new MTMVRefreshInfo(BuildMode.IMMEDIATE, RefreshMethod.COMPLETE,
                    new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL)));
            setJobInfo(new MTMVJobInfo("mv_job"));
            setMvProperties(new HashMap<>());
            setRelation(new MTMVRelation(Set.of(), Set.of(), Set.of(), Set.of(), Set.of()));
            setMvPartitionInfo(new MTMVPartitionInfo());
            Deencapsulation.setField(this, "ivmInfo", new IvmInfo());
            Deencapsulation.setField(this, "sessionVariables", new HashMap<>(
                    context.getSessionVariable().getAffectQueryResultInPlanVariables()));
            setBaseIndexId(1L);
            setIndexMeta(1L, "mv", List.of(new Column("id", PrimitiveType.INT, true)),
                    0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
            setPartitionInfo(new SinglePartitionInfo());
            addPartition(new Partition(1L, "p1", new MaterializedIndex(), null));
            addPartition(new Partition(2L, "p2", new MaterializedIndex(), null));
            MTMVRefreshSnapshot snapshot = new MTMVRefreshSnapshot();
            snapshot.updateSnapshots(Map.of("p1", new MTMVRefreshPartitionSnapshot(),
                    "p2", new MTMVRefreshPartitionSnapshot()), getPartitionNames());
            setRefreshSnapshot(snapshot);
        }

        @Override
        protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock,
                boolean addSessionVarGuard) {
            buildCount.incrementAndGet();
            return new MTMVCache(Pair.of(plan, null), plan, null, List.of(), Set.of(predicate));
        }
    }

    private static class BlockingCacheMTMV extends CacheMTMV {
        private final CountDownLatch firstBuildStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstBuild = new CountDownLatch(1);

        @Override
        protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock,
                boolean addSessionVarGuard) {
            MTMVCache cache = super.createRewriteCache(currentContext, needLock, addSessionVarGuard);
            if (buildCount.get() == 1) {
                firstBuildStarted.countDown();
                try {
                    Assertions.assertTrue(releaseFirstBuild.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(exception);
                }
            }
            return cache;
        }
    }
}
