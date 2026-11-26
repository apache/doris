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

package org.apache.doris.nereids;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.jobs.executor.TableCollectAndHookInitializer;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NereidsPlannerTest {

    @Test
    public void testInsertTargetSchemaCaptureWaitsForFullSchemaRebuild() throws Exception {
        Column key = new Column("a", Type.INT);
        key.setIsKey(true);
        List<Column> originalSchema = ImmutableList.of(key);
        List<Column> rebuiltSchema = ImmutableList.of(key, new Column("d", Type.INT));
        OlapTable target = Mockito.spy(new OlapTable(68363L, "target", originalSchema, KeysType.DUP_KEYS,
                new PartitionInfo(), new HashDistributionInfo(1, originalSchema)));
        target.setBaseIndexId(68364L);
        target.setIndexMeta(target.getBaseIndexId(), "target", originalSchema, 1, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        CountDownLatch schemaCleared = new CountDownLatch(1);
        CountDownLatch captureAttempted = new CountDownLatch(1);
        CountDownLatch finishRebuild = new CountDownLatch(1);
        AtomicBoolean schemaRebuilt = new AtomicBoolean();
        AtomicBoolean readLockAttempted = new AtomicBoolean();
        target.setNewFullSchema(new ArrayList<Column>(originalSchema) {
            @Override
            public void clear() {
                super.clear();
                schemaCleared.countDown();
                awaitLatch(finishRebuild);
            }
        });
        Mockito.doAnswer(invocation -> {
            readLockAttempted.set(true);
            captureAttempted.countDown();
            return invocation.callRealMethod();
        }).when(target).readLock();
        Mockito.doAnswer(invocation -> {
            captureAttempted.countDown();
            Assertions.assertTrue(schemaRebuilt.get(), "Target schema must not be copied during rebuild");
            return invocation.callRealMethod();
        }).when(target).getFullSchema();
        CascadesContext cascadesContext = createInsertCollectionContext(target);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        Future<?> writer = null;
        Future<?> collector = null;
        try {
            writer = workers.submit(() -> {
                target.writeLock();
                try {
                    target.getIndexMetaByIndexId(target.getBaseIndexId()).setSchema(rebuiltSchema);
                    target.getIndexMetaByIndexId(target.getBaseIndexId()).setSchemaVersion(2);
                    target.rebuildFullSchema();
                    schemaRebuilt.set(true);
                } finally {
                    target.writeUnlock();
                }
                return null;
            });
            awaitLatch(schemaCleared);
            collector = workers.submit(() -> {
                ConnectContext connectContext = cascadesContext.getConnectContext();
                connectContext.setThreadLocalInfo();
                try {
                    cascadesContext.newTableCollector(true).collect();
                } finally {
                    ConnectContext.remove();
                }
            });
            awaitLatch(captureAttempted);
            Assertions.assertTrue(readLockAttempted.get(), "Collection must acquire the target read lock first");
            finishRebuild.countDown();
            writer.get(10, TimeUnit.SECONDS);
            collector.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(rebuiltSchema, cascadesContext.getStatementContext().getInsertTargetSchema());
            Assertions.assertTrue(target.tryWriteLock(1, TimeUnit.SECONDS));
            target.writeUnlock();
        } finally {
            finishRebuild.countDown();
            if (writer != null) {
                writer.cancel(true);
            }
            if (collector != null) {
                collector.cancel(true);
            }
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
            cascadesContext.getStatementContext().close();
        }
    }

    @Test
    public void testInsertTargetSchemaCaptureReleasesReadLockOnFailure() {
        OlapTable target = Mockito.spy(new OlapTable(68363L, "target", ImmutableList.of(new Column("a", Type.INT)),
                KeysType.DUP_KEYS, new PartitionInfo(), new HashDistributionInfo()));
        Mockito.doThrow(new IllegalStateException("schema capture failed")).when(target).getFullSchema();
        CascadesContext cascadesContext = createInsertCollectionContext(target);
        try {
            Assertions.assertThrows(IllegalStateException.class,
                    () -> cascadesContext.newTableCollector(true).collect());
            Mockito.verify(target).readLock();
            Mockito.verify(target).readUnlock();
            Assertions.assertTrue(target.tryWriteLock(1, TimeUnit.SECONDS));
            target.writeUnlock();
        } finally {
            cascadesContext.getStatementContext().close();
        }
    }

    private CascadesContext createInsertCollectionContext(OlapTable target) {
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().enableDmlMaterializedViewRewrite = false;
        TableIf source = Mockito.mock(TableIf.class);
        String sql = "insert into internal.db.target select a from internal.db.source";
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0)) {
            @Override
            public void registerExternalTableForPreload(TableIf table, Optional<TableSnapshot> snapshot,
                    Optional<TableScanParams> scanParams) {
                Assertions.assertSame(source, table);
                Assertions.assertTrue(target.tryWriteLock(1, TimeUnit.SECONDS),
                        "Source collection must run after releasing the target schema capture lock");
                target.writeUnlock();
                super.registerExternalTableForPreload(table, snapshot, scanParams);
            }
        };
        connectContext.setStatementContext(statementContext);
        statementContext.getInsertTargetTables().put(ImmutableList.of("internal", "db", "target"), target);
        statementContext.getTables().put(ImmutableList.of("internal", "db", "source"), source);
        UnboundRelation relation = new UnboundRelation(new RelationId(0), ImmutableList.of("internal", "db", "source"));
        UnboundTableSink<?> sink = new UnboundTableSink<>(ImmutableList.of("internal", "db", "target"),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), relation);
        return CascadesContext.initContext(statementContext, sink, PhysicalProperties.ANY);
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), "Concurrent schema test rendezvous timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    @Test
    public void testCollectAndLockTableRecordsPreloadTimeWhenExecuted() {
        SummaryProfile summaryProfile = Mockito.mock(SummaryProfile.class);
        NereidsPlanner planner = createPlanner(
                ExternalMetadataPreloadResult.executed(2, 1, 123L), summaryProfile);

        planner.collectAndLockTable(false);

        Mockito.verify(summaryProfile).addNereidsPreloadExternalMetadataTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableStartTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableFinishTime(Mockito.anyLong());
    }

    @Test
    public void testCollectAndLockTableSkipsPreloadCounterWhenNotExecuted() {
        SummaryProfile summaryProfile = Mockito.mock(SummaryProfile.class);
        NereidsPlanner planner = createPlanner(
                ExternalMetadataPreloadResult.skipped(1, "skip preload"), summaryProfile);

        planner.collectAndLockTable(false);

        Mockito.verify(summaryProfile, Mockito.never()).addNereidsPreloadExternalMetadataTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableStartTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableFinishTime(Mockito.anyLong());
    }

    private NereidsPlanner createPlanner(ExternalMetadataPreloadResult preloadResult, SummaryProfile summaryProfile) {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        StatementContext statementContext = Mockito.spy(
                new StatementContext(connectContext, new OriginStatement("select 1", 0)));
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        TableCollectAndHookInitializer tableCollector = Mockito.mock(TableCollectAndHookInitializer.class);

        Mockito.when(connectContext.getExecutor()).thenReturn(executor);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-1");
        Mockito.when(executor.getSummaryProfile()).thenReturn(summaryProfile);
        Mockito.when(cascadesContext.newTableCollector(true, true)).thenReturn(tableCollector);
        Mockito.doAnswer(invocation -> {
            statementContext.setExternalMetadataPreloadResult(preloadResult);
            return null;
        }).when(tableCollector).collect();
        Mockito.doNothing().when(statementContext).lock();

        NereidsPlanner planner = new NereidsPlanner(statementContext);
        Deencapsulation.setField(planner, "cascadesContext", cascadesContext);
        return planner;
    }
}
