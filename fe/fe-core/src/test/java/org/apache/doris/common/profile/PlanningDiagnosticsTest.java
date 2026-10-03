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

package org.apache.doris.common.profile;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.PlanningDiagnostics.Phase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PlanningDiagnosticsTest {
    private final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
    private ConnectContext context;
    private ConnectContext previousContext;
    private SummaryProfile summary;
    private long savedThreshold;
    private long savedOperationThreshold;
    private long savedInterval;
    private boolean savedEnabled;
    private boolean savedUnitTest;
    private String savedQueryId;

    @BeforeEach
    public void setUp() {
        savedThreshold = Config.nereids_planning_log_threshold_ms;
        savedOperationThreshold = Config.nereids_planning_operation_log_threshold_ms;
        savedInterval = Config.nereids_planning_log_interval_ms;
        savedEnabled = Config.sys_log_enable_query_id;
        savedUnitTest = FeConstants.runningUnitTest;
        previousContext = ConnectContext.get();
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        Config.nereids_planning_log_threshold_ms = 5000;
        Config.nereids_planning_operation_log_threshold_ms = 1000;
        Config.nereids_planning_log_interval_ms = 30000;
        Config.sys_log_enable_query_id = true;
        FeConstants.runningUnitTest = true;
        context = new ConnectContext();
        context.setQueryId(new org.apache.doris.thrift.TUniqueId(1, 2));
        context.setStmtId(7);
        context.setThreadLocalInfo();
        summary = new SummaryProfile();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getSummaryProfile()).thenReturn(summary);
        context.setExecutor(executor);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
        Config.nereids_planning_log_threshold_ms = savedThreshold;
        Config.nereids_planning_operation_log_threshold_ms = savedOperationThreshold;
        Config.nereids_planning_log_interval_ms = savedInterval;
        Config.sys_log_enable_query_id = savedEnabled;
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @Test
    public void testFastPassIsQuietAndAuditContainsTimingsWithoutProfile() {
        context.getSessionVariable().enableProfile = false;
        summary.addNereidsPreloadExternalMetadataTime(12);
        summary.setNereidsCollectTablePartitionFinishTime(100);
        summary.setNereidsPreRewriteByMvFinishTime(108);
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> {
                PlanningDiagnostics.runPhase(context, Phase.CHOOSE_PLAN, () -> advance(7));
                PlanningDiagnostics.runPhase(context, Phase.POST_PROCESS, () -> advance(11));
            });
            Assertions.assertTrue(events(appender).isEmpty());
        }
        JsonObject audit = JsonParser.parseString(summary.getPlanTime()).getAsJsonObject();
        Assertions.assertEquals(12, audit.get("preload_external_metadata").getAsLong());
        Assertions.assertEquals(8, audit.get("pre_rewrite_mv").getAsLong());
        Assertions.assertEquals(7, audit.get("planning_choose_plan").getAsLong());
        Assertions.assertEquals(-1, audit.get("planning_analyze").getAsLong());
        audit.entrySet().forEach(entry -> {
            Assertions.assertTrue(entry.getValue().isJsonPrimitive());
            Assertions.assertTrue(entry.getValue().getAsJsonPrimitive().isNumber());
            Assertions.assertEquals(entry.getValue().getAsLong(), entry.getValue().getAsInt());
        });
        Assertions.assertEquals(11, phase("post_process").get("elapsed_ms").getAsLong());
        Assertions.assertEquals("not_run", phase("analyze").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    @Test
    public void testRunningReportRateLimitIdentityAndContextRestoration() {
        PlanningDiagnostics diagnostics = new PlanningDiagnostics(context, clock::get);
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            PlanningDiagnostics.execute(diagnostics, () -> {
                PlanningDiagnostics.runPhase(context, Phase.OPTIMIZE, () -> {
                    diagnostics.setCurrentJob(PlanningDiagnosticsTest.class);
                    advance(5000);
                    context.queryId().setLo(99);
                    ThreadContext.put(QueryLogContext.QUERY_ID, "checker");
                    context.checkTimeout(0);
                    Assertions.assertEquals("checker", ThreadContext.get(QueryLogContext.QUERY_ID));
                    context.checkTimeout(0);
                    Assertions.assertEquals(1, events(appender).size());
                    advance(30000);
                    context.checkTimeout(0);
                    Assertions.assertEquals(2, events(appender).size());
                    Assertions.assertEquals("1-2", events(appender).get(0).getContextData()
                            .getValue(QueryLogContext.QUERY_ID));
                    Assertions.assertTrue(appender.contains(Level.WARN, "\"job\":\"PlanningDiagnosticsTest\""));
                    Assertions.assertTrue(appender.contains(Level.WARN, "\"phase\":\"optimize\""));
                });
                return null;
            });
            int count = events(appender).size();
            diagnostics.reportIfSlow();
            Assertions.assertEquals(count, events(appender).size());
        }
    }

    @Test
    public void testDynamicDisableAndMinimumInterval() {
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, () -> {
                advance(5000);
                Config.nereids_planning_log_threshold_ms = 0;
                context.checkTimeout(0);
                Assertions.assertTrue(events(appender).isEmpty());
                Config.nereids_planning_log_threshold_ms = 1;
                Config.nereids_planning_log_interval_ms = 0;
                context.checkTimeout(0);
                advance(999);
                context.checkTimeout(0);
                Assertions.assertEquals(1, events(appender).size());
                advance(1);
                context.checkTimeout(0);
                Assertions.assertEquals(2, events(appender).size());
            }));
        }
    }

    @Test
    public void testFailurePreservesElapsedAndSkipsLaterStages() {
        IllegalStateException expected = new IllegalStateException("original failure");
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertSame(expected, Assertions.assertThrows(IllegalStateException.class,
                    () -> run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, () -> {
                        advance(3);
                        throw expected;
                    }))));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"failed_step\":\"analyze\""));
        }
        Assertions.assertEquals(3, phase("analyze").get("elapsed_ms").getAsLong());
        Assertions.assertEquals("failed", phase("analyze").get("status").getAsString());
        Assertions.assertEquals("not_run", phase("optimize").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    @Test
    public void testNestedAndRepeatedPlanningRestoreParentAndAccumulate() {
        run(() -> PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
            PlanningDiagnostics parent = context.getPlanningDiagnostics();
            run(() -> PlanningDiagnostics.runPhase(context, Phase.CHOOSE_PLAN, () -> advance(5)));
            Assertions.assertSame(parent, context.getPlanningDiagnostics());
            advance(2);
        }));
        run(() -> PlanningDiagnostics.runPhase(context, Phase.CHOOSE_PLAN, () -> advance(8)));
        Assertions.assertEquals(3, summary.getPlanningDetails().get("passes").getAsInt());
        Assertions.assertEquals(13, summary.getPlanningDetails().getAsJsonObject("phase_time_ms")
                .get("choose_plan").getAsLong());
        Assertions.assertEquals("not_run", phase("rewrite").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    @Test
    public void testNestedSlowOperationsAreDeferredUntilOuterPassEnds() {
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> {
                PlanningDiagnostics.LockHold hold = context.getPlanningDiagnostics().acquiredLock("catalog.db.outer");
                run(() -> PlanningDiagnostics.operation(context, "load_schema", "catalog.db.inner", -1, () -> {
                    advance(1000);
                    return null;
                }));
                Assertions.assertTrue(events(appender).isEmpty());
                hold.close();
            });
            Assertions.assertTrue(appender.contains(Level.WARN, "catalog.db.inner"));
            Assertions.assertTrue(appender.contains(Level.WARN, "catalog.db.outer"));
        }
    }

    @Test
    public void testRecoveredOperationDoesNotReplaceLaterFailure() {
        Assertions.assertThrows(IllegalStateException.class, () -> run(() -> {
            try {
                PlanningDiagnostics.operation(context, "load_schema", "catalog.db.table", -1, () -> {
                    throw new IllegalArgumentException("recovered");
                });
            } catch (IllegalArgumentException expected) {
                // Materialized-view alternatives can fail while planning continues.
            }
            PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
                throw new IllegalStateException("actual failure");
            });
        }));
        Assertions.assertEquals("rewrite", summary.getPlanningDetails().getAsJsonObject("last_pass")
                .get("failed_step").getAsString());
    }

    @Test
    public void testOperationsAreBoundedDeferredAndSupportDisabledQueryPrefix() {
        Config.sys_log_enable_query_id = false;
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> {
                for (int i = 0; i < 20; i++) {
                    PlanningDiagnostics.operation(context, "load_schema", "catalog.db.table", -1, () -> {
                        advance(1000);
                        return null;
                    });
                }
                Assertions.assertTrue(events(appender).isEmpty());
            });
            Assertions.assertEquals(17, events(appender).size());
            Assertions.assertTrue(appender.contains(Level.WARN, "Planning operation [1-2]"));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"omitted_operations\":4"));
        }
    }

    @Test
    public void testCheckerDoesNotWaitForStatementMonitorAndShowsHeldLock() throws Exception {
        StatementContext statement = new StatementContext(context, new OriginStatement("select 1", 0));
        TableIf first = table(1, "catalog.db.first");
        TableIf second = table(2, "catalog.db.second");
        statement.getTables().put(ImmutableList.of("first"), first);
        statement.getTables().put(ImmutableList.of("second"), second);
        CountDownLatch waiting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(first.tryReadLock(1, TimeUnit.MINUTES)).thenReturn(true);
        Mockito.when(second.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
            waiting.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return true;
        });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            CompletableFuture<Void> planning = CompletableFuture.runAsync(() -> run(() -> {
                try {
                    PlanningDiagnostics.runPhase(context, Phase.LOCK_TABLES, statement::lock);
                    advance(2000);
                } finally {
                    statement.releasePlannerResources();
                }
            }), worker);
            try {
                Assertions.assertTrue(waiting.await(10, TimeUnit.SECONDS));
                advance(5000);
                CompletableFuture.runAsync(() -> context.checkTimeout(0)).get(1, TimeUnit.SECONDS);
                Assertions.assertTrue(appender.contains(Level.WARN, "\"target\":\"catalog.db.second\""));
                Assertions.assertTrue(appender.contains(Level.WARN, "\"held_locks\":1"));
                Assertions.assertTrue(appender.contains(Level.WARN, "\"operation_timeout_ms\":60000"));
                Assertions.assertEquals(1, events(appender).size());
            } finally {
                release.countDown();
            }
            planning.get(10, TimeUnit.SECONDS);
            Mockito.verify(first).readUnlock();
            Mockito.verify(second).readUnlock();
            Assertions.assertTrue(appender.contains(Level.WARN, "\"operation\":\"read_lock_hold\""));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"elapsed_ms\":7000"));
        } finally {
            worker.shutdownNow();
            worker.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testLockTimeoutReleasesPreviouslyAcquiredLock() {
        StatementContext statement = new StatementContext(context, new OriginStatement("select 1", 0));
        TableIf first = table(1, "catalog.db.first");
        TableIf second = table(2, "catalog.db.second");
        statement.getTables().put(ImmutableList.of("first"), first);
        statement.getTables().put(ImmutableList.of("second"), second);
        Mockito.when(first.tryReadLock(1, TimeUnit.MINUTES)).thenReturn(true);
        Mockito.when(second.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
            advance(60000);
            return false;
        });
        Assertions.assertThrows(RuntimeException.class,
                () -> run(() -> PlanningDiagnostics.runPhase(context, Phase.LOCK_TABLES, statement::lock)));
        Mockito.verify(first).readUnlock();
        Mockito.verify(second, Mockito.never()).readUnlock();
        Assertions.assertEquals("lock_tables/read_lock_wait", summary.getPlanningDetails()
                .getAsJsonObject("last_pass").get("failed_step").getAsString());
    }

    @Test
    public void testExternalSchemaPartitionsAndSnapshotOperations() {
        ExternalTable table = Mockito.mock(ExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn("catalog.db.external").when(table).getNameWithFullQualifiers();
        Mockito.doAnswer(invocation -> {
            advance(1000);
            return Optional.of(new SchemaCacheValue(Collections.emptyList()));
        }).when(table).getSchemaCacheValue();
        Mockito.doReturn(true).when(table).supportInternalPartitionPruned();
        Mockito.doReturn(ImmutableList.of(Mockito.mock(Column.class))).when(table).getPartitionColumns(Optional.empty());
        Mockito.doAnswer(invocation -> {
            advance(2000);
            return Collections.emptyMap();
        }).when(table).getNameToPartitionItems(Optional.empty());
        PluginDrivenMvccExternalTable snapshotTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.when(snapshotTable.getNameWithFullQualifiers()).thenReturn("catalog.db.snapshot");
        org.apache.doris.datasource.ExternalDatabase db = Mockito.mock(org.apache.doris.datasource.ExternalDatabase.class);
        org.apache.doris.datasource.ExternalCatalog catalog = Mockito.mock(org.apache.doris.datasource.ExternalCatalog.class);
        Mockito.when(snapshotTable.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(snapshotTable.getName()).thenReturn("snapshot");
        Mockito.when(snapshotTable.loadSnapshot(Optional.empty(), Optional.empty())).thenAnswer(invocation -> {
            advance(3000);
            return Mockito.mock(MvccSnapshot.class);
        });
        StatementContext statement = new StatementContext(context, new OriginStatement("select 1", 0));
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, () -> {
                table.getBaseSchema();
                table.initSelectedPartitions(Optional.empty());
                statement.loadSnapshots(snapshotTable, Optional.empty(), Optional.empty());
            }));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"operation\":\"load_schema\""));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"operation\":\"load_partitions\""));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"operation\":\"load_snapshot\""));
            Assertions.assertEquals(6000, phase("analyze").get("elapsed_ms").getAsLong());
        }
    }

    @Test
    public void testPlannerEntryCleansUpAfterFailureAndExplainEarlyReturn() {
        StatementContext statement = Mockito.spy(new StatementContext(context, new OriginStatement("select 1", 0)));
        LogicalPlan input = Mockito.mock(LogicalPlan.class);
        NereidsPlanner planner = new NereidsPlanner(statement) {
            @Override
            protected LogicalPlan preprocess(LogicalPlan plan) {
                throw new IllegalArgumentException("preprocess failure");
            }
        };
        Assertions.assertThrows(IllegalArgumentException.class, () -> planner.planWithLock(input,
                org.apache.doris.nereids.properties.PhysicalProperties.ANY, ExplainLevel.NONE, false));
        Assertions.assertEquals("failed", phase("preprocess").get("status").getAsString());
        Assertions.assertSame(input, planner.planWithLock(input,
                org.apache.doris.nereids.properties.PhysicalProperties.ANY, ExplainLevel.PARSED_PLAN, false));
        Mockito.verify(statement, Mockito.times(2)).releasePlannerResources();
        Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsInt());
        Assertions.assertEquals("not_run", phase("analyze").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    @Test
    public void testMetadataFailureReportsTargetAndRetainsOriginalException() {
        ExternalTable table = Mockito.mock(ExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn("catalog.db.failed").when(table).getNameWithFullQualifiers();
        IllegalStateException expected = new IllegalStateException("metadata failure");
        Mockito.doAnswer(invocation -> {
            advance(5000);
            context.checkTimeout(0);
            throw expected;
        }).when(table).getSchemaCacheValue();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertSame(expected, Assertions.assertThrows(IllegalStateException.class,
                    () -> run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, table::getFullSchema))));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"target\":\"catalog.db.failed\""));
            Assertions.assertTrue(appender.contains(Level.WARN, "\"operation_elapsed_ms\":5000"));
        }
        JsonObject audit = JsonParser.parseString(summary.getPlanTime()).getAsJsonObject();
        Assertions.assertEquals(1, audit.get("planning_failed_passes").getAsInt());
        Assertions.assertEquals(1, audit.get("planning_analyze_failures").getAsInt());
        Assertions.assertEquals(5000, audit.get("planning_analyze").getAsInt());
        Assertions.assertEquals(-1, audit.get("planning_optimize").getAsInt());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    @Test
    public void testSummaryRoundTripAndOldProfileCompatibility() {
        run(() -> PlanningDiagnostics.runPhase(context, Phase.CHOOSE_PLAN, () -> advance(4)));
        String json = org.apache.doris.persist.gson.GsonUtils.GSON.toJson(summary);
        SummaryProfile restored = org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(json, SummaryProfile.class);
        Assertions.assertEquals(summary.getPlanTime(), restored.getPlanTime());
        JsonObject oldProfile = JsonParser.parseString(json).getAsJsonObject();
        oldProfile.keySet().removeIf(key -> key.startsWith("planning") || key.equals("lastPlanningPass"));
        SummaryProfile old = org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(oldProfile, SummaryProfile.class);
        JsonObject audit = JsonParser.parseString(old.getPlanTime()).getAsJsonObject();
        Assertions.assertEquals(0, audit.get("planning_passes").getAsInt());
        Assertions.assertEquals(-1, audit.get("planning_choose_plan").getAsInt());
    }

    private TableIf table(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn(name);
        Mockito.when(table.needReadLockWhenPlan()).thenReturn(true);
        return table;
    }

    private void run(Runnable action) {
        PlanningDiagnostics.execute(new PlanningDiagnostics(context, clock::get), () -> {
            action.run();
            return null;
        });
    }

    private void advance(long millis) {
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private JsonObject phase(String key) {
        return summary.getPlanningDetails().getAsJsonObject("last_pass").getAsJsonObject("phases")
                .getAsJsonObject(key);
    }

    private List<LogEvent> events(TestLogAppender appender) {
        return Deencapsulation.getField(appender, "events");
    }
}
