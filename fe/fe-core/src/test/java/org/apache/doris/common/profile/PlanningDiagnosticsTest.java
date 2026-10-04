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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.PlanningDiagnostics.Phase;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class PlanningDiagnosticsTest {
    private static final String QUERY_ID_KEY = "query_id";
    private final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
    private ConnectContext context;
    private ConnectContext previousContext;
    private SummaryProfile summary;
    private long savedThreshold;
    private long savedOperationThreshold;
    private long savedInterval;
    private boolean savedUnitTest;
    private String savedQueryId;

    @BeforeEach
    public void setUp() {
        savedThreshold = Config.nereids_planning_log_threshold_ms;
        savedOperationThreshold = Config.nereids_planning_operation_log_threshold_ms;
        savedInterval = Config.nereids_planning_log_interval_ms;
        savedUnitTest = FeConstants.runningUnitTest;
        previousContext = ConnectContext.get();
        savedQueryId = ThreadContext.get(QUERY_ID_KEY);
        Config.nereids_planning_log_threshold_ms = 5000;
        Config.nereids_planning_operation_log_threshold_ms = 1000;
        Config.nereids_planning_log_interval_ms = 30000;
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
            ThreadContext.remove(QUERY_ID_KEY);
        } else {
            ThreadContext.put(QUERY_ID_KEY, savedQueryId);
        }
        Config.nereids_planning_log_threshold_ms = savedThreshold;
        Config.nereids_planning_operation_log_threshold_ms = savedOperationThreshold;
        Config.nereids_planning_log_interval_ms = savedInterval;
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
                    ThreadContext.put(QUERY_ID_KEY, "checker");
                    context.checkTimeout(0);
                    Assertions.assertEquals("checker", ThreadContext.get(QUERY_ID_KEY));
                    context.checkTimeout(0);
                    Assertions.assertEquals(1, events(appender).size());
                    advance(30000);
                    context.checkTimeout(0);
                    Assertions.assertEquals(2, events(appender).size());
                    assertDiagnosticIdentity(events(appender).get(0), "1-2");
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
    public void testMissingAndZeroQueryIdsClearEventIdentityAndRestoreCaller() {
        for (org.apache.doris.thrift.TUniqueId queryId : new org.apache.doris.thrift.TUniqueId[] {
                null, new org.apache.doris.thrift.TUniqueId(0, 0)}) {
            context.setQueryId(queryId);
            ThreadContext.put(QUERY_ID_KEY, "caller");
            IllegalStateException failure = new IllegalStateException("planning failure without query identity");
            try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
                Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class, () -> run(() -> {
                    Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
                    throw failure;
                })));
                Assertions.assertEquals(1, events(appender).size());
                assertDiagnosticIdentity(events(appender).get(0), "");
                Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
                Assertions.assertNull(context.getPlanningDiagnostics());
            }
        }
    }

    @Test
    public void testNestedPassWithOwnIdKeepsSnapshotForCheckerAndDeferredEvents() {
        ConnectContext child = new ConnectContext();
        child.setQueryId(new org.apache.doris.thrift.TUniqueId(3, 4));
        ThreadContext.put(QUERY_ID_KEY, "caller");
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
                PlanningDiagnostics parent = context.getPlanningDiagnostics();
                PlanningDiagnostics.execute(new PlanningDiagnostics(child, clock::get), () -> {
                    PlanningDiagnostics.runPhase(child, Phase.OPTIMIZE, () -> {
                        child.queryId().setLo(99);
                        advance(5000);
                        context.checkTimeout(0);
                        Assertions.assertEquals(1, events(appender).size());
                        assertDiagnosticIdentity(events(appender).get(0), "3-4");
                        Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
                    });
                    return null;
                });
                Assertions.assertSame(parent, context.getPlanningDiagnostics());
                Assertions.assertNull(child.getPlanningDiagnostics());
                Assertions.assertEquals(1, events(appender).size(), "The child completion remains deferred");
                advance(30000);
                context.checkTimeout(0);
                Assertions.assertEquals(2, events(appender).size());
                assertDiagnosticIdentity(events(appender).get(1), "1-2");
                Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
            }));
            Assertions.assertEquals(4, events(appender).size());
            assertDiagnosticIdentity(events(appender).get(2), "3-4");
            assertDiagnosticIdentity(events(appender).get(3), "1-2");
            Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsInt());
            Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
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
    public void testInternalConnectionPlanningDefersLogsAndKeepsQueryIdentity() {
        StatementContext outerStatement = new StatementContext(context, new OriginStatement("select 1", 0));
        TableIf table = table(1, "catalog.db.outer");
        outerStatement.getTables().put(ImmutableList.of("outer"), table);
        Mockito.when(table.tryReadLock(1, TimeUnit.MINUTES)).thenReturn(true);
        // MV cache construction installs a separate connection without an executor or query ID.
        ConnectContext internal = new ConnectContext();
        StatementContext internalStatement = new StatementContext(internal, new OriginStatement("select 1", 0));
        NereidsPlanner planner = new NereidsPlanner(internalStatement) {
            @Override
            protected LogicalPlan preprocess(LogicalPlan plan) {
                throw new IllegalArgumentException("internal planning failure");
            }
        };
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            PlanningDiagnostics.plan(context, () -> {
                outerStatement.lock();
                try {
                    internal.setThreadLocalInfo();
                    Assertions.assertThrows(IllegalArgumentException.class, () -> planner.planWithLock(
                            Mockito.mock(LogicalPlan.class), org.apache.doris.nereids.properties.PhysicalProperties.ANY,
                            ExplainLevel.NONE, false));
                    Assertions.assertTrue(events(appender).isEmpty(), "Outer table locks are still held");
                    Assertions.assertNull(internal.getPlanningDiagnostics());
                } finally {
                    context.setThreadLocalInfo();
                    outerStatement.releasePlannerResources();
                }
                return null;
            });
            Mockito.verify(table).readUnlock();
            Assertions.assertTrue(appender.contains(Level.WARN, "\"failed_step\":\"preprocess\""));
            Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsInt());
            Assertions.assertEquals(1, summary.getPlanningDetails().get("failed_passes").getAsInt());
            events(appender).forEach(event -> assertDiagnosticIdentity(event, "1-2"));
        }
        Assertions.assertNull(context.getPlanningDiagnostics());
        // Reusing the same planner thread must not retain the preceding root or its summary/ID.
        internal.setThreadLocalInfo();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> PlanningDiagnostics.plan(internal, () -> {
                throw new IllegalArgumentException("independent planning failure");
            }));
            Assertions.assertEquals(1, events(appender).size());
            assertDiagnosticIdentity(events(appender).get(0), "");
        }
        Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsInt());
    }

    @Test
    public void testRunningReportFollowsInternalPassAndSharesRootRateLimit() {
        ConnectContext internal = new ConnectContext();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
                PlanningDiagnostics.LockHold hold = context.getPlanningDiagnostics().acquiredLock("catalog.db.outer");
                try {
                    advance(5000);
                    for (int i = 0; i < 2; i++) {
                        PlanningDiagnostics.execute(new PlanningDiagnostics(internal, clock::get), () ->
                                PlanningDiagnostics.operation(internal, "load_schema", "catalog.db.inner", -1, () -> {
                                    advance(1);
                                    context.checkTimeout(0);
                                    return null;
                                }));
                    }
                    Assertions.assertEquals(1, events(appender).size(), "Nested passes share the root interval");
                    JsonObject event = JsonParser.parseString(events(appender).get(0).getMessage().getFormattedMessage()
                            .split(": ", 2)[1]).getAsJsonObject();
                    Assertions.assertEquals("catalog.db.inner", event.get("target").getAsString());
                    Assertions.assertEquals(1, event.get("held_locks").getAsInt());
                    Assertions.assertEquals("catalog.db.outer", event.get("oldest_lock").getAsString());
                    Assertions.assertEquals(5001, event.get("root_elapsed_ms").getAsLong());
                    Assertions.assertEquals(1, event.get("operation_elapsed_ms").getAsLong());
                    advance(30000);
                    context.checkTimeout(0);
                    Assertions.assertEquals(2, events(appender).size());
                } finally {
                    hold.close();
                }
            }));
        }
    }

    @Test
    public void testRunningReportDoesNotUseStartsPublishedAfterItsTimestamp() throws Exception {
        Thread plannerThread = Thread.currentThread();
        CountDownLatch clockRead = new CountDownLatch(1);
        CountDownLatch resumeChecker = new CountDownLatch(1);
        PlanningDiagnostics diagnostics = new PlanningDiagnostics(context, () -> {
            long now = clock.get();
            if (Thread.currentThread() != plannerThread) {
                clockRead.countDown();
                try {
                    Assertions.assertTrue(resumeChecker.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return now;
        });
        ExecutorService checker = Executors.newSingleThreadExecutor();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            PlanningDiagnostics.execute(diagnostics, () -> {
                advance(5000);
                PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
                    CompletableFuture<Void> report = CompletableFuture.runAsync(diagnostics::reportIfSlow, checker);
                    try {
                        Assertions.assertTrue(clockRead.await(10, TimeUnit.SECONDS));
                        advance(10);
                        PlanningDiagnostics.operation(context, "load_schema", "catalog.db.next", -1, () -> {
                            PlanningDiagnostics.LockHold hold = diagnostics.acquiredLock("catalog.db.next");
                            try {
                                resumeChecker.countDown();
                                report.get(10, TimeUnit.SECONDS);
                                JsonObject event = JsonParser.parseString(events(appender).get(0).getMessage()
                                        .getFormattedMessage().split(": ", 2)[1]).getAsJsonObject();
                                for (String key : new String[] {"phase_elapsed_ms", "operation_elapsed_ms",
                                        "oldest_lock_hold_ms"}) {
                                    Assertions.assertTrue(event.get(key).getAsLong() >= 0, key);
                                }
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                hold.close();
                            }
                            return null;
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    } finally {
                        resumeChecker.countDown();
                    }
                });
                return null;
            });
        } finally {
            resumeChecker.countDown();
            checker.shutdownNow();
            Assertions.assertTrue(checker.awaitTermination(10, TimeUnit.SECONDS));
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
    public void testOperationsAreBoundedDeferredAndCarryQueryIdentity() {
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
            events(appender).forEach(event -> assertDiagnosticIdentity(event, "1-2"));
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
    public void testOperationWithoutDiagnosticsDoesNotResolveTarget() {
        Supplier<String> target = () -> {
            throw new AssertionError("Target must not be resolved without planning diagnostics");
        };
        Object expected = new Object();
        IllegalStateException failure = new IllegalStateException("original operation failure");
        for (ConnectContext testContext : new ConnectContext[] {null, context}) {
            AtomicInteger calls = new AtomicInteger();
            Assertions.assertNull(PlanningDiagnostics.current(testContext));
            Assertions.assertSame(expected,
                    PlanningDiagnostics.operation(testContext, "load_schema", target, -1, () -> {
                        calls.incrementAndGet();
                        return expected;
                    }));
            Assertions.assertEquals(1, calls.get());
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> PlanningDiagnostics.operation(testContext, "load_schema", target, -1, () -> {
                        calls.incrementAndGet();
                        throw failure;
                    })));
            Assertions.assertEquals(2, calls.get());
        }
    }

    @Test
    public void testOperationTargetIsCapturedOnceOnPlanningThread() throws Exception {
        Thread planningThread = Thread.currentThread();
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<String> target = new AtomicReference<>("catalog.db.original");
        ExecutorService checker = Executors.newSingleThreadExecutor();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE,
                    () -> PlanningDiagnostics.operation(context, "load_schema", () -> {
                        Assertions.assertSame(planningThread, Thread.currentThread());
                        calls.incrementAndGet();
                        return target.get();
                    }, -1, () -> {
                        target.set("catalog.db.changed");
                        advance(5000);
                        try {
                            String checkerIdentity = checker.submit(() -> {
                                ThreadContext.put(QUERY_ID_KEY, "checker");
                                context.checkTimeout(0);
                                return ThreadContext.get(QUERY_ID_KEY);
                            }).get(10, TimeUnit.SECONDS);
                            Assertions.assertEquals("checker", checkerIdentity);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        return null;
                    })));
            Assertions.assertEquals(1, calls.get());
            Assertions.assertTrue(appender.contains(Level.WARN, "\"status\":\"running\""));
            Assertions.assertTrue(appender.contains(Level.WARN, "Planning operation"));
            Assertions.assertEquals(2, events(appender).stream()
                    .filter(event -> event.getMessage().getFormattedMessage()
                            .contains("\"target\":\"catalog.db.original\""))
                    .count());
            Assertions.assertFalse(appender.contains(Level.WARN, "catalog.db.changed"));
            events(appender).forEach(event -> assertDiagnosticIdentity(event, "1-2"));
        } finally {
            checker.shutdownNow();
            Assertions.assertTrue(checker.awaitTermination(10, TimeUnit.SECONDS));
        }
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
        Mockito.doReturn(ImmutableList.of(Mockito.mock(Column.class))).when(table)
                .getPartitionColumns(Optional.empty());
        Mockito.doAnswer(invocation -> {
            advance(2000);
            return Collections.emptyMap();
        }).when(table).getNameToPartitionItems(Optional.empty());
        PluginDrivenMvccExternalTable snapshotTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.when(snapshotTable.getNameWithFullQualifiers()).thenReturn("catalog.db.snapshot");
        ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
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
    public void testMetadataWithoutDiagnosticsDoesNotResolveTarget() {
        ExternalTable table = Mockito.mock(ExternalTable.class, Mockito.CALLS_REAL_METHODS);
        // Schema reads need no catalog; resolving the diagnostic name would dereference this unset catalog.
        ExternalDatabase dbWithoutCatalog = Mockito.mock(ExternalDatabase.class);
        table.setDb(dbWithoutCatalog);
        List<Column> latestSchema = ImmutableList.of(Mockito.mock(Column.class));
        List<Column> pinnedSchema = ImmutableList.of(Mockito.mock(Column.class));
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Mockito.doReturn(Optional.of(new SchemaCacheValue(latestSchema))).when(table).getSchemaCacheValue();
        Mockito.doReturn(Optional.of(new SchemaCacheValue(pinnedSchema)))
                .when(table).getSchemaCacheValue(Optional.of(snapshot));

        PluginDrivenMvccExternalTable snapshotTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.doThrow(new AssertionError("Snapshot target must not be resolved without diagnostics"))
                .when(snapshotTable).getNameWithFullQualifiers();
        ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(snapshotTable.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(snapshotTable.getName()).thenReturn("snapshot");
        Mockito.when(snapshotTable.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(snapshot);

        for (ConnectContext testContext : new ConnectContext[] {null, context}) {
            ConnectContext.remove();
            if (testContext != null) {
                testContext.setThreadLocalInfo();
            }
            Assertions.assertSame(latestSchema, table.getFullSchema());
            Assertions.assertSame(pinnedSchema, table.getFullSchema(Optional.of(snapshot)));
            Assertions.assertSame(SelectedPartitions.NOT_PRUNED, table.initSelectedPartitions(Optional.empty()));
            StatementContext statement = new StatementContext(testContext, new OriginStatement("select 1", 0));
            statement.loadSnapshots(snapshotTable, Optional.empty(), Optional.empty());
            Assertions.assertSame(snapshot, statement.getSnapshot(snapshotTable).get());
        }
        Mockito.verify(table, Mockito.never()).getNameWithFullQualifiers();
        Mockito.verify(dbWithoutCatalog, Mockito.never()).getCatalog();
        Mockito.verify(snapshotTable, Mockito.never()).getNameWithFullQualifiers();
        Mockito.verify(snapshotTable, Mockito.times(2)).loadSnapshot(Optional.empty(), Optional.empty());
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

    @Test
    public void testCheckerWaitsForThresholdAndReportsBetweenPhases() {
        PlanningDiagnostics diagnostics = new PlanningDiagnostics(context, clock::get);
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            diagnostics.reportIfSlow();
            Assertions.assertTrue(events(appender).isEmpty(), "A pass has not started yet");
            PlanningDiagnostics.execute(diagnostics, () -> {
                advance(4999);
                diagnostics.reportIfSlow();
                Assertions.assertTrue(events(appender).isEmpty(), "The slow threshold has not elapsed");
                advance(1);
                diagnostics.reportIfSlow();
                Assertions.assertEquals(1, events(appender).size());
                JsonObject event = eventJson(events(appender).get(0));
                Assertions.assertEquals("running", event.get("status").getAsString());
                Assertions.assertEquals(5000, event.get("elapsed_ms").getAsLong());
                Assertions.assertEquals(0, event.get("phase_elapsed_ms").getAsLong());
                Assertions.assertEquals(0, event.get("operation_elapsed_ms").getAsLong());
                Assertions.assertFalse(event.has("phase"));
                return null;
            });
            Assertions.assertEquals(2, events(appender).size());
            diagnostics.reportIfSlow();
            Assertions.assertEquals(2, events(appender).size(), "A completed pass must no longer report");
            Assertions.assertEquals("completed", summary.getPlanningDetails().getAsJsonObject("last_pass")
                    .get("status").getAsString());
            Assertions.assertEquals("not_run", phase("analyze").get("status").getAsString());
        }
    }

    @Test
    public void testNestedOperationOverflowRemainsBoundedAtRoot() {
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> {
                run(() -> {
                    for (int i = 0; i < 20; i++) {
                        PlanningDiagnostics.operation(context, "load_schema", "catalog.db.inner", -1, () -> {
                            advance(1000);
                            return null;
                        });
                    }
                });
                Assertions.assertTrue(events(appender).isEmpty(), "Nested events must wait for root cleanup");
            });
            Assertions.assertEquals(17, events(appender).size());
            Assertions.assertEquals(16, events(appender).stream()
                    .filter(event -> event.getMessage().getFormattedMessage().startsWith("Planning operation"))
                    .count());
            JsonObject finished = eventJson(events(appender).get(16));
            Assertions.assertEquals(5, finished.get("omitted_operations").getAsLong(),
                    "Four child operations and its completion event did not fit");
            Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsLong(),
                    "Bounded logs must not discard summary passes");
            Assertions.assertEquals(40000, summary.getPlanningDetails().get("elapsed_ms").getAsLong());
        }
    }

    @Test
    public void testNestedLockReportIncludesOldestAncestorWithUnlockedMiddlePass() {
        ConnectContext middle = new ConnectContext();
        ConnectContext inner = new ConnectContext();
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            run(() -> PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> {
                try (PlanningDiagnostics.LockHold outerLock = context.getPlanningDiagnostics()
                        .acquiredLock("catalog.db.outer")) {
                    advance(100);
                    PlanningDiagnostics.execute(new PlanningDiagnostics(middle, clock::get), () ->
                            PlanningDiagnostics.execute(new PlanningDiagnostics(inner, clock::get), () -> {
                                try (PlanningDiagnostics.LockHold innerLock = inner.getPlanningDiagnostics()
                                        .acquiredLock("catalog.db.inner")) {
                                    advance(5000);
                                    context.checkTimeout(0);
                                    JsonObject event = eventJson(events(appender).get(0));
                                    Assertions.assertEquals(2, event.get("held_locks").getAsInt());
                                    Assertions.assertEquals("catalog.db.outer", event.get("oldest_lock").getAsString());
                                    Assertions.assertEquals(5100, event.get("oldest_lock_hold_ms").getAsLong());
                                }
                                return null;
                            }));
                    Assertions.assertNull(middle.getPlanningDiagnostics());
                    Assertions.assertNull(inner.getPlanningDiagnostics());
                    advance(30000);
                    context.checkTimeout(0);
                    Assertions.assertEquals(2, events(appender).size(), "Completed children remain deferred");
                    JsonObject event = eventJson(events(appender).get(1));
                    Assertions.assertEquals(1, event.get("held_locks").getAsInt());
                    Assertions.assertEquals("catalog.db.outer", event.get("oldest_lock").getAsString());
                    Assertions.assertEquals(35100, event.get("oldest_lock_hold_ms").getAsLong());
                }
            }));
        }
    }

    @Test
    public void testDisabledOperationThresholdStillReportsOriginalFailure() {
        Config.nereids_planning_log_threshold_ms = 0;
        Config.nereids_planning_operation_log_threshold_ms = 0;
        IllegalStateException failure = new IllegalStateException("metadata failure with logging thresholds disabled");
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, () -> {
                        PlanningDiagnostics.operation(context, "load_schema", "catalog.db.success", -1, () -> {
                            advance(2000);
                            return null;
                        });
                        PlanningDiagnostics.operation(context, "load_schema", "catalog.db.failed", -1, () -> {
                            advance(3);
                            throw failure;
                        });
                    }))));
            Assertions.assertEquals(2, events(appender).size(), "Only the failure and pass summary are emitted");
            Assertions.assertFalse(appender.contains(Level.WARN, "catalog.db.success"));
            JsonObject operation = eventJson(events(appender).get(0));
            Assertions.assertEquals("catalog.db.failed", operation.get("target").getAsString());
            Assertions.assertEquals("failed", operation.get("status").getAsString());
            Assertions.assertEquals(3, operation.get("elapsed_ms").getAsLong());
            Assertions.assertEquals("analyze/load_schema", summary.getPlanningDetails().getAsJsonObject("last_pass")
                    .get("failed_step").getAsString());
            Assertions.assertEquals(2003, phase("analyze").get("elapsed_ms").getAsLong());
        }
    }

    @Test
    public void testPlannerCleanupFailureRestoresContextForNextPass() {
        StatementContext statement = Mockito.spy(new StatementContext(context, new OriginStatement("select 1", 0)));
        NereidsPlanner planner = new NereidsPlanner(statement);
        LogicalPlan input = Mockito.mock(LogicalPlan.class);
        IllegalStateException failure = new IllegalStateException("planner resource close failed");
        Mockito.doThrow(failure).when(statement).releasePlannerResources();
        ThreadContext.put(QUERY_ID_KEY, "caller");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> planner.planWithLock(input, org.apache.doris.nereids.properties.PhysicalProperties.ANY,
                        ExplainLevel.PARSED_PLAN, false)));
        Assertions.assertEquals("release_resources", summary.getPlanningDetails().getAsJsonObject("last_pass")
                .get("failed_step").getAsString());
        Assertions.assertEquals("failed", phase("release_resources").get("status").getAsString());
        Assertions.assertEquals("not_run", phase("preprocess").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
        Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));

        Mockito.doNothing().when(statement).releasePlannerResources();
        Assertions.assertSame(input, planner.planWithLock(input,
                org.apache.doris.nereids.properties.PhysicalProperties.ANY, ExplainLevel.PARSED_PLAN, false));
        Assertions.assertEquals(2, summary.getPlanningDetails().get("passes").getAsLong());
        Assertions.assertEquals(1, summary.getPlanningDetails().get("failed_passes").getAsLong());
        Assertions.assertEquals("completed", phase("release_resources").get("status").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
        Assertions.assertEquals("caller", ThreadContext.get(QUERY_ID_KEY));
        Mockito.verify(statement, Mockito.times(2)).releasePlannerResources();
    }

    @Test
    public void testSummarySnapshotsStayIndependentAcrossSuccessfulAndFailedPasses() {
        run(() -> PlanningDiagnostics.runPhase(context, Phase.REWRITE, () -> advance(7)));
        JsonObject first = summary.getPlanningDetails();
        IllegalStateException failure = new IllegalStateException("analysis failed on next pass");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> run(() -> PlanningDiagnostics.runPhase(context, Phase.ANALYZE, () -> {
                    advance(3);
                    throw failure;
                }))));
        Assertions.assertEquals(1, first.get("passes").getAsLong());
        Assertions.assertEquals("completed", first.getAsJsonObject("last_pass").get("status").getAsString());
        Assertions.assertFalse(first.getAsJsonObject("phase_time_ms").has("analyze"));

        JsonObject current = summary.getPlanningDetails();
        current.getAsJsonObject("phase_time_ms").addProperty("rewrite", 999);
        current.getAsJsonObject("last_pass").addProperty("status", "completed");
        JsonObject audit = JsonParser.parseString(summary.getPlanTime()).getAsJsonObject();
        Assertions.assertEquals(2, audit.get("planning_passes").getAsInt());
        Assertions.assertEquals(1, audit.get("planning_failed_passes").getAsInt());
        Assertions.assertEquals(1, audit.get("planning_last_failed").getAsInt());
        Assertions.assertEquals(7, audit.get("planning_rewrite").getAsInt());
        Assertions.assertEquals(3, audit.get("planning_analyze").getAsInt());
        Assertions.assertEquals(1, audit.get("planning_analyze_failures").getAsInt());
    }

    @Test
    public void testAuditPlanningTimesSaturateWithoutLosingProfilePrecision() {
        Config.nereids_planning_log_threshold_ms = 0;
        long elapsed = (long) Integer.MAX_VALUE + 100;
        run(() -> PlanningDiagnostics.runPhase(context, Phase.OPTIMIZE, () -> advance(elapsed)));
        JsonObject audit = JsonParser.parseString(summary.getPlanTime()).getAsJsonObject();
        Assertions.assertEquals(Integer.MAX_VALUE, audit.get("planning_optimize").getAsInt());
        Assertions.assertEquals(-1, audit.get("planning_analyze").getAsInt());
        Assertions.assertEquals(elapsed, summary.getPlanningDetails().getAsJsonObject("phase_time_ms")
                .get("optimize").getAsLong());
        Assertions.assertEquals(elapsed, phase("optimize").get("elapsed_ms").getAsLong());
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

    private JsonObject eventJson(LogEvent event) {
        return JsonParser.parseString(event.getMessage().getFormattedMessage().split(": ", 2)[1]).getAsJsonObject();
    }

    private void assertDiagnosticIdentity(LogEvent event, String expected) {
        Assertions.assertEquals(expected, eventJson(event).get("query_id").getAsString());
        Assertions.assertEquals(expected, event.getContextData().getValue(QUERY_ID_KEY));
    }
}
