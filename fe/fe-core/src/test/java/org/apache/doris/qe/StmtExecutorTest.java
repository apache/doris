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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.qe.protocol.ResultSender;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StmtExecutorTest extends TestWithFeService {
    private static final String AI_RESOURCE_LOG_SECRET = "sk-test-secret";
    private static final String MASKED_STMT_FALLBACK = "/* masked statement unavailable */";

    @Override
    protected void runBeforeAll() throws Exception {
        Config.allow_replica_on_same_host = true;
        FeConstants.runningUnitTest = true;
        InternalSchemaInitializer.createDb();
        InternalSchemaInitializer.createTbl();
        createDatabase("testDb");
    }

    @Test
    public void testCommittedTsoErrorSurvivesPlannerWrapping() throws Exception {
        for (ErrorCode code : new ErrorCode[] {ErrorCode.ERR_INCR_WINDOW_NOT_READY,
                ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT}) {
            connectContext.getState().reset();
            IncrWindowNotReadyException rejected = new IncrWindowNotReadyException(code, "test reason",
                    2000, 3000, 1000, 1000, 5000);
            StmtExecutor executor = new StmtExecutor(connectContext, "select 1");
            try (MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class,
                    (planner, construction) -> Mockito.doThrow(new NereidsException(rejected.getMessage(), rejected))
                            .when(planner).plan(Mockito.any(StatementBase.class), Mockito.any(TQueryOptions.class)))) {
                executor.execute();
                Assertions.assertEquals(1, planners.constructed().size());
            }
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
            Assertions.assertEquals(code, connectContext.getState().getErrorCode());
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("requestedEndTimestampMs=2000"));
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("retryAfterMs=1000"));
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("timeoutMs=5000"));
        }
    }

    @Test
    public void testShow() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testShowNull() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"CANCELLED", "TIMEOUT"})
    public void testTerminateBeforeCoordinatorIsPublished(TStatusCode statusCode) {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        Status cancelReason = new Status(statusCode, "terminate before coordinator publication");
        Coordinator coordinator = Mockito.mock(Coordinator.class);

        stmtExecutor.cancel(cancelReason, false);
        stmtExecutor.setCoord(coordinator);

        Mockito.verify(coordinator).cancel(cancelReason);
    }

    @Test
    public void testCancelAfterCoordinatorIsPublished() {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        Status cancelReason = new Status(TStatusCode.CANCELLED, "cancel after coordinator publication");
        Coordinator coordinator = Mockito.mock(Coordinator.class);

        stmtExecutor.setCoord(coordinator);
        stmtExecutor.cancel(cancelReason, false);

        Mockito.verify(coordinator).cancel(cancelReason);
    }

    @Test
    public void testFirstTerminalReasonWinsAcrossCoordinatorPublication() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        Status timeout = new Status(TStatusCode.TIMEOUT, "first timeout");
        Status cancelled = new Status(TStatusCode.CANCELLED, "later cancellation");
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        CountDownLatch publicationReachedCoordinator = new CountDownLatch(1);
        CountDownLatch finishPublication = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        List<Status> delivered = new CopyOnWriteArrayList<>();
        Mockito.doAnswer(invocation -> {
            delivered.add(invocation.getArgument(0));
            if (calls.getAndIncrement() == 0) {
                publicationReachedCoordinator.countDown();
                Assertions.assertTrue(finishPublication.await(10, TimeUnit.SECONDS));
            }
            return null;
        }).when(coordinator).cancel(Mockito.any(Status.class));

        stmtExecutor.cancel(timeout, false);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<?> publication = executorService.submit(() -> stmtExecutor.setCoord(coordinator));
            Assertions.assertTrue(publicationReachedCoordinator.await(10, TimeUnit.SECONDS));
            stmtExecutor.cancel(cancelled, false);
            finishPublication.countDown();
            publication.get(10, TimeUnit.SECONDS);
        } finally {
            finishPublication.countDown();
            executorService.shutdownNow();
        }

        Assertions.assertEquals(2, delivered.size());
        Assertions.assertTrue(delivered.stream().allMatch(status -> status.getErrorCode() == TStatusCode.TIMEOUT));
        Assertions.assertTrue(delivered.stream().allMatch(status -> "first timeout".equals(status.getErrorMsg())));
    }

    // The deferral gate (#67503): a coordinator is kept alive past GetFlightInfo only when the BE
    // still depends on it (Coordinator.mustOutliveDispatch), and the execution timeout it
    // ran with is frozen at that moment. SET_VAR hint values are reverted when execute() ends, so
    // the idle reaper must not read the session value later.
    @Test
    public void testDeferForArrowFlightFreezesExecTimeoutInEffect() throws Exception {
        // Only an Arrow Flight SQL session defers a query's coordinator.
        ConnectContext flightContext = ConnectContext.forFlight("test-peer-identity");
        flightContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flightContext.setEnv(connectContext.getEnv());
        int savedIdleTimeout = Config.arrow_flight_deferred_query_idle_timeout_second;
        flightContext.setQueryId(new TUniqueId(0x67503L, 0x1L));
        try {
            Config.arrow_flight_deferred_query_idle_timeout_second = 1;
            flightContext.getSessionVariable().setQueryTimeoutS(1234);
            StmtExecutor stmtExecutor = new StmtExecutor(flightContext,
                    analyzeAndGetStmtByNereids("select 1", flightContext));
            Assertions.assertFalse(stmtExecutor.isDeferredForArrowFlight());
            Assertions.assertEquals(-1, stmtExecutor.getDeferredExecTimeoutS());

            stmtExecutor.deferForArrowFlight();

            Assertions.assertTrue(stmtExecutor.isDeferredForArrowFlight());
            Assertions.assertEquals(1234, stmtExecutor.getDeferredExecTimeoutS());
            // the reaper's bound is floored at the frozen value ...
            Assertions.assertEquals(1234_000L, FlightProtocolAdapter.deferredBoundMs(stmtExecutor,
                    Config.arrow_flight_deferred_query_idle_timeout_second));
            // ... even after the session value moved on, as it does when a SET_VAR hint is reverted
            flightContext.getSessionVariable().setQueryTimeoutS(5);
            Assertions.assertEquals(1234, stmtExecutor.getDeferredExecTimeoutS());
            Assertions.assertEquals(1234_000L, FlightProtocolAdapter.deferredBoundMs(stmtExecutor,
                    Config.arrow_flight_deferred_query_idle_timeout_second));
        } finally {
            flightContext.closeFlightSqlDeferredExecutors();
            Config.arrow_flight_deferred_query_idle_timeout_second = savedIdleTimeout;
        }
    }

    // A deferred query is finalized after the session may have run another statement -- a SET of the
    // SetSessionOptions action -- which gives the context a new query id and start time. The query is
    // unregistered under its own id (its registration, and the user's instance count, would leak
    // otherwise) and the reaper counts from its own start.
    @Test
    public void testDeferForArrowFlightFreezesTheQueryIdAndStartTime() throws Exception {
        ConnectContext flightContext = ConnectContext.forFlight("test-peer-identity");
        flightContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flightContext.setEnv(connectContext.getEnv());
        TUniqueId deferredId = new TUniqueId(0x67966L, 0x1L);
        TUniqueId laterId = new TUniqueId(0x67966L, 0x2L);
        flightContext.setQueryId(deferredId);
        flightContext.setStartTime();
        long deferredStart = flightContext.getStartTime();
        Coordinator coord = Mockito.mock(Coordinator.class);
        Mockito.when(coord.getQueryOptions()).thenReturn(new TQueryOptions());
        QeProcessorImpl.INSTANCE.registerQuery(deferredId, new QeProcessorImpl.QueryInfo(flightContext, "select 1", coord));
        try {
            StmtExecutor stmtExecutor = new StmtExecutor(flightContext,
                    analyzeAndGetStmtByNereids("select 1", flightContext));
            Assertions.assertNull(stmtExecutor.getDeferredQueryId());
            Assertions.assertEquals(-1L, stmtExecutor.getDeferredStartTimeMs());

            stmtExecutor.deferForArrowFlight();
            Assertions.assertEquals(deferredId, stmtExecutor.getDeferredQueryId());
            Assertions.assertEquals(deferredStart, stmtExecutor.getDeferredStartTimeMs());
            Assertions.assertEquals(Lists.newArrayList(stmtExecutor), flightContext.getFlightSqlDeferredExecutors());

            // Another statement of the session, before the deferred query is finalized.
            flightContext.setQueryId(laterId);
            flightContext.setStartTime();
            Assertions.assertEquals(deferredId, stmtExecutor.getDeferredQueryId());
            Assertions.assertEquals(deferredStart, stmtExecutor.getDeferredStartTimeMs());

            Assertions.assertSame(coord, QeProcessorImpl.INSTANCE.getCoordinator(deferredId));
            flightContext.closeFlightSqlDeferredExecutors();
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(deferredId));
            Assertions.assertTrue(flightContext.getFlightSqlDeferredExecutors().isEmpty());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(deferredId);
            flightContext.closeFlightSqlDeferredExecutors();
        }
    }

    // A deferred query is a closed object from the moment it is deferred (see FlightProtocolAdapter):
    // it is finalized after the session has moved on, and from whatever thread. Its profile was
    // published as RUNNING when it was deferred; the final update follows that decision, whatever
    // enable_profile says by then (a SET_VAR hint reverted, a SET of the SetSessionOptions action),
    // and adds only what ends the query: what the session's later statements did to its query id,
    // start time, database, state and variables does not reach the record.
    @Test
    public void testADeferredQueryIsFinalizedFromItsOwnRecordNotTheSessions() throws Exception {
        ConnectContext flightContext = ConnectContext.forFlight("test-peer-identity");
        flightContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flightContext.setEnv(connectContext.getEnv());
        flightContext.setDatabase("testDb");
        flightContext.getSessionVariable().enableProfile = true;
        TUniqueId deferredId = new TUniqueId(0x67966L, 0x3L);
        flightContext.setQueryId(deferredId);
        flightContext.setStartTime();
        Coordinator coord = Mockito.mock(Coordinator.class);
        Mockito.when(coord.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coord.getExecStatus()).thenReturn(Status.OK);
        Mockito.when(coord.getBeToInstancesNum()).thenReturn(Maps.newTreeMap());
        QeProcessorImpl.INSTANCE.registerQuery(deferredId,
                new QeProcessorImpl.QueryInfo(flightContext, "select 1", coord));
        flightContext.setThreadLocalInfo();
        try {
            StmtExecutor stmtExecutor = new StmtExecutor(flightContext,
                    analyzeAndGetStmtByNereids("select 1", flightContext));
            stmtExecutor.setCoord(coord);
            // What executeAndSendResult does: the RUNNING summary, then the deferral.
            stmtExecutor.updateProfile(false);
            stmtExecutor.deferForArrowFlight();
            RuntimeProfile summary = stmtExecutor.getProfile().getSummaryProfile().getSummary();
            Assertions.assertEquals(DebugUtil.printId(deferredId), summary.getInfoString(SummaryProfile.PROFILE_ID));
            Assertions.assertEquals("testDb", summary.getInfoString(SummaryProfile.DEFAULT_DB));
            Assertions.assertEquals("RUNNING", summary.getInfoString(SummaryProfile.TASK_STATE));
            Assertions.assertEquals("N/A", summary.getInfoString(SummaryProfile.END_TIME));

            // The session moves on: a SET enable_profile = false and a USE of the SetSessionOptions
            // action, each a statement with a query id and start time of its own.
            flightContext.getSessionVariable().enableProfile = false;
            flightContext.setQueryId(new TUniqueId(0x67966L, 0x4L));
            flightContext.setStartTime();
            flightContext.clearDatabase();
            flightContext.getState().reset();

            // Finalized from a thread that runs no command of the session (the timeout checker's,
            // say): the session's context is not the thread's.
            connectContext.setThreadLocalInfo();
            flightContext.closeFlightSqlDeferredExecutors();
            Mockito.verify(coord).close();
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(deferredId));
            // Finished under the decision it was published under ...
            Assertions.assertNotEquals("N/A", summary.getInfoString(SummaryProfile.END_TIME));
            Assertions.assertEquals("OK", summary.getInfoString(SummaryProfile.TASK_STATE));
            // ... and from its own record: what was recorded when it ran stays.
            Assertions.assertEquals(DebugUtil.printId(deferredId), summary.getInfoString(SummaryProfile.PROFILE_ID));
            Assertions.assertEquals("testDb", summary.getInfoString(SummaryProfile.DEFAULT_DB));
        } finally {
            connectContext.setThreadLocalInfo();
            QeProcessorImpl.INSTANCE.unregisterQuery(deferredId);
            flightContext.closeFlightSqlDeferredExecutors();
        }
    }

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259);
    // it is released later by finalizeArrowFlightQuery(), which closes the coordinator and then
    // unregisters the query. The close and the unregister must be independent: if coord.close()
    // throws, the query registration must still be released (the try/finally), otherwise the query
    // leaks in QeProcessorImpl forever. The thrown error is expected to propagate to the caller
    // (ConnectContext.closeFlightSqlDeferredExecutors), which catches and logs it.
    @Test
    public void testFinalizeArrowFlightQueryUnregistersQueryEvenIfCoordCloseThrows() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        TUniqueId queryId = new TUniqueId(0x6226259L, 0x62259L);
        connectContext.setQueryId(queryId);

        Coordinator coord = Mockito.mock(Coordinator.class);
        Mockito.when(coord.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.doThrow(new RuntimeException("coord close failed")).when(coord).close();
        stmtExecutor.setCoord(coord);

        // Simulate the in-flight query whose results DoGet is still pulling.
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(coord));
        Assertions.assertNotNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId));

        try {
            stmtExecutor.finalizeArrowFlightQuery();
            Assertions.fail("expected coord.close() failure to propagate after the query is unregistered");
        } catch (RuntimeException e) {
            Assertions.assertEquals("coord close failed", e.getMessage());
        }

        // The coordinator close was attempted (releases SplitSource + query queue slot) ...
        Mockito.verify(coord).close();
        // ... and despite it failing, the query registration was still released (no leak).
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId));
    }

    @Test
    @ResourceLock("global")
    public void testFailedDispatchRetryFinalizesProfileAfterLastAttempt() throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        connectContext.getSessionVariable().enableProfile = true;
        connectContext.getSessionVariable().autoProfileThresholdMs = 10_000;
        TUniqueId firstQueryId = new TUniqueId(0x22040L, 1L);
        TUniqueId retryQueryId = new TUniqueId(0x22040L, 2L);
        connectContext.setQueryId(firstQueryId);
        connectContext.setStartTime();
        StmtExecutor executor = new StmtExecutor(connectContext,
                analyzeAndGetStmtByNereids("select 1", connectContext));
        executor.getSummaryProfile().setQueryBeginTime(System.currentTimeMillis());
        ExecutionProfile firstExecutionProfile = new ExecutionProfile(firstQueryId, Collections.emptyList());
        ExecutionProfile retryExecutionProfile = new ExecutionProfile(retryQueryId, Collections.emptyList());
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.enable_profile = true;

        try {
            Coordinator firstCoord = Mockito.mock(Coordinator.class);
            Mockito.when(firstCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(firstCoord.getExecutionProfile()).thenReturn(firstExecutionProfile);
            executor.getProfile().addExecutionProfile(firstExecutionProfile);
            QeProcessorImpl.INSTANCE.registerQuery(firstQueryId, new QeProcessorImpl.QueryInfo(firstCoord));

            // Dispatch failed before publishing history.
            executor.finalizeQuery(true);
            Assertions.assertEquals(Long.MAX_VALUE, executor.getProfile().getQueryFinishTimestamp());
            Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
            Assertions.assertTrue(executor.getProfile().getExecutionProfiles().isEmpty());

            connectContext.setQueryId(retryQueryId);
            Coordinator retryCoord = Mockito.mock(Coordinator.class);
            Mockito.when(retryCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(retryCoord.getExecutionProfile()).thenReturn(retryExecutionProfile);
            executor.getProfile().addExecutionProfile(retryExecutionProfile);
            QeProcessorImpl.INSTANCE.registerQuery(retryQueryId, new QeProcessorImpl.QueryInfo(retryCoord));

            // Publish the successful retry.
            executor.updateProfile(false);
            Assertions.assertNotNull(manager.findProfileElementObject(executor.getProfile().getId()));
            executor.finalizeQuery();

            Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
            Assertions.assertNull(manager.getExecutionProfile(retryQueryId));
            Assertions.assertNull(manager.findProfileElementObject(executor.getProfile().getId()));
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(firstQueryId);
            QeProcessorImpl.INSTANCE.unregisterQuery(retryQueryId);
            manager.cleanProfile();
        }
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"OK", "INTERNAL_ERROR"})
    @ResourceLock("global")
    public void testRetryRetainsFinalProfileAboveStatementThreshold(TStatusCode finalStatus) throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        connectContext.getSessionVariable().enableProfile = true;
        connectContext.getSessionVariable().autoProfileThresholdMs = 5_000;
        TUniqueId firstQueryId = new TUniqueId(0x22040L, 3L);
        TUniqueId retryQueryId = new TUniqueId(0x22040L, 4L);
        connectContext.setQueryId(firstQueryId);
        connectContext.setStartTime();
        StmtExecutor executor = new StmtExecutor(connectContext,
                analyzeAndGetStmtByNereids("select 1", connectContext));
        long queryBeginTime = System.currentTimeMillis() - 6_100;
        executor.getSummaryProfile().setQueryBeginTime(queryBeginTime);
        ExecutionProfile firstExecutionProfile = new ExecutionProfile(firstQueryId, Collections.emptyList());
        ExecutionProfile retryExecutionProfile = new ExecutionProfile(retryQueryId, Collections.emptyList());
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.enable_profile = true;

        try {
            Coordinator firstCoord = Mockito.mock(Coordinator.class);
            Mockito.when(firstCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(firstCoord.getExecutionProfile()).thenReturn(firstExecutionProfile);
            executor.getProfile().addExecutionProfile(firstExecutionProfile);
            QeProcessorImpl.INSTANCE.registerQuery(firstQueryId, new QeProcessorImpl.QueryInfo(firstCoord));

            // Fetch failed after publishing history.
            executor.updateProfile(false);
            Assertions.assertNotNull(manager.findProfileElementObject(executor.getProfile().getId()));
            executor.finalizeQuery(true);
            Assertions.assertNull(manager.findProfileElementObject(executor.getProfile().getId()));
            Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
            Assertions.assertTrue(executor.getProfile().getExecutionProfiles().isEmpty());
            Assertions.assertEquals(Long.MAX_VALUE, executor.getProfile().getQueryFinishTimestamp());
            Assertions.assertEquals(queryBeginTime, executor.getSummaryProfile().getQueryBeginTime());

            connectContext.setQueryId(retryQueryId);
            Coordinator retryCoord = Mockito.mock(Coordinator.class);
            Mockito.when(retryCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(retryCoord.getExecutionProfile()).thenReturn(retryExecutionProfile);
            Mockito.when(retryCoord.getExecStatus()).thenReturn(new Status(finalStatus, "final attempt"));
            executor.setCoord(retryCoord);
            executor.getProfile().addExecutionProfile(retryExecutionProfile);
            QeProcessorImpl.INSTANCE.registerQuery(retryQueryId, new QeProcessorImpl.QueryInfo(retryCoord));
            executor.updateProfile(false);
            Assertions.assertNull(manager.findProfileElementObject(DebugUtil.printId(firstQueryId)));
            Assertions.assertNotNull(manager.findProfileElementObject(executor.getProfile().getId()));
            executor.finalizeQuery();
            Assertions.assertNull(manager.findProfileElementObject(DebugUtil.printId(firstQueryId)));
            Assertions.assertNotNull(manager.findProfileElementObject(DebugUtil.printId(retryQueryId)));
            Assertions.assertNotEquals(Long.MAX_VALUE, executor.getProfile().getQueryFinishTimestamp());
            Assertions.assertEquals(Collections.singletonList(retryExecutionProfile),
                    executor.getProfile().getExecutionProfiles());
            // Total duration is between one and two thresholds.
            long durationMs = executor.getProfile().getQueryFinishTimestamp() - queryBeginTime;
            Assertions.assertTrue(durationMs >= 5_000 && durationMs < 10_000);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(firstQueryId);
            QeProcessorImpl.INSTANCE.unregisterQuery(retryQueryId);
            manager.cleanProfile();
        }
    }

    @Test
    @ResourceLock("global")
    public void testSuccessfulRetryRendersMergedProfileAndActualRows(@TempDir Path profileDirectory) throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        connectContext.getSessionVariable().enableProfile = true;
        connectContext.getSessionVariable().autoProfileThresholdMs = 0;
        TUniqueId firstQueryId = new TUniqueId(0x22040L, 5L);
        TUniqueId retryQueryId = new TUniqueId(0x22040L, 6L);
        connectContext.setQueryId(firstQueryId);
        connectContext.setStartTime();
        StmtExecutor executor = new StmtExecutor(connectContext,
                analyzeAndGetStmtByNereids("select 1", connectContext));
        executor.getSummaryProfile().setQueryBeginTime(System.currentTimeMillis());
        Profile profile = executor.getProfile();
        Statistics statistics = new Statistics(0, Collections.emptyMap());
        PhysicalEmptyRelation physicalPlan = new PhysicalEmptyRelation(new RelationId(0), Collections.emptyList(),
                Optional.empty(), null, PhysicalProperties.ANY, statistics);
        int planNodeId = physicalPlan.getId();
        profile.setPhysicalPlan(physicalPlan);
        ExecutionProfile failedExecution = new ExecutionProfile(firstQueryId, Collections.singletonList(0));
        ExecutionProfile successfulExecution = new ExecutionProfile(retryQueryId, Collections.singletonList(0));
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.enable_profile = true;

        try {
            Coordinator firstCoord = Mockito.mock(Coordinator.class);
            Mockito.when(firstCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(firstCoord.getExecutionProfile()).thenReturn(failedExecution);
            profile.addExecutionProfile(failedExecution);
            QeProcessorImpl.INSTANCE.registerQuery(firstQueryId, new QeProcessorImpl.QueryInfo(firstCoord));
            Assertions.assertTrue(failedExecution.updateProfile(
                    createBackendRowsProfile(firstQueryId, planNodeId, 1000),
                    new TNetworkAddress("127.0.0.1", 9060), true).ok());
            executor.updateProfile(false);
            executor.finalizeQuery(true);

            connectContext.setQueryId(retryQueryId);
            Coordinator retryCoord = Mockito.mock(Coordinator.class);
            Mockito.when(retryCoord.getQueryOptions()).thenReturn(queryOptions);
            Mockito.when(retryCoord.getExecutionProfile()).thenReturn(successfulExecution);
            Mockito.when(retryCoord.getExecStatus()).thenReturn(Status.OK);
            executor.setCoord(retryCoord);
            profile.addExecutionProfile(successfulExecution);
            QeProcessorImpl.INSTANCE.registerQuery(retryQueryId, new QeProcessorImpl.QueryInfo(retryCoord));
            successfulExecution.addFragmentBackend(0, 1L);
            successfulExecution.addFragmentBackend(0, 2L);
            Assertions.assertTrue(successfulExecution.updateProfile(
                    createBackendRowsProfile(retryQueryId, planNodeId, 17),
                    new TNetworkAddress("127.0.0.1", 9060), true).ok());
            Assertions.assertTrue(successfulExecution.updateProfile(
                    createBackendRowsProfile(retryQueryId, planNodeId, 25),
                    new TNetworkAddress("127.0.0.2", 9060), true).ok());
            Assertions.assertTrue(successfulExecution.isCompleted());
            executor.updateProfile(false);
            executor.finalizeQuery();
            Assertions.assertNotNull(manager.findProfileElementObject(profile.getId()));

            String profileText = profile.getProfileByLevel();
            Assertions.assertTrue(profileText.contains("MergedProfile:"));
            Assertions.assertTrue(profileText.contains("Pipeline 0(instance_num=2)"));
            Assertions.assertTrue(profileText.contains("RowsProduced: sum 42"));
            Assertions.assertFalse(profileText.contains("build merged simple profile failed"));
            Assertions.assertFalse(profileText.contains("DetailProfile(" + DebugUtil.printId(firstQueryId) + ")"));
            Assertions.assertTrue(profileText.contains("DetailProfile(" + DebugUtil.printId(retryQueryId) + ")"));
            Assertions.assertEquals(42L, profile.rowsProducedMap.get(String.valueOf(planNodeId)));
            Assertions.assertEquals(42L, statistics.getActualRowCount());

            profile.writeToStorage(profileDirectory.toString());
            Profile storedProfile = Profile.read(profile.getProfileStoragePath());
            Assertions.assertNotNull(storedProfile);
            String storedText = storedProfile.getProfileByLevel();
            Assertions.assertTrue(storedText.contains("MergedProfile:"));
            Assertions.assertTrue(storedText.contains("RowsProduced: sum 42"));
            Assertions.assertFalse(storedText.contains("DetailProfile(" + DebugUtil.printId(firstQueryId) + ")"));
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(firstQueryId);
            QeProcessorImpl.INSTANCE.unregisterQuery(retryQueryId);
            manager.removeProfile(profile);
        }
    }

    private interface CoordinatorConfigurer {
        void configure(Coordinator coordinator) throws Exception;
    }

    @ParameterizedTest
    @ValueSource(strings = {"SUCCESS", "EXHAUSTED", "PLANNING_FAILURE", "RPC", "DIRECT",
            "FLIGHT_SUCCESS", "FLIGHT_DEFERRED", "FETCH_FAILURE_SAME_ID", "FETCH_FAILURE_NEW_ID", "FETCH_FAILURE_PLANNING"})
    @ResourceLock("global")
    public void testCloudReplanDispatchProfileLifecycle(String outcome) throws Exception {
        String oldCloudUniqueId = Config.cloud_unique_id;
        int oldRetryTime = Config.max_query_retry_time;
        SessionVariable originalSession = connectContext.getSessionVariable();
        SessionVariable session = new SessionVariable();
        session.enableProfile = true;
        session.profileLevel = 3;
        session.autoProfileThresholdMs = 0;
        session.enableSqlCache = false;
        session.cloudCluster = "profile-replan-test";
        ConnectContext context = Mockito.spy(connectContext);
        boolean flight = outcome.startsWith("FLIGHT_");
        FlightProtocolAdapter flightAdapter = flight ? new FlightProtocolAdapter("profile-replan-test") : null;
        if (flight) {
            Mockito.doReturn(flightAdapter).when(context).getProtocolAdapter();
            Mockito.doAnswer(invocation -> flightAdapter.returnsResultFromLocal(context))
                    .when(context).isReturnResultFromLocal();
        }
        context.setSessionVariable(session);
        Mockito.doReturn(Mockito.mock(ResultSender.class)).when(context).getResultSender();
        Mockito.doReturn(connectContext.getComputeGroup()).when(context).getComputeGroup();
        context.setStartTime();
        TUniqueId firstQueryId = new TUniqueId(0x22040L, 7L);
        context.setQueryId(firstQueryId);
        StmtExecutor executor = Mockito.spy(new StmtExecutor(context, "select 1"));
        Method parse = StmtExecutor.class.getDeclaredMethod("parseByNereids");
        parse.setAccessible(true);
        parse.invoke(executor);
        LogicalPlanAdapter statement = (LogicalPlanAdapter) executor.getParsedStmt();
        statement.setResultExprs(Collections.emptyList());
        statement.setColLabels(Lists.newArrayList());
        Assertions.assertTrue(executor.isProfileSafeStmt());
        Profile profile = executor.getProfile();
        profile.getSummaryProfile().setQueryBeginTime(context.getStartTime());
        ProfileManager manager = ProfileManager.getInstance();
        boolean fetchFailure = outcome.startsWith("FETCH_FAILURE_");
        Planner planner = fetchFailure ? Mockito.mock(NereidsPlanner.class) : Mockito.mock(Planner.class);
        Statistics failedStatistics = Mockito.mock(Statistics.class);
        Statistics successfulStatistics = Mockito.mock(Statistics.class);
        Mockito.when(failedStatistics.printColumnStats()).thenReturn("abandoned-plan-column-statistics\n");
        Mockito.when(successfulStatistics.printColumnStats()).thenReturn("successful-plan-column-statistics\n");
        Field plannerField = StmtExecutor.class.getDeclaredField("planner");
        plannerField.setAccessible(true);
        Method handle = StmtExecutor.class.getDeclaredMethod("handleQueryWithRetry", TUniqueId.class);
        handle.setAccessible(true);
        List<ExecutionProfile> executions = Lists.newArrayList();
        List<PhysicalEmptyRelation> plans = Lists.newArrayList();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger failedAttemptCallbacks = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            TUniqueId queryId = invocation.getArgument(0);
            context.setQueryId(queryId);
            context.getProtocolAdapter().beforeAttempt(context);
            context.setStartTime();
            profile.getSummaryProfile().setQueryBeginTime(context.getStartTime());
            if (attempts.incrementAndGet() > 1) {
                Assertions.assertEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
                Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
                Assertions.assertTrue(profile.getExecutionProfiles().isEmpty());
                Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(firstQueryId));
                Assertions.assertEquals(1, failedAttemptCallbacks.get());
                if (fetchFailure) {
                    Assertions.assertNull(profile.getPhysicalPlan());
                    Assertions.assertTrue(profile.rowsProducedMap.isEmpty());
                }
                if ("PLANNING_FAILURE".equals(outcome) || "FETCH_FAILURE_PLANNING".equals(outcome)) {
                    context.getState().setError("terminal planning failure");
                    throw new UserException("terminal planning failure");
                }
            }
            if (fetchFailure) {
                int relationId = attempts.get() == 1 || "FETCH_FAILURE_SAME_ID".equals(outcome) ? 1 : 2;
                PhysicalEmptyRelation plan = new PhysicalEmptyRelation(new RelationId(relationId),
                        Collections.emptyList(), Optional.empty(), null, PhysicalProperties.ANY,
                        attempts.get() == 1 ? failedStatistics : successfulStatistics);
                plans.add(plan);
                FragmentIdMapping<DistributedPlan> distributedPlans = null;
                if (attempts.get() == 1) {
                    DistributedPlan distributedPlan = Mockito.mock(DistributedPlan.class);
                    Mockito.when(distributedPlan.toString(Mockito.anyInt())).thenReturn("abandoned-distributed-plan");
                    distributedPlans = new FragmentIdMapping<>();
                    distributedPlans.put(new PlanFragmentId(0), distributedPlan);
                    context.getBackendSelectionProfile().recordQuerySelection(
                            new BackendSelection.SelectionHint("abandoned-backend-selection",
                                    BackendSelection.Mode.PREFER, "test"),
                            BackendSelection.QuerySelectionResult.PREFERRED_HIT);
                }
                Mockito.when(((NereidsPlanner) planner).getDistributedPlans()).thenReturn(distributedPlans);
                Mockito.when(((NereidsPlanner) planner).getPhysicalPlan()).thenReturn(plan);
                Mockito.when(((NereidsPlanner) planner).getPhysicalRelations())
                        .thenReturn(Collections.singletonList(plan));
            }
            plannerField.set(executor, planner);
            try {
                handle.invoke(executor, queryId);
            } catch (InvocationTargetException e) {
                context.getState().setError(e.getCause().getMessage());
                throw e.getCause();
            }
            return null;
        }).when(executor).execute(Mockito.any(TUniqueId.class));
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        CoordinatorConfigurer configureCoordinator = coordinator -> {
            TUniqueId queryId = context.queryId();
            ExecutionProfile execution = new ExecutionProfile(queryId, Collections.singletonList(0));
            executions.add(execution);
            TQueryOptions options = new TQueryOptions();
            options.enable_profile = true;
            Mockito.when(coordinator.getQueryOptions()).thenReturn(options);
            Mockito.when(coordinator.getExecutionProfile()).thenReturn(execution);
            Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
            Mockito.when(coordinator.mustOutliveDispatch()).thenReturn("FLIGHT_DEFERRED".equals(outcome));
            Mockito.when(coordinator.getNext()).thenAnswer(fetch -> {
                if (fetchFailure && executions.size() == 1) {
                    String failedText = profile.getProfileByLevel();
                    Assertions.assertTrue(failedText.contains("abandoned-plan-column-statistics"));
                    Assertions.assertTrue(failedText.contains("abandoned-distributed-plan"));
                    Assertions.assertTrue(failedText.contains("abandoned-backend-selection"));
                    Assertions.assertEquals(1000L,
                            profile.rowsProducedMap.get(String.valueOf(plans.get(0).getId())));
                    throw new UserException(SystemInfoService.ERROR_E230);
                }
                return new RowBatch();
            });
            Mockito.doAnswer(dispatch -> {
                Assertions.assertSame(execution, manager.getExecutionProfile(queryId));
                Assertions.assertSame(coordinator, QeProcessorImpl.INSTANCE.getCoordinator(queryId));
                if (fetchFailure) {
                    execution.addFragmentBackend(0, 1L);
                    Assertions.assertTrue(execution.updateProfile(
                            createBackendRowsProfile(queryId, plans.get(plans.size() - 1).getId(),
                                    executions.size() == 1 ? 1000 : 42),
                            new TNetworkAddress("127.0.0.1", 9060), true).ok());
                }
                if (executions.size() == 1) {
                    QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(queryId),
                            failedAttemptCallbacks::incrementAndGet);
                    if (flight) {
                        Assertions.assertFalse(context.isReturnResultFromLocal());
                        Assertions.assertFalse(execution.isCompleted());
                    }
                    if (fetchFailure) {
                        return null;
                    }
                    if ("RPC".equals(outcome)) {
                        throw new RpcException("test-be", SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
                    }
                    throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
                }
                return null;
            }).when(coordinator).exec();
        };
        Mockito.when(factory.createCoordinator(Mockito.eq(context), Mockito.eq(planner), Mockito.any()))
                .thenAnswer(invocation -> {
                    Coordinator coordinator = Mockito.mock(Coordinator.class);
                    configureCoordinator.configure(coordinator);
                    return coordinator;
                });

        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class);
                MockedConstruction<NereidsCoordinator> coordinators = Mockito.mockConstruction(NereidsCoordinator.class,
                        (coordinator, construction) -> configureCoordinator.configure(coordinator))) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            Config.cloud_unique_id = "profile-replan-test";
            Config.max_query_retry_time = "EXHAUSTED".equals(outcome) ? 0 : 1;
            context.setThreadLocalInfo();
            if ("SUCCESS".equals(outcome) || flight
                    || (fetchFailure && !"FETCH_FAILURE_PLANNING".equals(outcome))) {
                executor.queryRetry(firstQueryId);
                Assertions.assertEquals(2, attempts.get());
                ExecutionProfile successful = executions.get(1);
                Assertions.assertEquals(Collections.singletonList(successful), profile.getExecutionProfiles());
                Assertions.assertEquals(DebugUtil.printId(successful.getQueryId()), profile.getId());
                Assertions.assertNotNull(manager.findProfileElementObject(profile.getId()));
                Assertions.assertNull(manager.findProfileElementObject(DebugUtil.printId(firstQueryId)));
                Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
                if (flight) {
                    Assertions.assertEquals(1, failedAttemptCallbacks.get());
                    Assertions.assertEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
                    Assertions.assertSame(successful, manager.getExecutionProfile(successful.getQueryId()));
                    Assertions.assertSame(executor.getCoord(),
                            QeProcessorImpl.INSTANCE.getCoordinator(successful.getQueryId()));
                    Assertions.assertEquals("FLIGHT_DEFERRED".equals(outcome), executor.isDeferredForArrowFlight());
                    if (executor.isDeferredForArrowFlight()) {
                        JsonObject pending = JsonParser.parseString(profile.getSummaryProfile().getExecutionSummary()
                                .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS)).getAsJsonObject();
                        Assertions.assertEquals(1, pending.size());
                        Assertions.assertFalse(pending.has(DebugUtil.printId(successful.getQueryId())));
                        flightAdapter.closeDeferredExecutors();
                    } else {
                        executor.finalizeQuery();
                    }
                    Assertions.assertEquals(1, failedAttemptCallbacks.get());
                }
                Assertions.assertTrue(successful.getQueryFinishTime() > 0);
                Assertions.assertEquals(TimeUtils.longToTimeString(context.getStartTime()),
                        profile.getSummaryProfile().getSummary().getInfoString(SummaryProfile.START_TIME));
                if (fetchFailure) {
                    String finalText = profile.getProfileByLevel();
                    Assertions.assertTrue(finalText.contains("PhysicalPlan:"));
                    Assertions.assertTrue(finalText.contains("successful-plan-column-statistics"));
                    Assertions.assertFalse(finalText.contains("abandoned-plan-column-statistics"));
                    Assertions.assertFalse(finalText.contains("abandoned-distributed-plan"));
                    Assertions.assertFalse(finalText.contains("abandoned-backend-selection"));
                    Assertions.assertFalse(profile.rowsProducedMap.containsKey(String.valueOf(plans.get(0).getId())));
                    Assertions.assertEquals(42L, profile.rowsProducedMap.get(String.valueOf(plans.get(1).getId())));
                    Mockito.verify(failedStatistics, Mockito.times(1)).printColumnStats();
                }
            } else {
                Class<? extends Exception> errorType = "RPC".equals(outcome) ? RpcException.class : UserException.class;
                if ("DIRECT".equals(outcome)) {
                    Assertions.assertThrows(errorType, () -> executor.execute(firstQueryId));
                } else {
                    Assertions.assertThrows(errorType, () -> executor.queryRetry(firstQueryId));
                }
                if ("PLANNING_FAILURE".equals(outcome) || "FETCH_FAILURE_PLANNING".equals(outcome)) {
                    Assertions.assertEquals(2, attempts.get());
                    Assertions.assertTrue(profile.getExecutionProfiles().isEmpty());
                    Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
                    Assertions.assertNull(profile.getPhysicalPlan());
                    String finalText = profile.getProfileByLevel();
                    Assertions.assertFalse(finalText.contains("abandoned-plan-column-statistics"));
                    Assertions.assertFalse(finalText.contains("abandoned-distributed-plan"));
                    Assertions.assertFalse(finalText.contains("abandoned-backend-selection"));
                } else {
                    Assertions.assertEquals(1, attempts.get());
                    Assertions.assertEquals(Collections.singletonList(executions.get(0)), profile.getExecutionProfiles());
                    Assertions.assertNotNull(manager.findProfileElementObject(DebugUtil.printId(firstQueryId)));
                    Assertions.assertSame(executions.get(0), manager.getExecutionProfile(firstQueryId));
                    Assertions.assertTrue(executions.get(0).getQueryFinishTime() > 0);
                }
            }
            int failedAttempts = "PLANNING_FAILURE".equals(outcome)
                    || "FETCH_FAILURE_PLANNING".equals(outcome) ? 2 : 1;
            Assertions.assertEquals(Integer.toString(failedAttempts), profile.getSummaryProfile().getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_TIMES));
            JsonObject attemptDetails = JsonParser.parseString(profile.getSummaryProfile().getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS)).getAsJsonObject();
            Assertions.assertEquals(attempts.get(), attemptDetails.size());
            Assertions.assertEquals("FAILED", attemptDetails.getAsJsonObject(DebugUtil.printId(firstQueryId))
                    .get("state").getAsString());
            JsonObject finalAttempt = attemptDetails.getAsJsonObject(DebugUtil.printId(context.queryId()));
            Assertions.assertEquals(context.getState().getStateType() == QueryState.MysqlStateType.ERR ? "FAILED" : "SUCCEEDED",
                    finalAttempt.get("state").getAsString());
            Assertions.assertEquals(fetchFailure ? 1 : 0, coordinators.constructed().size());
            Assertions.assertNotEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
            Assertions.assertEquals(context.getStartTime(), profile.getSummaryProfile().getQueryBeginTime());
            for (ExecutionProfile execution : executions) {
                Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(execution.getQueryId()));
            }
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.max_query_retry_time = oldRetryTime;
            connectContext.setSessionVariable(originalSession);
            connectContext.setThreadLocalInfo();
            for (ExecutionProfile execution : executions) {
                QeProcessorImpl.INSTANCE.unregisterQuery(execution.getQueryId());
            }
            manager.removeProfile(profile);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @ResourceLock("global")
    public void testInnerRetryClearsRenderedActualRows(boolean terminalFailure,
            @TempDir Path profileDirectory) throws Exception {
        String oldCloudUniqueId = Config.cloud_unique_id;
        int oldRetryTime = Config.max_query_retry_time;
        SessionVariable originalSession = connectContext.getSessionVariable();
        SessionVariable session = new SessionVariable();
        session.enableProfile = true;
        session.autoProfileThresholdMs = 0;
        session.enableSqlCache = false;
        ConnectContext context = Mockito.spy(connectContext);
        context.setSessionVariable(session);
        Mockito.doReturn(Mockito.mock(ResultSender.class)).when(context).getResultSender();
        context.getMysqlChannel().reset();
        context.setStartTime();
        TUniqueId firstQueryId = new TUniqueId(0x22040L, 19L);
        context.setQueryId(firstQueryId);
        StmtExecutor executor = new StmtExecutor(context, "select 1");
        Method parse = StmtExecutor.class.getDeclaredMethod("parseByNereids");
        parse.setAccessible(true);
        parse.invoke(executor);
        LogicalPlanAdapter statement = (LogicalPlanAdapter) executor.getParsedStmt();
        statement.setOrigStmt(new OriginStatement("select 1", 0));
        statement.setResultExprs(Collections.emptyList());
        statement.setColLabels(Lists.newArrayList());
        Profile profile = executor.getProfile();
        profile.getSummaryProfile().setQueryBeginTime(context.getStartTime());
        Statistics rootStatistics = new Statistics(1, Collections.emptyMap());
        Statistics childStatistics = new Statistics(1, Collections.emptyMap());
        PhysicalEmptyRelation leaf = new PhysicalEmptyRelation(new RelationId(0), Collections.emptyList(),
                Optional.empty(), null, PhysicalProperties.ANY, null);
        PhysicalLimit<Plan> child = new PhysicalLimit<>(1, 0, LimitPhase.GLOBAL, Optional.empty(), null,
                PhysicalProperties.ANY, childStatistics, leaf);
        PhysicalLimit<Plan> plan = new PhysicalLimit<>(1, 0, LimitPhase.GLOBAL, Optional.empty(), null,
                PhysicalProperties.ANY, rootStatistics, child);
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getPhysicalPlan()).thenReturn(plan);
        Field plannerField = StmtExecutor.class.getDeclaredField("planner");
        plannerField.setAccessible(true);
        plannerField.set(executor, planner);
        List<ExecutionProfile> executions = Lists.newArrayList();
        ProfileManager manager = ProfileManager.getInstance();
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.eq(context), Mockito.eq(planner), Mockito.any()))
                .thenAnswer(invocation -> {
                    TUniqueId queryId = context.queryId();
                    boolean firstAttempt = executions.isEmpty();
                    ExecutionProfile execution = new ExecutionProfile(queryId, Lists.newArrayList(0, 1));
                    executions.add(execution);
                    Coordinator coordinator = Mockito.mock(Coordinator.class);
                    TQueryOptions options = new TQueryOptions();
                    options.enable_profile = true;
                    Mockito.when(coordinator.getQueryOptions()).thenReturn(options);
                    Mockito.when(coordinator.getExecutionProfile()).thenReturn(execution);
                    Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
                    Mockito.doAnswer(dispatch -> {
                        if (!firstAttempt) {
                            Assertions.assertSame(plan, profile.getPhysicalPlan());
                            Assertions.assertTrue(profile.rowsProducedMap.isEmpty());
                            Assertions.assertEquals(-1L, rootStatistics.getActualRowCount());
                            Assertions.assertEquals(-1L, childStatistics.getActualRowCount());
                            Assertions.assertNull(manager.getExecutionProfile(firstQueryId));
                            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(firstQueryId));
                        }
                        execution.addFragmentBackend(0, 1L);
                        execution.addFragmentBackend(1, 1L);
                        TNetworkAddress backend = new TNetworkAddress("127.0.0.1", 9060);
                        Assertions.assertTrue(execution.updateProfile(
                                createBackendRowsProfile(queryId, plan.getId(), firstAttempt ? 1000 : 42),
                                backend, true).ok());
                        if (firstAttempt) {
                            TQueryProfile childReport = createBackendRowsProfile(queryId, child.getId(), 500);
                            childReport.setFragmentIdToProfile(Collections.singletonMap(1,
                                    childReport.getFragmentIdToProfile().get(0)));
                            Assertions.assertTrue(execution.updateProfile(childReport, backend, true).ok());
                        }
                        return null;
                    }).when(coordinator).exec();
                    Mockito.when(coordinator.getNext()).thenAnswer(fetch -> {
                        if (firstAttempt) {
                            String firstText = profile.getProfileByLevel();
                            Assertions.assertTrue(firstText.contains("actualRows=1000"));
                            Assertions.assertTrue(firstText.contains("actualRows=500"));
                            Assertions.assertEquals(500L, childStatistics.getActualRowCount());
                            throw new RpcException("test-be", "fetch failed before sending results");
                        }
                        if (terminalFailure) {
                            throw new RpcException("test-be", "terminal fetch failure");
                        }
                        return new RowBatch();
                    });
                    return coordinator;
                });
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            Config.cloud_unique_id = "";
            Config.max_query_retry_time = 1;
            context.setThreadLocalInfo();
            Method handle = StmtExecutor.class.getDeclaredMethod("handleQueryWithRetry", TUniqueId.class);
            handle.setAccessible(true);
            if (terminalFailure) {
                InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                        () -> handle.invoke(executor, firstQueryId));
                Assertions.assertInstanceOf(RpcException.class, failure.getCause());
            } else {
                handle.invoke(executor, firstQueryId);
            }
            Assertions.assertEquals(2, executions.size());
            Assertions.assertEquals(Collections.singletonList(executions.get(1)), profile.getExecutionProfiles());
            Assertions.assertFalse(executions.get(1).isCompleted());
            String details = profile.getSummaryProfile().getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS);
            JsonObject attempts = JsonParser.parseString(details).getAsJsonObject();
            Assertions.assertEquals(2, attempts.size());
            Assertions.assertEquals("FAILED", attempts.getAsJsonObject(DebugUtil.printId(firstQueryId))
                    .get("state").getAsString());
            JsonObject last = attempts.getAsJsonObject(DebugUtil.printId(context.queryId()));
            Assertions.assertEquals(terminalFailure ? "FAILED" : "SUCCEEDED", last.get("state").getAsString());
            Assertions.assertTrue(last.get("durationMs").getAsLong() >= 0);
            String finalText = profile.getProfileByLevel();
            Assertions.assertTrue(finalText.contains("QueryRetryTimes: " + (terminalFailure ? 2 : 1)));
            Assertions.assertTrue(finalText.contains("actualRows=42"));
            Assertions.assertFalse(finalText.contains("actualRows=1000"));
            Assertions.assertFalse(finalText.contains("actualRows=500"));
            Assertions.assertEquals(42L, rootStatistics.getActualRowCount());
            Assertions.assertEquals(-1L, childStatistics.getActualRowCount());
            Assertions.assertFalse(profile.rowsProducedMap.containsKey(String.valueOf(child.getId())));
            profile.writeToStorage(profileDirectory.toString());
            Profile stored = Profile.read(profile.getProfileStoragePath());
            Assertions.assertNotNull(stored);
            Assertions.assertTrue(stored.getProfileByLevel().contains("QueryRetryTimes: " + (terminalFailure ? 2 : 1)));
            Assertions.assertEquals(details, stored.getSummaryProfile().getExecutionSummary()
                    .getInfoString(SummaryProfile.QUERY_RETRY_DETAILS));
            Assertions.assertFalse(stored.getProfileByLevel().contains("actualRows=500"));
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.max_query_retry_time = oldRetryTime;
            connectContext.setSessionVariable(originalSession);
            connectContext.setThreadLocalInfo();
            for (ExecutionProfile execution : executions) {
                QeProcessorImpl.INSTANCE.unregisterQuery(execution.getQueryId());
            }
            manager.removeProfile(profile);
        }
    }

    private static TQueryProfile createBackendRowsProfile(TUniqueId queryId, int planNodeId, long rowsProduced) {
        TRuntimeProfileNode pipeline = createProfileNode("Pipeline 0", 1);
        TRuntimeProfileNode task = createProfileNode("PipelineTask 0", 1);
        TRuntimeProfileNode operator = createProfileNode("AGGREGATION_OPERATOR (nereids_id=" + planNodeId + ")", 1);
        operator.setMetadata(planNodeId);
        TRuntimeProfileNode commonCounters = createProfileNode("CommonCounters", 0);
        TCounter rows = new TCounter("RowsProduced", TUnit.UNIT, rowsProduced);
        rows.setLevel(1);
        commonCounters.setCounters(Collections.singletonList(rows));
        commonCounters.setChildCountersMap(Collections.singletonMap(RuntimeProfile.ROOT_COUNTER,
                Collections.singleton("RowsProduced")));
        TRuntimeProfileTree tree = new TRuntimeProfileTree();
        tree.setNodes(Lists.newArrayList(pipeline, task, operator, commonCounters));
        TDetailedReportParams report = new TDetailedReportParams();
        report.setProfile(tree);
        report.setIsFragmentLevel(false);
        TQueryProfile queryProfile = new TQueryProfile();
        queryProfile.setQueryId(queryId);
        queryProfile.putToFragmentIdToProfile(0, Collections.singletonList(report));
        return queryProfile;
    }

    private static TRuntimeProfileNode createProfileNode(String name, int children) {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        node.setName(name);
        node.setNumChildren(children);
        node.setCounters(Collections.emptyList());
        node.setMetadata(0);
        node.setIndent(true);
        node.setInfoStrings(Collections.emptyMap());
        node.setInfoStringsDisplayOrder(Collections.emptyList());
        node.setChildCountersMap(Collections.emptyMap());
        node.setTimestamp(0);
        return node;
    }

    @Test
    public void testKill() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testKillOtherFail() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1000");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testKillNoCtx() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testSet() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testDdlFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "CREATE FILE \\\"ca.pem\\\"\\n\"\n"
                + "                + \"PROPERTIES\\n\"\n"
                + "                + \"(\\n\"\n"
                + "                + \"   \\\"url\\\" = \\\"https://test.bj.bcebos.com/kafka-key/ca.pem\\\",\\n\"\n"
                + "                + \"   \\\"catalog\\\" = \\\"kafka\\\"\\n\"\n"
                + "                + \");");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUse() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use nondb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalog() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalogFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.nondb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testBlockSqlAst() throws Exception {
        useDatabase("testDb");
        Config.block_sql_ast_names = "CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        StmtExecutor executor = new StmtExecutor(connectContext, "CREATE FILE \"ca.pem\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   \"url\" = \"https://test.bj.bcebos.com/kafka-key/ca.pem\",\n"
                + "   \"catalog\" = \"kafka\"\n"
                + ");");
        try {
            executor.execute();
        } catch (Exception ignore) {
            // do nothing
            ignore.printStackTrace();
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        Config.block_sql_ast_names = "AlterStmt, CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "CREATE FILE \"ca.pem\"\n"
                + "PROPERTIES\n"
                + "(\"file_type\" = \"PEM\")");
        try {
            executor.execute();
        } catch (Exception ignore) {
            ignore.printStackTrace();
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        Config.block_sql_ast_names = "CreateFunctionStmt, CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (\n"
                + "   \"file\"=\"file:///path/to/java-udf-demo-jar-with-dependencies.jar\",\n"
                + "   \"symbol\"=\"org.apache.doris.udf.AddOne\",\n"
                + "   \"always_nullable\"=\"true\",\n"
                + "   \"type\"=\"JAVA_UDF\"\n"
                + ");");
        try {
            executor.execute();
        } catch (Exception ignore) {
            ignore.printStackTrace();
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());

        Config.block_sql_ast_names = "";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testClearDeleteExistingFilesInPlan() throws Exception {
        Planner planner = Mockito.mock(Planner.class);
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        ResultFileSink resultFileSink = Mockito.mock(ResultFileSink.class);
        Mockito.when(fragment.getSink()).thenReturn(resultFileSink);
        Mockito.when(planner.getFragments()).thenReturn(Lists.newArrayList(fragment));

        StmtExecutor executor = new StmtExecutor(connectContext, "");
        Field plannerField = StmtExecutor.class.getDeclaredField("planner");
        plannerField.setAccessible(true);
        plannerField.set(executor, planner);

        Method clearMethod = StmtExecutor.class.getDeclaredMethod("clearDeleteExistingFilesInPlan");
        clearMethod.setAccessible(true);
        clearMethod.invoke(executor);

        Mockito.verify(resultFileSink).setDeleteExistingFiles(false);
    }

    @Test
    public void testParseByNereidsSetsParsedStatementOnStatementContext() throws Exception {
        // This test verifies the fix for a bug in multi-FE environments where
        // parseByNereids() did not propagate the parsed statement to the
        // StatementContext. In the proxy flow (e.g., when a follower FE forwards
        // a query to the master FE), the StmtExecutor is created via the proxy
        // constructor which creates a fresh StatementContext without a
        // parsedStatement. Without the fix, statementContext.getParsedStatement()
        // remains null, causing SessionVariable.canUseNereidsDistributePlanner()
        // to return false, which leads EnvFactory.createCoordinator() to create
        // a legacy Coordinator instead of NereidsCoordinator, resulting in
        // "fragment has no children" error.

        // Simulate the proxy flow: StmtExecutor(ConnectContext, OriginStatement, boolean isProxy)
        StmtExecutor executor = new StmtExecutor(connectContext,
                new OriginStatement("select 1", 0), true);

        // Before parsing, statementContext should exist but parsedStatement should be null
        Assertions.assertNotNull(connectContext.getStatementContext());
        Assertions.assertNull(connectContext.getStatementContext().getParsedStatement(),
                "ParsedStatement should be null before parseByNereids() in proxy flow");

        // Trigger parseByNereids via reflection (it's private)
        Method parseByNereidsMethod = StmtExecutor.class.getDeclaredMethod("parseByNereids");
        parseByNereidsMethod.setAccessible(true);
        parseByNereidsMethod.invoke(executor);

        // After parsing, parsedStatement should be set on the StatementContext
        org.apache.doris.analysis.StatementBase parsedStatement
                = connectContext.getStatementContext().getParsedStatement();
        Assertions.assertNotNull(parsedStatement,
                "ParsedStatement should not be null after parseByNereids() in proxy flow");
        Assertions.assertTrue(
                parsedStatement instanceof org.apache.doris.nereids.glue.LogicalPlanAdapter,
                "ParsedStatement should be a LogicalPlanAdapter after parseByNereids(), but was: "
                        + (parsedStatement == null ? "null" : parsedStatement.getClass().getName()));
    }

    @Test
    public void testShouldDisableCloudVersionCacheOnRetryForE230() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        String originalDeployMode = Config.deploy_mode;
        long originalPartitionTtl = connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs;
        long originalTableTtl = connectContext.getSessionVariable().cloudTableVersionCacheTtlMs;
        try {
            Config.cloud_unique_id = "test-cloud-id";
            StmtExecutor executor = new StmtExecutor(connectContext, "select 1");

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 1000L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 1000L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = some other error"));
            // null error message must not trigger the disable.
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(null));

            // Non-cloud mode must never disable the version cache, even on E-230.
            Config.cloud_unique_id = "";
            Config.deploy_mode = "";
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
            Config.cloud_unique_id = "test-cloud-id";

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 0L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 1000L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 1000L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 0L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 0L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 0L;
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
            Config.deploy_mode = originalDeployMode;
            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = originalPartitionTtl;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = originalTableTtl;
        }
    }

    @Test
    public void testEmptyOriginStmtSkipsAuditMaskingReparse() throws Exception {
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan logicalPlan = Mockito.mock(
                org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class,
                Mockito.withSettings().extraInterfaces(
                        org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption.class));
        Mockito.doThrow(new AssertionError("empty SQL should not trigger audit masking reparse"))
                .when((org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption) logicalPlan)
                .geneEncryptionSQL("");

        org.apache.doris.analysis.StatementBase parsedStmt = new org.apache.doris.nereids.glue.LogicalPlanAdapter(
                logicalPlan, new org.apache.doris.nereids.StatementContext());
        parsedStmt.setOrigStmt(new OriginStatement("", 0));
        StmtExecutor executor = new StmtExecutor(connectContext, parsedStmt);

        // Empty internal SQL must bypass audit masking reparsing in both logging paths.
        Method getStmtForLogging = StmtExecutor.class.getDeclaredMethod("getStmtForLogging", String.class);
        getStmtForLogging.setAccessible(true);
        Assertions.assertEquals("", getStmtForLogging.invoke(executor, ""));

        Method getStmtForLoggingBeforeParse = StmtExecutor.class.getDeclaredMethod("getStmtForLoggingBeforeParse");
        getStmtForLoggingBeforeParse.setAccessible(true);
        Assertions.assertEquals("", getStmtForLoggingBeforeParse.invoke(executor));
    }

    @Test
    public void testNeedAuditEncryptionStatementLogsMaskedSql() throws Exception {
        String resourceName = newAiResourceName();
        boolean originalPrintRequest = Config.enable_print_request_before_execution;
        Config.enable_print_request_before_execution = true;
        try (TestLogAppender appender = TestLogAppender.attach(StmtExecutor.class)) {
            connectContext.getState().reset();
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, buildCreateAiResourceSql(resourceName,
                    AI_RESOURCE_LOG_SECRET));
            stmtExecutor.execute();

            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, AI_RESOURCE_LOG_SECRET));
            Assertions.assertTrue(appender.contains(org.apache.logging.log4j.Level.INFO, "*XXX"));
            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.DEBUG, AI_RESOURCE_LOG_SECRET));
            Assertions.assertTrue(appender.contains(org.apache.logging.log4j.Level.DEBUG, "*XXX"));
        } finally {
            Config.enable_print_request_before_execution = originalPrintRequest;
        }
        connectContext.getState().reset();
        StmtExecutor showExecutor = new StmtExecutor(connectContext, "");
        showExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testAlterResourceSuccessLogDoesNotPrintResourceObject() throws Exception {
        String resourceName = newAiResourceName();
        createResource(buildCreateAiResourceSql(resourceName, AI_RESOURCE_LOG_SECRET));
        String alterSql = "ALTER RESOURCE \"" + resourceName + "\" PROPERTIES ("
                + "\"ai.api_key\" = \"sk-updated-secret\")";
        String fullResourceJson = Env.getCurrentEnv().getResourceMgr().getResource(resourceName).toString();

        try (TestLogAppender appender = TestLogAppender.attach(ResourceMgr.class)) {
            connectContext.getState().reset();
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterSql);
            stmtExecutor.execute();

            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, "sk-updated-secret"));
            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, "\"properties\""));
            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, fullResourceJson));
        }
    }

    @Test
    public void testGetStmtForLoggingFailsClosedWhenMaskingThrows() throws Exception {
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan logicalPlan = Mockito.mock(
                org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class,
                Mockito.withSettings().extraInterfaces(
                        org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption.class));
        Mockito.doThrow(new IllegalStateException("masking failed"))
                .when((org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption) logicalPlan)
                .geneEncryptionSQL(Mockito.anyString());

        org.apache.doris.analysis.StatementBase parsedStmt = new org.apache.doris.nereids.glue.LogicalPlanAdapter(
                logicalPlan, new org.apache.doris.nereids.StatementContext());
        parsedStmt.setOrigStmt(new OriginStatement("CREATE EXTERNAL RESOURCE \"ai_resource\" PROPERTIES ("
                + "\"ai.api_key\" = \"" + AI_RESOURCE_LOG_SECRET + "\")", 0));
        StmtExecutor executor = new StmtExecutor(connectContext, parsedStmt);

        Method getStmtForLogging = StmtExecutor.class.getDeclaredMethod("getStmtForLogging", String.class);
        getStmtForLogging.setAccessible(true);
        Assertions.assertEquals(MASKED_STMT_FALLBACK, getStmtForLogging.invoke(executor,
                parsedStmt.getOrigStmt().originStmt));
    }

    @Test
    public void testGetStmtForLoggingBeforeParseFailsClosedOnParseError() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext,
                "CREATE EXTERNAL RESOURCE \"broken_ai_resource\" PROPERTIES (\"ai.api_key\" = \""
                        + AI_RESOURCE_LOG_SECRET + "\"");

        Method getStmtForLoggingBeforeParse = StmtExecutor.class.getDeclaredMethod("getStmtForLoggingBeforeParse");
        getStmtForLoggingBeforeParse.setAccessible(true);
        Assertions.assertEquals(MASKED_STMT_FALLBACK, getStmtForLoggingBeforeParse.invoke(executor));
    }

    @Test
    public void testCancelForwardsToCancelDelegate() {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        AtomicInteger forwarded = new AtomicInteger();
        stmtExecutor.setCancelDelegate(status -> forwarded.incrementAndGet());
        stmtExecutor.cancel(Status.CANCELLED, false);
        Assertions.assertEquals(1, forwarded.get());
        // The delegate is scoped to the nested work only: once cleared, later
        // cancellations on this executor must not reach it again.
        stmtExecutor.clearCancelDelegate();
        stmtExecutor.cancel(Status.CANCELLED, false);
        Assertions.assertEquals(1, forwarded.get());
    }

    private void createResource(String sql) throws Exception {
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    // Use unique resource names to keep log-masking tests isolated across the PER_CLASS test fixture.
    private static String newAiResourceName() {
        return "ai_resource_log_test_" + System.nanoTime();
    }

    // Build resource SQL with a caller-provided name so tests do not share catalog state.
    private static String buildCreateAiResourceSql(String resourceName, String apiKey) {
        return "CREATE EXTERNAL RESOURCE \"" + resourceName + "\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   \"type\" = \"ai\",\n"
                + "   \"ai.provider_type\" = \"openai\",\n"
                + "   \"ai.endpoint\" = \"https://api.test\",\n"
                + "   \"ai.model_name\" = \"gpt-test\",\n"
                + "   \"ai.api_key\" = \"" + apiKey + "\"\n"
                + ");";
    }
}
