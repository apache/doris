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
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
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
