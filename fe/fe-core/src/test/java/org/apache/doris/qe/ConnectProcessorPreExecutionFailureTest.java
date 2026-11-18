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
import org.apache.doris.arrowflight.DorisFlightSqlProducer;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * A statement that throws before it executes must be reported as failed (error state, one audit
 * record), not as a success. Over Arrow Flight SQL such a SELECT used to be answered with the
 * synthetic one-row StatusResult table meant for SET/USE.
 */
public class ConnectProcessorPreExecutionFailureTest extends TestWithFeService {

    private CountingProcessor processor;

    @BeforeEach
    public void setUp() {
        connectContext.setThreadLocalInfo();
        connectContext.getState().reset();
        processor = new CountingProcessor(connectContext);
    }

    @Test
    public void testSetupFailureIsReportedAsThatStatement() throws ConnectionException {
        processor.failOnResolveCall = 1;
        processor.failure = new IllegalStateException("cannot build the executor");

        processor.handleQuery("SELECT 1");

        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType(),
                "a statement that never ran must not report success");
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("cannot build the executor"),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(1, processor.audits.size());
        Assertions.assertEquals("SELECT 1", processor.audits.get(0).stmt);
        Assertions.assertNotNull(processor.audits.get(0).parsedStmt, "audited as the statement that failed");
        Assertions.assertEquals(MysqlStateType.ERR, processor.audits.get(0).state, "audited with its error");
        Assertions.assertNull(connectContext.queryId(), "must not carry another statement's query id");
    }

    @Test
    public void testSetupUserExceptionIsReportedWithItsErrorCode() throws ConnectionException {
        processor.failOnResolveCall = 1;
        processor.failure = new UserException("setup refused");

        processor.handleQuery("SELECT 1");

        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("setup refused"),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(QueryState.ErrType.ANALYSIS_ERR, connectContext.getState().getErrType());
        Assertions.assertEquals(1, processor.audits.size());
    }

    @Test
    public void testFailureBeforeStatementLoopIsReported() throws ConnectionException {
        processor.failParse = new IllegalStateException("cannot prepare the request");

        processor.handleQuery("SELECT 1");

        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("cannot prepare the request"),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(1, processor.audits.size());
        Assertions.assertEquals("SELECT 1", processor.audits.get(0).stmt);
        Assertions.assertNull(processor.audits.get(0).parsedStmt);
    }

    @Test
    public void testExecutionFailureKeepsItsErrorAndOneAudit() throws ConnectionException {
        processor.handleQuery("SELECT * FROM no_such_db_for_this_test.no_such_table");

        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("no_such_db_for_this_test"),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(1, processor.audits.size());
    }

    @Test
    public void testMultiStatementSetupFailureStopsTheRequest() throws ConnectionException {
        // The second statement fails before it executes; the third must not run.
        processor.failOnResolveCall = 2;
        processor.failure = new IllegalStateException("second statement setup failed");

        processor.handleQuery("SET @pre_exec_a = 1; SET @pre_exec_b = 2; SET @pre_exec_c = 3");

        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertEquals(2, processor.resolveCalls, "the statement after the failed one must not run");
        Assertions.assertEquals(2, processor.audits.size());
        Assertions.assertTrue(processor.audits.get(0).stmt.contains("@pre_exec_a"), processor.audits.get(0).stmt);
        Assertions.assertNotEquals(MysqlStateType.ERR, processor.audits.get(0).state);
        Assertions.assertTrue(processor.audits.get(1).stmt.contains("@pre_exec_b"), processor.audits.get(1).stmt);
        Assertions.assertFalse(processor.audits.get(1).stmt.contains("@pre_exec_a"), processor.audits.get(1).stmt);
        Assertions.assertEquals(MysqlStateType.ERR, processor.audits.get(1).state);
        Assertions.assertNull(connectContext.queryId(), "the failed statement must not take the first one's query id");
    }

    @Test
    public void testCleanupFailureAfterReportedSuccessIsNotReportedAgain() throws ConnectionException {
        // The statement ran and was audited; closing its statement context then fails.
        processor.failCloseAfterAudit = true;

        processor.handleQuery("SET @pre_exec_cleanup = 1");

        Assertions.assertNotEquals(MysqlStateType.ERR, connectContext.getState().getStateType(),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(1, processor.audits.size(), "a reported statement must not be audited twice");
    }

    @Test
    public void testConnectionExceptionStillPropagates() {
        processor.failOnResolveCall = 1;
        processor.failure = new ConnectionException("client went away");

        Assertions.assertThrows(ConnectionException.class, () -> processor.handleQuery("SELECT 1"));
        Assertions.assertTrue(processor.audits.isEmpty());
    }

    // ---------- The real Arrow Flight SQL producer path ---------- //

    @Test
    public void testFlightSetupFailureFailsGetFlightInfo() throws Exception {
        ConnectContext flight = newFlightContext();
        try (DorisFlightSqlProducer producer = newProducer(flight);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class);
                MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                        (mock, context) -> {
                            throw new IllegalStateException("cannot build the executor");
                        })) {
            Assertions.assertThrows(FlightRuntimeException.class, () -> getFlightInfo(producer, "SELECT 1"));
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertEquals(0, flight.getFlightSqlChannel().resultNum(), "no StatusResult may be published");
            audit.verify(() -> AuditLogHelper.logAuditLog(Mockito.eq(flight), Mockito.anyString(),
                    Mockito.any(), Mockito.any(), Mockito.anyBoolean()), Mockito.times(1));
        } finally {
            flight.getFlightSqlChannel().close();
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testFlightFailureBeforeStatementLoopFailsGetFlightInfo() throws Exception {
        ConnectContext flight = newFlightContext();
        try (DorisFlightSqlProducer producer = newProducer(flight);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class);
                MockedStatic<SqlDialectHelper> dialect = Mockito.mockStatic(SqlDialectHelper.class)) {
            dialect.when(() -> SqlDialectHelper.convertSqlByDialect(Mockito.anyString(), Mockito.any()))
                    .thenThrow(new IllegalStateException("dialect conversion failed"));
            Assertions.assertThrows(FlightRuntimeException.class, () -> getFlightInfo(producer, "SELECT 1"));
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertEquals(0, flight.getFlightSqlChannel().resultNum(), "no StatusResult may be published");
            audit.verify(() -> AuditLogHelper.logAuditLog(Mockito.eq(flight), Mockito.anyString(),
                    Mockito.any(), Mockito.any(), Mockito.anyBoolean()), Mockito.times(1));
        } finally {
            flight.getFlightSqlChannel().close();
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testFlightSetStillAnswersOk() throws Exception {
        ConnectContext flight = newFlightContext();
        try (DorisFlightSqlProducer producer = newProducer(flight)) {
            FlightInfo info = getFlightInfo(producer, "SET query_timeout = 17");
            Assertions.assertNotNull(info);
            Assertions.assertNotEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertEquals(1, flight.getFlightSqlChannel().resultNum());
            Assertions.assertEquals(17, flight.getSessionVariable().getQueryTimeoutS());
        } finally {
            flight.getFlightSqlChannel().close();
            connectContext.setThreadLocalInfo();
        }
    }

    private ConnectContext newFlightContext() {
        ConnectContext flight = ConnectContext.forFlight("pre-exec-peer");
        flight.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flight.setRemoteIP(connectContext.getRemoteIP());
        flight.setEnv(Env.getCurrentEnv());
        flight.setSessionVariable(VariableMgr.cloneSessionVariable(connectContext.getSessionVariable()));
        return flight;
    }

    private static DorisFlightSqlProducer newProducer(ConnectContext flight) {
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessions.getConnectContext("pre-exec-peer")).thenReturn(flight);
        return new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 0), sessions);
    }

    private static FlightInfo getFlightInfo(DorisFlightSqlProducer producer, String sql) {
        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("pre-exec-peer");
        return producer.getFlightInfoStatement(CommandStatementQuery.newBuilder().setQuery(sql).build(),
                callContext, FlightDescriptor.command(new byte[0]));
    }

    private static final class Audit {
        final String stmt;
        final StatementBase parsedStmt;
        final MysqlStateType state;

        Audit(String stmt, StatementBase parsedStmt, MysqlStateType state) {
            this.stmt = stmt;
            this.parsedStmt = parsedStmt;
            this.state = state;
        }
    }

    private static class CountingProcessor extends ConnectProcessor {
        // Fail the n-th per-statement resolveWorkloadGroupName call (1-based); 0 never fails.
        int failOnResolveCall;
        Exception failure;
        RuntimeException failParse;
        boolean failCloseAfterAudit;
        int resolveCalls;
        final List<Audit> audits = new ArrayList<>();

        CountingProcessor(ConnectContext context) {
            super(context);
        }

        // Only an injection point: it runs inside the statement loop, before the executor is built.
        @Override
        protected void resolveWorkloadGroupName() {
            resolveCalls++;
            if (resolveCalls == failOnResolveCall) {
                CountingProcessor.<RuntimeException>sneakyThrow(failure);
            }
            super.resolveWorkloadGroupName();
        }

        // Only an injection point: it runs before the statement loop.
        @Override
        protected List<StatementBase> parseWithFallback(String originStmt, String convertedStmt,
                SessionVariable sessionVariable) throws ConnectionException {
            if (failParse != null) {
                throw failParse;
            }
            return super.parseWithFallback(originStmt, convertedStmt, sessionVariable);
        }

        @Override
        protected void auditAfterExec(String origStmt, StatementBase parsedStmt,
                Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
            audits.add(new Audit(origStmt, parsedStmt, ctx.getState().getStateType()));
            if (failCloseAfterAudit) {
                ctx.getStatementContext().close();
                StatementContext failingClose = Mockito.mock(StatementContext.class);
                Mockito.doThrow(new IllegalStateException("Release resource failed")).when(failingClose).close();
                ctx.setStatementContext(failingClose);
            }
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
            throw (T) throwable;
        }
    }
}
