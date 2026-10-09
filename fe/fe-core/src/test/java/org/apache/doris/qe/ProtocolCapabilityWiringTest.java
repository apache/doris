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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.FlightSqlConnectProcessor;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.protocol.RecordingMysqlChannel;
import org.apache.doris.qe.protocol.RecordingMysqlChannel.RecordedPacket;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The execution layer does not branch on the protocol a connection speaks; it asks the
 * connection's {@code ProtocolAdapter}. The adapter tests pin what each adapter answers; these
 * tests drive real statements through the processors and the executor and check that the answer
 * is what decides, where a unit test of the adapter alone cannot show it.
 */
public class ProtocolCapabilityWiringTest extends TestWithFeService {
    private static final String DB_NAME = "protocol_capability_wiring_db";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse(DB_NAME);
    }

    // On a follower a query is forwarded to the master when the session asks for it
    // (ForceForwardAllQueriesTest); an Arrow Flight SQL session cannot take the master's answer
    // and is refused before any rpc, a MySQL connection goes on to forward.
    @Test
    public void testForwardedQueryIsRefusedOnAFlightSessionBeforeTheRpc() throws Exception {
        Env env = Env.getCurrentEnv();
        FrontendNodeType originalFeType = env.getFeType();
        AtomicBoolean canRead = Deencapsulation.getField(env, "canRead");
        boolean originalCanRead = canRead.get();
        boolean originalForceForward = Config.force_forward_all_queries;
        Deencapsulation.setField(env, "feType", FrontendNodeType.FOLLOWER);
        canRead.set(true);
        Config.force_forward_all_queries = false;
        try {
            ConnectContext flight = flightContext();
            flight.getSessionVariable().forceForwardAllQueries = true;
            StmtExecutor refused = new StmtExecutor(flight, analyzeAndGetStmtByNereids("select 1", flight));

            UserException e = Assertions.assertThrows(UserException.class, refused::execute);

            Assertions.assertTrue(e.getMessage().contains("not supported on an Arrow Flight SQL connection"),
                    e.getMessage());
            Assertions.assertFalse(refused.hasForwardedToMaster());
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());

            ConnectContext mysql = createDefaultCtx();
            mysql.setDatabase(DB_NAME);
            mysql.setThreadLocalInfo();
            mysql.getSessionVariable().forceForwardAllQueries = true;
            StmtExecutor forwarded = new StmtExecutor(mysql, analyzeAndGetStmtByNereids("select 1", mysql));
            try {
                forwarded.execute();
            } catch (Exception expected) {
                // There is no master to reach from a unit test; what matters is that the
                // statement was handed to the forwarding path instead of being refused.
            }
            Assertions.assertTrue(forwarded.hasForwardedToMaster());
        } finally {
            Deencapsulation.setField(env, "feType", originalFeType);
            canRead.set(originalCanRead);
            Config.force_forward_all_queries = originalForceForward;
            connectContext.setThreadLocalInfo();
        }
    }

    // The previous statement of a Flight request left its result on a backend; the next one is
    // answered by this frontend again, because the processor tells the adapter where a statement
    // starts.
    @Test
    public void testAStatementStartsOnTheFrontendWhateverThePreviousOneDid() throws Exception {
        ConnectContext flight = flightContext();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(flight);
        adapter.beforeQuery(flight);
        Assertions.assertFalse(flight.isReturnResultFromLocal());
        try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(flight)) {
            processor.handleQuery("show variables like 'wait_timeout'");

            Assertions.assertNotEquals(MysqlStateType.ERR, flight.getState().getStateType(),
                    flight.getState().getErrorMessage());
            Assertions.assertTrue(flight.isReturnResultFromLocal());
            Assertions.assertEquals(1, adapter.getChannel().resultNum());
        } finally {
            adapter.getChannel().close();
            connectContext.setThreadLocalInfo();
        }
    }

    // The master answers a forwarded query for the client of the frontend that forwarded it: the
    // packets it collects and the response that ends them follow the capabilities the request
    // carries, restored into the proxy context by the adapter.
    @Test
    public void testMasterAnswersAForwardedQueryWithTheClientsCapabilities() throws Exception {
        for (boolean deprecateEof : new boolean[] {true, false}) {
            int flags = MysqlCapability.DEFAULT_CAPABILITY.getFlags();
            if (!deprecateEof) {
                flags &= ~MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit();
            }
            TMasterOpRequest request = new TMasterOpRequest();
            request.setDb(DB_NAME);
            request.setUser("root");
            request.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
            request.setSql("select 1");
            request.setMysqlCapability(flags);
            request.setClientDeprecatedEOF(deprecateEof);
            ConnectContext proxy = ConnectContext.forMysqlProxy("session-1");
            try {
                TMasterOpResult result = new MysqlConnectProcessor(proxy).proxyExecute(request);

                // A query's state is EOF, the end of its result set; only an OK state sets statusCode 0.
                Assertions.assertEquals(MysqlStateType.EOF.name(), result.getStatus(),
                        proxy.getState().getErrorMessage());
                // column count, one column definition, the terminator the client expects, one row
                List<ByteBuffer> packets = result.getQueryResultBufList();
                Assertions.assertEquals(deprecateEof ? 3 : 4, packets.size());
                Assertions.assertEquals(1, MysqlProto.readVInt(packets.get(0).duplicate()));
                if (!deprecateEof) {
                    ByteBuffer eof = packets.get(2);
                    Assertions.assertEquals(0xFE, Byte.toUnsignedInt(eof.get(eof.position())));
                    Assertions.assertEquals(5, eof.remaining());
                }
                // and the packet that ends the result set is an EOF, or an OK with the 0xFE header
                ByteBuffer end = result.packet;
                Assertions.assertEquals(0xFE, Byte.toUnsignedInt(end.get(end.position())));
                Assertions.assertEquals(deprecateEof, end.remaining() > 5);
                Assertions.assertEquals(deprecateEof, result.isClientDeprecatedEofApplied());
            } finally {
                connectContext.setThreadLocalInfo();
            }
        }
    }

    // A query whose backend rpc failed is run again under a new query id on a MySQL connection: its
    // client has seen nothing of the failed attempt. A Flight session gets the failure instead,
    // because the backend endpoints the failed attempt registered would have to be withdrawn
    // first. The mocked backend answers the first exec_plan_fragment rpc with a TIMEOUT status,
    // which the coordinator raises as the RpcException the executor retries on.
    @Test
    public void testAFailedQueryIsRetriedOnlyWhereTheClientCannotTell() throws Exception {
        createTable("create table retry_tbl (k int) distributed by hash(k) buckets 1"
                + " properties ('replication_num' = '1')");
        try {
            ConnectContext mysql = createDefaultCtx();
            mysql.setDatabase(DB_NAME);
            mysql.setThreadLocalInfo();
            // The table is empty; without this the scan folds into an empty relation the frontend
            // answers by itself and no backend rpc is ever sent.
            mysql.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            StmtExecutor retried = executorFor(mysql, "select k from retry_tbl");
            TUniqueId firstQueryId = new TUniqueId(1, 1);
            int callsBefore = DefaultPBackendServiceImpl.getExecPlanFragmentCalls();
            DefaultPBackendServiceImpl.failNextExecPlanFragments(1);

            retried.execute(firstQueryId);

            Assertions.assertEquals(MysqlStateType.EOF, mysql.getState().getStateType(),
                    mysql.getState().getErrorMessage());
            // The rpc that failed, then at least the one that succeeded, under a new query id.
            Assertions.assertEquals(0, DefaultPBackendServiceImpl.getPendingExecPlanFragmentFailures());
            Assertions.assertTrue(DefaultPBackendServiceImpl.getExecPlanFragmentCalls() - callsBefore >= 2);
            Assertions.assertNotEquals(firstQueryId, mysql.queryId());

            ConnectContext flight = flightContext();
            flight.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            StmtExecutor failed = executorFor(flight, "select k from retry_tbl");
            TUniqueId flightQueryId = new TUniqueId(2, 2);
            DefaultPBackendServiceImpl.failNextExecPlanFragments(1);

            RpcException e = Assertions.assertThrows(RpcException.class, () -> failed.execute(flightQueryId));

            Assertions.assertTrue(e.getMessage().contains("injected exec_plan_fragment timeout"), e.getMessage());
            Assertions.assertEquals(0, DefaultPBackendServiceImpl.getPendingExecPlanFragmentFailures());
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertEquals(flightQueryId, flight.queryId());
        } finally {
            DefaultPBackendServiceImpl.failNextExecPlanFragments(0);
            connectContext.setThreadLocalInfo();
            dropTable("retry_tbl", true);
        }
    }

    // A query that fails with an error the statement is replanned on is run again by queryRetry
    // without going back through the processor: it is the executor that starts every attempt
    // where the statement started. The first attempt of this Flight query moved its result to
    // the backend and registered an endpoint there before it failed; the second fails while it
    // is planned, before it could run a query. The session must not be left believing a result
    // is waiting on the backend -- that state would keep its cleanup (StatementContext.close,
    // the query registration) waiting for a DoGet that never comes -- and the endpoint the failed
    // attempt registered must not be delivered.
    @Test
    public void testEveryAttemptOfAReplannedFlightQueryStartsOnTheFrontend() throws Exception {
        createTable("create table replan_tbl (k int) distributed by hash(k) buckets 1"
                + " properties ('replication_num' = '1')");
        try {
            ConnectContext flight = flightContext();
            flight.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            StmtExecutor executor = Mockito.spy(executorFor(flight, "select k from replan_tbl"));
            flight.setExecutor(executor);
            // The second attempt fails before its query is run.
            Mockito.doCallRealMethod().doThrow(new AnalysisException("injected planning failure"))
                    .when(executor).checkBlockRulesByRegex(Mockito.any());
            DefaultPBackendServiceImpl.failNextExecPlanFragments(1,
                    SystemInfoService.ERROR_E230 + " injected replan error");

            executor.execute();

            Assertions.assertEquals(0, DefaultPBackendServiceImpl.getPendingExecPlanFragmentFailures());
            Mockito.verify(executor, Mockito.times(2)).checkBlockRulesByRegex(Mockito.any());
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertTrue(flight.getState().getErrorMessage().contains("injected planning failure"),
                    flight.getState().getErrorMessage());
            Assertions.assertTrue(flight.isReturnResultFromLocal());
            Assertions.assertTrue(flight.getFlightSqlEndpointsLocations().isEmpty());
        } finally {
            DefaultPBackendServiceImpl.failNextExecPlanFragments(0);
            connectContext.setThreadLocalInfo();
            dropTable("replan_tbl", true);
        }
    }

    // A Flight request may deliver one result only, and it must be the last statement's: a query
    // that ran on the backend and turns out not to be the last statement is refused by the adapter
    // after it executed. What executed is audited (and counted as a query) whatever the adapter
    // decides next; the audit row of the refused query carries the refusal.
    @Test
    public void testARefusedFlightStatementIsAuditedBeforeTheRequestStops() throws Exception {
        createTable("create table audit_tbl (k int) distributed by hash(k) buckets 1"
                + " properties ('replication_num' = '1')");
        try {
            ConnectContext flight = flightContext();
            flight.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            List<String> auditedStatements = new ArrayList<>();
            List<MysqlStateType> auditedStates = new ArrayList<>();
            List<ErrorCode> auditedErrorCodes = new ArrayList<>();
            FlightSqlConnectProcessor processor = Mockito.spy(new FlightSqlConnectProcessor(flight));
            Mockito.doAnswer(invocation -> {
                auditedStatements.add(invocation.getArgument(0));
                auditedStates.add(flight.getState().getStateType());
                auditedErrorCodes.add(flight.getState().getErrorCode());
                return null;
            }).when(processor).auditAfterExec(Mockito.anyString(), Mockito.any(StatementBase.class), Mockito.any(),
                    Mockito.anyBoolean());
            try {
                processor.handleQuery("select k from audit_tbl; set @x = 1");
            } finally {
                processor.close();
            }

            // The request stopped at the refused query: it was audited, with the refusal, and the
            // SET after it never ran.
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());
            Assertions.assertEquals(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT,
                    flight.getState().getErrorCode());
            Assertions.assertEquals(List.of("select k from audit_tbl"), auditedStatements);
            Assertions.assertEquals(List.of(MysqlStateType.ERR), auditedStates);
            Assertions.assertEquals(List.of(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT), auditedErrorCodes);
        } finally {
            connectContext.setThreadLocalInfo();
            dropTable("audit_tbl", true);
        }
    }

    // An internal executor (the dry run of RefreshMTMVCommand) answers the client of the session
    // that issued the statement, encoded with that client's capabilities: it is handed the
    // caller's sender, as its own session is connected to nobody.
    @Test
    public void testAnInternalExecutorAnswersOnTheCallersConnection() throws Exception {
        // The caller's client did not deprecate EOF; an internal session's default capabilities do.
        RecordingMysqlChannel callerChannel = new RecordingMysqlChannel();
        ConnectContext caller = new ConnectContext(new MysqlProtocolAdapter(callerChannel));
        ConnectContext internal = createDefaultCtx();
        internal.setDatabase(DB_NAME);
        internal.setThreadLocalInfo();
        try {
            StmtExecutor executor = executorFor(internal, "select 1");

            executor.executeInternalQueryAndSend((LogicalPlanAdapter) executor.getParsedStmt(),
                    caller.getResultSender());

            // column count, one column definition, the terminator the caller's client expects, one row
            List<RecordedPacket> packets = callerChannel.getOutbound();
            Assertions.assertEquals(4, packets.size());
            Assertions.assertArrayEquals(new byte[] {1}, packets.get(0).getPayload());
            Assertions.assertEquals(0xFE, Byte.toUnsignedInt(packets.get(2).getPayload()[0]));
            Assertions.assertEquals(5, packets.get(2).getPayload().length);
            Assertions.assertArrayEquals(new byte[] {1, '1'}, packets.get(3).getPayload());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    // An executor the way the connect processor builds one: the parsed statement carries the
    // original text, and the statement context knows it (so a NereidsCoordinator runs the query).
    private static StmtExecutor executorFor(ConnectContext ctx, String sql) {
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(sql, 0));
        ctx.setStatementContext(statementContext);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, statementContext);
        adapter.setOrigStmt(statementContext.getOriginStatement());
        return new StmtExecutor(ctx, adapter);
    }

    private ConnectContext flightContext() {
        ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setDatabase(DB_NAME);
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
