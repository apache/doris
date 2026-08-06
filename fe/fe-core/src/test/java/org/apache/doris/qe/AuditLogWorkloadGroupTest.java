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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.proto.Data;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.service.arrowflight.FlightSqlConnectProcessor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests that the workload_group field on ConnectContext (which is written into the
 * audit_log by {@link AuditLogHelper}) is always resolved before any command that
 * may emit an audit log is dispatched. Covers every code path that writes the audit
 * log when a query fails before reaching Coordinator.exec().
 */
public class AuditLogWorkloadGroupTest {

    private static final String SESSION_WORKLOAD_GROUP = "wg_session";
    private static final String USER_PROPERTY_WORKLOAD_GROUP = "wg_user";

    private boolean originalEnableWorkloadGroup;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        originalEnableWorkloadGroup = Config.enable_workload_group;
        Config.enable_workload_group = true;
    }

    @After
    public void tearDown() {
        Config.enable_workload_group = originalEnableWorkloadGroup;
        ConnectContext.remove();
    }

    // ---------- Direct tests of resolveWorkloadGroupName() ---------- //

    @Test
    public void testResolveWorkloadGroupNameDisabled() {
        Config.enable_workload_group = false;
        ConnectContext ctx = newContextWithSessionWorkloadGroup(SESSION_WORKLOAD_GROUP);
        // Pre-populate to confirm the method always clears first.
        ctx.setWorkloadGroupName("stale");

        runResolveUnderMockEnv(ctx, null);

        Assert.assertEquals("", ctx.getWorkloadGroupName());
    }

    @Test
    public void testResolveWorkloadGroupNameFromSessionVariable() {
        ConnectContext ctx = newContextWithSessionWorkloadGroup(SESSION_WORKLOAD_GROUP);

        runResolveUnderMockEnv(ctx, USER_PROPERTY_WORKLOAD_GROUP);

        // Session variable has highest priority.
        Assert.assertEquals(SESSION_WORKLOAD_GROUP, ctx.getWorkloadGroupName());
    }

    @Test
    public void testResolveWorkloadGroupNameFromUserProperty() {
        ConnectContext ctx = newContextWithSessionWorkloadGroup("");

        runResolveUnderMockEnv(ctx, USER_PROPERTY_WORKLOAD_GROUP);

        Assert.assertEquals(USER_PROPERTY_WORKLOAD_GROUP, ctx.getWorkloadGroupName());
    }

    @Test
    public void testResolveWorkloadGroupNameFallbackToDefault() {
        ConnectContext ctx = newContextWithSessionWorkloadGroup("");

        runResolveUnderMockEnv(ctx, null);

        Assert.assertEquals(WorkloadGroupMgr.DEFAULT_GROUP_NAME, ctx.getWorkloadGroupName());
    }

    // ---------- Entry-point tests: every audit-logging code path ---------- //

    @Test
    public void testMysqlDispatchResolvesBeforeComQuery() throws Exception {
        verifyDispatchResolvesForCommand(buildQueryPacket("select 1"));
    }

    @Test
    public void testMysqlDispatchResolvesBeforeComStmtPrepare() throws Exception {
        ByteBuffer pkt = buildPacketWithCode(COM_STMT_PREPARE_CODE,
                "select 1".getBytes(StandardCharsets.UTF_8));
        verifyDispatchResolvesForCommand(pkt);
    }

    @Test
    public void testMysqlDispatchResolvesBeforeComStmtExecute() throws Exception {
        // COM_STMT_EXECUTE payload: stmtId(4) + flags(1) + iteration_count(4). We just
        // need valid bytes; the processor returns early because no prepared stmt exists,
        // which is fine — we only care that resolveWorkloadGroupName() ran first.
        ByteBuffer pkt = ByteBuffer.allocate(1 + 4 + 1 + 4);
        pkt.put(COM_STMT_EXECUTE_CODE);
        for (int i = 0; i < 9; i++) {
            pkt.put((byte) 0);
        }
        pkt.flip();
        verifyDispatchResolvesForCommand(pkt);
    }

    @Test
    public void testMysqlDispatchResolvesBeforeComFieldList() throws Exception {
        byte[] tableName = "t\0".getBytes(StandardCharsets.UTF_8);
        ByteBuffer pkt = ByteBuffer.allocate(1 + tableName.length);
        pkt.put(COM_FIELD_LIST_CODE);
        pkt.put(tableName);
        pkt.flip();
        verifyDispatchResolvesForCommand(pkt);
    }

    @Test
    public void testFlightSqlHandleQueryResolvesWorkloadGroup() throws Exception {
        ConnectContext ctx = newContextWithSessionWorkloadGroup(SESSION_WORKLOAD_GROUP);
        RecordingFlightSqlProcessor processor = new RecordingFlightSqlProcessor(ctx);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockEnvAuth(mockedEnv, null);
            try {
                processor.handleQuery("select 1");
            } catch (ConnectionException ignored) {
                // ignored — this unit test does not run a real query pipeline.
            } catch (RuntimeException ignored) {
                // ignored
            }
        }

        Assert.assertTrue("resolveWorkloadGroupName must be called before super.handleQuery",
                processor.resolvedBeforeHandleQuery);
        Assert.assertEquals(SESSION_WORKLOAD_GROUP, ctx.getWorkloadGroupName());
    }

    // ---------- Multi-statement per-iteration re-resolve ---------- //

    /**
     * Regression test for the "stale audit workload_group" issue on multi-statement
     * requests. {@link ConnectProcessor#executeQuery} splits a single packet into
     * N statements. If an earlier statement in the batch (effectively) changed
     * {@code @@workload_group}, a later statement that fails before
     * {@code Coordinator.exec} must still be audited with the post-change value,
     * because {@link ConnectProcessor#resolveWorkloadGroupName} is invoked at the
     * top of every loop iteration — not only once per packet in dispatch().
     */
    @Test
    public void testExecuteQueryResolvesWorkloadGroupPerStatement() throws Exception {
        final String firstStmtWg = "wg_first";
        final String secondStmtWg = "wg_second";

        ConnectContext ctx = newContextWithSessionWorkloadGroup(firstStmtWg);

        List<StatementBase> parsedStmts = Arrays.asList(
                Mockito.mock(StatementBase.class),
                Mockito.mock(StatementBase.class));
        List<String> auditedWorkloadGroups = new ArrayList<>();
        int[] resolveCallCount = new int[]{0};

        MultiStmtRecordingProcessor processor = new MultiStmtRecordingProcessor(
                ctx, parsedStmts, auditedWorkloadGroups, resolveCallCount);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedConstruction<StmtExecutor> mc = Mockito.mockConstruction(StmtExecutor.class,
                        Mockito.withSettings().defaultAnswer(Mockito.RETURNS_DEEP_STUBS),
                        (mock, mockCtx) -> {
                            if (mockCtx.getCount() == 1) {
                                // First statement simulates "SET workload_group = wg_second"
                                // by mutating the session variable inside execute().
                                Mockito.doAnswer(inv -> {
                                    ctx.getSessionVariable().setWorkloadGroup(secondStmtWg);
                                    return null;
                                }).when(mock).execute();
                            }
                        })) {
            mockEnvAuth(mockedEnv, null);
            try {
                processor.executeQuery("select 1;select 2");
            } catch (Exception ignored) {
                // Not running a real query pipeline; tolerate any pipeline failures.
            }
        }

        // resolveWorkloadGroupName() must be called at least once per statement,
        // in addition to the caller-site invocation verified by the dispatch tests.
        Assert.assertTrue("resolveWorkloadGroupName must be called for every statement"
                        + " in the multi-stmt loop, got " + resolveCallCount[0],
                resolveCallCount[0] >= 2);
        // Both statements must be audited.
        Assert.assertEquals(2, auditedWorkloadGroups.size());
        // Statement 1 is audited with the initial session-variable value.
        Assert.assertEquals(firstStmtWg, auditedWorkloadGroups.get(0));
        // Statement 2 must be audited with the post-change value — *not* the stale
        // value that the old once-per-packet resolve would have left on ctx.
        Assert.assertEquals(secondStmtWg, auditedWorkloadGroups.get(1));
    }

    // ---------- Helpers ---------- //

    private void verifyDispatchResolvesForCommand(ByteBuffer packet) throws Exception {
        ConnectContext ctx = newContextWithSessionWorkloadGroup(SESSION_WORKLOAD_GROUP);
        ctx.setWorkloadGroupName("stale");
        RecordingMysqlProcessor processor = new RecordingMysqlProcessor(ctx);
        // inject packet into the private packetBuf field
        java.lang.reflect.Field f = MysqlConnectProcessor.class.getDeclaredField("packetBuf");
        f.setAccessible(true);
        f.set(processor, packet);

        Method dispatch = MysqlConnectProcessor.class.getDeclaredMethod("dispatch");
        dispatch.setAccessible(true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockEnvAuth(mockedEnv, null);
            try {
                dispatch.invoke(processor);
            } catch (Exception ignored) {
                // The real command handlers are short-circuited; failures are fine.
            }
        }

        Assert.assertTrue("resolveWorkloadGroupName must be invoked by dispatch()",
                processor.resolveCalled);
        // The resolution must have set the session-variable value, not left the stale one.
        Assert.assertEquals(SESSION_WORKLOAD_GROUP, ctx.getWorkloadGroupName());
    }

    private ConnectContext newContextWithSessionWorkloadGroup(String wg) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.getSessionVariable().setWorkloadGroup(wg);
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /**
     * Invokes resolveWorkloadGroupName() through a minimal subclass while mocking
     * Env.getCurrentEnv().getAuth().getWorkloadGroup() to return {@code userGroup}.
     */
    private void runResolveUnderMockEnv(ConnectContext ctx, String userGroup) {
        DirectResolveProcessor processor = new DirectResolveProcessor(ctx);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockEnvAuth(mockedEnv, userGroup);
            processor.resolveWorkloadGroupName();
        }
    }

    private void mockEnvAuth(MockedStatic<Env> mockedEnv, String userGroup) {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getWorkloadGroup(Mockito.anyString())).thenReturn(userGroup);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    private ByteBuffer buildQueryPacket(String sql) {
        return buildPacketWithCode(COM_QUERY_CODE, sql.getBytes(StandardCharsets.UTF_8));
    }

    // MysqlCommand code constants (see MysqlCommand.java).
    private static final byte COM_QUERY_CODE = 3;
    private static final byte COM_FIELD_LIST_CODE = 4;
    private static final byte COM_STMT_PREPARE_CODE = 22;
    private static final byte COM_STMT_EXECUTE_CODE = 23;

    private ByteBuffer buildPacketWithCode(byte code, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(1 + payload.length);
        buf.put(code);
        buf.put(payload);
        buf.flip();
        return buf;
    }

    // ---------- Recording subclasses ---------- //

    /** Subclass used purely to invoke the protected resolveWorkloadGroupName(). */
    private static final class DirectResolveProcessor extends ConnectProcessor {
        DirectResolveProcessor(ConnectContext ctx) {
            super(ctx);
        }

        @Override
        public void processOnce() {
        }
    }

    /**
     * Records whether resolveWorkloadGroupName() was called by dispatch() and
     * short-circuits every handler so the test does not have to mock the full
     * query/field-list pipeline.
     */
    private static final class RecordingMysqlProcessor extends MysqlConnectProcessor {
        boolean resolveCalled;

        RecordingMysqlProcessor(ConnectContext ctx) {
            super(ctx);
        }

        @Override
        protected void resolveWorkloadGroupName() {
            resolveCalled = true;
            super.resolveWorkloadGroupName();
        }

        @Override
        public void executeQuery(String originStmt) {
            // short-circuit; do not run the real query pipeline.
        }

        @Override
        protected void handleFieldList(String tableName) {
        }

        @Override
        protected void handleExecute(
                org.apache.doris.nereids.trees.plans.commands.PrepareCommand command,
                long stmtId,
                PreparedStatementContext prepCtx,
                ByteBuffer packetBuf,
                org.apache.doris.thrift.TUniqueId queryId) {
        }

        // processOnce is not used here; dispatch() is invoked directly via reflection.
    }

    /**
     * Records that resolveWorkloadGroupName() was invoked before super.handleQuery()
     * is called inside FlightSqlConnectProcessor.handleQuery.
     */
    private static final class RecordingFlightSqlProcessor extends FlightSqlConnectProcessor {
        boolean resolveCalled;
        boolean resolvedBeforeHandleQuery;

        RecordingFlightSqlProcessor(ConnectContext ctx) {
            super(ctx);
        }

        @Override
        protected void resolveWorkloadGroupName() {
            resolveCalled = true;
            super.resolveWorkloadGroupName();
        }

        @Override
        public void executeQuery(String originStmt) {
            // Record that resolve ran before the query pipeline starts.
            resolvedBeforeHandleQuery = resolveCalled;
        }
    }

    /**
     * Test harness for the multi-statement per-iteration resolve test. Overrides
     * {@code parseWithFallback} to return a pre-built statement list and
     * {@code auditAfterExec} to record the workload group at audit time. Every
     * call to {@code resolveWorkloadGroupName} is counted.
     */
    private static final class MultiStmtRecordingProcessor extends ConnectProcessor {
        private final List<StatementBase> parsedStmts;
        private final List<String> auditedWorkloadGroups;
        private final int[] resolveCallCount;

        MultiStmtRecordingProcessor(ConnectContext ctx, List<StatementBase> parsedStmts,
                List<String> auditedWorkloadGroups, int[] resolveCallCount) {
            super(ctx);
            this.connectType = ConnectType.MYSQL;
            this.parsedStmts = parsedStmts;
            this.auditedWorkloadGroups = auditedWorkloadGroups;
            this.resolveCallCount = resolveCallCount;
        }

        @Override
        protected void resolveWorkloadGroupName() {
            resolveCallCount[0]++;
            super.resolveWorkloadGroupName();
        }

        @Override
        protected List<StatementBase> parseWithFallback(String originStmt, String convertedStmt,
                SessionVariable sessionVariable) {
            return parsedStmts;
        }

        @Override
        protected void auditAfterExec(String origStmt, StatementBase parsedStmt,
                Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
            // Capture the resolved workload group at the moment the audit is emitted,
            // which is exactly the value that would be written into audit_log.
            auditedWorkloadGroups.add(ctx.getWorkloadGroupName());
        }

        @Override
        public void processOnce() {
        }
    }
}
