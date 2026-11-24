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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.nereids.spm.SPMForwardedSession;
import org.apache.doris.nereids.spm.SPMUtils;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

/**
 * Building the forward request for the master FE must not reach for a MysqlChannel unconditionally:
 * an Arrow Flight SQL session has none, so every statement a Flight connection forwards (any DDL on
 * a non-master FE, or anything at all under force_forward_all_queries) used to fail with
 * "getMysqlChannel not in mysql connection". CLIENT_DEPRECATE_EOF is a MySQL protocol capability and
 * is only carried for MySQL connections.
 */
public class FEOpExecutorFlightForwardTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testFlightSessionForwardsWithoutMysqlChannel() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext context = ConnectContext.forFlight("test-peer-identity");
            prepare(context);

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertFalse(request.isSetClientDeprecatedEOF());
            Assertions.assertEquals("select 1", request.getSql());
        }
    }

    @Test
    public void testMysqlSessionKeepsCarryingDeprecatedEof() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext context = new ConnectContext();
            prepare(context);
            context.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
            context.getMysqlChannel().setClientDeprecatedEOF();

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertTrue(request.isSetClientDeprecatedEOF());
            Assertions.assertTrue(request.isClientDeprecatedEOF());
        }
    }

    @Test
    public void testMysqlSessionWithoutDeprecatedEof() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext context = new ConnectContext();
            prepare(context);
            context.setCapability(new MysqlCapability(MysqlCapability.DEFAULT_CAPABILITY.getFlags()
                    & ~MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit()));

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertTrue(request.isSetClientDeprecatedEOF());
            Assertions.assertFalse(request.isClientDeprecatedEOF());
        }
    }

    @Test
    public void testForwardedQueryExplainRetriesTransportFailure() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            Env env = Env.getCurrentEnv();
            Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
            Mockito.when(env.getMasterRpcPort()).thenReturn(9020);
            Mockito.when(env.getJournalObservable()).thenReturn(Mockito.mock(JournalObservable.class));
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            ConnectContext context = new ConnectContext();
            prepare(context);
            context.setEnv(env);

            String sql = "EXPLAIN SELECT 1";
            StatementBase statement = new NereidsParser().parseSQL(sql).get(0);
            statement.setOrigStmt(new OriginStatement(sql, 0));
            StmtExecutor executor = new StmtExecutor(context, statement);
            Deencapsulation.setField(executor, "redirectStatus", RedirectStatus.NO_FORWARD);

            FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
            TMasterOpResult result = new TMasterOpResult();
            result.setStatus(QueryState.MysqlStateType.EOF.name());
            Mockito.when(client.forward(Mockito.any()))
                    .thenThrow(new TTransportException(TTransportException.END_OF_FILE))
                    .thenReturn(result);
            GenericPool<FrontendService.Client> originalPool = ClientPool.frontendPool;
            GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
            Mockito.when(pool.borrowObject(Mockito.any(), Mockito.anyInt())).thenReturn(client);
            Mockito.when(pool.reopenOrClear(Mockito.any(), Mockito.eq(client), Mockito.anyInt())).thenReturn(true);
            ClientPool.frontendPool = pool;
            try {
                Deencapsulation.invoke(executor, "forwardToMaster");
                Mockito.verify(client, Mockito.times(2)).forward(Mockito.any());
                Mockito.verify(pool).returnObject(Mockito.any(), Mockito.eq(client));
                Assertions.assertEquals(QueryState.MysqlStateType.EOF.name(), executor.getProxyStatus());
            } finally {
                ClientPool.frontendPool = originalPool;
            }
        }
    }

    @Test
    public void testForwardedBaselineManagementRestoresDisabledRowsWithRewriteOff() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext owner = new ConnectContext();
            prepare(owner);
            owner.getSessionVariable().setEnableSpmRewrite(false);
            long enabled = addBaseline(owner, "SELECT 1 AS enabled_row", BaselineStatus.ENABLED);
            long disabled = addBaseline(owner, "SELECT 2 AS disabled_row", BaselineStatus.DISABLED);

            for (String sql : List.of("SHOW BASELINE PLANS WHERE scope = 'SESSION'",
                    "ALTER BASELINE PLAN " + disabled + " ENABLE",
                    "DROP BASELINE PLAN " + disabled,
                    "CREATE SESSION BASELINE PLAN 'SELECT 3' WITH 'SELECT 3'")) {
                String payload = buildSpmPayload(owner, sql);
                ConnectContext receiver = new ConnectContext();
                receiver.getSessionVariable().setEnableSpmRewrite(false);
                SPMForwardedSession.importInto(receiver, payload);

                Assertions.assertEquals(2, receiver.getSessionBaselineStore().getAllBaselines().size(), sql);
                Assertions.assertEquals(BaselineStatus.ENABLED,
                        receiver.getSessionBaselineStore().getBaseline(enabled).getStatus(), sql);
                Assertions.assertEquals(BaselineStatus.DISABLED,
                        receiver.getSessionBaselineStore().getBaseline(disabled).getStatus(), sql);
            }
            Assertions.assertEquals("", buildSpmPayload(owner, "SELECT 1 AS enabled_row"));
            Assertions.assertEquals("", buildSpmPayload(owner, "CREATE DATABASE unrelated_database"));
        }
    }

    private static String buildSpmPayload(ConnectContext owner, String sql) throws AnalysisException {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getParsedStmt()).thenReturn(new NereidsParser().parseSQL(sql).get(0));
        owner.setExecutor(executor);
        return new TestFEOpExecutor(owner, sql).build().getSessionVariables()
                .get(SessionVariable.SPM_FORWARDED_SESSION_BASELINES);
    }

    private static long addBaseline(ConnectContext owner, String sql, BaselineStatus status) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql(sql);
        plan.setPlanSql(sql);
        plan.setPlanFrozen(false);
        plan.setBindSqlDigest(sql);
        plan.setBindSqlHash(SPMUtils.hashOf(sql));
        plan.setStatus(status);
        return owner.getSessionBaselineStore().createBaseline(plan);
    }

    private static MockedStatic<Env> mockSelfNode() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }

    private static void prepare(ConnectContext context) {
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        context.setRemoteIP("127.0.0.1");
    }

    private static class TestFEOpExecutor extends FEOpExecutor {
        private TestFEOpExecutor(ConnectContext context) {
            this(context, "select 1");
        }

        private TestFEOpExecutor(ConnectContext context, String sql) {
            super(new TNetworkAddress("127.0.0.1", 9010), new OriginStatement(sql, 0), context, true);
        }

        private TMasterOpRequest build() throws AnalysisException {
            return buildStmtForwardParams();
        }
    }
}
