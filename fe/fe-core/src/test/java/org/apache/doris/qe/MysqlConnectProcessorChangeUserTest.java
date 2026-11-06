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

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.Principal;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlConnectProcessorChangeUserTest {
    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private static final UserIdentity BOB = UserIdentity.createAnalyzedUserIdentWithIp("bob", "%");
    private static final NameSpaceContext ORIGINAL_NAMESPACE =
            new NameSpaceContext("original_catalog", "original_db", 17);

    private boolean savedRunningUnitTest;
    private Env env;
    private Auth auth;
    private MockedStatic<Env> mockedEnv;
    private ConnectPoolMgr pool;
    private ConnectContext context;
    private MysqlChannel channel;
    private MysqlConnectProcessor processor;
    private final List<byte[]> responses = new ArrayList<>();
    private final AtomicInteger tempTableDrops = new AtomicInteger();
    private Runnable onResponse = () -> { };
    private Runnable onAuthentication = () -> { };

    @BeforeEach
    public void setUp() throws Exception {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = ConnectPoolTestSupport.envAllowing(1);
        auth = env.getAuth();
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        InternalCatalog catalog = env.getInternalCatalog();
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        Mockito.doAnswer(call -> {
            ByteBuffer packet = ((ByteBuffer) call.getArgument(0)).duplicate();
            byte[] bytes = new byte[packet.remaining()];
            packet.get(bytes);
            onResponse.run();
            responses.add(bytes);
            return null;
        }).when(channel).sendAndFlush(Mockito.any(ByteBuffer.class));
        MysqlProtocolAdapter adapter = new MysqlProtocolAdapter(channel);
        MysqlHandshakePacket handshake = Mockito.mock(MysqlHandshakePacket.class);
        Mockito.when(handshake.getAuthPluginData()).thenReturn(new byte[20]);
        adapter.setHandshakePacket(handshake);
        context = new ConnectContext(adapter);
        context.setEnv(env);
        context.setCurrentUserIdentity(ALICE);
        context.setRemoteIP("127.0.0.1");
        context.restoreNameSpaceContext(ORIGINAL_NAMESPACE);
        context.setConnectionId(1);
        pool = new ConnectPoolMgr(10);
        Assertions.assertEquals(-1, context.registerClientSession(pool));
        Mockito.doAnswer(call -> {
            onAuthentication.run();
            List<UserIdentity> identities = call.getArgument(4);
            identities.add(UserIdentity.createAnalyzedUserIdentWithIp(call.getArgument(0), "%"));
            return null;
        }).when(auth).checkPassword(Mockito.anyString(), Mockito.anyString(), Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.anyList());
        processor = new MysqlConnectProcessor(context) {
            @Override
            protected void resolveWorkloadGroupName() {
            }
        };
    }

    @AfterEach
    public void tearDown() {
        try {
            context.cleanup();
        } finally {
            ConnectContext.remove();
            mockedEnv.close();
            FeConstants.runningUnitTest = savedRunningUnitTest;
        }
    }

    @Test
    public void successfulChangeCommitsFreshSessionAndTransfersQuota() throws Exception {
        ComputeGroup targetGroup = prepareRouting("bob");
        SessionContext session = Mockito.mock(SessionContext.class);
        context.setSessionContext(session);
        context.setAuthenticatedRoles(Set.of("old_role"));
        context.setAuthenticatedPrincipal(Mockito.mock(Principal.class));
        context.setIsTempUser(true);
        Mockito.when(auth.getQueryTimeout("alice")).thenReturn(123);
        Mockito.when(auth.getInsertTimeout("alice")).thenReturn(456);
        Mockito.when(auth.getQueryTimeout("bob")).thenReturn(234);
        Mockito.when(auth.getInsertTimeout("bob")).thenReturn(567);
        context.getSessionVariable().setQueryTimeoutS(1);
        context.getSessionVariable().setInsertTimeoutS(2);
        context.getSessionVariable().setCloudCluster("old_group");
        context.addLastDBOfCatalog("warehouse", "old_db");
        Mockito.doAnswer(call -> {
            assertFreshAuthentication(ConnectContext.get());
            Assertions.assertEquals("bob", ConnectContext.get().getQualifiedUser());
            return 1L;
        }).when(auth).getMaxConn("bob");
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertNotSame(context, candidate);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertEquals(BOB, candidate.getCurrentUserIdentity());
            Assertions.assertEquals("127.0.0.1", candidate.getRemoteIP());
            Assertions.assertNull(candidate.getLastDBOfCatalog("warehouse"));
            candidate.changeDefaultCatalog(call.getArgument(1));
            return null;
        }).when(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.eq("warehouse"));
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertSame(SessionContext.empty(), SessionContext.current());
            Assertions.assertNotSame(context.getSessionVariable(), candidate.getSessionVariable());
            candidate.restoreNameSpaceContext(new NameSpaceContext(candidate.getDefaultCatalog(), "new_db", 29));
            return null;
        }).when(env).changeDb(Mockito.any(ConnectContext.class), Mockito.eq("new_db"));

        changeUser("bob", "warehouse.new_db");

        assertOk();
        Assertions.assertEquals(BOB, context.getCurrentUserIdentity());
        assertNamespace("warehouse", "new_db", 29);
        assertCounts(0, 1, 1);
        assertFreshAuthentication(context);
        Assertions.assertEquals(234, context.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals(567, context.getSessionVariable().getInsertTimeoutS());
        Assertions.assertEquals("", context.getSessionVariable().getCloudCluster());
        assertFreshRouting(targetGroup);
        Assertions.assertNull(context.getLastDBOfCatalog("warehouse"));
        Assertions.assertNull(ConnectContext.get());
        ping();
        context.cleanup();
        assertCounts(0, 0, 0);
    }

    @Test
    public void emptyDatabaseClearsNamespaceOnlyAfterSuccessfulChange() throws Exception {
        changeUser("bob", "");

        assertOk();
        Assertions.assertEquals(BOB, context.getCurrentUserIdentity());
        assertNamespace(InternalCatalog.INTERNAL_CATALOG_NAME, "", -1);
        assertCounts(0, 1, 1);
    }

    @ParameterizedTest
    @CsvSource({"'', warehouse, ''", "inventory, warehouse, inventory", "other.inventory, other, inventory"})
    public void targetInitialCatalogAppliesBeforeRequestedDatabase(String requestedDb, String expectedCatalog,
            String expectedDb) throws Exception {
        configureInitialCatalog();
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertEquals(expectedCatalog, candidate.getDefaultCatalog());
            candidate.restoreNameSpaceContext(new NameSpaceContext(candidate.getDefaultCatalog(),
                    call.getArgument(1), 29));
            return null;
        }).when(env).changeDb(Mockito.any(ConnectContext.class), Mockito.anyString());

        changeUser("bob", requestedDb);

        assertOk();
        assertNamespace(expectedCatalog, expectedDb, expectedDb.isEmpty() ? -1 : 29);
        Mockito.verify(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.eq("warehouse"));
        assertCounts(0, 1, 1);
    }

    @Test
    public void initialCatalogFailureSendsErrorBeforeClosing() throws Exception {
        configureInitialCatalog();
        Mockito.doThrow(new DdlException("initial catalog unavailable", ErrorCode.ERR_BAD_DB_ERROR))
                .when(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.eq("warehouse"));
        expectErrorBeforeClose(1, 0, 1);

        changeUser("bob", "");

        assertError(ErrorCode.ERR_BAD_DB_ERROR);
        assertOriginalIdentityAndNamespace();
        assertCounts(0, 0, 0);
        assertDisconnected();
    }

    @Test
    public void authenticationFailureSendsErrorBeforeClosingOriginalSession() throws Exception {
        TransactionEntry txn = populateSessionState();
        rejectAuthentication("bob");
        expectErrorBeforeClose(1, 0, 1);

        changeUser("bob", "");

        assertError(ErrorCode.ERR_ACCESS_DENIED_ERROR);
        assertOriginalIdentityAndNamespace();
        assertResetState();
        Mockito.verify(txn).abortTransaction();
        assertCounts(0, 0, 0);
        Mockito.verify(auth, Mockito.never()).getMaxConn("bob");
        assertDisconnected();
    }

    @Test
    public void quotaRefusalSendsErrorAndClosesOnlyTheChangingConnection() throws Exception {
        ConnectContext existingBob = ConnectPoolTestSupport.mysqlConnection(env, BOB);
        existingBob.setConnectionId(2);
        Assertions.assertEquals(-1, existingBob.registerClientSession(pool));
        try {
            expectErrorBeforeClose(1, 1, 2);
            changeUser("bob", "");

            assertError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS);
            assertOriginalIdentityAndNamespace();
            assertCounts(0, 1, 1);
            Assertions.assertSame(existingBob, pool.getContext(2));
            assertDisconnected();
        } finally {
            existingBob.cleanup();
        }
    }

    @Test
    public void databaseFailureDoesNotCommitIdentityOrPartialCatalogChange() throws Exception {
        context.getSessionVariable().setCloudCluster("original_group");
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            candidate.changeDefaultCatalog("warehouse");
            return null;
        }).when(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.eq("warehouse"));
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertNotSame(context, candidate);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertEquals(BOB, candidate.getCurrentUserIdentity());
            Assertions.assertNotSame(context.getSessionVariable(), candidate.getSessionVariable());
            candidate.getSessionVariable().setCloudCluster("rejected_group");
            throw new DdlException("Unknown database missing", ErrorCode.ERR_BAD_DB_ERROR);
        }).when(env).changeDb(Mockito.any(ConnectContext.class), Mockito.eq("missing"));
        expectErrorBeforeClose(1, 0, 1);

        changeUser("bob", "warehouse.missing");

        assertError(ErrorCode.ERR_BAD_DB_ERROR);
        assertOriginalIdentityAndNamespace();
        assertCounts(0, 0, 0);
        // The old value was reset before authentication; the candidate's rejected value was not committed.
        Assertions.assertEquals("", context.getSessionVariable().getCloudCluster());
        Assertions.assertNull(ConnectContext.get());
        assertDisconnected();
    }

    @Test
    public void sameUserChangeResetsSessionBeforeAuthentication() throws Exception {
        ComputeGroup targetGroup = prepareRouting("alice");
        Mockito.when(auth.getMaxConn("alice")).thenReturn(2L);
        Mockito.when(auth.getQueryTimeout("alice")).thenReturn(123);
        Mockito.when(auth.getInsertTimeout("alice")).thenReturn(456);
        TransactionEntry txn = populateSessionState();
        context.setSessionContext(Mockito.mock(SessionContext.class));
        context.setAuthenticatedPrincipal(Mockito.mock(Principal.class));
        context.setAuthenticatedRoles(Set.of("old_role"));
        context.setIsTempUser(true);

        changeUser("alice", "");

        assertOk();
        Assertions.assertEquals(ALICE, context.getCurrentUserIdentity());
        assertResetState();
        assertFreshAuthentication(context);
        assertFreshRouting(targetGroup);
        Assertions.assertEquals(123, context.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals(456, context.getSessionVariable().getInsertTimeoutS());
        assertNamespace(InternalCatalog.INTERNAL_CATALOG_NAME, "", -1);
        assertCounts(1, 0, 1);
        Mockito.verify(txn).abortTransaction();
        ping();
    }

    @Test
    public void sameUserAuthenticationFailureStillResetsThenCloses() throws Exception {
        TransactionEntry txn = populateSessionState();
        rejectAuthentication("alice");
        expectErrorBeforeClose(1, 0, 1);

        changeUser("alice", "");

        assertError(ErrorCode.ERR_ACCESS_DENIED_ERROR);
        assertResetState();
        Mockito.verify(txn).abortTransaction();
        assertCounts(0, 0, 0);
        assertDisconnected();
    }

    @Test
    public void sameUserAtQuotaLimitStillResetsThenCloses() throws Exception {
        TransactionEntry txn = populateSessionState();
        expectErrorBeforeClose(1, 0, 1);

        changeUser("alice", "");

        assertError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS);
        assertResetState();
        Mockito.verify(txn).abortTransaction();
        assertCounts(0, 0, 0);
        assertDisconnected();
    }

    @Test
    public void resetFailureSendsErrorAndDoesNotAuthenticate() throws Exception {
        TransactionEntry txn = Mockito.mock(TransactionEntry.class);
        Mockito.when(txn.isTxnModel()).thenReturn(true);
        Mockito.when(txn.abortTransaction()).thenThrow(new Exception("rollback failed")).thenReturn(1L);
        context.setTxnEntry(txn);
        expectErrorBeforeClose(1, 0, 1);

        changeUser("bob", "");

        assertError(ErrorCode.ERR_UNKNOWN_ERROR);
        Assertions.assertTrue(context.getState().getErrorMessage().contains("rollback transaction failed"));
        Mockito.verify(auth, Mockito.never()).checkPassword(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.anyList());
        Mockito.verify(txn, Mockito.times(2)).abortTransaction();
        Assertions.assertNull(context.getTxnEntry());
        assertCounts(0, 0, 0);
        assertDisconnected();
    }

    private TransactionEntry populateSessionState() throws Exception {
        TransactionEntry txn = Mockito.mock(TransactionEntry.class);
        Mockito.when(txn.isTxnModel()).thenReturn(true);
        context.setTxnEntry(txn);
        VariableMgr.setVar(context.getSessionVariable(),
                new SetVar(SessionVariable.SQL_SELECT_LIMIT, new StringLiteral("0")));
        context.getSessionVariable().setQueryTimeoutS(1);
        context.getSessionVariable().setInsertTimeoutS(2);
        context.setUserVar("old_var", new StringLiteral("old_value"));
        context.addLastDBOfCatalog("remembered_catalog", "remembered_db");
        context.getSessionVariable().enableServeSidePreparedStatement = true;
        PrepareCommand preparedCommand = Mockito.mock(PrepareCommand.class);
        Mockito.when(preparedCommand.placeholderCount()).thenReturn(1);
        PreparedStatementContext prepared = new PreparedStatementContext(preparedCommand, context, null, "select ?");
        prepared.appendLongData(0, ByteBuffer.wrap(new byte[] {1, 2, 3}), 1024);
        prepared.setEmptyCursorOpen(true);
        context.addPreparedStatementContext("7", prepared);
        Assertions.assertTrue(prepared.hasLongData());
        Assertions.assertTrue(prepared.isEmptyCursorOpen());
        context.setQueryId(new TUniqueId(11, 12));
        context.resetQueryId();
        context.setQueryId(new TUniqueId(13, 14));
        context.setTraceId("old_trace");
        ConnectScheduler scheduler = Mockito.mock(ConnectScheduler.class);
        context.setConnectScheduler(scheduler);
        context.updateReturnRows(10);
        context.setOrUpdateInsertResult(1, "label", "original_db", "old_table", TransactionStatus.VISIBLE, 1, 0);

        context.addTempTableToDB("original_db", "old_temp");
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        InternalCatalog catalog = env.getInternalCatalog();
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(catalog.getDb("original_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getTable("old_temp")).thenReturn(Optional.of(table));
        Mockito.doAnswer(call -> {
            context.removeTempTableFromDB("original_db", "old_temp");
            tempTableDrops.incrementAndGet();
            return null;
        }).when(catalog).dropTableWithoutCheck(database, table, false, true);
        onAuthentication = () -> {
            assertResetState();
            Mockito.verify(scheduler).removeOldTraceId("old_trace");
        };
        return txn;
    }

    private void assertResetState() {
        Assertions.assertNull(context.getTxnEntry());
        Assertions.assertEquals(1, tempTableDrops.get());
        Assertions.assertTrue(context.getDbToTempTableNamesMap().isEmpty());
        Assertions.assertTrue(context.getUserVars().isEmpty());
        Assertions.assertEquals(-1, context.getSessionVariable().getSqlSelectLimit());
        Assertions.assertNull(context.getPreparedStementContext("7"));
        Assertions.assertNull(context.queryId());
        Assertions.assertNull(context.getLastQueryId());
        Assertions.assertNull(context.traceId());
        Assertions.assertNull(context.getInsertResult());
        Assertions.assertEquals(0, context.getReturnRows());
        Assertions.assertNull(context.getLastDBOfCatalog("remembered_catalog"));
    }

    private void assertFreshAuthentication(ConnectContext ctx) {
        Assertions.assertTrue(ctx.getAuthenticatedRoles().isEmpty());
        Assertions.assertNull(ctx.getAuthenticatedPrincipal());
        Assertions.assertFalse(ctx.getIsTempUser());
        Assertions.assertSame(SessionContext.empty(), ctx.getSessionContext());
    }

    private ComputeGroup prepareRouting(String targetUser) {
        context.setComputeGroup(Mockito.mock(ComputeGroup.class));
        context.cloudCluster = "old_cached_group";
        context.setEffectiveCloudCluster("old_effective_group");
        ComputeGroup targetGroup = Mockito.mock(ComputeGroup.class);
        Mockito.when(auth.getComputeGroup(targetUser)).thenReturn(targetGroup);
        return targetGroup;
    }

    private void configureInitialCatalog() throws Exception {
        Mockito.when(auth.getInitCatalog("bob")).thenReturn("warehouse");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog("warehouse")).thenReturn(catalog);
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertEquals(BOB, candidate.getCurrentUserIdentity());
            candidate.changeDefaultCatalog(call.getArgument(1));
            return null;
        }).when(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.anyString());
    }

    private void assertFreshRouting(ComputeGroup targetGroup) throws Exception {
        Assertions.assertSame(targetGroup, context.getComputeGroup());
        Assertions.assertNull(context.cloudCluster);
        Assertions.assertNull(context.getEffectiveCloudCluster());
    }

    private void rejectAuthentication(String user) throws Exception {
        Mockito.doAnswer(call -> {
            onAuthentication.run();
            throw new AuthenticationException("wrong password");
        }).when(auth).checkPassword(Mockito.eq(user), Mockito.anyString(), Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.anyList());
    }

    private void expectErrorBeforeClose(int alice, int bob, int total) {
        onResponse = () -> {
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
            Assertions.assertTrue(context.isClosingAfterResponse());
            assertOriginalIdentityAndNamespace();
            assertCounts(alice, bob, total);
            Assertions.assertSame(context, pool.getContext(1));
            Mockito.verify(channel, Mockito.never()).close();
        };
    }

    private void assertDisconnected() {
        Assertions.assertTrue(context.isClosingOrClosed());
        Assertions.assertNull(pool.getContext(1));
        Mockito.verify(channel).close();
        Assertions.assertFalse(context.runCommand(() -> Assertions.fail("closed session accepted a command")));
    }

    private void changeUser(String user, String db) throws Exception {
        MysqlSerializer packet = MysqlSerializer.newInstance();
        packet.writeInt1(0x11); // COM_CHANGE_USER
        packet.writeNulTerminateString(user);
        packet.writeInt1(1);
        packet.writeInt1(42);
        packet.writeNulTerminateString(db);
        packet.writeInt2(33);
        packet.writeNulTerminateString(MysqlHandshakePacket.AUTH_PLUGIN_NAME);
        command(packet.toByteBuffer());
    }

    private void ping() throws Exception {
        command(ByteBuffer.wrap(new byte[] {0x0e})); // COM_PING
        assertOk();
    }

    private void command(ByteBuffer packet) throws Exception {
        int before = responses.size();
        Mockito.when(channel.fetchOnePacket()).thenReturn(packet);
        Assertions.assertTrue(context.runCommand(processor::processOnce));
        Assertions.assertEquals(before + 1, responses.size());
    }

    private void assertOk() {
        Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType(),
                context.getState().getErrorMessage());
        Assertions.assertEquals(0, Byte.toUnsignedInt(responses.get(responses.size() - 1)[0]));
    }

    private void assertError(ErrorCode code) {
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(code, context.getState().getErrorCode());
        Assertions.assertEquals(0xff, Byte.toUnsignedInt(responses.get(responses.size() - 1)[0]));
    }

    private void assertOriginalIdentityAndNamespace() {
        Assertions.assertEquals(ALICE, context.getCurrentUserIdentity());
        assertNamespace(ORIGINAL_NAMESPACE.getDefaultCatalog(), ORIGINAL_NAMESPACE.getCurrentDb(),
                ORIGINAL_NAMESPACE.getCurrentDbId());
    }

    private void assertNamespace(String catalog, String db, long dbId) {
        Assertions.assertEquals(catalog, context.getDefaultCatalog());
        Assertions.assertEquals(db, context.getDatabase());
        Assertions.assertEquals(dbId, context.getCurrentDbId());
    }

    private void assertCounts(int alice, int bob, int total) {
        Assertions.assertEquals(alice, userCount(ALICE));
        Assertions.assertEquals(bob, userCount(BOB));
        Assertions.assertEquals(total, pool.getConnectionNum());
    }

    private int userCount(UserIdentity identity) {
        AtomicInteger counter = pool.getUserConnectionMap().get(identity.getQualifiedUser());
        return counter == null ? 0 : counter.get();
    }
}
