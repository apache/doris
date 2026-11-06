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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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

    @BeforeEach
    public void setUp() throws Exception {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = ConnectPoolTestSupport.envAllowing(1);
        auth = env.getAuth();
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        Mockito.doAnswer(call -> {
            ByteBuffer packet = ((ByteBuffer) call.getArgument(0)).duplicate();
            byte[] bytes = new byte[packet.remaining()];
            packet.get(bytes);
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
    public void successfulChangeCommitsValidatedNamespaceAndTransfersQuota() throws Exception {
        SessionContext session = Mockito.mock(SessionContext.class);
        context.setSessionContext(session);
        context.setAuthenticatedRoles(Set.of("role"));
        Mockito.doAnswer(call -> {
            Assertions.assertEquals("bob", ConnectContext.get().getQualifiedUser());
            Assertions.assertEquals(Set.of("role"), ConnectContext.get().getAuthenticatedRoles());
            return 1L;
        }).when(auth).getMaxConn("bob");
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertNotSame(context, candidate);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertEquals(BOB, candidate.getCurrentUserIdentity());
            Assertions.assertEquals("127.0.0.1", candidate.getRemoteIP());
            candidate.changeDefaultCatalog(call.getArgument(1));
            return null;
        }).when(env).changeCatalog(Mockito.any(ConnectContext.class), Mockito.eq("warehouse"));
        Mockito.doAnswer(call -> {
            ConnectContext candidate = call.getArgument(0);
            Assertions.assertSame(candidate, ConnectContext.get());
            Assertions.assertSame(session, SessionContext.current());
            Assertions.assertNotSame(context.getSessionVariable(), candidate.getSessionVariable());
            candidate.restoreNameSpaceContext(new NameSpaceContext(candidate.getDefaultCatalog(), "new_db", 29));
            return null;
        }).when(env).changeDb(Mockito.any(ConnectContext.class), Mockito.eq("new_db"));

        changeUser("bob", "warehouse.new_db");

        assertOk();
        Assertions.assertEquals(BOB, context.getCurrentUserIdentity());
        assertNamespace("warehouse", "new_db", 29);
        assertCounts(0, 1, 1);
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

    @Test
    public void authenticationFailureKeepsOriginalSessionAndQuota() throws Exception {
        Mockito.doThrow(new AuthenticationException("wrong password")).when(auth).checkPassword(
                Mockito.eq("bob"), Mockito.anyString(), Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.anyList());

        changeUser("bob", "");

        assertError(ErrorCode.ERR_ACCESS_DENIED_ERROR);
        assertOriginalSession();
        assertCounts(1, 0, 1);
        Mockito.verify(auth, Mockito.never()).getMaxConn("bob");
        ping();
        assertOriginalSession();
    }

    @Test
    public void quotaRefusalKeepsOriginalSessionAndOtherUsersConnection() throws Exception {
        ConnectContext existingBob = ConnectPoolTestSupport.mysqlConnection(env, BOB);
        existingBob.setConnectionId(2);
        Assertions.assertEquals(-1, existingBob.registerClientSession(pool));
        try {
            changeUser("bob", "");

            assertError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS);
            assertOriginalSession();
            assertCounts(1, 1, 2);
            Assertions.assertSame(existingBob, pool.getContext(2));
            ping();
            context.cleanup();
            assertCounts(0, 1, 1);
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

        changeUser("bob", "warehouse.missing");

        assertError(ErrorCode.ERR_BAD_DB_ERROR);
        assertOriginalSession();
        assertCounts(1, 0, 1);
        Assertions.assertEquals("original_group", context.getSessionVariable().getCloudCluster());
        Assertions.assertNull(ConnectContext.get());
        ping();
        assertOriginalSession();
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

    private void assertOriginalSession() {
        Assertions.assertEquals(ALICE, context.getCurrentUserIdentity());
        assertNamespace(ORIGINAL_NAMESPACE.getDefaultCatalog(), ORIGINAL_NAMESPACE.getCurrentDb(),
                ORIGINAL_NAMESPACE.getCurrentDbId());
        Assertions.assertSame(context, pool.getContext(1));
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
