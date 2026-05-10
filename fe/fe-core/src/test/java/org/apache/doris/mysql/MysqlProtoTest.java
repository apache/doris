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

package org.apache.doris.mysql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.AuthenticatorManager;
import org.apache.doris.mysql.authenticate.ldap.LdapAuthenticator;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;

public class MysqlProtoTest {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MysqlProtoTest.class);
    private static final String PASSWORD_CLEAR_TEXT = "123456";

    private MysqlChannel channel = Mockito.mock(MysqlChannel.class);
    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private Auth auth = Mockito.mock(Auth.class);
    private AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private LdapManager ldapManager = Mockito.mock(LdapManager.class);
    private MysqlClearTextPacket clearTextPacket = Mockito.mock(MysqlClearTextPacket.class);
    private StreamConnection streamConnection = Mockito.mock(StreamConnection.class);
    private LdapAuthenticator ldapAuthenticator = Mockito.mock(LdapAuthenticator.class);
    private AuthenticatorManager authenticatorManager = Mockito.mock(AuthenticatorManager.class);

    private MockedStatic<MysqlPassword> mockedMysqlPassword;
    private MockedStatic<Env> mockedEnv;

    @Before
    public void setUp() throws DdlException, AuthenticationException, IOException {
        FeConstants.runningUnitTest = true;

        // mock StreamConnection.getPeerAddress() to avoid NPE in MysqlChannel constructor
        Mockito.when(streamConnection.getPeerAddress())
                .thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));

        // static mocks
        mockedMysqlPassword = Mockito.mockStatic(MysqlPassword.class);
        mockedMysqlPassword.when(() -> MysqlPassword.createRandomString(20)).thenReturn(new byte[20]);

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        // mock auth
        Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                Mockito.any(PrivPredicate.class))).thenReturn(true);

        Mockito.doAnswer(inv -> {
            List<UserIdentity> currentUser = inv.getArgument(4);
            UserIdentity userIdentity = new UserIdentity("user", "192.168.1.1");
            currentUser.add(userIdentity);
            return null;
        }).when(auth).checkPassword(Mockito.anyString(), Mockito.anyString(), Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.any(List.class));

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getAuthenticatorManager()).thenReturn(authenticatorManager);

        Mockito.when(authenticatorManager.authenticate(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.any(MysqlChannel.class), Mockito.any(MysqlSerializer.class),
                Mockito.any(MysqlAuthPacket.class), Mockito.any(MysqlHandshakePacket.class))).thenReturn(true);

        Mockito.when(catalog.getDbNullable(Mockito.anyString())).thenReturn(new Database());
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.doNothing().when(env).changeDb(Mockito.nullable(ConnectContext.class), Mockito.anyString());

        // channel.getSerializer() must return a real serializer for negotiate() to work
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
    }

    @After
    public void tearDown() {
        if (mockedMysqlPassword != null) {
            mockedMysqlPassword.close();
        }
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    private ConnectContext createContext() throws Exception {
        ConnectContext context = new ConnectContext(streamConnection);
        Field channelField = ConnectContext.class.getDeclaredField("mysqlChannel");
        channelField.setAccessible(true);
        channelField.set(context, channel);
        return context;
    }

    private void mockChannel(String user, boolean sendOk) throws Exception {
        // mock channel
        Mockito.doAnswer(inv -> {
            if (!sendOk) {
                throw new IOException();
            }
            return null;
        }).when(channel).sendAndFlush(Mockito.any(ByteBuffer.class));

        // mock auth packet
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        // capability
        serializer.writeInt4(MysqlCapability.DEFAULT_CAPABILITY.getFlags());
        // max packet size
        serializer.writeInt4(1024000);
        // character set
        serializer.writeInt1(33);
        // reserved
        serializer.writeBytes(new byte[23]);
        // user name
        serializer.writeNulTerminateString(user);
        // plugin data
        serializer.writeInt1(20);
        byte[] buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }
        serializer.writeBytes(buf);
        // database
        serializer.writeNulTerminateString("database");

        ByteBuffer buffer = serializer.toByteBuffer();
        Mockito.when(channel.fetchOnePacket()).thenReturn(buffer);
        Mockito.when(channel.getRemoteIp()).thenReturn("192.168.1.1");
    }

    private void mockMysqlClearTextPacket(String password) throws IOException {
        Mockito.when(clearTextPacket.getPassword()).thenReturn(password);
        Mockito.when(clearTextPacket.readFrom(Mockito.any(ByteBuffer.class))).thenReturn(true);
    }

    private void mockPassword(boolean res) {
        // mock password
        mockedMysqlPassword.when(() -> MysqlPassword.checkScramble(Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.any(byte[].class))).thenReturn(res);
        mockedMysqlPassword.when(() -> MysqlPassword.createRandomString(20)).thenReturn(new byte[20]);
        mockedMysqlPassword.when(() -> MysqlPassword.getSaltFromPassword(
                Mockito.any(byte[].class))).thenReturn(new byte[20]);
    }

    private void mockAccess() throws Exception {
    }

    private void mockLdap(boolean userExist) throws IOException {
        Config.authentication_type = "ldap";

        Mockito.when(ldapAuthenticator.authenticate(Mockito.any(AuthenticateRequest.class))).thenReturn(new AuthenticateResponse(true));
        Mockito.when(ldapManager.checkUserPasswd(Mockito.anyString(), Mockito.anyString())).thenReturn(userExist);
        Mockito.when(ldapManager.doesUserExist(Mockito.anyString())).thenReturn(userExist);
    }

    private void mockInitCatalog(CatalogMgr catalogMgr, ConnectContext context, String initCatalog)
            throws Exception {
        Mockito.when(auth.getInitCatalog(Mockito.any())).thenReturn(initCatalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.doNothing().when(env).changeCatalog(Mockito.eq(context), Mockito.anyString());
    }

    @Test
    public void testNegotiate() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        ConnectContext context = createContext();
        context.setEnv(env);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateInitCatalog() throws Exception {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        ConnectContext context = createContext();
        context.setEnv(env);
        context.setThreadLocalInfo();
        String initCatalog = "external_catalog";
        mockInitCatalog(catalogMgr, context, initCatalog);
        Assert.assertTrue(MysqlProto.negotiate(context));

        Mockito.verify(env, Mockito.times(1)).changeCatalog(context, initCatalog);
    }

    @Test
    public void testNegotiateSendFail() throws Exception {
        mockChannel("user", false);
        mockPassword(true);
        mockAccess();
        ConnectContext context = createContext();
        MysqlProto.negotiate(context);
        Assert.assertFalse(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateNoUser() throws Exception {
        mockChannel("", true);
        mockPassword(true);
        mockAccess();
        ConnectContext context = createContext();
        Assert.assertFalse(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateClientClosedConnectionDuringHandshake() throws Exception {
        Mockito.when(channel.fetchOnePacket()).thenReturn(null);

        ConnectContext context = createContext();
        Assert.assertFalse(MysqlProto.negotiate(context));
        Assert.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, context.getState().getErrorCode());
        Assert.assertEquals("Client closed connection during handshake", context.getState().getErrorMessage());
    }

    @Test
    public void testNegotiateRejectsSslRequestWhenServerSslDisabled() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt4(MysqlCapability.SSL_CAPABILITY.getFlags());

        Mockito.when(channel.fetchOnePacket()).thenReturn(serializer.toByteBuffer());

        ConnectContext context = createContext();
        Assert.assertFalse(MysqlProto.negotiate(context));
        Assert.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, context.getState().getErrorCode());
        Assert.assertEquals("Client requested TLS/SSL, but Doris FE MySQL SSL is disabled",
                context.getState().getErrorMessage());
    }

    @Test
    public void testNegotiateSendsAuthenticatorErrorWhenResponseNotSent() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();

        Mockito.doAnswer(inv -> {
            ConnectContext ctx = inv.getArgument(0);
            ctx.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR, "Authentication failed.");
            return false;
        }).when(authenticatorManager).authenticate(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.any(MysqlChannel.class), Mockito.any(MysqlSerializer.class),
                Mockito.any(MysqlAuthPacket.class), Mockito.any(MysqlHandshakePacket.class));

        Mockito.when(channel.isSend()).thenReturn(false);

        ConnectContext context = createContext();
        Assert.assertFalse(MysqlProto.negotiate(context));
        Assert.assertEquals(ErrorCode.ERR_ACCESS_DENIED_ERROR, context.getState().getErrorCode());

        Mockito.verify(channel, Mockito.times(2)).sendAndFlush(Mockito.any(ByteBuffer.class));
    }

    @Test
    public void testNegotiateDoesNotResendAuthenticatorErrorWhenResponseAlreadySent() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();

        Mockito.doAnswer(inv -> {
            ConnectContext ctx = inv.getArgument(0);
            ctx.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR, "Authentication failed.");
            return false;
        }).when(authenticatorManager).authenticate(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.any(MysqlChannel.class), Mockito.any(MysqlSerializer.class),
                Mockito.any(MysqlAuthPacket.class), Mockito.any(MysqlHandshakePacket.class));

        Mockito.when(channel.isSend()).thenReturn(true);

        ConnectContext context = createContext();
        Assert.assertFalse(MysqlProto.negotiate(context));

        Mockito.verify(channel, Mockito.times(1)).sendAndFlush(Mockito.any(ByteBuffer.class));
    }

    @Test
    public void testNegotiateLdap() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        mockMysqlClearTextPacket(PASSWORD_CLEAR_TEXT);
        mockLdap(true);
        ConnectContext context = createContext();
        context.setEnv(env);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
        Config.authentication_type = "default";
    }

    @Test
    public void testNegotiateLdapRoot() throws Exception {
        mockChannel("root", true);
        mockPassword(true);
        mockAccess();
        mockLdap(false);
        mockMysqlClearTextPacket("654321");
        ConnectContext context = createContext();
        context.setEnv(env);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
        Config.authentication_type = "default";
    }

    @Test
    public void testRead() throws UnsupportedEncodingException {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(200);
        serializer.writeInt2(65535);
        serializer.writeInt3(65537);
        serializer.writeInt4(123456789);
        serializer.writeInt6(1234567896);
        serializer.writeInt8(1234567898);
        serializer.writeVInt(1111123452);
        // string
        serializer.writeBytes("hello".getBytes("utf-8"));
        serializer.writeLenEncodedString("world");
        serializer.writeNulTerminateString("i have dream");
        serializer.writeEofString("you have dream too");

        ByteBuffer buffer = serializer.toByteBuffer();
        Assert.assertEquals(200, MysqlProto.readInt1(buffer));
        Assert.assertEquals(65535, MysqlProto.readInt2(buffer));
        Assert.assertEquals(65537, MysqlProto.readInt3(buffer));
        Assert.assertEquals(123456789, MysqlProto.readInt4(buffer));
        Assert.assertEquals(1234567896, MysqlProto.readInt6(buffer));
        Assert.assertEquals(1234567898, MysqlProto.readInt8(buffer));
        Assert.assertEquals(1111123452, MysqlProto.readVInt(buffer));

        Assert.assertEquals("hello", new String(MysqlProto.readFixedString(buffer, 5)));
        Assert.assertEquals("world", new String(MysqlProto.readLenEncodedString(buffer)));
        Assert.assertEquals("i have dream", new String(MysqlProto.readNulTerminateString(buffer)));
        Assert.assertEquals("you have dream too", new String(MysqlProto.readEofString(buffer)));
    }
}
