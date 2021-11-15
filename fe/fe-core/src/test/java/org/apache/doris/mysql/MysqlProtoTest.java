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

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.ldap.LdapAuthenticate;
import org.apache.doris.ldap.LdapClient;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

public class MysqlProtoTest {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MysqlProtoTest.class);
    private static final String PASSWORD_CLEAR_TEXT = "123456";

    @Mocked
    private MysqlChannel channel;
    @Mocked
    private MysqlPassword password;
    @Mocked
    private Catalog catalog;
    @Mocked
    private PaloAuth auth;
    @Mocked
    private LdapClient ldapClient;
    @Mocked
    private LdapAuthenticate ldapAuthenticate;
    @Mocked
    private MysqlClearTextPacket clearTextPacket;

    @Before
    public void setUp() throws DdlException {

        // mock auth
        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkPassword(anyString, anyString, (byte[]) any, (byte[]) any, (List<UserIdentity>) any);
                minTimes = 0;
                result = new Delegate() {
                    boolean fakeCheckPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
                                              List<UserIdentity> currentUser) {
                        UserIdentity userIdentity = new UserIdentity("default_cluster:user", "192.168.1.1");
                        currentUser.add(userIdentity);
                        return true;
                    }
                };

                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = new Database();

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                catalog.changeDb((ConnectContext) any, anyString);
                minTimes = 0;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };

    }

    private void mockChannel(String user, boolean sendOk) throws Exception {
        // mock channel
        new Expectations() {
            {
                channel.sendAndFlush((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    void sendAndFlush(ByteBuffer packet) throws IOException {
                        if (!sendOk) {
                            throw new IOException();
                        }
                    }
                };
            }
        };

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
        new Expectations() {
            {
                channel.fetchOnePacket();
                minTimes = 0;
                result = buffer;

                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";
            }
        };
    }

    private void mockMysqlClearTextPacket(String password) throws IOException {
        new Expectations() {
            {
                clearTextPacket.getPassword();
                minTimes = 0;
                result = password;

                clearTextPacket.readFrom((ByteBuffer) any);
                minTimes = 0;
                result = true;
            }
        };
    }

    private void mockPassword(boolean res) {
        // mock password
        new Expectations(password) {
            {
                MysqlPassword.checkScramble((byte[]) any, (byte[]) any, (byte[]) any);
                minTimes = 0;
                result = res;

                MysqlPassword.createRandomString(20);
                minTimes = 0;
                result = new byte[20];

                MysqlPassword.getSaltFromPassword((byte[]) any);
                minTimes = 0;
                result = new byte[20];
            }
        };
    }

    private void mockAccess() throws Exception {
    }

    private void mockLdap(String user, boolean userExist) {
        LdapConfig.ldap_authentication_enabled = true;

        new Expectations() {
            {
                LdapAuthenticate.authenticate((ConnectContext) any, anyString, anyString);
                minTimes = 0;
                result = new Delegate() {
                    boolean fakeLdapAuthenticate(ConnectContext context, String password, String qualifiedUser) {
                        return password.equals(PASSWORD_CLEAR_TEXT)
                                && ClusterNamespace.getNameFromFullName(qualifiedUser).equals(user);
                    }
                };

                LdapClient.doesUserExist(anyString);
                minTimes = 0;
                result = userExist;
            }
        };
    }

    @Test
    public void testNegotiate() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        ConnectContext context = new ConnectContext(null);
        context.setCatalog(catalog);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateSendFail() throws Exception {
        mockChannel("user", false);
        mockPassword(true);
        mockAccess();
        ConnectContext context = new ConnectContext(null);
        MysqlProto.negotiate(context);
        Assert.assertFalse(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateInvalidPasswd() throws Exception {
        mockChannel("user", true);
        mockPassword(false);
        mockAccess();
        ConnectContext context = new ConnectContext(null);
        Assert.assertTrue(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateNoUser() throws Exception {
        mockChannel("", true);
        mockPassword(true);
        mockAccess();
        ConnectContext context = new ConnectContext(null);
        Assert.assertFalse(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateLdap() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        mockMysqlClearTextPacket(PASSWORD_CLEAR_TEXT);
        mockLdap("user", true);
        ConnectContext context = new ConnectContext(null);
        context.setCatalog(catalog);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateLdapInvalidPasswd() throws Exception {
        mockChannel("user", true);
        mockPassword(true);
        mockAccess();
        mockMysqlClearTextPacket("654321");
        mockLdap("user", true);
        ConnectContext context = new ConnectContext(null);
        context.setCatalog(catalog);
        context.setThreadLocalInfo();
        Assert.assertFalse(MysqlProto.negotiate(context));
    }

    @Test
    public void testNegotiateLdapRoot() throws Exception {
        mockChannel("root", true);
        mockPassword(true);
        mockAccess();
        mockLdap("root", false);
        mockMysqlClearTextPacket("654321");
        ConnectContext context = new ConnectContext(null);
        context.setCatalog(catalog);
        context.setThreadLocalInfo();
        Assert.assertTrue(MysqlProto.negotiate(context));
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
