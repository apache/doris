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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.UserPropertyMgr;
import org.apache.doris.qe.ConnectContext;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({Catalog.class, ConnectContext.class, MysqlPassword.class, UserPropertyMgr.class})
public class MysqlProtoTest {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MysqlProtoTest.class);
    private MysqlChannel channel;

    private Catalog catalog;

    @Before
    public void setUp() throws DdlException {

        // mock auth
        PaloAuth auth = EasyMock.createMock(PaloAuth.class);
        EasyMock.expect(auth.checkGlobalPriv(EasyMock.anyObject(ConnectContext.class),
                EasyMock.anyObject(PrivPredicate.class))).andReturn(true).anyTimes();

        EasyMock.expect(auth.checkPassword(EasyMock.anyString(), EasyMock.anyString(), (byte[]) EasyMock.anyObject(),
                (byte[]) EasyMock.anyObject(), (List<UserIdentity>) EasyMock.anyObject())).andDelegateTo(
                        new WrappedAuth() {
                            @Override
                            public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd,
                                    byte[] randomString,
                                    List<UserIdentity> currentUser) {
                                UserIdentity userIdentity = new UserIdentity("defaut_cluster:user", "192.168.1.1");
                                currentUser.add(userIdentity);
                                return true;
                            }
                        }).anyTimes();
        EasyMock.replay(auth);

        // Mock catalog
        catalog = EasyMock.createMock(Catalog.class);
        EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(new Database()).anyTimes();
        EasyMock.expect(catalog.getAuth()).andReturn(auth).anyTimes();
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        catalog.changeDb(EasyMock.anyObject(ConnectContext.class), EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();

        EasyMock.replay(catalog);
        PowerMock.replay(Catalog.class);
    }

    private void mockChannel(String user, boolean sendOk) throws Exception {
        // mock channel
        channel = EasyMock.createMock(MysqlChannel.class);
        // channel.sendOnePacket(EasyMock.anyObject(ByteBuffer.class));
        channel.sendAndFlush(EasyMock.anyObject(ByteBuffer.class));
        if (sendOk) {
            EasyMock.expectLastCall().anyTimes();
        } else {
            EasyMock.expectLastCall().andThrow(new IOException()).anyTimes();
        }

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
        EasyMock.expect(channel.fetchOnePacket()).andReturn(buffer).anyTimes();
        EasyMock.expect(channel.getRemoteIp()).andReturn("192.168.1.1").anyTimes();
        EasyMock.replay(channel);

        PowerMock.expectNew(MysqlChannel.class, EasyMock.anyObject(SocketChannel.class)).andReturn(channel).anyTimes();
        PowerMock.replay(MysqlChannel.class);
    }

    private void mockPassword(boolean result) {
        // mock password
        PowerMock.mockStatic(MysqlPassword.class);
        EasyMock.expect(MysqlPassword.checkScramble(
                EasyMock.anyObject(byte[].class), EasyMock.anyObject(byte[].class),
                EasyMock.anyObject(byte[].class)))
                .andReturn(result).anyTimes();
        EasyMock.expect(MysqlPassword.createRandomString(20)).andReturn(new byte[20]).anyTimes();
        EasyMock.expect(MysqlPassword.getSaltFromPassword(EasyMock.isA(byte[].class)))
                .andReturn(new byte[20]).anyTimes();
        PowerMock.replay(MysqlPassword.class);
    }

    private void mockAccess() throws Exception {
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

    @Test(expected = IOException.class)
    public void testNegotiateSendFail() throws Exception {
        mockChannel("user", false);
        mockPassword(true);
        mockAccess();
        ConnectContext context = new ConnectContext(null);
        MysqlProto.negotiate(context);
        Assert.fail("No Exception throws.");
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
