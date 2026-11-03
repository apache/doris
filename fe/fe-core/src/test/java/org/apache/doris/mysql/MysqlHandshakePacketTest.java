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

import com.google.common.primitives.Bytes;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MysqlHandshakePacketTest {
    private static final int MULTI_RESULT_FLAGS = MysqlCapability.Flag.CLIENT_MULTI_RESULTS.getFlagBit()
            | MysqlCapability.Flag.CLIENT_PS_MULTI_RESULTS.getFlagBit();

    private byte[] buf;
    private MysqlCapability capability;

    @Mocked
    MysqlPassword mysqlPassword;

    @Before
    public void setUp() {
        buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }

        new Expectations() {
            {
                MysqlPassword.createRandomString(20);
                minTimes = 0;
                result = buf;
            }
        };

        capability = new MysqlCapability(0);
    }

    @Test
    public void testWrite() {
        MysqlHandshakePacket packet = new MysqlHandshakePacket(1090);
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);

        packet.writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();

        // assert protocol version
        Assert.assertEquals(10, MysqlProto.readInt1(buffer));
        // server version
        Assert.assertEquals("5.7.99", new String(MysqlProto.readNulTerminateString(buffer)));
        // connection id
        Assert.assertEquals(1090, MysqlProto.readInt4(buffer));
        // plugin data 1
        byte[] pluginData1 = MysqlProto.readFixedString(buffer, 8);
        Assert.assertEquals(0, MysqlProto.readInt1(buffer));
        int flags = 0;
        flags = MysqlProto.readInt2(buffer);
        // char set
        Assert.assertEquals(33, MysqlProto.readInt1(buffer));
        // status flags
        Assert.assertEquals(0, MysqlProto.readInt2(buffer));
        // capability flags
        flags |= MysqlProto.readInt2(buffer) << 16;
        Assert.assertEquals(MysqlProto.SERVER_USE_SSL
                ? MysqlCapability.SSL_CAPABILITY.getFlags() : MysqlCapability.DEFAULT_CAPABILITY.getFlags(), flags);
        MysqlCapability advertisedCapability = new MysqlCapability(flags);
        Assert.assertTrue(advertisedCapability.isConnectAttrs());
        Assert.assertTrue(advertisedCapability.isPluginAuthDataLengthEncoded());
        // The multi-statement flags MySQL advertises, in the upper two bytes: a driver that masks
        // what it asks for with the advertised set (Connector/J 8 and 9) keeps CLIENT_MULTI_STATEMENTS
        // only when they are there.
        Assert.assertTrue(advertisedCapability.isClientMultiStatements());
        Assert.assertEquals(MULTI_RESULT_FLAGS, flags & MULTI_RESULT_FLAGS);
        // length of plugin data
        Assert.assertEquals(21, MysqlProto.readInt1(buffer));
        // length of plugin data
        byte[] toCheck = new byte[10];
        byte[] reserved = MysqlProto.readFixedString(buffer, 10);
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(toCheck[i], reserved[i]);
        }
        byte[] pluginData2 = MysqlProto.readFixedString(buffer, 12);
        byte[] pluginData = Bytes.concat(pluginData1, pluginData2);
        for (int i = 0; i < 20; ++i) {
            Assert.assertEquals(buf[i], pluginData[i]);
        }

        // one byte
        Assert.assertEquals(0, MysqlProto.readInt1(buffer));
        Assert.assertEquals(22, buffer.remaining());
    }
}
