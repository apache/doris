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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class MysqlHandshakePacketTest {
    private byte[] buf;
    private MysqlCapability capability;
    private MockedStatic<MysqlPassword> mockedMysqlPassword;

    @BeforeEach
    public void setUp() {
        buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }

        mockedMysqlPassword = Mockito.mockStatic(MysqlPassword.class);
        mockedMysqlPassword.when(() -> MysqlPassword.createRandomString(20)).thenReturn(buf);

        capability = new MysqlCapability(0);
    }

    @AfterEach
    public void tearDown() {
        if (mockedMysqlPassword != null) {
            mockedMysqlPassword.close();
        }
    }

    @Test
    public void testWrite() {
        MysqlHandshakePacket packet = new MysqlHandshakePacket(1090);
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);

        packet.writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();

        // assert protocol version
        Assertions.assertEquals(10, MysqlProto.readInt1(buffer));
        // server version
        Assertions.assertEquals("5.7.99", new String(MysqlProto.readNulTerminateString(buffer)));
        // connection id
        Assertions.assertEquals(1090, MysqlProto.readInt4(buffer));
        // plugin data 1
        byte[] pluginData1 = MysqlProto.readFixedString(buffer, 8);
        Assertions.assertEquals(0, MysqlProto.readInt1(buffer));
        int flags = 0;
        flags = MysqlProto.readInt2(buffer);
        // char set
        Assertions.assertEquals(33, MysqlProto.readInt1(buffer));
        // status flags
        Assertions.assertEquals(0, MysqlProto.readInt2(buffer));
        // capability flags
        flags |= MysqlProto.readInt2(buffer) << 16;
        Assertions.assertEquals(MysqlProto.SERVER_USE_SSL
                ? MysqlCapability.SSL_CAPABILITY.getFlags() : MysqlCapability.DEFAULT_CAPABILITY.getFlags(), flags);
        MysqlCapability advertisedCapability = new MysqlCapability(flags);
        Assertions.assertTrue(advertisedCapability.isConnectAttrs());
        Assertions.assertTrue(advertisedCapability.isPluginAuthDataLengthEncoded());
        // length of plugin data
        Assertions.assertEquals(21, MysqlProto.readInt1(buffer));
        // length of plugin data
        byte[] toCheck = new byte[10];
        byte[] reserved = MysqlProto.readFixedString(buffer, 10);
        for (int i = 0; i < 10; ++i) {
            Assertions.assertEquals(toCheck[i], reserved[i]);
        }
        byte[] pluginData2 = MysqlProto.readFixedString(buffer, 12);
        byte[] pluginData = Bytes.concat(pluginData1, pluginData2);
        for (int i = 0; i < 20; ++i) {
            Assertions.assertEquals(buf[i], pluginData[i]);
        }

        // one byte
        Assertions.assertEquals(0, MysqlProto.readInt1(buffer));
        Assertions.assertEquals(22, buffer.remaining());
    }

}
