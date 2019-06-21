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
import org.junit.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({MysqlPassword.class})
public class MysqlHandshakePacketTest {
    private byte[] buf;
    private MysqlCapability capability;

    @Before
    public void setUp() {
        buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }
        PowerMock.mockStatic(MysqlPassword.class);
        EasyMock.expect(MysqlPassword.createRandomString(20)).andReturn(buf).anyTimes();
        PowerMock.replay(MysqlPassword.class);

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
        Assert.assertEquals("5.1.0", new String(MysqlProto.readNulTerminateString(buffer)));
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
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY.getFlags(), flags);
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
