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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MysqlAuthPacketTest {
    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
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
        serializer.writeNulTerminateString("palo-user");
        // plugin data
        serializer.writeInt1(20);
        byte[] buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }
        serializer.writeBytes(buf);
        // database
        serializer.writeNulTerminateString("testDb");

        byteBuffer = serializer.toByteBuffer();
    }

    @Test
    public void testRead() {
        MysqlAuthPacket packet = new MysqlAuthPacket();
        Assert.assertTrue(packet.readFrom(byteBuffer));
        Assert.assertEquals("palo-user", packet.getUser());
        Assert.assertEquals("testDb", packet.getDb());
    }

}
