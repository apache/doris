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

import org.apache.doris.qe.QueryState;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MysqlOkPacketTest {
    private MysqlCapability capability;

    @Before
    public void setUp() {
        capability = new MysqlCapability(MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit());
    }

    @Test
    public void testWrite() {
        MysqlOkPacket packet = new MysqlOkPacket(new QueryState());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);
        packet.writeTo(serializer);

        ByteBuffer buffer = serializer.toByteBuffer();

        // assert OK packet indicator 0x00
        Assert.assertEquals(0x00, MysqlProto.readInt1(buffer));

        // assert affect rows vint: 0
        Assert.assertEquals(0x00, MysqlProto.readVInt(buffer));

        // assert last insert id, vint: 0
        Assert.assertEquals(0x00, MysqlProto.readVInt(buffer));

        // assert status flags, int2: 0
        Assert.assertEquals(0x00, MysqlProto.readInt2(buffer));

        // assert warnings, int2: 0
        Assert.assertEquals(0x00, MysqlProto.readInt2(buffer));

        // assert info, eof string: "OK"
        // Assert.assertEquals("OK", new String(MysqlProto.readEofString(buffer)));

        Assert.assertEquals(0, buffer.remaining());
    }

}
