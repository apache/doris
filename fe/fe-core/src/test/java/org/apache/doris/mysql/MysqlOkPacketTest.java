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

        // When infoMessage is empty, an empty len-encoded string (0x00) should still be written.
        // This is required because OkPacket.parse() in MySQL Connector/J unconditionally reads
        // STRING_LENENC for info. Without this byte, the driver throws
        // ArrayIndexOutOfBoundsException when CLIENT_DEPRECATE_EOF is negotiated.
        Assert.assertEquals(0x00, MysqlProto.readVInt(buffer));
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test
    public void testWritePayloadSizeGreaterThan5() {
        // When CLIENT_DEPRECATE_EOF is negotiated, the driver distinguishes between
        // EOF packets (payload <= 5) and ResultSet OK packets (payload > 5).
        // MysqlOkPacket payload must be > 5 to avoid being misidentified as EOF.
        // Payload: 0x00(1) + affected_rows(1) + last_insert_id(1) + status(2) + warnings(2) + info_len(1) = 8
        MysqlOkPacket packet = new MysqlOkPacket(new QueryState());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);
        packet.writeTo(serializer);

        ByteBuffer buffer = serializer.toByteBuffer();
        int payloadLength = buffer.remaining();
        Assert.assertTrue("OK packet payload should be > 5 for CLIENT_DEPRECATE_EOF compatibility, got: "
                + payloadLength, payloadLength > 5);
    }
}
