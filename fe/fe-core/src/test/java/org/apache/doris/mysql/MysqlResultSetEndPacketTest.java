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

public class MysqlResultSetEndPacketTest {
    private MysqlCapability capability;

    @Before
    public void setUp() {
        capability = new MysqlCapability(MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit());
    }

    @Test
    public void testWriteHeader() {
        // The ResultSet OK packet must start with 0xFE (same as EOF)
        MysqlResultSetEndPacket packet = new MysqlResultSetEndPacket(new QueryState());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);
        packet.writeTo(serializer);

        ByteBuffer buffer = serializer.toByteBuffer();

        // assert header: 0xFE
        Assert.assertEquals(0xFE, MysqlProto.readInt1(buffer));
    }

    @Test
    public void testWriteFields() {
        // Verify all fields are written correctly
        MysqlResultSetEndPacket packet = new MysqlResultSetEndPacket(new QueryState());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);
        packet.writeTo(serializer);

        ByteBuffer buffer = serializer.toByteBuffer();

        // header: 0xFE
        Assert.assertEquals(0xFE, MysqlProto.readInt1(buffer));

        // affected_rows: int<lenenc> = 0
        Assert.assertEquals(0, MysqlProto.readVInt(buffer));

        // last_insert_id: int<lenenc> = 0
        Assert.assertEquals(0, MysqlProto.readVInt(buffer));

        // status_flags: int<2> = 0
        Assert.assertEquals(0, MysqlProto.readInt2(buffer));

        // warnings: int<2> = 0
        Assert.assertEquals(0, MysqlProto.readInt2(buffer));

        // info: string<lenenc> (empty, length = 0)
        Assert.assertEquals(0, MysqlProto.readVInt(buffer));

        // no remaining bytes
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test
    public void testPayloadSizeGreaterThan5() {
        // When CLIENT_DEPRECATE_EOF is negotiated, the MySQL driver uses isResultSetOKPacket()
        // to detect end-of-result-set. This method requires:
        //   (byteBuffer[0] & 0xff) == 0xFE && payloadLength > 5
        // The traditional EOF packet has payloadLength == 5, so the ResultSet OK packet
        // MUST have payloadLength > 5 to be correctly identified.
        MysqlResultSetEndPacket packet = new MysqlResultSetEndPacket(new QueryState());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);
        packet.writeTo(serializer);

        ByteBuffer buffer = serializer.toByteBuffer();
        int payloadLength = buffer.remaining();

        // Payload: 0xFE(1) + affected_rows(1) + last_insert_id(1) + status(2) + warnings(2) + info(1) = 8
        Assert.assertTrue("ResultSet OK packet payload must be > 5 for isResultSetOKPacket(), got: "
                + payloadLength, payloadLength > 5);
    }

    @Test
    public void testDiffersFromEofPacket() {
        // A traditional EOF packet has exactly 5 bytes payload.
        // The ResultSet OK packet must have > 5 bytes to be distinguishable.
        MysqlEofPacket eofPacket = new MysqlEofPacket(new QueryState());
        MysqlSerializer eofSerializer = MysqlSerializer.newInstance(capability);
        eofPacket.writeTo(eofSerializer);
        int eofPayloadLength = eofSerializer.toByteBuffer().remaining();

        MysqlResultSetEndPacket rsEndPacket = new MysqlResultSetEndPacket(new QueryState());
        MysqlSerializer rsEndSerializer = MysqlSerializer.newInstance(capability);
        rsEndPacket.writeTo(rsEndSerializer);
        int rsEndPayloadLength = rsEndSerializer.toByteBuffer().remaining();

        // EOF payload should be <= 5
        Assert.assertTrue("EOF packet payload should be <= 5, got: " + eofPayloadLength,
                eofPayloadLength <= 5);
        // ResultSet OK payload should be > 5
        Assert.assertTrue("ResultSet OK packet payload should be > 5, got: " + rsEndPayloadLength,
                rsEndPayloadLength > 5);
    }
}
