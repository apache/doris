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

import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/** The compressed protocol is offered only when the configuration lists zlib, and it rides the handshake. */
public class MysqlCapabilityCompressionTest {

    private String savedAlgorithms;

    @BeforeEach
    public void save() {
        savedAlgorithms = Config.mysql_compression_algorithms;
    }

    @AfterEach
    public void restore() {
        Config.mysql_compression_algorithms = savedAlgorithms;
    }

    @Test
    public void testOffByDefault() {
        Config.mysql_compression_algorithms = "";
        MysqlCapability capability = MysqlCapability.serverCapability();
        Assertions.assertFalse(capability.isCompress());
        Assertions.assertEquals(MysqlCapability.DEFAULT_CAPABILITY.getFlags(), capability.getFlags());
        Assertions.assertFalse(MysqlCapability.compressionAdvertised());
    }

    @Test
    public void testZlibListedAdvertisesTheFlag() {
        Config.mysql_compression_algorithms = "zlib";
        MysqlCapability capability = MysqlCapability.serverCapability();
        Assertions.assertTrue(capability.isCompress());
        Assertions.assertEquals(MysqlCapability.DEFAULT_CAPABILITY.getFlags()
                | MysqlCapability.Flag.CLIENT_COMPRESS.getFlagBit(), capability.getFlags());
        Assertions.assertTrue(capability.toString().contains("CLIENT_COMPRESS"));
    }

    @Test
    public void testUnsupportedAlgorithmsAdvertiseNothing() {
        Config.mysql_compression_algorithms = "zstd";
        Assertions.assertFalse(MysqlCapability.serverCapability().isCompress());
    }

    @Test
    public void testNegotiationKeepsTheFlagOnlyWhenBothSidesSetIt() {
        Config.mysql_compression_algorithms = "zlib";
        MysqlCapability server = MysqlCapability.serverCapability();
        MysqlCapability clientWith = new MysqlCapability(MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()
                | MysqlCapability.Flag.CLIENT_COMPRESS.getFlagBit());
        MysqlCapability clientWithout = new MysqlCapability(MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit());
        Assertions.assertTrue(new MysqlCapability(server.getFlags() & clientWith.getFlags()).isCompress());
        Assertions.assertFalse(new MysqlCapability(server.getFlags() & clientWithout.getFlags()).isCompress());

        Config.mysql_compression_algorithms = "";
        server = MysqlCapability.serverCapability();
        Assertions.assertFalse(new MysqlCapability(server.getFlags() & clientWith.getFlags()).isCompress(),
                "a client asking for compression the server never offered gets none");
    }

    @Test
    public void testHandshakeCarriesTheAdvertisedCapability() {
        Config.mysql_compression_algorithms = "zlib";
        MysqlCapability server = MysqlCapability.serverCapability();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        new MysqlHandshakePacket(1090, server).writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();
        Assertions.assertEquals(10, MysqlProto.readInt1(buffer));          // protocol version
        MysqlProto.readNulTerminateString(buffer);                         // server version
        Assertions.assertEquals(1090, MysqlProto.readInt4(buffer));        // connection id
        buffer.position(buffer.position() + 8);                            // auth plugin data part 1
        Assertions.assertEquals(0, MysqlProto.readInt1(buffer));           // filler
        int lowerFlags = MysqlProto.readInt2(buffer);
        Assertions.assertEquals(server.getFlags() & 0xFFFF, lowerFlags);
        Assertions.assertTrue((lowerFlags & MysqlCapability.Flag.CLIENT_COMPRESS.getFlagBit()) != 0);

        Config.mysql_compression_algorithms = "";
        serializer = MysqlSerializer.newInstance();
        new MysqlHandshakePacket(1090, MysqlCapability.serverCapability()).writeTo(serializer);
        buffer = serializer.toByteBuffer();
        MysqlProto.readInt1(buffer);
        MysqlProto.readNulTerminateString(buffer);
        MysqlProto.readInt4(buffer);
        buffer.position(buffer.position() + 8);
        MysqlProto.readInt1(buffer);
        Assertions.assertEquals(0, MysqlProto.readInt2(buffer) & MysqlCapability.Flag.CLIENT_COMPRESS.getFlagBit());
    }
}
