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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MysqlProtoLenEncStringTest {

    // A length-encoded string whose declared length exceeds the bytes actually
    // present must be rejected, not passed to new byte[(int) length]. Before the
    // bound, a 0xFE lead byte plus an 8-byte length let a handful of bytes request
    // ~2 GiB, and a high-bit length cast to a negative size.
    @Test
    public void readLenEncodedStringRejectsOversizedLength() {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 0xFE); // 8-byte length follows
        buffer.put(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F, 0, 0, 0, 0}); // 0x7FFFFFFF
        buffer.flip();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> MysqlProto.readLenEncodedString(buffer));
    }

    @Test
    public void readLenEncodedStringRejectsNegativeCastLength() {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 0xFE);
        buffer.put(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0}); // (int) -> -1
        buffer.flip();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> MysqlProto.readLenEncodedString(buffer));
    }

    @Test
    public void readLenEncodedStringAcceptsValidPayload() {
        byte[] payload = "abc".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + payload.length);
        buffer.put((byte) payload.length); // single-byte length < 251
        buffer.put(payload);
        buffer.flip();
        Assert.assertArrayEquals(payload, MysqlProto.readLenEncodedString(buffer));
    }
}
