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

package org.apache.doris.foundation.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class ByteBufferUtilTest {

    @Test
    public void testGetUnsignedByte() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF});
        Assertions.assertEquals(255, ByteBufferUtil.getUnsignedByte(buffer));
        Assertions.assertEquals(1, buffer.position());
    }

    @Test
    public void testGetUnsignedShort() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFE});
        Assertions.assertEquals(65534, ByteBufferUtil.getUnsignedShort(buffer));
        Assertions.assertEquals(2, buffer.position());
    }

    @Test
    public void testGetUnsignedInt() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(0x89ABCDEF);
        buffer.flip();

        Assertions.assertEquals(2309737967L, ByteBufferUtil.getUnsignedInt(buffer));
        Assertions.assertEquals(Integer.BYTES, buffer.position());
    }
}
