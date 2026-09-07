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

package org.apache.doris.statistics.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class InternalQueryBufferTest {
    private InternalQueryBuffer internalQueryBuffer;

    @BeforeEach
    public void setUp() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put((byte) 6);
        buffer.put("field1".getBytes());

        buffer.put((byte) 3);
        buffer.put("123".getBytes());

        buffer.put((byte) 3);
        buffer.put("0.1".getBytes());

        buffer.put((byte) 7);
        buffer.put("18.2322".getBytes());

        internalQueryBuffer = new InternalQueryBuffer(buffer);
    }

    @Test
    public void testData() {
        byte[] result = internalQueryBuffer.data();
        Assertions.assertEquals(1024, result.length);
    }

    @Test
    public void testLength() {
        int result = internalQueryBuffer.length();
        Assertions.assertEquals(1024, result);
    }

    @Test
    public void testPosition() {
        int result = internalQueryBuffer.position();
        // (1 + 6) + (1 + 3) + (1 + 3) + (1 + 7)
        Assertions.assertEquals(23, result);
    }

    @Test
    public void testReadBytesWithLength() {
        internalQueryBuffer.clear();
        byte[] result1 = internalQueryBuffer.readBytesWithLength();
        Assertions.assertArrayEquals("field1".getBytes(), result1);

        byte[] result2 = internalQueryBuffer.readBytesWithLength();
        Assertions.assertArrayEquals("123".getBytes(), result2);

        byte[] result3 = internalQueryBuffer.readBytesWithLength();
        Assertions.assertArrayEquals("0.1".getBytes(), result3);
    }

    @Test
    public void testReadStringWithLength() {
        internalQueryBuffer.clear();
        String result1 = internalQueryBuffer.readStringWithLength();
        Assertions.assertEquals("field1", result1);

        String result2 = internalQueryBuffer.readStringWithLength();
        Assertions.assertEquals("123", result2);

        String result3 = internalQueryBuffer.readStringWithLength();
        Assertions.assertEquals("0.1", result3);
    }

    @Test
    public void testReadStringWithLengthByCharset() throws Exception {
        internalQueryBuffer.clear();
        String result1 = internalQueryBuffer.readStringWithLength("UTF-8");
        Assertions.assertEquals("field1", result1);

        String result2 = internalQueryBuffer.readStringWithLength("UTF-8");
        Assertions.assertEquals("123", result2);

        String result3 = internalQueryBuffer.readStringWithLength("UTF-8");
        Assertions.assertEquals("0.1", result3);
    }

    @Test
    public void testReadIntAndFloatAndDouble() {
        internalQueryBuffer.clear();
        String result1 = internalQueryBuffer.readStringWithLength();
        Assertions.assertEquals("field1", result1);

        int result2 = internalQueryBuffer.readInt();
        Assertions.assertEquals(123, result2);

        float result3 = internalQueryBuffer.readFloat();
        Assertions.assertEquals(0.1, result3, 0.0001);

        double result4 = internalQueryBuffer.readDouble();
        Assertions.assertEquals(18.2322, result4, 0.0001);
    }
}
