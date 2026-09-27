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

package org.apache.doris.planner;

import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * The FE probe encoding must be byte-identical to what the BE write path inserts into the bloom
 * filter: the raw bytes of a string, and the little-endian two's-complement value of a fixed-width
 * integer. Any difference would be a false negative, which drops a tablet that has the value.
 */
public class GlobalPointProbeEncodingTest {

    private static byte[] littleEndian(long value, int width) {
        ByteBuffer buffer = ByteBuffer.allocate(width).order(ByteOrder.LITTLE_ENDIAN);
        switch (width) {
            case 1:
                buffer.put((byte) value);
                break;
            case 2:
                buffer.putShort((short) value);
                break;
            case 4:
                buffer.putInt((int) value);
                break;
            default:
                buffer.putLong(value);
                break;
        }
        return buffer.array();
    }

    private static void assertIntegerEncoding(PrimitiveType type, int width, long... values) {
        Column column = new Column("c", type);
        for (long v : values) {
            Assertions.assertArrayEquals(littleEndian(v, width),
                    GlobalPointIndexPruner.encodeProbeValue(new IntLiteral(v), column), type + " value " + v);
        }
    }

    @Test
    public void testIntegerTypes() {
        assertIntegerEncoding(PrimitiveType.TINYINT, 1, 0, -1, -5, Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertIntegerEncoding(PrimitiveType.SMALLINT, 2, 0, -1, 12345, Short.MIN_VALUE, Short.MAX_VALUE);
        assertIntegerEncoding(PrimitiveType.INT, 4, 0, -1, -123456789, Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertIntegerEncoding(PrimitiveType.BIGINT, 8, 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testStringTypes() {
        for (PrimitiveType type : new PrimitiveType[] {PrimitiveType.VARCHAR, PrimitiveType.STRING}) {
            Column column = new Column("c", type);
            // Two 3-byte characters, and a 4-byte one (outside the BMP).
            String threeByte = new StringBuilder().appendCodePoint(0x4F60).appendCodePoint(0x597D).toString();
            String fourByte = new StringBuilder().appendCodePoint(0x1F600).append("emoji").toString();
            for (String s : new String[] {"hello", "user_123", threeByte, fourByte, ""}) {
                Assertions.assertArrayEquals(s.getBytes(StandardCharsets.UTF_8),
                        GlobalPointIndexPruner.encodeProbeValue(new StringLiteral(s), column), type + " \"" + s + "\"");
            }
        }
    }

    @Test
    public void testUnsupportedTypesAreNotEncoded() {
        Assertions.assertNull(GlobalPointIndexPruner.encodeProbeValue(new FloatLiteral(1.5, Type.DOUBLE),
                new Column("c", PrimitiveType.DOUBLE)));
        Assertions.assertNull(GlobalPointIndexPruner.encodeProbeValue(new IntLiteral(1L),
                new Column("c", PrimitiveType.LARGEINT)));
        Assertions.assertNull(GlobalPointIndexPruner.encodeProbeValue(new StringLiteral("a"),
                new Column("c", PrimitiveType.CHAR)));
        // A literal of the wrong kind for the column type is not encoded either.
        Assertions.assertNull(GlobalPointIndexPruner.encodeProbeValue(new StringLiteral("1"),
                new Column("c", PrimitiveType.INT)));
    }
}
