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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.mysql.MysqlSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MysqlParameterLiteralTest {
    private static final MysqlColType[] STRING_TYPES = {MysqlColType.MYSQL_TYPE_STRING,
            MysqlColType.MYSQL_TYPE_VARSTRING, MysqlColType.MYSQL_TYPE_VARCHAR};

    @Test
    public void testStringWireTypesPreserveInvalidUtf8() {
        byte[] allBytes = new byte[256];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) i;
        }
        byte[][] values = {allBytes, {0, (byte) 0x80, (byte) 0xff, 65, 0, (byte) 0xc3, 40},
                {(byte) 0xc3}, {(byte) 0xc0, (byte) 0x80}, {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
                {(byte) 0xf4, (byte) 0x90, (byte) 0x80, (byte) 0x80}};
        for (MysqlColType type : STRING_TYPES) {
            for (byte[] bytes : values) {
                Literal literal = bind(type, bytes);
                Assertions.assertEquals(new VarBinaryLiteral(bytes), literal);
                Assertions.assertArrayEquals(bytes,
                        ((org.apache.doris.analysis.VarBinaryLiteral) literal.toLegacyLiteral()).getValue());
            }
        }
    }

    @Test
    public void testValidUtf8RetainsTextTypesAndBytes() {
        // A real U+FFFD is valid text; it must not be mistaken for a decoding replacement.
        for (String text : new String[] {"", "ASCII", "中文", "emoji: 🙂", "a\0b", "�"}) {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            for (MysqlColType type : STRING_TYPES) {
                Literal literal = bind(type, bytes);
                Assertions.assertEquals(type == MysqlColType.MYSQL_TYPE_VARCHAR
                        ? new VarcharLiteral(text) : new StringLiteral(text), literal);
                Assertions.assertEquals(type == MysqlColType.MYSQL_TYPE_VARCHAR
                        ? VarcharLiteral.class : StringLiteral.class, literal.getClass());
                Assertions.assertArrayEquals(bytes, ((String) literal.getValue()).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    public void testBlobRemainsBinaryForValidUtf8() {
        for (MysqlColType type : new MysqlColType[] {MysqlColType.MYSQL_TYPE_TINY_BLOB,
                MysqlColType.MYSQL_TYPE_MEDIUM_BLOB, MysqlColType.MYSQL_TYPE_LONG_BLOB,
                MysqlColType.MYSQL_TYPE_BLOB}) {
            Assertions.assertEquals(new VarBinaryLiteral("中文".getBytes(StandardCharsets.UTF_8)),
                    bind(type, "中文".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private Literal bind(MysqlColType type, byte[] bytes) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeLenEncodedBytes(bytes);
        serializer.writeInt1(42); // The next parameter must not be consumed by UTF-8 validation.
        ByteBuffer packet = serializer.toByteBuffer();
        Literal literal = Literal.getLiteralByMysqlType(type, false, packet);
        Assertions.assertEquals(1, packet.remaining());
        Assertions.assertEquals(42, packet.get());
        return literal;
    }
}
