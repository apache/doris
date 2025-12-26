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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MysqlSerializerVarbinaryTest {

    private static int leUInt2(byte[] a, int off) {
        return (a[off] & 0xFF) | ((a[off + 1] & 0xFF) << 8);
    }

    private static long leUInt4(byte[] a, int off) {
        return (a[off] & 0xFFL)
                | ((a[off + 1] & 0xFFL) << 8)
                | ((a[off + 2] & 0xFFL) << 16)
                | ((a[off + 3] & 0xFFL) << 24);
    }

    private static int skipLenEncodedString(byte[] buf, int offset) {
        int lenFirst = buf[offset] & 0xFF;
        // For these tests we only emit small strings (len < 251), so varint is 1 byte
        int lenBytes = 1;
        int strLen = lenFirst;
        return offset + lenBytes + strLen;
    }

    private static int skipFieldHeaderStrings(byte[] buf) {
        int off = 0;
        // 1) Catalog: "def"
        off = skipLenEncodedString(buf, off);
        // 2) Schema: ""
        off = skipLenEncodedString(buf, off);
        // 3) Table: ""
        off = skipLenEncodedString(buf, off);
        // 4) Origin Table: ""
        off = skipLenEncodedString(buf, off);
        // 5) Name: e.g. "c"
        off = skipLenEncodedString(buf, off);
        // 6) Original Name: e.g. "c"
        off = skipLenEncodedString(buf, off);
        // 7) length of following fields: always 0x0c (single byte vint)
        // writeVInt(0x0c) => one byte 12
        off += 1;
        return off;
    }

    @Test
    public void testFieldPacketForVarbinary() {
        MysqlSerializer ser = MysqlSerializer.newInstance();
        String colName = "c";
        Type type = ScalarType.createVarbinaryType(10);
        ser.writeField(colName, type);
        byte[] out = ser.toArray();

        int off = skipFieldHeaderStrings(out);
        // charset index: 2 bytes, little-endian
        int charset = leUInt2(out, off);
        Assertions.assertEquals(63, charset); // binary collation
        off += 2;
        // column length: 4 bytes LE
        long displayLen = leUInt4(out, off);
        Assertions.assertEquals(10L, displayLen);
        off += 4;
        // column type: 1 byte (we don't assert exact code value here)
        off += 1;
        // flags: 2 bytes, should include BINARY_FLAG (128)
        int flags = leUInt2(out, off);
        Assertions.assertEquals(128, flags);
        off += 2;
        // decimals: 1 byte
        Assertions.assertEquals(0, out[off] & 0xFF);
        off += 1;
        // filler: 2 bytes zeros
        Assertions.assertEquals(0, leUInt2(out, off));
    }

    @Test
    public void testFieldPacketForVarcharUsesUtf8Collation() {
        MysqlSerializer ser = MysqlSerializer.newInstance();
        String colName = "name";
        Type type = ScalarType.createVarchar(10);
        ser.writeField(colName, type);
        byte[] out = ser.toArray();

        int off = skipFieldHeaderStrings(out);
        int charset = leUInt2(out, off);
        Assertions.assertEquals(33, charset); // utf8_general_ci
        off += 2;
        long displayLen = leUInt4(out, off);
        // current implementation defaults to 255 for non numeric/time types if not special-cased
        Assertions.assertEquals(255L, displayLen);
        off += 4; // type
        off += 1;
        int flags = leUInt2(out, off);
        Assertions.assertEquals(0, flags); // not BINARY
    }

    @Test
    public void testWriteLenEncodedBytesPreservesNullByte() {
        MysqlSerializer ser = MysqlSerializer.newInstance();
        byte[] raw = new byte[] { 'a', 0x00, 'b' };
        ser.writeLenEncodedBytes(raw);
        ByteBuffer bb = ser.toByteBuffer();
        // first byte is the length 3
        Assertions.assertEquals(3, bb.get() & 0xFF);
        byte[] got = new byte[3];
        bb.get(got);
        Assertions.assertArrayEquals(raw, got);
    }

    @Test
    public void testWriteLenEncodedStringAndBytesProduceSameForAscii() {
        MysqlSerializer ser1 = MysqlSerializer.newInstance();
        MysqlSerializer ser2 = MysqlSerializer.newInstance();
        String s = "abc";
        ser1.writeLenEncodedString(s);
        ser2.writeLenEncodedBytes(s.getBytes(StandardCharsets.UTF_8));
        Assertions.assertArrayEquals(ser1.toArray(), ser2.toArray());
    }
}
