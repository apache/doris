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

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MysqlSerializerTimestampTzTest {

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
        int strLen = buf[offset] & 0xFF;
        return offset + 1 + strLen;
    }

    private static int skipFieldHeaderStrings(byte[] buf) {
        int off = 0;
        off = skipLenEncodedString(buf, off); // Catalog: "def"
        off = skipLenEncodedString(buf, off); // Schema: ""
        off = skipLenEncodedString(buf, off); // Table: ""
        off = skipLenEncodedString(buf, off); // Origin Table: ""
        off = skipLenEncodedString(buf, off); // Name
        off = skipLenEncodedString(buf, off); // Original Name
        return off + 1; // Length of the fixed fields, emitted as writeVInt(0x0c).
    }

    @Test
    public void testFieldPacketForTimestampTzUsesStringMetadata() {
        MysqlSerializer ser = MysqlSerializer.newInstance();
        Type type = ScalarType.createTimeStampTzType(6);
        ser.writeField("ts", type);
        byte[] out = ser.toArray();

        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING, type.getPrimitiveType().toMysqlType());

        int off = skipFieldHeaderStrings(out);
        int charset = leUInt2(out, off);
        Assertions.assertEquals(33, charset); // utf8_general_ci
        off += 2;

        long displayLen = leUInt4(out, off);
        Assertions.assertEquals(32L, displayLen);
        off += 4;

        int colType = out[off] & 0xFF;
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING.getCode(), colType);
        off += 1;

        int flags = leUInt2(out, off);
        Assertions.assertEquals(0, flags);
        off += 2;

        Assertions.assertEquals(0, out[off] & 0xFF);
    }
}
