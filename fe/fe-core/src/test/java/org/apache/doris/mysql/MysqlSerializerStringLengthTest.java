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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the MySQL column-definition packet length for string types: CHAR/VARCHAR must carry the
 * length declared in the DDL (not a hardcoded 255), otherwise clients that size buffers from
 * result-set metadata (e.g. ODBC) silently truncate longer values. STRING is unbounded (2GB) and
 * is capped at the VARCHAR maximum so the 4-byte length field stays sane.
 */
public class MysqlSerializerStringLengthTest {
    private static int skipLenEncodedString(byte[] buffer, int offset) {
        return offset + 1 + (buffer[offset] & 0xFF);
    }

    private static int fieldMetadataOffset(byte[] buffer) {
        int offset = 0;
        for (int i = 0; i < 6; i++) {
            offset = skipLenEncodedString(buffer, offset);
        }
        return offset + 1;
    }

    private static int littleEndianInt(byte[] buffer, int offset) {
        return (buffer[offset] & 0xFF)
                | ((buffer[offset + 1] & 0xFF) << 8)
                | ((buffer[offset + 2] & 0xFF) << 16)
                | ((buffer[offset + 3] & 0xFF) << 24);
    }

    private static int advertisedLength(ScalarType type) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeField("col", type);
        byte[] field = serializer.toArray();
        int metadataOffset = fieldMetadataOffset(field);
        // metadata: 2-byte charset, then 4-byte column length.
        return littleEndianInt(field, metadataOffset + 2);
    }

    @Test
    public void testVarcharReportsDeclaredLength() {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Assertions.assertEquals(255, serializer.getMysqlTypeLength(ScalarType.createVarcharType(255)));
        Assertions.assertEquals(1000, serializer.getMysqlTypeLength(ScalarType.createVarcharType(1000)));
        Assertions.assertEquals(4000, serializer.getMysqlTypeLength(ScalarType.createVarcharType(4000)));
        Assertions.assertEquals(65533, serializer.getMysqlTypeLength(ScalarType.createVarcharType(65533)));
    }

    @Test
    public void testCharReportsDeclaredLength() {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Assertions.assertEquals(1, serializer.getMysqlTypeLength(ScalarType.createCharType(1)));
        Assertions.assertEquals(10, serializer.getMysqlTypeLength(ScalarType.createCharType(10)));
        Assertions.assertEquals(255, serializer.getMysqlTypeLength(ScalarType.createCharType(255)));
    }

    @Test
    public void testStringCappedAtVarcharMax() {
        // STRING is unbounded (up to 2GB); the raw length would overflow the 4-byte length field,
        // so it is capped at the VARCHAR maximum (MySQL TEXT semantics).
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Assertions.assertEquals(ScalarType.MAX_VARCHAR_LENGTH,
                serializer.getMysqlTypeLength(ScalarType.createStringType()));
    }

    @Test
    public void testColumnDefinitionPacketCarriesDeclaredLength() {
        Assertions.assertEquals(1000, advertisedLength(ScalarType.createVarcharType(1000)));
        Assertions.assertEquals(65533, advertisedLength(ScalarType.createVarcharType(65533)));
        Assertions.assertEquals(10, advertisedLength(ScalarType.createCharType(10)));
    }

    @Test
    public void testUnhandledTypeKeepsDefault() {
        // Types without a length concept must keep the existing 255 default.
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Assertions.assertEquals(255, serializer.getMysqlTypeLength(ScalarType.createHllType()));
    }
}
