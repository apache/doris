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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MysqlSerializerDateTimeTest {
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

    @Test
    public void testTimestampNsMetadata() {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        ScalarType type = ScalarType.createTimeStampNsType();
        Assertions.assertTrue(type.isTimeStampNs());
        Assertions.assertEquals(29, serializer.getMysqlTypeLength(type));
        Assertions.assertEquals(0, serializer.getMysqlDecimals(type));

        serializer.writeField("dt", type);
        byte[] field = serializer.toArray();
        int metadataOffset = fieldMetadataOffset(field);
        int mysqlTypeOffset = metadataOffset + 2 + 4;
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING.getCode(),
                field[mysqlTypeOffset] & 0xFF);
        int decimalsOffset = mysqlTypeOffset + 1 + 2;
        Assertions.assertEquals(0, field[decimalsOffset] & 0xFF);
    }

    @Test
    public void testDatetimeV2MicrosecondMetadata() {
        for (int scale = 0; scale <= 6; scale++) {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            ScalarType type = ScalarType.createDatetimeV2Type(scale);
            Assertions.assertEquals(scale, serializer.getMysqlDecimals(type));

            serializer.writeField("dt", type);
            byte[] field = serializer.toArray();
            int metadataOffset = fieldMetadataOffset(field);
            int mysqlTypeOffset = metadataOffset + 2 + 4;
            Assertions.assertEquals(MysqlColType.MYSQL_TYPE_DATETIME.getCode(),
                    field[mysqlTypeOffset] & 0xFF);
        }
    }
}
