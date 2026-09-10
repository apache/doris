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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.util.SerializationUtil;

import java.io.ObjectStreamClass;
import java.io.ObjectStreamConstants;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Keeps the Java-serialized Iceberg system-table task wire compatible with Iceberg 1.10.1. */
final class IcebergSystemTableSerialization {
    static final long ICEBERG_1_10_1_SCHEMA_UID = 6812231194765760118L;
    private static final long ICEBERG_1_11_0_SCHEMA_UID = -1265875184407129845L;
    private static final long LOCAL_SCHEMA_UID = ObjectStreamClass.lookup(Schema.class).getSerialVersionUID();
    private static final byte[] SCHEMA_CLASS_NAME = Schema.class.getName().getBytes(StandardCharsets.UTF_8);

    private IcebergSystemTableSerialization() {
    }

    static String serializeToBase64(Object value) {
        byte[] bytes = SerializationUtil.serializeToBytes(value);
        rewriteSchemaUid(bytes, ICEBERG_1_11_0_SCHEMA_UID, ICEBERG_1_10_1_SCHEMA_UID);
        return new String(Base64.getMimeEncoder().encode(bytes), StandardCharsets.UTF_8);
    }

    static <T> T deserializeFromBase64(String base64) {
        byte[] bytes = Base64.getMimeDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
        rewriteSchemaUid(bytes, ICEBERG_1_10_1_SCHEMA_UID, ICEBERG_1_11_0_SCHEMA_UID);
        return SerializationUtil.deserializeFromBytes(bytes);
    }

    static long schemaUid(String base64) {
        byte[] bytes = Base64.getMimeDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
        int offset = schemaUidOffset(bytes);
        return offset < 0 ? Long.MIN_VALUE : ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong();
    }

    private static void rewriteSchemaUid(byte[] bytes, long fromUid, long toUid) {
        // The 1.10 and 1.11 Schema classes have identical serialized fields. Pinning only this known descriptor
        // to the legacy UID lets old and new BE readers share one task wire while future layouts fail closed.
        if (LOCAL_SCHEMA_UID != ICEBERG_1_11_0_SCHEMA_UID) {
            throw new IllegalStateException("Unsupported Iceberg Schema serialVersionUID: " + LOCAL_SCHEMA_UID);
        }
        int offset = schemaUidOffset(bytes);
        if (offset >= 0 && ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong() == fromUid) {
            ByteBuffer.wrap(bytes, offset, Long.BYTES).putLong(toUid);
        }
    }

    private static int schemaUidOffset(byte[] bytes) {
        int descriptorSize = 1 + Short.BYTES + SCHEMA_CLASS_NAME.length + Long.BYTES;
        for (int i = 0; i <= bytes.length - descriptorSize; i++) {
            if (bytes[i] != ObjectStreamConstants.TC_CLASSDESC
                    || bytes[i + 1] != (byte) (SCHEMA_CLASS_NAME.length >>> 8)
                    || bytes[i + 2] != (byte) SCHEMA_CLASS_NAME.length) {
                continue;
            }
            boolean matches = true;
            for (int j = 0; j < SCHEMA_CLASS_NAME.length; j++) {
                if (bytes[i + 3 + j] != SCHEMA_CLASS_NAME[j]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return i + 3 + SCHEMA_CLASS_NAME.length;
            }
        }
        return -1;
    }
}
