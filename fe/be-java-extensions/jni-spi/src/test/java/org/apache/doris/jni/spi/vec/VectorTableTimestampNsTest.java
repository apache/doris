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

package org.apache.doris.jni.spi.vec;

import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.utils.TypeNativeBytes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TIMESTAMP_NS crosses the JNI boundary as signed Unix-epoch nanoseconds. This pins the encoding at
 * both ends of the range and through every container type a scanner can nest it in.
 */
class VectorTableTimestampNsTest {

    @BeforeAll
    static void useUnsafeAllocation() {
        // The memory tracker natives are registered by BE; in a unit test there is no BE.
        OffHeap.setTesting();
    }

    @Test
    void timestampNsRoundTripsThroughAVectorTableIncludingNestedTypes() {
        LocalDateTime[] values = {
                LocalDateTime.of(1677, 9, 21, 0, 12, 43, 145224192),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999),
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(2024, 2, 29, 12, 34, 56, 123456789),
                LocalDateTime.of(2024, 2, 29, 12, 34, 56),
                LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854775807),
                null
        };
        Assertions.assertEquals(Long.MIN_VALUE, TypeNativeBytes.convertToTimestampNs(values[0]));
        Assertions.assertEquals(-1, TypeNativeBytes.convertToTimestampNs(values[1]));
        Assertions.assertEquals(0, TypeNativeBytes.convertToTimestampNs(values[2]));
        Assertions.assertEquals(Long.MAX_VALUE, TypeNativeBytes.convertToTimestampNs(values[5]));

        ColumnType[] types = {
                ColumnType.parseType("ts", "timestamp_ns"),
                ColumnType.parseType("items", "array<timestamp_ns>"),
                ColumnType.parseType("by_name", "map<string,timestamp_ns>"),
                ColumnType.parseType("record", "struct<ts:timestamp_ns,items:array<timestamp_ns>>")
        };
        Assertions.assertEquals(ColumnType.Type.TIMESTAMP_NS, types[0].getType());
        Assertions.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[1].getChildTypes().get(0).getType());
        Assertions.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[2].getChildTypes().get(1).getType());
        Assertions.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[3].getChildTypes().get(0).getType());

        @SuppressWarnings("unchecked")
        List<Object>[] arrays = (List<Object>[]) new List<?>[values.length];
        @SuppressWarnings("unchecked")
        Map<Object, Object>[] maps = (Map<Object, Object>[]) new Map<?, ?>[values.length];
        @SuppressWarnings("unchecked")
        Map<String, Object>[] structs = (Map<String, Object>[]) new Map<?, ?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            arrays[i] = new ArrayList<>();
            arrays[i].add(values[i]);
            maps[i] = new HashMap<>();
            maps[i].put("value", values[i]);
            structs[i] = new HashMap<>();
            structs[i].put("ts", values[i]);
            structs[i].put("items", arrays[i]);
        }

        VectorTable writable = VectorTable.createWritableTable(
                types, new String[] {"ts", "items", "by_name", "record"}, values.length);
        try {
            writable.appendData(0, values, true);
            writable.appendData(1, arrays, true);
            writable.appendData(2, maps, true);
            writable.appendData(3, structs, true);
            long scalarData = writable.getColumn(0).dataAddress();
            Assertions.assertEquals(Long.MIN_VALUE, OffHeap.getLong(null, scalarData));
            Assertions.assertEquals(-1, OffHeap.getLong(null, scalarData + Long.BYTES));
            Assertions.assertEquals(0, OffHeap.getLong(null, scalarData + 2L * Long.BYTES));
            Assertions.assertEquals(Long.MAX_VALUE, OffHeap.getLong(null, scalarData + 5L * Long.BYTES));
            Object[][] restored = writable.getMaterializedData();
            Assertions.assertArrayEquals(values, restored[0]);
            for (int i = 0; i < values.length; ++i) {
                Assertions.assertEquals(arrays[i], restored[1][i]);
                Assertions.assertEquals(maps[i], restored[2][i]);
                Assertions.assertEquals(structs[i], restored[3][i]);
            }
        } finally {
            writable.close();
        }
    }
}
