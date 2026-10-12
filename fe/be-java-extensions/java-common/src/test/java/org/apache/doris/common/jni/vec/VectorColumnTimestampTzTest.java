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

package org.apache.doris.common.jni.vec;

import org.apache.doris.common.jni.utils.OffHeap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

class VectorColumnTimestampTzTest {
    @Test
    void rejectsOutOfRangeScalarAndBatchValuesButPreservesNulls() {
        OffHeap.setTesting();
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("ts", "timestamptz(6)"), 1);
        try {
            LocalDateTime min = LocalDateTime.of(0, 1, 1, 0, 0);
            LocalDateTime max = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
            column.appendTimeStampTz(min);
            column.appendNull(ColumnType.Type.TIMESTAMPTZ);
            column.appendTimeStampTz(new LocalDateTime[] {null, min, max}, true);
            Assertions.assertArrayEquals(new LocalDateTime[] {min, null, null, min, max},
                    column.getTimeStampTzColumn(0, 5));
            for (LocalDateTime invalid : Arrays.asList(min.minusNanos(1000), max.plusNanos(1000))) {
                Assertions.assertThrows(IllegalArgumentException.class, () -> column.appendTimeStampTz(invalid));
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> column.appendTimeStampTz(new LocalDateTime[] {invalid}, false));
            }
        } finally {
            column.close();
        }
    }

    @Test
    void preservesYearZeroArrayElements() {
        OffHeap.setTesting();
        VectorColumn column = VectorColumn.createWritableColumn(
                ColumnType.parseType("events", "array<timestamptz(6)>"), 1);
        try {
            LocalDateTime min = LocalDateTime.of(0, 1, 1, 0, 0);
            Object[] values = column.newObjectContainerArray(1);
            values[0] = new java.util.ArrayList<>(Arrays.asList(min, null, min.plusNanos(1000)));
            column.appendObjectColumn(values, true);
            Assertions.assertEquals(values[0], column.getObjectColumn(0, 1)[0]);
        } finally {
            column.close();
        }
    }

    @Test
    void rejectsOutOfRangeNestedValues() {
        OffHeap.setTesting();
        for (String type : new String[] {"array<timestamptz(6)>", "map<string,timestamptz(6)>",
                "struct<ts:timestamptz(6)>"}) {
            VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("nested", type), 1);
            try {
                LocalDateTime invalid = LocalDateTime.of(10000, 1, 1, 0, 0);
                Object value = type.startsWith("array") ? new java.util.ArrayList<>(Arrays.asList(invalid))
                        : new java.util.HashMap<>(java.util.Collections.singletonMap("ts", invalid));
                Object[] batch = column.newObjectContainerArray(1);
                batch[0] = value;
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> column.appendObjectColumn(batch, true));
            } finally {
                column.close();
            }
        }
    }
}
