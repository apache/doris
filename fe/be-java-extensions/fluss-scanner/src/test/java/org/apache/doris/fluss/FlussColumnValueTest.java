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

package org.apache.doris.fluss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlussColumnValueTest {
    @Test
    public void testJniUtcYearBounds() {
        org.apache.doris.jni.spi.utils.OffHeap.setTesting();
        org.apache.doris.jni.spi.vec.ColumnType columnType =
                org.apache.doris.jni.spi.vec.ColumnType.parseType("ts", "timestamptz(6)");
        for (String text : new String[] {"0000-12-31T23:59:59Z", "+10000-01-01T00:00:00Z"}) {
            java.time.Instant instant = java.time.Instant.parse(text);
            FlussColumnValue value = new FlussColumnValue("America/Los_Angeles");
            value.setIdx(0, columnType, org.apache.fluss.types.DataTypes.TIMESTAMP_LTZ(6), null);
            value.setRow(org.apache.fluss.row.GenericRow.of(org.apache.fluss.row.TimestampLtz.fromInstant(instant)));
            org.apache.doris.jni.spi.vec.VectorColumn column =
                    org.apache.doris.jni.spi.vec.VectorColumn.createWritableColumn(columnType, 1);
            try {
                // Doris accepts year zero; only the UTC instant outside years 0..9999 is invalid.
                if (text.startsWith("0000")) {
                    column.appendValue(value);
                    Assertions.assertEquals(java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC),
                            column.getTimeStampTzColumn(0, 1)[0]);
                } else {
                    Assertions.assertThrows(IllegalArgumentException.class, () -> column.appendValue(value));
                }
            } finally {
                column.close();
            }
        }
    }

}
