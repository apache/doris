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


package org.apache.doris.trinoconnector;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

class TrinoTimestampSemanticsTest {
    @Test
    void testZonedTimestampsUseUtcCarrier() {
        for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
            for (String text : new String[] {"2020-01-02T04:01:00.111333Z", "1969-12-31T23:59:59.999333Z"}) {
                Instant instant = Instant.parse(text);
                TimeZoneKey key = TimeZoneKey.getTimeZoneKey(zone);
                TimestampWithTimeZoneType type = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6);
                BlockBuilder block = type.createBlockBuilder(null, 1);
                int picosOfMilli = instant.getNano() % 1_000_000 * 1_000;
                type.writeObject(block, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        instant.toEpochMilli(), picosOfMilli, key));
                TrinoConnectorColumnValue value = new TrinoConnectorColumnValue();
                value.setTrinoType(type);
                value.setBlock(block.build());
                value.setPosition(0);
                Assertions.assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), value.getTimeStampTz());

                TimestampWithTimeZoneType millisType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);
                BlockBuilder millisBlock = millisType.createBlockBuilder(null, 1);
                millisType.writeLong(millisBlock, DateTimeEncoding.packDateTimeWithZone(instant.toEpochMilli(), key));
                value.setTrinoType(millisType);
                value.setBlock(millisBlock.build());
                Assertions.assertEquals(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(instant.toEpochMilli()), ZoneOffset.UTC), value.getTimeStampTz());
            }
        }
    }

    @Test
    public void testJniUtcYearBounds() {
        org.apache.doris.jni.spi.utils.OffHeap.setTesting();
        org.apache.doris.jni.spi.vec.ColumnType columnType =
                org.apache.doris.jni.spi.vec.ColumnType.parseType("ts", "timestamptz(6)");
        for (String text : new String[] {"0000-12-31T23:59:59Z", "+10000-01-01T00:00:00Z"}) {
            java.time.Instant instant = java.time.Instant.parse(text);
            TimestampWithTimeZoneType type = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6);
            BlockBuilder block = type.createBlockBuilder(null, 1);
            type.writeObject(block, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                    instant.toEpochMilli(), 0, TimeZoneKey.UTC_KEY));
            TrinoConnectorColumnValue value = new TrinoConnectorColumnValue();
            value.setTrinoType(type);
            value.setBlock(block.build());
            value.setPosition(0);
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
