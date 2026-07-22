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

package org.apache.doris.paimon;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class PaimonArrowConverterTest {

    @Test
    public void testTimestampWithoutTimeZonePreservesDstGapWallClock() {
        LocalDateTime wallClock = LocalDateTime.parse("2024-03-10T02:30:00.123456");
        long micros = wallClock.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, null);

        PaimonArrowConverter converter = new PaimonArrowConverter(
                ZoneId.of("America/Los_Angeles"));
        Timestamp result = converter.toPaimonTimestamp(
                micros, arrowType, new TimestampType(6));

        Assertions.assertEquals(
                wallClock, result.toLocalDateTime());
    }

    @Test
    public void testLocalZonedTimestampPreservesInstant() {
        LocalDateTime wallClock = LocalDateTime.parse("2024-01-15T10:30:00.123456");
        long civilMicros = wallClock.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, null);

        PaimonArrowConverter converter = new PaimonArrowConverter(
                ZoneId.of("Asia/Shanghai"));
        Timestamp result = converter.toPaimonTimestamp(
                civilMicros, arrowType, new LocalZonedTimestampType(6));

        long expectedMicros = wallClock.toEpochSecond(ZoneOffset.ofHours(8)) * 1_000_000L
                + wallClock.getNano() / 1_000L;
        Assertions.assertEquals(expectedMicros, result.toMicros());
        Assertions.assertEquals(expectedMicros,
                converter.toPaimonTimestamp(
                        wallClock, new LocalZonedTimestampType(6)).toMicros());
    }

    @Test
    public void testPaimonWriteRejectsTimezoneInArrowType() {
        PaimonArrowConverter converter = new PaimonArrowConverter(ZoneId.of("UTC"));
        ArrowType.Timestamp arrowType = new ArrowType.Timestamp(
                TimeUnit.MICROSECOND, "Asia/Shanghai");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> converter.toPaimonTimestamp(
                        0, arrowType, new TimestampType(6)));
    }
}
