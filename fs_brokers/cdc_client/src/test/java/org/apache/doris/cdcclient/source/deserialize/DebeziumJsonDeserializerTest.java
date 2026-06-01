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

package org.apache.doris.cdcclient.source.deserialize;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;

/** Unit tests for {@link DebeziumJsonDeserializer}. */
class DebeziumJsonDeserializerTest {

    private final DebeziumJsonDeserializer deserializer = new DebeziumJsonDeserializer();

    // ─── convertTimestamp ─────────────────────────────────────────────────────

    @Test
    void microTimestamp_negativeSubMillisecond_doesNotThrow() {
        // micros = -877 ⇒ 1969-12-31 23:59:59.999123. Signed `/` `%` produced a
        // negative nanoOfMillisecond and tripped TimestampData's >= 0 check.
        Object out = invokeConvertTimestamp(MicroTimestamp.SCHEMA_NAME, -877L);
        assertEquals("1969-12-31 23:59:59.999123", out.toString());
    }

    @Test
    void microTimestamp_positive_unchanged() {
        Object out = invokeConvertTimestamp(MicroTimestamp.SCHEMA_NAME, 1_234_567L);
        assertEquals("1970-01-01 00:00:01.234567", out.toString());
    }

    @Test
    void microTimestamp_negativeIntegerMillis_unchanged() {
        // micros = -1000 ⇒ 1969-12-31 23:59:59.999, negative but no sub-millisecond
        // (the old code happened to produce the right result here; protect that path).
        Object out = invokeConvertTimestamp(MicroTimestamp.SCHEMA_NAME, -1000L);
        assertEquals("1969-12-31 23:59:59.999", out.toString());
    }

    @Test
    void nanoTimestamp_negativeSubMillisecond_doesNotThrow() {
        // nanos = -877_000 ⇒ 1969-12-31 23:59:59.999123.
        Object out = invokeConvertTimestamp(NanoTimestamp.SCHEMA_NAME, -877_000L);
        assertEquals("1969-12-31 23:59:59.999123", out.toString());
    }

    private Object invokeConvertTimestamp(String typeName, Object dbzObj) {
        try {
            Method m =
                    DebeziumJsonDeserializer.class.getDeclaredMethod(
                            "convertTimestamp", String.class, Object.class);
            m.setAccessible(true);
            return m.invoke(deserializer, typeName, dbzObj);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    // ─── convertZoneTime ──────────────────────────────────────────────────────
    // timetz arrives as a UTC-normalized ISO string; the method renders it to
    // serverTimeZone (mirrors timestamptz) and emits an offset-less wall clock.

    @Test
    void zoneTime_utc_keepsUtcWallClock() {
        deserializer.setServerTimeZone(ZoneId.of("UTC"));
        assertEquals("12:00:00.123456", invokeConvertZoneTime("12:00:00.123456Z"));
    }

    @Test
    void zoneTime_plus08_shiftsForward() {
        deserializer.setServerTimeZone(ZoneId.of("+08:00"));
        // 12:00 UTC rendered at +08 -> 20:00
        assertEquals("20:00:00.123456", invokeConvertZoneTime("12:00:00.123456Z"));
    }

    @Test
    void zoneTime_minus05_wrapsAcrossMidnight() {
        deserializer.setServerTimeZone(ZoneId.of("-05:00"));
        // 01:00 UTC rendered at -05 -> 20:00 (time-of-day wraps; date dropped)
        assertEquals("20:00:00.123456", invokeConvertZoneTime("01:00:00.123456Z"));
    }

    @Test
    void zoneTime_wholeSecond_omitsFraction() {
        deserializer.setServerTimeZone(ZoneId.of("UTC"));
        // LocalTime.toString drops seconds when both second and nano are zero
        assertEquals("00:00", invokeConvertZoneTime("00:00:00Z"));
    }

    private Object invokeConvertZoneTime(Object dbzObj) {
        try {
            Method m =
                    DebeziumJsonDeserializer.class.getDeclaredMethod("convertZoneTime", Object.class);
            m.setAccessible(true);
            return m.invoke(deserializer, dbzObj);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
