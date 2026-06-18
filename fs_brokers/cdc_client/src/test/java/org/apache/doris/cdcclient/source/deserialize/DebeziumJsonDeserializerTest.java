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

import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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

    // ─── convertToTime (MySQL TIME full range) ────────────────────────────────
    // MySQL TIME spans [-838:59:59, 838:59:59] (Debezium MicroTime/NanoTime, long micros/nanos).
    // In-range values keep the LocalTime format; out-of-range (negative or >=24h) must format
    // as ±HH:MM:SS[.ffffff] instead of falling back to the raw long literal.

    @Test
    void microTime_zero_isMidnight() {
        assertEquals("00:00", invokeConvertToTime(MicroTime.SCHEMA_NAME, 0L));
    }

    @Test
    void microTime_inRange_withMicros() {
        assertEquals("12:34:56.123456", invokeConvertToTime(MicroTime.SCHEMA_NAME, 45_296_123_456L));
    }

    @Test
    void microTime_inRange_upperBound() {
        assertEquals("23:59:59.999999", invokeConvertToTime(MicroTime.SCHEMA_NAME, 86_399_999_999L));
    }

    @Test
    void microTime_negative_mysqlLowerBound() {
        // MySQL '-838:59:59' = -3_020_399_000_000 micros; must not fall back to the raw long.
        assertEquals("-838:59:59", invokeConvertToTime(MicroTime.SCHEMA_NAME, -3_020_399_000_000L));
    }

    @Test
    void microTime_over24h_mysqlUpperBound() {
        // MySQL '838:59:59.999999' = 3_020_399_999_999 micros.
        assertEquals(
                "838:59:59.999999", invokeConvertToTime(MicroTime.SCHEMA_NAME, 3_020_399_999_999L));
    }

    @Test
    void nanoTime_negative_mysqlLowerBound() {
        assertEquals(
                "-838:59:59", invokeConvertToTime(NanoTime.SCHEMA_NAME, -3_020_399_000_000_000L));
    }

    private Object invokeConvertToTime(String schemaName, Object dbzObj) {
        try {
            Schema schema = SchemaBuilder.int64().name(schemaName).optional().build();
            Method m =
                    DebeziumJsonDeserializer.class.getDeclaredMethod(
                            "convertToTime", Object.class, Schema.class);
            m.setAccessible(true);
            return m.invoke(deserializer, dbzObj, schema);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    // ─── convertZoneTime ──────────────────────────────────────────────────────
    // timetz arrives as a UTC-normalized ISO string (Debezium ZonedTime). cdc keeps it
    // verbatim with the offset preserved, independent of serverTimeZone, since a
    // date-less time cannot resolve a named zone's DST. Mirrors Debezium/PostgreSQL.

    @Test
    void zoneTime_utc_preservesOffset() {
        deserializer.setServerTimeZone(ZoneId.of("UTC"));
        assertEquals("12:00:00.123456Z", invokeConvertZoneTime("12:00:00.123456Z"));
    }

    @Test
    void zoneTime_plus08_serverTimeZoneIgnored() {
        deserializer.setServerTimeZone(ZoneId.of("+08:00"));
        // serverTimeZone must not shift timetz; the offset-bearing string is kept as-is
        assertEquals("12:00:00.123456Z", invokeConvertZoneTime("12:00:00.123456Z"));
    }

    @Test
    void zoneTime_minus05_serverTimeZoneIgnored() {
        deserializer.setServerTimeZone(ZoneId.of("-05:00"));
        assertEquals("01:00:00.123456Z", invokeConvertZoneTime("01:00:00.123456Z"));
    }

    @Test
    void zoneTime_dstZone_notShifted() {
        // a DST zone's offset is date-dependent; timetz has no date, so it must not be
        // shifted -- the input is returned unchanged regardless of New York DST.
        deserializer.setServerTimeZone(ZoneId.of("America/New_York"));
        assertEquals("12:00:00.123456Z", invokeConvertZoneTime("12:00:00.123456Z"));
    }

    @Test
    void zoneTime_wholeSecond_keepsSeconds() {
        deserializer.setServerTimeZone(ZoneId.of("UTC"));
        assertEquals("00:00:00Z", invokeConvertZoneTime("00:00:00Z"));
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
