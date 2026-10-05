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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Bits;
import io.debezium.data.geometry.Geometry;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTime;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;

/**
 * Unit tests for {@link DebeziumJsonDeserializer}'s {@code convertInternal} dispatch, covering the
 * timezone-independent value conversions (dates, times, decimals, bits, primitives). These guard
 * the self-written type mapping against calendar drift and type-handling regressions without needing
 * a real database.
 */
class DebeziumConvertInternalTest {

    private final DebeziumJsonDeserializer deserializer = new DebeziumJsonDeserializer();

    // ─── io.debezium.time.Date (epoch days) ───────────────────────────────────

    @Test
    void date_ancientYears_notShifted() {
        assertEquals("0017-08-12", convertDate(LocalDate.of(17, 8, 12)));
        assertEquals("0001-01-01", convertDate(LocalDate.of(1, 1, 1)));
        assertEquals("0033-03-03", convertDate(LocalDate.of(33, 3, 3)));
        assertEquals("0444-04-04", convertDate(LocalDate.of(444, 4, 4)));
    }

    @Test
    void date_preEpochAndModern() {
        assertEquals("1969-12-31", convertDate(LocalDate.of(1969, 12, 31)));
        assertEquals("2020-07-17", convertDate(LocalDate.of(2020, 7, 17)));
    }

    @Test
    void nullValue_returnsNull() {
        assertNull(convertInternal(dateSchema(), null));
        assertNull(convertInternal(SchemaBuilder.string().build(), null));
    }

    // ─── Time / MicroTime / NanoTime (timezone-independent) ────────────────────

    @Test
    void microTime_withMicros() {
        // 18:00:22.123456 since midnight
        long micros = ((18L * 3600 + 22) * 1_000_000L) + 123_456L;
        assertEquals(
                "18:00:22.123456",
                convertInternal(namedSchema(SchemaBuilder.int64(), MicroTime.SCHEMA_NAME), micros));
    }

    @Test
    void nanoTime_wholeSecond() {
        long nanos = (9L * 3600 + 9 * 60 + 9) * 1_000_000_000L;
        assertEquals(
                "09:09:09",
                convertInternal(namedSchema(SchemaBuilder.int64(), NanoTime.SCHEMA_NAME), nanos));
    }

    @Test
    void time_millisOfDay() {
        int millis = (int) ((1L * 3600 + 2 * 60 + 3) * 1000L);
        assertEquals(
                "01:02:03",
                convertInternal(namedSchema(SchemaBuilder.int32(), Time.SCHEMA_NAME), millis));
    }

    // ─── Decimal (precise / string / double handling modes) ────────────────────

    @Test
    void decimal_preciseBytes() {
        Schema schema = Decimal.builder(4).build();
        byte[] unscaled = BigInteger.valueOf(1_234_567L).toByteArray(); // 123.4567 @ scale 4
        assertEquals(new BigDecimal("123.4567"), convertInternal(schema, unscaled));
    }

    @Test
    void decimal_stringMode() {
        Schema schema = Decimal.builder(2).build();
        assertEquals(new BigDecimal("99.50"), convertInternal(schema, "99.50"));
    }

    @Test
    void decimal_doubleMode() {
        Schema schema = Decimal.builder(1).build();
        assertEquals(BigDecimal.valueOf(5.5d), convertInternal(schema, 5.5d));
    }

    // ─── Bits passthrough ──────────────────────────────────────────────────────

    @Test
    void bits_passThroughBytes() {
        Schema schema = namedSchema(SchemaBuilder.bytes(), Bits.LOGICAL_NAME);
        byte[] value = new byte[] {0x01, 0x02};
        assertArrayEquals(value, (byte[]) convertInternal(schema, value));
    }

    // ─── Array / Binary ────────────────────────────────────────────────────────

    @Test
    void array_ofInts_withNullElement() {
        Schema schema = SchemaBuilder.array(SchemaBuilder.int32().build()).optional().build();
        assertEquals(
                java.util.Arrays.asList(1L, null, 3L),
                convertInternal(schema, java.util.Arrays.asList(1, null, 3)));
    }

    @Test
    void binary_byteArrayPassThrough() {
        byte[] data = {1, 2, 3};
        assertArrayEquals(data, (byte[]) convertInternal(SchemaBuilder.bytes().build(), data));
    }

    @Test
    void binary_byteBufferCopied() {
        byte[] data = {9, 8, 7};
        assertArrayEquals(
                data,
                (byte[]) convertInternal(SchemaBuilder.bytes().build(), java.nio.ByteBuffer.wrap(data)));
    }

    // ─── Unnamed primitives ────────────────────────────────────────────────────

    @Test
    void primitive_int64_toLong() {
        assertEquals(42L, convertInternal(SchemaBuilder.int64().build(), 42L));
    }

    @Test
    void primitive_boolean() {
        assertEquals(true, convertInternal(SchemaBuilder.bool().build(), true));
    }

    // ─── Timestamp (epoch millis, UTC-anchored) ───────────────────────────────

    @Test
    void timestamp_epoch() {
        assertEquals(
                "1970-01-01 00:00:00.0",
                convertInternal(namedSchema(SchemaBuilder.int64(), Timestamp.SCHEMA_NAME), 0L)
                        .toString());
    }

    @Test
    void timestamp_ancient_notShifted() {
        long millis =
                LocalDateTime.of(16, 7, 13, 17, 17, 17).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(
                "0016-07-13 17:17:17.0",
                convertInternal(namedSchema(SchemaBuilder.int64(), Timestamp.SCHEMA_NAME), millis)
                        .toString());
    }

    // ─── ZonedTimestamp (TIMESTAMPTZ, server-zone dependent) ──────────────────

    @Test
    void zonedTimestamp_renderedInServerZone() {
        deserializer.setServerTimeZone(ZoneId.of("UTC"));
        assertEquals(
                "2020-07-17 18:00:22.0",
                convertInternal(
                                namedSchema(SchemaBuilder.string(), ZonedTimestamp.SCHEMA_NAME),
                                "2020-07-17T18:00:22Z")
                        .toString());
    }

    // ─── Geometry (PG point -> GeoJSON string) ────────────────────────────────

    @Test
    void geometry_pointToGeoJson() throws Exception {
        // WKB for POINT(1 2), little-endian.
        byte[] wkb =
                new byte[] {
                    0x01, // little endian
                    0x01, 0x00, 0x00, 0x00, // type = point
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF0, 0x3F, // x = 1.0
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 // y = 2.0
                };
        Schema geomSchema = Geometry.builder().optional().build();
        Struct geom = new Struct(geomSchema).put("wkb", wkb).put("srid", 4326);

        Object out = convertInternal(geomSchema, geom);

        JsonNode node = new ObjectMapper().readTree((String) out);
        assertEquals("Point", node.get("type").asText());
        assertEquals(4326, node.get("srid").asInt());
        assertEquals(1.0, node.get("coordinates").get(0).asDouble());
        assertEquals(2.0, node.get("coordinates").get(1).asDouble());
    }

    // ─── helpers ───────────────────────────────────────────────────────────────

    private String convertDate(LocalDate date) {
        return (String) convertInternal(dateSchema(), (int) date.toEpochDay());
    }

    private static Schema dateSchema() {
        return namedSchema(SchemaBuilder.int32(), io.debezium.time.Date.SCHEMA_NAME);
    }

    private static Schema namedSchema(SchemaBuilder builder, String name) {
        return builder.name(name).optional().build();
    }

    private Object convertInternal(Schema schema, Object dbzObj) {
        try {
            Method m =
                    DebeziumJsonDeserializer.class.getDeclaredMethod(
                            "convertInternal", Schema.class, Object.class);
            m.setAccessible(true);
            return m.invoke(deserializer, schema, dbzObj);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
