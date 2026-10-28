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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

public class IcebergPartitionUtilsCoverageTest {
    @Test
    public void compoundPartitionWireFormatPreservesEscapingAndExactValues() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "text", Types.StringType.get()),
                Types.NestedField.optional(2, "nullable", Types.StringType.get()),
                Types.NestedField.optional(3, "amount", Types.DecimalType.of(38, 18)),
                Types.NestedField.optional(4, "date", Types.DateType.get()),
                Types.NestedField.optional(5, "timestamp", Types.TimestampNanoType.withoutZone()),
                Types.NestedField.optional(6, "binary", Types.BinaryType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("text").identity("nullable")
                .identity("amount").identity("date").identity("timestamp").identity("binary").build();
        PartitionData original = new PartitionData(spec.partitionType());
        List<Object> values = Arrays.asList("null/\"quoted\"\\line\n\u0000雪", null,
                new BigDecimal("-99999999999999999999.000000000000000001"), -1,
                -1L, ByteBuffer.wrap(new byte[] {0, (byte) 0xff, (byte) 0x80, 34, 92}));
        for (int i = 0; i < values.size(); i++) {
            original.set(i, values.get(i));
        }

        for (ZoneId zone : Arrays.asList(ZoneOffset.UTC, ZoneId.of("America/New_York"))) {
            List<String> encoded = IcebergUtils.parsePartitionValuesFromJson(
                    IcebergUtils.getPartitionDataJson(original, spec, zone.getId()));
            Assertions.assertEquals(values.size(), encoded.size());
            Assertions.assertEquals(values.get(0), encoded.get(0));
            Assertions.assertNull(encoded.get(1));
            PartitionData decoded = new PartitionData(spec.partitionType());
            for (int i = 0; i < encoded.size(); i++) {
                decoded.set(i, IcebergUtils.parsePartitionValueFromString(
                        encoded.get(i), spec.partitionType().fields().get(i).type()));
            }
            // PartitionData stores binary as byte[], whose container equality compares array identities.
            for (int i = 0; i < original.size(); i++) {
                Assertions.assertEquals(original.get(i), decoded.get(i), "partition field " + i);
            }
            Assertions.assertEquals(18, ((BigDecimal) decoded.get(2)).scale());
        }
    }

    @Test
    public void calendarRangesAgreeWithSdkTransformsAcrossEpochAndLeapBoundaries() throws Exception {
        List<LocalDateTime> timestamps = Arrays.asList(
                LocalDateTime.of(1600, 2, 29, 23, 59, 59, 999999000),
                LocalDateTime.of(1900, 3, 1, 0, 0),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(2000, 2, 29, 23, 59, 59, 999999000),
                LocalDateTime.of(2100, 3, 1, 0, 0));
        for (Type type : Arrays.asList(Types.DateType.get(),
                Types.TimestampType.withoutZone(), Types.TimestampType.withZone())) {
            for (String name : Arrays.asList("year", "month", "day", "hour")) {
                if (type.typeId() == Type.TypeID.DATE && name.equals("hour")) {
                    continue;
                }
                Transform<Object, Integer> transform = calendarTransform(name);
                for (LocalDateTime timestamp : timestamps) {
                    int ordinal = transform.bind(type).apply(internalValue(type, timestamp));
                    org.apache.doris.catalog.Type dorisType = IcebergUtils.icebergTypeToDorisType(type, true, true);
                    com.google.common.collect.Range<org.apache.doris.catalog.PartitionKey> range =
                            IcebergUtils.getPartitionRange(Integer.toString(ordinal), name,
                                    java.util.Collections.singletonList(
                                            new org.apache.doris.catalog.Column("value", dorisType)));
                    LocalDateTime lower = parseBound(range.lowerEndpoint().getKeys().get(0).getStringValue(), type);
                    LocalDateTime upper = parseBound(range.upperEndpoint().getKeys().get(0).getStringValue(), type);
                    LocalDateTime row = type.typeId() == Type.TypeID.DATE
                            ? timestamp.toLocalDate().atStartOfDay() : timestamp;
                    Assertions.assertFalse(row.isBefore(lower), name + " " + timestamp);
                    Assertions.assertTrue(row.isBefore(upper), name + " " + timestamp);
                    // Both ends must describe exactly one SDK transform ordinal, including negative ordinals.
                    Assertions.assertEquals(ordinal, transform.bind(type).apply(internalValue(type, lower)));
                    LocalDateTime last = type.typeId() == Type.TypeID.DATE
                            ? upper.minusDays(1) : upper.minusNanos(1000);
                    Assertions.assertEquals(ordinal, transform.bind(type).apply(internalValue(type, last)));
                    Assertions.assertEquals(ordinal + 1, transform.bind(type).apply(internalValue(type, upper)));
                }
            }
        }
    }

    @Test
    public void binarySerializationDoesNotConsumeDirectOrReadOnlyBuffers() {
        ByteBuffer direct = ByteBuffer.allocateDirect(6);
        direct.put(new byte[] {99, 0, (byte) 0xfe, (byte) 0xff, 1, 99});
        direct.position(1);
        direct.limit(5);
        for (ByteBuffer buffer : Arrays.asList(direct, direct.asReadOnlyBuffer(), direct.slice())) {
            int position = buffer.position();
            int limit = buffer.limit();
            for (Type type : Arrays.asList(Types.BinaryType.get(), Types.FixedType.ofLength(4))) {
                String encoded = IcebergUtils.serializePartitionValue(type, buffer, "UTC");
                Assertions.assertEquals("0x00feff01", encoded);
                Assertions.assertEquals(buffer, IcebergUtils.parsePartitionValueFromString(
                        encoded, type));
                Assertions.assertEquals(position, buffer.position());
                Assertions.assertEquals(limit, buffer.limit());
            }
        }
    }

    private static Transform<Object, Integer> calendarTransform(String name) {
        switch (name) {
            case "year":
                return Transforms.year();
            case "month":
                return Transforms.month();
            case "day":
                return Transforms.day();
            case "hour":
                return Transforms.hour();
            default:
                throw new IllegalArgumentException(name);
        }
    }

    private static Object internalValue(Type type, LocalDateTime timestamp) {
        return type.typeId() == Type.TypeID.DATE ? (Object) (int) timestamp.toLocalDate().toEpochDay()
                : timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + timestamp.getNano() / 1000;
    }

    private static LocalDateTime parseBound(String bound, Type type) {
        return type.typeId() == Type.TypeID.DATE ? LocalDate.parse(bound).atStartOfDay()
                : LocalDateTime.parse(bound.replace("+00:00", "").replace(' ', 'T'));
    }
}
