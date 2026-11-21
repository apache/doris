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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.TypeChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

class DeltaLakePartitionValuesTest {
    private static String normalizeValue(DataType type, String value) {
        return DeltaLakePartitionValues.normalizeValue(new StructField("p", type, true), value,
                ZoneOffset.UTC, name -> { });
    }

    private static Map<String, String> normalize(StructType schema, List<String> columns, boolean mapped,
            Map<String, String> values) {
        return DeltaLakePartitionValues.normalize(schema, columns, mapped, values, ZoneOffset.UTC, name -> { });
    }

    @Test
    void normalizesNumericDateAndBooleanValues() {
        Assertions.assertEquals("-128", normalizeValue(ByteType.BYTE, "-128"));
        Assertions.assertEquals("32767", normalizeValue(ShortType.SHORT, "32767"));
        Assertions.assertEquals("42", normalizeValue(IntegerType.INTEGER, "0042"));
        Assertions.assertEquals("9223372036854775807", normalizeValue(
                LongType.LONG, "9223372036854775807"));
        Assertions.assertEquals("1.25", normalizeValue(FloatType.FLOAT, "1.25"));
        Assertions.assertEquals("1.25", normalizeValue(DoubleType.DOUBLE, "1.25"));
        Assertions.assertEquals("100.00", normalizeValue(new DecimalType(10, 2), "1.0000E2"));
        Assertions.assertEquals("true", normalizeValue(BooleanType.BOOLEAN, "TRUE"));
        Assertions.assertEquals("2024-02-29", normalizeValue(DateType.DATE, "2024-02-29"));
    }

    @Test
    void preservesNullEmptyBinaryAndPathCharacters() {
        for (String value : Arrays.asList(null, "", "__HIVE_DEFAULT_PARTITION__", "a/b=c%20+d 中文\n")) {
            Assertions.assertEquals(value, normalizeValue(StringType.STRING, value));
            Assertions.assertEquals(value, normalizeValue(BinaryType.BINARY, value));
        }
        Assertions.assertNull(normalizeValue(IntegerType.INTEGER, null));
    }

    @Test
    void legacyTimestampsUseTheSessionZoneAcrossGapsAndOverlaps() {
        TimeZone previous = TimeZone.getDefault();
        StructField instant = new StructField("p", TimestampType.TIMESTAMP, true);
        StructField wall = new StructField("wall", TimestampNTZType.TIMESTAMP_NTZ, true);
        ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
        try {
            for (String jvmZone : List.of("UTC", "Asia/Shanghai", "America/Los_Angeles")) {
                TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
                Set<String> observed = new HashSet<>();
                for (String input : List.of("2024-03-10T02:30:00.123456Z", "2024-03-10T10:30:00.123456+08:00")) {
                    Assertions.assertEquals("2024-03-10T02:30:00.123456Z",
                            DeltaLakePartitionValues.normalizeValue(instant, input, losAngeles, observed::add));
                }
                Assertions.assertEquals("2024-03-10T02:30:00.123456", DeltaLakePartitionValues.normalizeValue(
                        wall, "2024-03-10 02:30:00.123456", losAngeles, observed::add));
                Assertions.assertTrue(observed.isEmpty());
                Assertions.assertEquals("2024-03-10T10:30:00.123456Z", DeltaLakePartitionValues.normalizeValue(
                        instant, "2024-03-10 02:30:00.123456", losAngeles, observed::add));
                Assertions.assertEquals("2024-11-03T08:30:00Z", DeltaLakePartitionValues.normalizeValue(
                        instant, "2024-11-03T01:30:00", losAngeles, observed::add));
                Assertions.assertEquals(Set.of("p"), observed);
                observed.clear();
                Assertions.assertEquals("2024-11-03T01:30:00Z", DeltaLakePartitionValues.normalizeValue(
                        instant, "2024-11-03T01:30:00", ZoneOffset.UTC, observed::add));
                Assertions.assertEquals(Set.of("p"), observed);
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @Test
    void readsDatePartitionsAtMidnightOnlyForTheWidenedField() {
        StructField widened = new StructField("p", TimestampNTZType.TIMESTAMP_NTZ, true)
                .withTypeChanges(List.of(new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)));
        StructType schema = new StructType().add(widened).add("native_ntz", TimestampNTZType.TIMESTAMP_NTZ);
        List<String> columns = List.of("p", "native_ntz");
        String date = "2024-02-29";
        String midnight = "2024-02-29 00:00:00";
        String timestamp = "2024-03-01 12:34:56.123456";
        Map<String, String> partition = normalize(schema, columns, false,
                Map.of("p", date, "native_ntz", timestamp));
        Assertions.assertEquals(Map.of("p", "2024-02-29T00:00:00", "native_ntz", "2024-03-01T12:34:56.123456"),
                partition);
        Assertions.assertEquals(partition, normalize(schema, columns, false,
                Map.of("p", midnight, "native_ntz", timestamp)));
        Assertions.assertEquals("2024-03-01T12:34:56.123456", normalize(schema,
                List.of("p"), false, Map.of("p", timestamp)).get("p"));
        Assertions.assertThrows(DorisConnectorException.class, () -> normalize(
                schema, columns, false, Map.of("p", date, "native_ntz", date)));
        Assertions.assertThrows(DorisConnectorException.class, () -> normalize(
                schema, List.of("p"), false, Map.of("p", "2023-02-29")));
        Set<String> observed = new HashSet<>();
        Assertions.assertNull(DeltaLakePartitionValues.normalize(
                schema, List.of("p"), false, Map.of("p", ""), ZoneOffset.UTC, observed::add).get("p"));
        Assertions.assertEquals(Set.of("p"), observed);
    }

    @Test
    void mapsPhysicalKeysToOrderedLogicalPartitionColumns() {
        StructType schema = new StructType().add(new StructField("Renamed", IntegerType.INTEGER, true,
                FieldMetadata.builder().putLong("delta.columnMapping.id", 1)
                        .putString("delta.columnMapping.physicalName", "col-uuid").build()))
                .add(new StructField("Other", StringType.STRING, true,
                        FieldMetadata.builder().putLong("delta.columnMapping.id", 2)
                                .putString("delta.columnMapping.physicalName", "other-uuid").build()));
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("other-uuid", null);
        raw.put("col-uuid", "0042");
        Map<String, String> normalized = normalize(schema,
                Arrays.asList("Renamed", "Other"), true, raw);
        Assertions.assertEquals(Arrays.asList("Renamed", "Other"), Arrays.asList(normalized.keySet().toArray()));
        Assertions.assertEquals("42", normalized.get("Renamed"));
        Assertions.assertNull(normalized.get("Other"));
        Assertions.assertThrows(DorisConnectorException.class, () -> normalize(schema,
                Collections.singletonList("Renamed"), true, Collections.emptyMap()));
    }

    @Test
    void recordsOnlyEmptyStringsDecodedAsNullUnderTheirLogicalNames() {
        FieldMetadata key = FieldMetadata.builder().putString("delta.columnMapping.physicalName", "old_key").build();
        FieldMetadata label = FieldMetadata.builder()
                .putString("delta.columnMapping.physicalName", "old_label").build();
        for (DataType type : List.of(BooleanType.BOOLEAN, ByteType.BYTE, ShortType.SHORT, IntegerType.INTEGER,
                LongType.LONG, FloatType.FLOAT, DoubleType.DOUBLE, new DecimalType(9, 2), DateType.DATE,
                TimestampType.TIMESTAMP, TimestampNTZType.TIMESTAMP_NTZ)) {
            StructType schema = new StructType().add(new StructField("Key", type, true, key))
                    .add(new StructField("label", StringType.STRING, true, label));
            List<String> names = List.of("Key", "label");
            Set<String> emptyColumns = new HashSet<>();
            Map<String, String> raw = new LinkedHashMap<>();
            raw.put("old_key", null);
            raw.put("old_label", "");
            Map<String, String> jsonNull = DeltaLakePartitionValues.normalize(
                    schema, names, true, raw, ZoneOffset.UTC, emptyColumns::add);
            Assertions.assertTrue(emptyColumns.isEmpty());
            Assertions.assertNull(jsonNull.get("Key"));
            Assertions.assertEquals("", jsonNull.get("label"));
            raw.put("old_key", "");
            Assertions.assertEquals(jsonNull,
                    DeltaLakePartitionValues.normalize(schema, names, true, raw, ZoneOffset.UTC, emptyColumns::add));
            Assertions.assertEquals(Set.of("Key"), emptyColumns);
            raw.put("old_key", " ");
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> DeltaLakePartitionValues.normalize(
                            schema, names, true, raw, ZoneOffset.UTC, emptyColumns::add));
        }
    }

    @Test
    void rejectsInvalidTypedValuesInsteadOfCoercingThem() {
        for (String value : List.of("128", "null")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> normalizeValue(ByteType.BYTE, value));
        }
        StructType schema = new StructType().add("flag", BooleanType.BOOLEAN);
        Assertions.assertThrows(DorisConnectorException.class, () -> normalize(schema,
                Collections.singletonList("flag"), false, Collections.singletonMap("flag", "garbage")));
        Assertions.assertThrows(java.time.DateTimeException.class,
                () -> normalizeValue(DateType.DATE, "2023-02-29"));
    }
}
