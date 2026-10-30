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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

class DeltaLakePartitionValuesTest {
    @Test
    void normalizesNumericDateAndBooleanValues() {
        Assertions.assertEquals("-128", DeltaLakePartitionValues.normalizeValue(ByteType.BYTE, "-128"));
        Assertions.assertEquals("32767", DeltaLakePartitionValues.normalizeValue(ShortType.SHORT, "32767"));
        Assertions.assertEquals("42", DeltaLakePartitionValues.normalizeValue(IntegerType.INTEGER, "0042"));
        Assertions.assertEquals("9223372036854775807", DeltaLakePartitionValues.normalizeValue(
                LongType.LONG, "9223372036854775807"));
        Assertions.assertEquals("1.25", DeltaLakePartitionValues.normalizeValue(FloatType.FLOAT, "1.25"));
        Assertions.assertEquals("1.25", DeltaLakePartitionValues.normalizeValue(DoubleType.DOUBLE, "1.25"));
        Assertions.assertEquals("100.00", DeltaLakePartitionValues.normalizeValue(new DecimalType(10, 2), "1.0000E2"));
        Assertions.assertEquals("true", DeltaLakePartitionValues.normalizeValue(BooleanType.BOOLEAN, "TRUE"));
        Assertions.assertEquals("2024-02-29", DeltaLakePartitionValues.normalizeValue(DateType.DATE, "2024-02-29"));
    }

    @Test
    void preservesNullEmptyBinaryAndPathCharacters() {
        for (String value : Arrays.asList(null, "", "__HIVE_DEFAULT_PARTITION__", "a/b=c%20+d 中文\n")) {
            Assertions.assertEquals(value, DeltaLakePartitionValues.normalizeValue(StringType.STRING, value));
            Assertions.assertEquals(value, DeltaLakePartitionValues.normalizeValue(BinaryType.BINARY, value));
        }
        Assertions.assertNull(DeltaLakePartitionValues.normalizeValue(IntegerType.INTEGER, null));
    }

    @Test
    void instantAndWallClockRenderingIgnoreJvmDefaultTimezone() {
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : Arrays.asList("UTC", "Asia/Shanghai", "America/Los_Angeles")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (String input : Arrays.asList("2024-03-10 02:30:00.123456", "2024-03-10T02:30:00.123456Z",
                        "2024-03-10T10:30:00.123456+08:00")) {
                    Assertions.assertEquals("2024-03-10T02:30:00.123456Z",
                            DeltaLakePartitionValues.normalizeValue(TimestampType.TIMESTAMP, input), zone);
                }
                Assertions.assertEquals("2024-03-10T02:30:00.123456", DeltaLakePartitionValues.normalizeValue(
                        TimestampNTZType.TIMESTAMP_NTZ, "2024-03-10 02:30:00.123456"), zone);
            }
        } finally {
            TimeZone.setDefault(previous);
        }
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
        Map<String, String> normalized = DeltaLakePartitionValues.normalize(schema,
                Arrays.asList("Renamed", "Other"), true, raw);
        Assertions.assertEquals(Arrays.asList("Renamed", "Other"), Arrays.asList(normalized.keySet().toArray()));
        Assertions.assertEquals("42", normalized.get("Renamed"));
        Assertions.assertNull(normalized.get("Other"));
        Assertions.assertThrows(DorisConnectorException.class, () -> DeltaLakePartitionValues.normalize(schema,
                Collections.singletonList("Renamed"), true, Collections.emptyMap()));
    }

    @Test
    void rejectsInvalidTypedValuesInsteadOfCoercingThem() {
        for (String value : Arrays.asList("128", "", "null")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> DeltaLakePartitionValues.normalizeValue(ByteType.BYTE, value));
        }
        StructType schema = new StructType().add("flag", BooleanType.BOOLEAN);
        Assertions.assertThrows(DorisConnectorException.class, () -> DeltaLakePartitionValues.normalize(schema,
                Collections.singletonList("flag"), false, Collections.singletonMap("flag", "garbage")));
        Assertions.assertThrows(java.time.DateTimeException.class,
                () -> DeltaLakePartitionValues.normalizeValue(DateType.DATE, "2023-02-29"));
    }
}
