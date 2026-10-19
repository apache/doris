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

package org.apache.doris.iceberg;

import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IcebergSysTableColumnValueTest {
    @Test
    public void preservesMicrosAndTimestampSemantics() {
        long[] micros = {-1000001L, -1L, 1L, 999L, 1000L, 1234567L};
        String[] expected = {"1969-12-31T23:59:58.999999", "1969-12-31T23:59:59.999999",
                "1970-01-01T00:00:00.000001", "1970-01-01T00:00:00.000999",
                "1970-01-01T00:00:00.001", "1970-01-01T00:00:01.234567"};
        for (int i = 0; i < micros.length; i++) {
            LocalDateTime utc = LocalDateTime.parse(expected[i]);
            for (String zone : Arrays.asList("UTC", "Asia/Shanghai")) {
                ColumnValue ntz = new IcebergSysTableColumnValue(micros[i], zone, Types.TimestampType.withoutZone());
                ColumnValue tz = new IcebergSysTableColumnValue(micros[i], zone, Types.TimestampType.withZone());
                Assertions.assertEquals(utc, ntz.getDateTime());
                Assertions.assertEquals(zone.equals("UTC") ? utc : utc.plusHours(8), tz.getDateTime());
                Assertions.assertEquals(utc, tz.getTimeStampTz());
            }
        }
    }

    @Test
    public void propagatesTypesThroughNestedContainers() {
        Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(2, "tz", Types.TimestampType.withZone()));
        PartitionData record = new PartitionData(struct);
        record.set(0, -1L);
        record.set(1, -1L);
        List<ColumnValue> fields = new ArrayList<>();
        new IcebergSysTableColumnValue(record, "Asia/Shanghai", struct)
                .unpackStruct(Arrays.asList(1, 0), fields);
        LocalDateTime utc = LocalDateTime.parse("1969-12-31T23:59:59.999999");
        Assertions.assertEquals(utc.plusHours(8), fields.get(0).getDateTime());
        Assertions.assertEquals(utc, fields.get(1).getDateTime());

        List<ColumnValue> elements = new ArrayList<>();
        new IcebergSysTableColumnValue(Collections.singletonList(-1L), "Asia/Shanghai",
                Types.ListType.ofOptional(3, Types.TimestampType.withoutZone())).unpackArray(elements);
        Assertions.assertEquals(utc, elements.get(0).getDateTime());

        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();
        new IcebergSysTableColumnValue(Collections.singletonMap(-1L, -1L), "Asia/Shanghai",
                Types.MapType.ofOptional(4, 5, Types.TimestampType.withoutZone(), Types.TimestampType.withZone()))
                .unpackMap(keys, values);
        Assertions.assertEquals(utc, keys.get(0).getDateTime());
        Assertions.assertEquals(utc.plusHours(8), values.get(0).getDateTime());
    }
}
