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

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergSysTableColumnValueTest {
    @Test
    public void testTimestampMicrosPreservePrecisionAndNtzWallTime() {
        long[] microsValues = {-1_000_001L, -1L, 0L, 1L, 999L, 1_000L, 1_234_567L};
        for (long micros : microsValues) {
            LocalDateTime expected = LocalDateTime.ofEpochSecond(
                    Math.floorDiv(micros, 1_000_000L),
                    Math.toIntExact(Math.floorMod(micros, 1_000_000L) * 1_000L),
                    ZoneOffset.UTC);
            IcebergSysTableColumnValue value = new IcebergSysTableColumnValue(
                    micros, "Asia/Shanghai", Types.TimestampType.withoutZone());
            Assert.assertEquals(expected, value.getDateTime());
            Assert.assertEquals(expected, value.getTimeStampTz());
        }
    }

    @Test
    public void testTimestampTzPreservesPrecisionAndUsesSessionTimeZone() {
        long micros = 1_234_567L;
        IcebergSysTableColumnValue value = new IcebergSysTableColumnValue(
                micros, "Asia/Shanghai", Types.TimestampType.withZone());

        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 8, 0, 1, 234_567_000), value.getDateTime());
        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 1, 234_567_000), value.getTimeStampTz());
    }

    @Test
    public void testNestedTimestampTzUsesSessionTimeZone() {
        Types.StructType metricType = Types.StructType.of(
                Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()));
        Types.MapType readableMetricsType = Types.MapType.ofOptional(
                2, 3, Types.StringType.get(), metricType);
        Map<String, StructLike> readableMetrics = Collections.singletonMap(
                "metric", new ArrayStructLike(1_234_567L));
        IcebergSysTableColumnValue value = new IcebergSysTableColumnValue(
                readableMetrics, "Asia/Shanghai", readableMetricsType);

        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();
        value.unpackMap(keys, values);
        List<ColumnValue> metricFields = new ArrayList<>();
        values.get(0).unpackStruct(Collections.singletonList(0), metricFields);

        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 8, 0, 1, 234_567_000),
                metricFields.get(0).getDateTime());
    }

    private static class ArrayStructLike implements StructLike {
        private final Object[] values;

        private ArrayStructLike(Object... values) {
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            values[pos] = value;
        }
    }
}
