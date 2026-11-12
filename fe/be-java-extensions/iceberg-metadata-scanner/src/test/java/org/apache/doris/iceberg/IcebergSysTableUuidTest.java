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

import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.ColumnValue;
import org.apache.doris.jni.spi.vec.VectorColumn;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IcebergSysTableUuidTest {
    private static final UUID UUID_VALUE = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final UUID HIGH_UUID = UUID.fromString("fedcba98-7654-3210-fedc-ba9876543210");

    @BeforeAll
    public static void enableOffHeapTesting() {
        OffHeap.setTesting();
    }

    @Test
    public void preservesUuidReadableMetricsBounds() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.UUIDType.get()));
        Map<Integer, ByteBuffer> lower = Collections.singletonMap(1,
                Conversions.toByteBuffer(Types.UUIDType.get(), UUID_VALUE));
        Map<Integer, ByteBuffer> upper = Collections.singletonMap(1,
                Conversions.toByteBuffer(Types.UUIDType.get(), HIGH_UUID));
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("uuid-metrics.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(1L)
                .withMetrics(new Metrics(2L, null, Collections.singletonMap(1, 2L),
                        Collections.singletonMap(1, 0L), null, lower, upper))
                .build();
        Types.StructType metricsType = MetricsUtil.readableMetricsSchema(schema,
                new Schema(DataFile.getType(Types.StructType.of()).fields()))
                .findType("readable_metrics").asStructType();
        StructLike metrics = MetricsUtil.readableMetricsStruct(schema, file, metricsType);
        Map<?, ?> restored = (Map<?, ?>) roundTrip(metrics, metricsType,
                "struct<id:struct<column_size:bigint,value_count:bigint,null_value_count:bigint,"
                        + "nan_value_count:bigint,lower_bound:uuid,upper_bound:uuid>>");
        Map<?, ?> bounds = (Map<?, ?>) restored.get("id");
        Assertions.assertEquals(UUID_VALUE, bounds.get("lower_bound"));
        Assertions.assertEquals(HIGH_UUID, bounds.get("upper_bound"));
        Assertions.assertEquals(2L, bounds.get("value_count"));
    }

    @Test
    public void preservesUuidIdentityPartitionsAndNulls() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.UUIDType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        PartitionData partition = new PartitionData(spec.partitionType());
        for (UUID value : Arrays.asList(UUID_VALUE, HIGH_UUID, null)) {
            partition.set(0, value);
            Assertions.assertEquals(Collections.singletonMap("id", value),
                    roundTrip(partition, spec.partitionType(), "struct<id:uuid>"));
        }
        Assertions.assertNull(roundTrip(null, spec.partitionType(), "struct<id:uuid>"));
    }

    @Test
    public void preservesUuidBitsAndNullsThroughAllConstructors() {
        UUID[] values = {UUID_VALUE, new UUID(0L, 0L), new UUID(-1L, -1L),
                new UUID(Long.MIN_VALUE, Long.MAX_VALUE), HIGH_UUID, null};
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("id", "uuid"), 1);
        try {
            for (UUID value : values) {
                column.appendValue(new IcebergSysTableColumnValue(value));
                column.appendValue(new IcebergSysTableColumnValue(value, "UTC"));
                column.appendValue(new IcebergSysTableColumnValue(value, "UTC", Types.UUIDType.get()));
            }
            for (int i = 0; i < values.length; i++) {
                Assertions.assertArrayEquals(new UUID[] {values[i], values[i], values[i]},
                        column.getUuidColumn(i * 3, (i + 1) * 3));
            }
        } finally {
            column.close();
        }
    }

    @Test
    public void preservesUuidListsAndMapKeysAndValues() {
        List<UUID> elements = Arrays.asList(UUID_VALUE, null, HIGH_UUID);
        Types.ListType listType = Types.ListType.ofOptional(1, Types.UUIDType.get());
        Assertions.assertEquals(elements, roundTrip(elements, listType, "array<uuid>"));
        Assertions.assertNull(roundTrip(null, listType, "array<uuid>"));

        Map<UUID, UUID> entries = new LinkedHashMap<>();
        entries.put(UUID_VALUE, HIGH_UUID);
        entries.put(new UUID(0L, 0L), null);
        Types.MapType mapType = Types.MapType.ofOptional(1, 2, Types.UUIDType.get(), Types.UUIDType.get());
        Assertions.assertEquals(entries, roundTrip(entries, mapType, "map<uuid,uuid>"));
        Assertions.assertNull(roundTrip(null, mapType, "map<uuid,uuid>"));
    }

    @Test
    public void preservesUuidInMixedAndReorderedStructFields() {
        Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, "count", Types.IntegerType.get()),
                Types.NestedField.optional(2, "label", Types.StringType.get()),
                Types.NestedField.optional(3, "id", Types.UUIDType.get()));
        PartitionData record = new PartitionData(structType);
        record.set(0, 7);
        record.set(1, "uuid-partition");
        record.set(2, UUID_VALUE);
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("count", 7);
        expected.put("label", "uuid-partition");
        expected.put("id", UUID_VALUE);
        Assertions.assertEquals(expected, roundTrip(record, structType, "struct<count:int,label:string,id:uuid>"));

        List<ColumnValue> projected = new ArrayList<>();
        new IcebergSysTableColumnValue(record, "UTC", structType)
                .unpackStruct(Arrays.asList(2, 0, 1), projected);
        Assertions.assertEquals(UUID_VALUE, projected.get(0).getUuid());
        Assertions.assertEquals(7, projected.get(1).getInt());
        Assertions.assertEquals("uuid-partition", projected.get(2).getString());
    }

    @Test
    public void preservesBinaryMetadataOutput() {
        byte[] bytes = {0, -1, 0};
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("value", "varbinary"), 1);
        try {
            column.appendValue(new IcebergSysTableColumnValue(ByteBuffer.wrap(bytes), "UTC", Types.BinaryType.get()));
            // Java output uses [length, address] records; BE input uses StringView records.
            long record = column.dataAddress();
            Assertions.assertEquals(bytes.length, OffHeap.getLong(null, record));
            long address = OffHeap.getLong(null, record + Long.BYTES);
            Assertions.assertArrayEquals(bytes, OffHeap.getByte(null, address, bytes.length));
        } finally {
            column.close();
        }
    }

    private static Object roundTrip(Object value, Type icebergType, String dorisType) {
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("value", dorisType), 1);
        try {
            column.appendValue(new IcebergSysTableColumnValue(value, "UTC", icebergType));
            return column.getObjectColumn(0, 1)[0];
        } finally {
            column.close();
        }
    }
}
