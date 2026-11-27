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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IcebergMetadataTimestampNsTest {
    @BeforeAll
    public static void useUnsafeAllocation() {
        // The memory tracker natives are registered by BE; these tests run in Java.
        OffHeap.setTesting();
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai"})
    public void preservesNanosecondBoundsAndNulls(String zone) {
        long[] nanos = {Long.MIN_VALUE, -1_000_000_001L, -1_000_000_000L, -999_999_999L, -1L,
                0L, 1L, 999_999_999L, 1_000_000_000L, 1_234_567_890L,
                DateTimeUtil.nanosFromTimestamp(LocalDateTime.of(2024, 2, 29, 12, 34, 56, 123456789)),
                Long.MAX_VALUE};
        Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
        VectorColumn column = VectorColumn.createWritableColumn(
                ColumnType.parseType("bound", "timestamp_ns"), nanos.length + 1);
        try {
            for (int i = 0; i < nanos.length; i++) {
                Object bound = Conversions.fromByteBuffer(type, Conversions.toByteBuffer(type, nanos[i]));
                column.appendValue(new IcebergSysTableColumnValue(bound, zone, type));
                Assertions.assertEquals(DateTimeUtil.timestampFromNanos(nanos[i]), column.getTimestampNs(i));
                Assertions.assertEquals(nanos[i], OffHeap.getLong(null, column.dataAddress() + i * Long.BYTES));
            }
            column.appendValue(new IcebergSysTableColumnValue(null, zone, type));
            Assertions.assertTrue(column.isNullAt(nanos.length));
        } finally {
            column.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai"})
    public void propagatesNanosecondTypesThroughContainers(String zone) {
        Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
        Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, "ns", type),
                Types.NestedField.optional(2, "micros", Types.TimestampType.withoutZone()));
        PartitionData record = new PartitionData(struct);
        record.set(0, -1L);
        record.set(1, -1L);
        ColumnValue[] values = {
                new IcebergSysTableColumnValue(Arrays.asList(-1L, null), zone,
                        Types.ListType.ofOptional(3, type)),
                new IcebergSysTableColumnValue(Map.of("ts", -1L), zone,
                        Types.MapType.ofOptional(4, 5, Types.StringType.get(), type)),
                new IcebergSysTableColumnValue(record, zone, struct)
        };
        String[] types = {"array<timestamp_ns>", "map<string,timestamp_ns>",
                "struct<ns:timestamp_ns,micros:datetimev2(6)>"};
        LocalDateTime expected = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999);
        Object[] expectedValues = {Arrays.asList(expected, null), Map.of("ts", expected),
                Map.of("ns", expected, "micros", expected.withNano(999999000))};
        for (int i = 0; i < types.length; i++) {
            VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("nested", types[i]), 1);
            try {
                column.appendValue(values[i]);
                Assertions.assertEquals(expectedValues[i], column.getObjectColumn(0, 1)[0]);
            } finally {
                column.close();
            }
        }
    }

    @Test
    public void preservesUntypedMicrosecondConstructors() {
        long micros = 1_234_567L;
        LocalDateTime utc = LocalDateTime.of(1970, 1, 1, 0, 0, 1, 234567000);
        Assertions.assertEquals(utc.plusHours(8), new IcebergSysTableColumnValue(micros).getDateTime());
        Assertions.assertEquals(utc, new IcebergSysTableColumnValue(micros, "UTC").getDateTime());
        Assertions.assertEquals(utc.plusHours(8),
                new IcebergSysTableColumnValue(micros, "Asia/Shanghai").getDateTime());
        Assertions.assertEquals(utc, new IcebergSysTableColumnValue(micros, "Asia/Shanghai").getTimeStampTz());
    }

    @ParameterizedTest
    @CsvSource({"FILES,UTC", "FILES,Asia/Shanghai", "PARTITIONS,UTC", "PARTITIONS,Asia/Shanghai"})
    public void scansSerializedNanosecondMetadata(MetadataTableType metadataType, String zone,
            @TempDir Path warehouse) throws IOException {
        Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
        Schema schema = new Schema(Types.NestedField.optional(1, "ts_ns", type));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("ts_ns").build();
        Table table = new HadoopTables(new Configuration()).create(schema, spec,
                Map.of("format-version", "3"), warehouse.resolve("t").toString());
        LocalDateTime expected = LocalDateTime.of(2024, 2, 29, 12, 34, 56, 123456789);
        long nanos = DateTimeUtil.nanosFromTimestamp(expected);
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, nanos);
        Map<Integer, ByteBuffer> bounds = Map.of(1, Conversions.toByteBuffer(type, nanos));
        Metrics metrics = new Metrics(1L, Map.of(1, 8L), Map.of(1, 1L), Map.of(1, 0L),
                Map.of(), bounds, bounds);
        table.newAppend().appendFile(DataFiles.builder(spec)
                .withPath(warehouse.resolve("t/data/f.parquet").toString())
                .withFileSizeInBytes(10).withPartition(partition).withMetrics(metrics).build()).commit();

        Table metadata = MetadataTableUtils.createMetadataTableInstance(table, metadataType);
        // Request readable_metrics before partition to exercise the projected row order as well.
        boolean files = metadataType == MetadataTableType.FILES;
        Schema projection = files
                ? new Schema(metadata.schema().findField("readable_metrics"), metadata.schema().findField("partition"))
                : new Schema(metadata.schema().findField("partition"));
        int rows = 0;
        try (CloseableIterable<FileScanTask> tasks = metadata.newScan().project(projection).planFiles()) {
            for (FileScanTask task : tasks) {
                Map<String, String> params = new HashMap<>();
                params.put("serialized_split", SerializationUtil.serializeToBase64(task));
                params.put("time_zone", zone);
                params.put("required_fields", files ? "readable_metrics,partition" : "partition");
                String partitionType = "struct<ts_ns:timestamp_ns>";
                String metricsType = "struct<ts_ns:struct<column_size:bigint,value_count:bigint,"
                        + "null_value_count:bigint,nan_value_count:bigint,"
                        + "lower_bound:timestamp_ns,upper_bound:timestamp_ns>>";
                params.put("required_types", files ? metricsType + "#" + partitionType : partitionType);
                IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(16, params);
                scanner.open();
                try {
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(1, scanner.getTable().getNumRows());
                    int partitionIndex = files ? 1 : 0;
                    Assertions.assertEquals(expected,
                            scanner.getTable().getColumn(partitionIndex).getStruct(0).get("ts_ns"));
                    if (files) {
                        Map<?, ?> readable = (Map<?, ?>) scanner.getTable().getColumn(0).getStruct(0).get("ts_ns");
                        Assertions.assertEquals(expected, readable.get("lower_bound"));
                        Assertions.assertEquals(expected, readable.get("upper_bound"));
                    }
                    rows += scanner.getTable().getNumRows();
                    scanner.releaseTable();
                    Assertions.assertEquals(0, scanner.getNextBatchMeta());
                } finally {
                    scanner.releaseTable();
                    scanner.close();
                }
            }
        }
        Assertions.assertEquals(1, rows);
    }
}
