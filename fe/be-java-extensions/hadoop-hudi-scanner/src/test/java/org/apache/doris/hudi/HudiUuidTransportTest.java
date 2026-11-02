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

package org.apache.doris.hudi;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class HudiUuidTransportTest {
    private static final UUID UUID_VALUE = UUID.fromString("fedcba98-7654-3210-8123-456789abcdef");

    private byte[] uuidBytes() {
        return ByteBuffer.allocate(16).putLong(UUID_VALUE.getMostSignificantBits())
                .putLong(UUID_VALUE.getLeastSignificantBits()).array();
    }

    @Test
    public void testBinaryAndTextUuidCarriers() {
        HadoopHudiColumnValue value = new HadoopHudiColumnValue(ZoneOffset.UTC);
        value.setField(ColumnType.parseType("id", "uuid"),
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
        BytesWritable padded = new BytesWritable(uuidBytes());
        padded.setCapacity(64);
        for (Object carrier : Arrays.asList(padded, new Text(UUID_VALUE.toString()),
                new BytesWritable(UUID_VALUE.toString().getBytes(StandardCharsets.UTF_8)))) {
            value.setRow(carrier);
            Assert.assertEquals(UUID_VALUE, value.getUuid());
        }
        value.setField(ColumnType.parseType("id", "uuid"),
                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
        value.setRow(uuidBytes());
        Assert.assertEquals(UUID_VALUE, value.getUuid());
        value.setField(ColumnType.parseType("id", "uuid"),
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        value.setRow(UUID_VALUE.toString());
        Assert.assertEquals(UUID_VALUE, value.getUuid());
        value.setField(ColumnType.parseType("id", "uuid"),
                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
        value.setRow(new byte[8]);
        Assert.assertThrows(IllegalArgumentException.class, value::getUuid);
    }

    @Test
    public void testNestedUuidThroughHudiWritableSerdeAndJniVector() throws Exception {
        OffHeap.setTesting();
        for (boolean fixed : new boolean[] {false, true}) {
            Schema uuid = LogicalTypes.uuid().addToSchema(fixed
                    ? Schema.createFixed("uuid_fixed", null, null, 16) : Schema.create(Schema.Type.STRING));
            Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), uuid);
            Schema array = Schema.createArray(nullable);
            Schema record = Schema.createRecord("uuid_record", null, null, false);
            record.setFields(Arrays.asList(new Schema.Field("ids", array),
                    new Schema.Field("by_name", Schema.createMap(nullable)), new Schema.Field("id", nullable)));
            Object id = fixed ? new GenericData.Fixed(uuid, uuidBytes()) : UUID_VALUE.toString();
            GenericData.Record row = new GenericData.Record(record);
            row.put("ids", new GenericData.Array<>(array, Arrays.asList(id, null)));
            Map<String, Object> byName = new HashMap<>();
            byName.put("present", id);
            byName.put("absent", null);
            row.put("by_name", byName);
            row.put("id", null);
            ParquetHiveSerDe serde = new ParquetHiveSerDe();
            Properties properties = new Properties();
            properties.setProperty("columns", "value");
            properties.setProperty("columns.types", "struct<ids:array<binary>,by_name:map<string,binary>,id:binary>");
            serde.initialize(new Configuration(false), properties);
            StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
            StructField field = inspector.getStructFieldRef("value");
            HadoopHudiColumnValue value = new HadoopHudiColumnValue(ZoneOffset.UTC);
            ColumnType type = ColumnType.parseType("value", "struct<ids:array<uuid>,by_name:map<string,uuid>,id:uuid>");
            value.setField(type, field.getFieldObjectInspector());
            value.setRow(HoodieRealtimeRecordReaderUtils.avroToArrayWritable(row, record));
            VectorColumn column = VectorColumn.createWritableColumn(type, 1);
            try {
                column.appendValue(value);
                Map<?, ?> restored = column.getStructColumn(0, 1)[0];
                Assert.assertEquals(Arrays.asList(UUID_VALUE, null), restored.get("ids"));
                Assert.assertEquals(UUID_VALUE, ((Map<?, ?>) restored.get("by_name")).get("present"));
                Assert.assertNull(((Map<?, ?>) restored.get("by_name")).get("absent"));
                Assert.assertNull(restored.get("id"));
            } finally {
                column.close();
            }
        }
    }

    @Test
    public void testParquetStringAndFixedUuidWithNormalizedHiveSchema() throws Exception {
        Path directory = Files.createTempDirectory("hudi-uuid");
        try {
            for (boolean fixed : new boolean[] {false, true}) {
                Path file = directory.resolve(fixed + ".parquet");
                Schema uuid = LogicalTypes.uuid().addToSchema(fixed
                        ? Schema.createFixed("uuid_fixed", null, null, 16) : Schema.create(Schema.Type.STRING));
                Schema record = Schema.createRecord("uuid_record", null, null, false);
                record.setFields(Arrays.asList(new Schema.Field("id", uuid)));
                GenericRecord row = new GenericData.Record(record);
                row.put("id", fixed ? new GenericData.Fixed(uuid, uuidBytes()) : UUID_VALUE.toString());
                JobConf conf = new JobConf(false);
                conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
                org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                        .withSchema(record).withConf(conf).build()) {
                    writer.write(row);
                }
                conf.set("columns", "id");
                conf.set("columns.types", "binary");
                conf.set("hive.io.file.read.all.columns", "true");
                Properties properties = new Properties();
                properties.setProperty("columns", "id");
                properties.setProperty("columns.types", "binary");
                ParquetHiveSerDe serde = new ParquetHiveSerDe();
                serde.initialize(conf, properties);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                StructField field = inspector.getStructFieldRef("id");
                try (RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat()
                        .getRecordReader(new FileSplit(hadoopPath, 0, Files.size(file), new String[0]),
                                conf, Reporter.NULL)) {
                    ArrayWritable writable = reader.createValue();
                    Assert.assertTrue(reader.next(reader.createKey(), writable));
                    HadoopHudiColumnValue value = new HadoopHudiColumnValue(ZoneOffset.UTC);
                    value.setField(ColumnType.parseType("id", "uuid"), field.getFieldObjectInspector());
                    value.setRow(inspector.getStructFieldData(serde.deserialize(writable), field));
                    Assert.assertEquals(UUID_VALUE, value.getUuid());
                    Assert.assertFalse(reader.next(reader.createKey(), writable));
                } finally {
                    Files.deleteIfExists(file);
                }
            }
        } finally {
            try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files::iterator) {
                    Files.deleteIfExists(file);
                }
            }
            Files.delete(directory);
        }
    }

    @Test
    public void testScannerUsesRequestedJniTypesInsteadOfHiveCarriers() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("data_file_length", "0");
        params.put("serde", ParquetHiveSerDe.class.getName());
        params.put("required_fields", "ids,id");
        params.put("columns_types", "array<uuid>#uuid");
        params.put("hudi_column_names", "id,unused,ids");
        params.put("hudi_column_types", "binary#string#array<binary>");
        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1, params);
        Method init = HadoopHudiJniScanner.class.getDeclaredMethod("initRequiredColumnsAndTypes");
        init.setAccessible(true);
        init.invoke(scanner);
        Field types = HadoopHudiJniScanner.class.getDeclaredField("requiredTypes");
        types.setAccessible(true);
        ColumnType[] actual = (ColumnType[]) types.get(scanner);
        Assert.assertEquals(ColumnType.Type.UUID, actual[0].getChildTypes().get(0).getType());
        Assert.assertEquals(ColumnType.Type.UUID, actual[1].getType());
        Method properties = HadoopHudiJniScanner.class.getDeclaredMethod("getReaderProperties");
        properties.setAccessible(true);
        Assert.assertEquals("binary,string,array<binary>",
                ((Properties) properties.invoke(scanner)).getProperty("columns.types"));
        params.put("required_fields", "");
        params.put("columns_types", "");
        HadoopHudiJniScanner emptyScanner = new HadoopHudiJniScanner(1, params);
        init.invoke(emptyScanner);
        Assert.assertEquals(0, ((ColumnType[]) types.get(emptyScanner)).length);
    }
}
