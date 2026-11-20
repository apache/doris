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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.types.Types;

/** Independent Avro writers and Iceberg readers supply the SQL regression's oracle. */
public final class CreateAvroFixtures {
    private CreateAvroFixtures() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "rest");
        properties.put("uri", "http://rest:8181");
        properties.put("warehouse", "s3://warehouse/wh/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");
        Catalog catalog = CatalogUtil.buildIcebergCatalog("demo", properties, null);
        Namespace namespace = Namespace.of(args[0]);
        SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
        if (!namespaces.namespaceExists(namespace)) {
            namespaces.createNamespace(namespace);
        }
        for (FileFormat[] formats : Arrays.asList(
                new FileFormat[] {FileFormat.AVRO, FileFormat.AVRO},
                new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET},
                new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO},
                new FileFormat[] {FileFormat.ORC, FileFormat.AVRO})) {
            createTable(catalog, namespace, formats[0], formats[1]);
        }
        createDstTable(catalog, namespace);
        createStandalone(catalog.loadTable(TableIdentifier.of(namespace, "avro_avro")), args[0]);
    }

    private static EncryptedOutputFile output(Table table, FileFormat format) {
        return EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(
                table.location() + "/data/" + UUID.randomUUID() + "." + format.name().toLowerCase()));
    }

    private static void createTable(Catalog catalog, Namespace ns, FileFormat dataFormat,
            FileFormat deleteFormat) throws Exception {
        String name = dataFormat.name().toLowerCase() + "_" + deleteFormat.name().toLowerCase();
        TableIdentifier identifier = TableIdentifier.of(ns, name);
        catalog.dropTable(identifier, true);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "old_id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "key", Types.LongType.get()),
                Types.NestedField.optional(3, "payload", Types.StructType.of(
                        Types.NestedField.optional(4, "note", Types.StringType.get()))));
        Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        GenericAppenderFactory factory = new GenericAppenderFactory(schema, table.spec());
        DataWriter<Record> writer = factory.newDataWriter(output(table, dataFormat), dataFormat, null);
        try (writer) {
            for (int i = 1; i <= 5; ++i) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("old_id", i);
                record.setField("key", i == 5 ? null : i * 10L);
                if (i != 3) {
                    GenericRecord nested = GenericRecord.create(schema.findType("payload").asStructType());
                    nested.setField("note", "row-" + i);
                    record.setField("payload", nested);
                }
                writer.write(record);
            }
        }
        DataFile data = writer.toDataFile();
        table.newAppend().appendFile(data).commit();
        table.updateSchema().updateColumn("old_id", Types.LongType.get()).renameColumn("old_id", "id")
                .addColumn("added", Types.StringType.get()).commit();
        long before = table.currentSnapshot().snapshotId();
        verify(table, Arrays.asList(1L, 2L, 3L, 4L, 5L));
        org.apache.iceberg.Schema keys = table.schema().select("key");
        factory = new GenericAppenderFactory(table.schema(), table.spec(), new int[] {2}, keys, null);
        EqualityDeleteWriter<Record> equality = factory.newEqDeleteWriter(
                output(table, deleteFormat), deleteFormat, null);
        try (equality) {
            GenericRecord key = GenericRecord.create(keys);
            key.setField("key", 20L);
            equality.write(key);
        }
        table.newRowDelta().addDeletes(equality.toDeleteFile()).commit();
        long afterEquality = table.currentSnapshot().snapshotId();
        verify(table, Arrays.asList(1L, 3L, 4L, 5L));
        PositionDeleteWriter<Record> position = factory.newPosDeleteWriter(
                output(table, deleteFormat), deleteFormat, null);
        try (position) {
            position.write(PositionDelete.<Record>create().set(data.path(), 3L, null));
        }
        table.newRowDelta().addDeletes(position.toDeleteFile()).commit();
        verify(table, Arrays.asList(1L, 3L, 5L));
        System.out.println("AVRO_SNAPSHOT " + name + " " + before + " " + afterEquality);
    }

    private static void verify(Table table, List<Long> expected) throws Exception {
        List<Long> actual = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record row : records) actual.add(((Number) row.getField("id")).longValue());
        }
        Collections.sort(actual);
        if (!expected.equals(actual)) throw new IllegalStateException("Invalid Iceberg oracle: " + actual);
    }

    private static void createDstTable(Catalog catalog, Namespace ns) throws Exception {
        TableIdentifier identifier = TableIdentifier.of(ns, "avro_dst");
        catalog.dropTable(identifier, true);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "event_time", Types.TimestampType.withZone()));
        Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        GenericAppenderFactory factory = new GenericAppenderFactory(schema, table.spec());
        DataWriter<Record> writer = factory.newDataWriter(output(table, FileFormat.AVRO), FileFormat.AVRO, null);
        try (writer) {
            for (int id = 1; id <= 2; ++id) {
                GenericRecord row = GenericRecord.create(schema);
                row.setField("id", (long) id);
                row.setField("event_time", OffsetDateTime.ofInstant(
                        Instant.parse(id == 1 ? "2024-11-03T05:30:00Z" : "2024-11-03T06:30:00Z"), ZoneOffset.UTC));
                writer.write(row);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        org.apache.iceberg.Schema keys = schema.select("event_time");
        factory = new GenericAppenderFactory(schema, table.spec(), new int[] {2}, keys, null);
        EqualityDeleteWriter<Record> delete = factory.newEqDeleteWriter(output(table, FileFormat.AVRO),
                FileFormat.AVRO, null);
        try (delete) {
            GenericRecord row = GenericRecord.create(keys);
            row.setField("event_time", OffsetDateTime.parse("2024-11-03T05:30:00Z"));
            delete.write(row);
        }
        table.newRowDelta().addDeletes(delete.toDeleteFile()).commit();
        verify(table, Arrays.asList(2L));
    }

    private static void createStandalone(Table table, String namespace) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"r","fields":[
                  {"name":"id","type":"long"},
                  {"name":"label","type":["null","string"]},
                  {"name":"items","type":{"type":"array","items":["null","int"]}},
                  {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
                  {"name":"day","type":{"type":"int","logicalType":"date"}},
                  {"name":"clock","type":{"type":"long","logicalType":"local-timestamp-millis"}}
                ]}
                """);
        Map<String, CodecFactory> codecs = Map.of("null", CodecFactory.nullCodec(),
                "deflate", CodecFactory.deflateCodec(6), "snappy", CodecFactory.snappyCodec(),
                "zstandard", CodecFactory.zstandardCodec(3));
        for (Map.Entry<String, CodecFactory> codec : codecs.entrySet()) {
            String path = "s3://warehouse/" + namespace + "/tvf/" + codec.getKey() + ".avro";
            try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.setCodec(codec.getValue());
                writer.create(schema, table.io().newOutputFile(path).createOrOverwrite());
                for (long id = 1; id <= 5; ++id) {
                    GenericData.Record row = new GenericData.Record(schema);
                    row.put("id", id);
                    row.put("label", id == 3 ? null : "row-" + id);
                    row.put("items", Arrays.asList((int) id, null));
                    row.put("amount", ByteBuffer.wrap(BigDecimal.valueOf(id * -12345, 2).unscaledValue().toByteArray()));
                    row.put("day", -1);
                    row.put("clock", 0L);
                    writer.append(row);
                    writer.sync();
                }
            }
        }
        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, table.io().newOutputFile("s3://warehouse/" + namespace + "/tvf/empty.avro")
                    .createOrOverwrite());
        }
        try (java.io.InputStream input = table.io().newInputFile(
                "s3://warehouse/" + namespace + "/tvf/snappy.avro").newStream()) {
            System.out.println("AVRO_STREAM_BASE64 " + Base64.getEncoder().encodeToString(input.readAllBytes()));
        }
        System.out.println("AVRO_TVF_READY " + namespace);
    }
}
