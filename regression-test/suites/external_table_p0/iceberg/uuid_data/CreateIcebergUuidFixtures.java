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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;

/** Real Iceberg UUID data/default/delete fixtures, authored and checked by Iceberg itself. */
public final class CreateIcebergUuidFixtures {
    private static final UUID NORMAL = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final UUID ZERO = UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static final UUID HIGH = UUID.fromString("80000000-0000-0000-0000-000000000000");
    private static final UUID MAX = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
    private static final UUID OTHER = UUID.fromString("61626364-6566-6768-696a-6b6c6d6e6f70");

    private CreateIcebergUuidFixtures() {
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
        if (args.length > 1 && args[1].equals("verify-writes")) {
            for (String format : Arrays.asList("parquet", "orc")) {
                for (boolean mapping : Arrays.asList(false, true)) {
                    Table table = catalog.loadTable(TableIdentifier.of(namespace,
                            "uuid_write_" + format + "_" + mapping));
                    Map<Integer, UUID> actual = new HashMap<>();
                    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                        for (Record row : records) {
                            actual.put((Integer) row.getField("id"), (UUID) row.getField("u"));
                        }
                    }
                    Map<Integer, UUID> expected = new HashMap<>();
                    expected.put(11, NORMAL);
                    expected.put(12, MAX);
                    expected.put(13, null);
                    if (!expected.equals(actual)) {
                        throw new IllegalStateException("Invalid Doris UUID write: " + table + " " + actual);
                    }
                    System.out.println("UUID_WRITE_VERIFIED " + table.name());
                }
            }
            return;
        }
        SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
        if (!namespaces.namespaceExists(namespace)) {
            namespaces.createNamespace(namespace);
        }
        for (String format : Arrays.asList("parquet", "orc")) {
            for (boolean mapping : Arrays.asList(false, true)) {
                TableIdentifier identifier = TableIdentifier.of(namespace,
                        "uuid_write_" + format + "_" + mapping);
                catalog.dropTable(identifier, true);
                Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "u", Types.UUIDType.get()));
                catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(),
                        Map.of("format-version", "2", "write.format.default", format));
            }
        }
        for (FileFormat dataFormat : Arrays.asList(FileFormat.PARQUET, FileFormat.ORC)) {
            for (FileFormat deleteFormat : Arrays.asList(FileFormat.PARQUET, FileFormat.ORC)) {
                String name = "uuid_" + dataFormat.name().toLowerCase()
                        + "_" + deleteFormat.name().toLowerCase();
                TableIdentifier identifier = TableIdentifier.of(namespace, name);
                catalog.dropTable(identifier, true);
                Schema original = new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "payload", Types.StructType.of(
                                Types.NestedField.optional(3, "marker", Types.IntegerType.get()))));
                Table table = catalog.createTable(identifier, original, PartitionSpec.unpartitioned(),
                        Map.of("format-version", "3"));
                // Iceberg 1.10.1's ORC oracle cannot materialize UUID initial defaults. Use
                // Parquet for the historical file; ORC data still exercises mixed-format scans.
                append(table, FileFormat.PARQUET, 0, Arrays.asList(NORMAL, NORMAL), false);
                table.updateSchema()
                        .addColumn("u", Types.UUIDType.get(), null, Literal.of(NORMAL))
                        .addColumn("payload", "u", Types.UUIDType.get(), null, Literal.of(NORMAL))
                        .commit();
                // Old files omit both UUID leaves; new files contain values and explicit NULLs.
                DataFile data = append(table, dataFormat, 2,
                        Arrays.asList(NORMAL, ZERO, HIGH, MAX, null, OTHER), true);
                long before = table.currentSnapshot().snapshotId();
                verify(table, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7));
                appendDelete(table, deleteFormat, Arrays.asList(NORMAL, MAX, null));
                verify(table, Arrays.asList(3, 4, 7));
                long afterEquality = table.currentSnapshot().snapshotId();
                appendDeletionVector(table, data);
                verify(table, Arrays.asList(3, 4));
                System.out.println("UUID_SNAPSHOT " + name + " " + before + " " + afterEquality);
            }
        }
    }

    private static EncryptedOutputFile output(Table table, String suffix) {
        return EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(
                table.location() + "/data/" + UUID.randomUUID() + suffix));
    }

    private static GenericRecord row(Schema schema, int id, UUID value, boolean physicalUuid) {
        GenericRecord row = GenericRecord.create(schema);
        row.setField("id", id);
        if (id != 1) {
            GenericRecord payload = GenericRecord.create(schema.findType("payload").asStructType());
            payload.setField("marker", id);
            if (physicalUuid) {
                payload.setField("u", value);
            }
            row.setField("payload", payload);
        }
        if (physicalUuid) {
            row.setField("u", value);
        }
        return row;
    }

    private static DataFile append(Table table, FileFormat format, int firstId, List<UUID> values,
            boolean physicalUuid) throws Exception {
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer = factory.newDataWriter(output(table, "." + format.name().toLowerCase()),
                format, null);
        try (writer) {
            for (int i = 0; i < values.size(); ++i) {
                writer.write(row(table.schema(), firstId + i, values.get(i), physicalUuid));
            }
        }
        DataFile file = writer.toDataFile();
        table.newAppend().appendFile(file).commit();
        return file;
    }

    private static void appendDelete(Table table, FileFormat format, List<UUID> values) throws Exception {
        Schema keys = table.schema().select("u");
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec(),
                new int[] {table.schema().findField("u").fieldId()}, keys, null);
        EqualityDeleteWriter<Record> writer = factory.newEqDeleteWriter(
                output(table, ".delete." + format.name().toLowerCase()), format, null);
        try (writer) {
            for (UUID value : values) {
                GenericRecord key = GenericRecord.create(keys);
                key.setField("u", value);
                writer.write(key);
            }
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private static void appendDeletionVector(Table table, DataFile data) throws Exception {
        OutputFileFactory files = OutputFileFactory.builderFor(table, 0, 1)
                .format(FileFormat.PUFFIN).build();
        BaseDVFileWriter writer = new BaseDVFileWriter(files, path -> PositionDeleteIndex.empty());
        try (BaseDVFileWriter ignored = writer) {
            writer.delete(data.path().toString(), 5, table.spec(), null);
        }
        table.newRowDelta().addDeletes(writer.result().deleteFiles().get(0)).commit();
    }

    private static void verify(Table table, List<Integer> expected) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record row : records) {
                int id = (Integer) row.getField("id");
                ids.add(id);
                if (id <= 2 && !NORMAL.equals(row.getField("u"))) {
                    throw new IllegalStateException("Iceberg default and physical UUID differ: " + row);
                }
            }
        }
        ids.sort(Integer::compareTo);
        if (!ids.equals(expected)) {
            throw new IllegalStateException("Unexpected Iceberg survivors " + ids + ", expected " + expected);
        }
    }
}
