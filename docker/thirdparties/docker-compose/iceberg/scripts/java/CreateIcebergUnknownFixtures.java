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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** Generates unknown-column fixtures; physical schemas deliberately retain the same field IDs. */
public final class CreateIcebergUnknownFixtures {
    private CreateIcebergUnknownFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: CreateIcebergUnknownFixtures <create|evolve> <namespace>");
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "rest");
        properties.put("uri", "http://rest:8181");
        properties.put("warehouse", "s3://warehouse/wh/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");
        Catalog catalog = CatalogUtil.buildIcebergCatalog("demo", properties, null);
        Namespace namespace = Namespace.of(args[1]);
        if (!((SupportsNamespaces) catalog).namespaceExists(namespace)) {
            ((SupportsNamespaces) catalog).createNamespace(namespace);
        }
        for (FileFormat format : Arrays.asList(FileFormat.PARQUET, FileFormat.ORC)) {
            String suffix = format.name().toLowerCase(Locale.ROOT);
            TableIdentifier valid = TableIdentifier.of(namespace, "unknown_read_" + suffix);
            if ("create".equals(args[0])) {
                create(catalog, valid, format, false);
                create(catalog, TableIdentifier.of(namespace, "unknown_physical_" + suffix), format, true);
                TableIdentifier empty = TableIdentifier.of(namespace, "unknown_empty_" + suffix);
                catalog.dropTable(empty);
                catalog.buildTable(empty, schema(true, false)).withProperty("format-version", "3").create();
            } else if ("evolve".equals(args[0])) {
                Table table = catalog.loadTable(valid);
                // The SDK update API does not implement UNKNOWN promotion on all supported versions.
                // Publish the spec-permitted schema evolution directly, retaining all field IDs.
                TableOperations ops = ((BaseTable) table).operations();
                TableMetadata base = ops.current();
                Schema promoted = TypeUtil.reassignIds(schema(false, false), table.schema());
                Schema evolved = new Schema(table.schema().schemaId() + 1, promoted.columns());
                ops.commit(base, TableMetadata.buildFrom(base).setCurrentSchema(evolved, base.lastColumnId()).build());
                table.refresh();
                Schema physical = table.schema().select("id", "u");
                GenericRecord row = GenericRecord.create(physical);
                row.setField("id", 4);
                row.setField("u", "now known");
                append(table, physical, format, Collections.singletonList(row));
            } else {
                throw new IllegalArgumentException("Unknown action: " + args[0]);
            }
        }
    }

    private static Schema schema(boolean unknown, boolean omitUnknown) {
        org.apache.iceberg.types.Type leaf = unknown ? Types.UnknownType.get() : Types.StringType.get();
        Types.StructType struct = omitUnknown
                ? Types.StructType.of(Types.NestedField.optional(4, "known", Types.IntegerType.get()))
                : Types.StructType.of(Types.NestedField.optional(4, "known", Types.IntegerType.get()),
                        Types.NestedField.optional(5, "u", leaf));
        Types.StructType element = omitUnknown
                ? Types.StructType.of(Types.NestedField.optional(8, "known", Types.IntegerType.get()))
                : Types.StructType.of(Types.NestedField.optional(8, "known", Types.IntegerType.get()),
                        Types.NestedField.optional(9, "u", leaf));
        Types.StructType value = omitUnknown
                ? Types.StructType.of(Types.NestedField.optional(13, "known", Types.IntegerType.get()))
                : Types.StructType.of(Types.NestedField.optional(13, "known", Types.IntegerType.get()),
                        Types.NestedField.optional(14, "u", leaf));
        java.util.List<Types.NestedField> fields = new java.util.ArrayList<>();
        fields.add(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        if (!omitUnknown) {
            fields.add(Types.NestedField.optional(2, "u", leaf));
        }
        fields.add(Types.NestedField.optional(3, "s", struct));
        fields.add(Types.NestedField.optional(6, "a", Types.ListType.ofOptional(7, element)));
        fields.add(Types.NestedField.optional(10, "m", Types.MapType.ofOptional(11, 12, Types.StringType.get(), value)));
        if (!omitUnknown) {
            fields.add(Types.NestedField.optional(15, "null_array", Types.ListType.ofOptional(16, leaf)));
            fields.add(Types.NestedField.optional(17, "null_map",
                    Types.MapType.ofOptional(18, 19, Types.StringType.get(), leaf)));
        }
        return new Schema(fields);
    }

    private static void create(Catalog catalog, TableIdentifier id, FileFormat format, boolean present)
            throws Exception {
        catalog.dropTable(id);
        // Use MOR so UPDATE/DELETE/MERGE reach the UNKNOWN write rejection.
        Table table = catalog.buildTable(id, schema(true, false))
                .withProperty("format-version", "3")
                .withProperty("write.delete.mode", "merge-on-read")
                .withProperty("write.update.mode", "merge-on-read")
                .withProperty("write.merge.mode", "merge-on-read")
                .withProperty("write.format.default", format.name().toLowerCase(Locale.ROOT)).create();
        // Valid files omit UNKNOWN. The companion fixture intentionally stores non-NULL STRING
        // fields under those IDs to prove readers ignore the values AND their pruning statistics.
        Schema physical = TypeUtil.reassignIds(schema(false, !present), table.schema());
        GenericRecord nullParent = GenericRecord.create(physical);
        nullParent.setField("id", 1);
        GenericRecord populated = GenericRecord.create(physical);
        populated.setField("id", 2);
        populated.setField("s", child(physical.findType("s").asStructType(), present));
        populated.setField("a", Arrays.asList(child(physical.findType("a").asListType()
                .elementType().asStructType(), present), null));
        populated.setField("m", Collections.singletonMap("key",
                child(physical.findType("m").asMapType().valueType().asStructType(), present)));
        GenericRecord empty = GenericRecord.create(physical);
        empty.setField("id", 3);
        empty.setField("a", Collections.emptyList());
        empty.setField("m", Collections.emptyMap());
        if (present) {
            nullParent.setField("u", "must be ignored");
            populated.setField("u", "must be ignored");
            empty.setField("u", "must be ignored");
            populated.setField("null_array", Arrays.asList("ignored", null));
            populated.setField("null_map", Collections.singletonMap("key", "ignored"));
            empty.setField("null_array", Collections.emptyList());
            empty.setField("null_map", Collections.emptyMap());
        }
        append(table, physical, format, Arrays.asList(nullParent, populated, empty));
    }

    private static Record child(Types.StructType type, boolean present) {
        GenericRecord record = GenericRecord.create(type);
        record.setField("known", 20);
        if (present) {
            record.setField("u", "must be ignored");
        }
        return record;
    }

    private static void append(Table table, Schema physical, FileFormat format,
            java.util.List<? extends Record> records) throws Exception {
        String path = table.location() + "/data/" + UUID.randomUUID() + "."
                + format.name().toLowerCase(Locale.ROOT);
        DataWriter<Record> writer = new GenericAppenderFactory(physical, table.spec()).newDataWriter(
                EncryptedFiles.encryptedOutput(table.io().newOutputFile(path), EncryptionKeyMetadata.EMPTY),
                format, null);
        try (DataWriter<Record> closeable = writer) {
            for (Record record : records) {
                closeable.write(record);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }
}
