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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** Normal schema evolution leaves the historical data file untouched. */
public final class CreateNestedPredicateCacheFixture {
    private CreateNestedPredicateCacheFixture() {
    }

    private static DataFile dataFile(Table table) throws Exception {
        try (var files = table.newScan().planFiles()) {
            var iterator = files.iterator();
            DataFile file = iterator.next().file().copy();
            if (iterator.hasNext()) {
                throw new IllegalStateException("Expected one historical data file");
            }
            return file;
        }
    }

    private static void create(Catalog catalog, TableIdentifier identifier) throws Exception {
        catalog.dropTable(identifier, true);
        // BIGINT avoids an implicit INT-to-BIGINT modulo cast, which is deliberately uncacheable.
        Schema schema = new Schema(Types.NestedField.optional(1, "s", Types.StructType.of(
                Types.NestedField.optional(3, "x", Types.LongType.get()),
                Types.NestedField.optional(4, "keep", Types.IntegerType.get()))),
                Types.NestedField.optional(2, "k", Types.LongType.get()));
        Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "3", "write.format.default", "parquet"));
        OutputFile output = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".parquet");
        FileAppender<Record> appender = Parquet.write(output).schema(schema)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (FileAppender<Record> writer = appender) {
            for (int id = 0; id < 8192; ++id) {
                GenericRecord row = GenericRecord.create(schema);
                if (id < 4096 || id >= 6144) {
                    GenericRecord nested = GenericRecord.create(schema.findType("s").asStructType());
                    nested.setField("x", 0L);
                    nested.setField("keep", 7);
                    row.setField("s", nested);
                }
                row.setField("k", id < 6144 ? 1L : 0L);
                writer.add(row);
            }
        }
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(output.location())
                .withFormat(FileFormat.PARQUET).withRecordCount(8192)
                .withFileSizeInBytes(appender.length()).build()).commit();
        verify(table, 0L);
    }

    private static void verify(Table table, long expectedX) throws Exception {
        int rows = 0;
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record row : records) {
                Record nested = (Record) row.getField("s");
                boolean nullParent = rows >= 4096 && rows < 6144;
                if (nullParent != (nested == null)
                        || !Objects.equals(row.getField("k"), rows < 6144 ? 1L : 0L)
                        || (nested != null && (!Objects.equals(nested.getField("x"), expectedX)
                                || !Objects.equals(nested.getField("keep"), 7)))) {
                    throw new IllegalStateException("SDK row mismatch at " + rows);
                }
                ++rows;
            }
        }
        if (rows != 8192) {
            throw new IllegalStateException("SDK row count mismatch: " + rows);
        }
        System.out.println("CACHE_FIXTURE rows=" + rows + " x=" + expectedX);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>();
        if (args.length == 3) {
            properties.put("type", "hadoop");
            properties.put("warehouse", args[2]);
        } else {
            properties.put("type", "rest");
            properties.put("uri", "http://rest:8181");
            properties.put("warehouse", "s3://warehouse/wh/");
            properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", "http://minio:9000");
            properties.put("s3.path-style-access", "true");
            properties.put("s3.region", "us-east-1");
        }
        Catalog catalog = CatalogUtil.buildIcebergCatalog("nested_cache", properties, new Configuration());
        try (AutoCloseable ignored = (AutoCloseable) catalog) {
            Namespace namespace = Namespace.of(args[1]);
            SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
            if (!namespaces.namespaceExists(namespace)) {
                namespaces.createNamespace(namespace);
            }
            TableIdentifier identifier = TableIdentifier.of(namespace, "nested_default");
            if (args[0].equals("create")) {
                create(catalog, identifier);
            } else if (args[0].equals("evolve")) {
                Table table = catalog.loadTable(identifier);
                DataFile before = dataFile(table);
                int root = table.schema().findField("s").fieldId();
                int old = table.schema().findField("s.x").fieldId();
                long snapshot = table.currentSnapshot().snapshotId();
                table.updateSchema().deleteColumn("s.x").commit();
                table.updateSchema().addColumn("s", "x", Types.LongType.get(), Literal.of(9L)).commit();
                DataFile after = dataFile(table);
                var child = table.schema().findField("s.x");
                if (root != table.schema().findField("s").fieldId() || old == child.fieldId()
                        || !Objects.equals(child.initialDefault(), 9L)
                        || snapshot != table.currentSnapshot().snapshotId()
                        || !before.path().equals(after.path())
                        || before.fileSizeInBytes() != after.fileSizeInBytes()) {
                    throw new IllegalStateException("Schema evolution changed historical file identity");
                }
                verify(table, 9L);
                System.out.println("CACHE_EVOLVED root=" + root + " old=" + old + " new=" + child.fieldId());
            } else {
                throw new IllegalArgumentException("Expected create or evolve");
            }
        }
    }
}
