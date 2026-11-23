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

import java.util.Map;
import java.util.UUID;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** Real data and delete payloads for format v4 read compatibility with SDK 1.12. */
public final class IcebergV4Fixture {
    private static final String DB = "iceberg_v4_db";
    private static final String WAREHOUSE = "s3://iceberg-encryption/v4";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    public static void reset(HiveCatalog catalog, S3FileIO cleanup) throws Exception {
        Namespace namespace = Namespace.of(DB);
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace, Map.of("location", "file:/tmp/" + DB));
        }
        for (String name : new String[] {"fresh_v4", "relative_v4", "upgraded_v2", "upgraded_v3"}) {
            TableIdentifier identifier = TableIdentifier.of(DB, name);
            catalog.dropTable(identifier, false);
            String location = WAREHOUSE + "/" + name;
            cleanup.deletePrefix(location + "/");
            boolean upgrade = name.startsWith("upgraded");
            boolean relative = name.startsWith("relative");
            Table table = catalog.buildTable(identifier, SCHEMA).withLocation(location)
                    .withProperties(Map.of(TableProperties.FORMAT_VERSION, upgrade ? "2" : "4")).create();
            DataFile first = writeData(table, "first", 1, 8, relative);
            table.newFastAppend().appendFile(first).commit();
            if (upgrade) {
                addLegacyDeletes(table, first);
                if (name.endsWith("v3")) {
                    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
                }
                table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
                table.newFastAppend().appendFile(writeData(table, "second", 9, 10, false)).commit();
            } else {
                DataFile second = writeData(table, "second", 101, 104, relative);
                table.newFastAppend().appendFile(second).commit();
                String dvPath = table.location() + "/data/deletes-" + UUID.randomUUID() + ".puffin";
                BaseDVFileWriter writer = new BaseDVFileWriter(() -> table.io().newOutputFile(dvPath), path -> null);
                try (BaseDVFileWriter closeable = writer) {
                    closeable.delete(first.location(), 1, table.spec(), first.partition());
                    closeable.delete(second.location(), 0, table.spec(), second.partition());
                }
                RowDelta delta = table.newRowDelta();
                for (DeleteFile file : writer.result().deleteFiles()) {
                    delta.addDeletes(relative ? FileMetadata.deleteFileBuilder(table.spec()).copy(file)
                            .withPath(file.location().substring(table.location().length() + 1)).build() : file);
                }
                delta.commit();
            }
            if (((BaseTable) table).operations().current().formatVersion() != 4
                    || table.currentSnapshot().dataManifests(table.io()).stream()
                            .noneMatch(file -> FileFormat.fromFileName(file.path()) == FileFormat.PARQUET)) {
                throw new IllegalStateException("Fixture must contain a real v4 Parquet manifest");
            }
        }
    }

    private static DataFile writeData(Table table, String name, int first, int last, boolean relative)
            throws Exception {
        String path = table.location() + "/data/" + name + ".parquet";
        DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path))
                .forTable(table).createWriterFunc(GenericParquetWriter::create).build();
        try (DataWriter<Record> closeable = writer) {
            for (int id = first; id <= last; id++) {
                GenericRecord row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                closeable.write(row);
            }
        }
        return relative ? DataFiles.builder(table.spec()).copy(writer.toDataFile())
                .withPath("data/" + name + ".parquet").build() : writer.toDataFile();
    }

    private static void addLegacyDeletes(Table table, DataFile data) throws Exception {
        PositionDeleteWriter<Record> position = Parquet.writeDeletes(
                table.io().newOutputFile(table.location() + "/data/position.parquet"))
                .forTable(table).createWriterFunc(GenericParquetWriter::create).rowSchema(null).buildPositionWriter();
        try (PositionDeleteWriter<Record> closeable = position) {
            closeable.write(PositionDelete.<Record>create().set(data.location(), 1));
            closeable.write(PositionDelete.<Record>create().set(data.location(), 6));
        }
        EqualityDeleteWriter<Record> equality = Parquet.writeDeletes(
                table.io().newOutputFile(table.location() + "/data/equality.parquet"))
                .forTable(table).createWriterFunc(GenericParquetWriter::create)
                .rowSchema(SCHEMA).equalityFieldIds(1).buildEqualityWriter();
        try (EqualityDeleteWriter<Record> closeable = equality) {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", 5);
            closeable.write(row);
        }
        table.newRowDelta().addDeletes(position.toDeleteFile()).addDeletes(equality.toDeleteFile()).commit();
    }
}
