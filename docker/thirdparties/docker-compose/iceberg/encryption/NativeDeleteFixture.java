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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** Controlled native-reader fixtures. Public synthetic file keys deliberately need no KMS. */
public final class NativeDeleteFixture {
    private static final String DB = "iceberg_encryption_delete_db";
    private static final String WAREHOUSE = "s3://iceberg-encryption/native-deletes";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    public static void reset(HiveCatalog catalog, S3FileIO cleanup) throws Exception {
        Namespace namespace = Namespace.of(DB);
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace, Map.of("location", "file:/tmp/" + DB));
        }
        for (int version : new int[] {2, 3}) {
            for (int mode = 0; mode < 8; mode++) {
                String name = "native_deletes_" + mode + "_v" + version;
                TableIdentifier identifier = TableIdentifier.of(DB, name);
                catalog.dropTable(identifier, false);
                String location = WAREHOUSE + "/" + name;
                cleanup.deletePrefix(location + "/");
                Table table = catalog.buildTable(identifier, SCHEMA).withLocation(location)
                        .withProperties(Map.of(TableProperties.FORMAT_VERSION, "2")).create();
                DataFile data = writeData(table, (mode & 1) != 0);
                table.newAppend().appendFile(data).commit();
                DeleteFile position = writePosition(table, data, (mode & 2) != 0);
                DeleteFile equality = writeEquality(table, (mode & 4) != 0);
                table.newRowDelta().addDeletes(position).addDeletes(equality).commit();
                if (version == 3) {
                    // V3 retains older Parquet position deletes after a V2 snapshot is upgraded.
                    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
                }
            }
        }
    }

    private static String path(Table table, String kind) {
        return table.location() + "/data/" + kind + "-" + UUID.randomUUID() + ".parquet";
    }

    private static DataFile writeData(Table table, boolean encrypted) throws Exception {
        Parquet.DataWriteBuilder builder = Parquet.writeData(table.io().newOutputFile(path(table, "data")))
                .forTable(table).createWriterFunc(GenericParquetWriter::create);
        if (encrypted) {
            SyntheticMetadata key = new SyntheticMetadata('d');
            builder.withFileEncryptionKey(key.encryptionKey()).withAADPrefix(key.aadPrefix()).withKeyMetadata(key);
        }
        DataWriter<Record> writer = builder.build();
        try (DataWriter<Record> closeable = writer) {
            for (int value = 1; value <= 8; value++) {
                GenericRecord row = GenericRecord.create(SCHEMA);
                row.setField("id", value);
                closeable.write(row);
            }
        }
        return writer.toDataFile();
    }

    private static Parquet.DeleteWriteBuilder deleteBuilder(Table table, String kind, boolean encrypted, char key) {
        Parquet.DeleteWriteBuilder builder = Parquet.writeDeletes(table.io().newOutputFile(path(table, kind)))
                .forTable(table).createWriterFunc(GenericParquetWriter::create);
        if (encrypted) {
            SyntheticMetadata metadata = new SyntheticMetadata(key);
            builder.withFileEncryptionKey(metadata.encryptionKey()).withAADPrefix(metadata.aadPrefix())
                    .withKeyMetadata(metadata);
        }
        return builder;
    }

    private static DeleteFile writePosition(Table table, DataFile data, boolean encrypted) throws Exception {
        PositionDeleteWriter<Record> writer = deleteBuilder(table, "position", encrypted, 'p').rowSchema(null).buildPositionWriter();
        try (PositionDeleteWriter<Record> closeable = writer) {
            closeable.write(PositionDelete.<Record>create().set(data.location(), 1));
            closeable.write(PositionDelete.<Record>create().set(data.location(), 6));
        }
        return writer.toDeleteFile();
    }

    private static DeleteFile writeEquality(Table table, boolean encrypted) throws Exception {
        EqualityDeleteWriter<Record> writer = deleteBuilder(table, "equality", encrypted, 'e')
                .rowSchema(SCHEMA).equalityFieldIds(1).buildEqualityWriter();
        try (EqualityDeleteWriter<Record> closeable = writer) {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", 5);
            closeable.write(row);
        }
        return writer.toDeleteFile();
    }

    private static List<Record> records(Table table, String path, Schema schema, boolean encrypted, char key) {
        Parquet.ReadBuilder builder = Parquet.read(table.io().newInputFile(path)).project(schema)
                .createReaderFunc(file -> GenericParquetReaders.buildReader(schema, file));
        if (encrypted) {
            SyntheticMetadata metadata = new SyntheticMetadata(key);
            builder.withFileEncryptionKey(metadata.encryptionKey()).withAADPrefix(metadata.aadPrefix());
        }
        List<Record> result = new ArrayList<>();
        try (CloseableIterable<Record> reader = builder.build()) {
            for (Record row : reader) {
                result.add(row.copy());
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Native delete reference read failed", e);
        }
        return result;
    }

    private static boolean matchesKey(ByteBuffer actual, char key, long length) {
        // SDK writers add the file length when closing; normalize that field before comparing key/AAD.
        return actual != null && EncryptionUtil.setFileLength(actual, length).equals(
                EncryptionUtil.setFileLength(new SyntheticMetadata(key).buffer(), length));
    }

    public static String verify(HiveCatalog catalog) throws Exception {
        int tables = 0;
        for (int version : new int[] {2, 3}) {
            for (int mode = 0; mode < 8; mode++) {
                Table table = catalog.loadTable(TableIdentifier.of(DB, "native_deletes_" + mode + "_v" + version));
                try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                    int files = 0;
                    for (FileScanTask task : tasks) {
                        files++;
                        ByteBuffer dataMetadata = task.file().keyMetadata();
                        if ((mode & 1) == 0 ? dataMetadata != null
                                : !matchesKey(dataMetadata, 'd', task.file().fileSizeInBytes())) {
                            throw new IllegalStateException("Data key metadata differs from fixture mode " + mode
                                    + ", bytes=" + (dataMetadata == null ? -1 : dataMetadata.remaining()));
                        }
                        try (InputStream input = table.io().newInputFile(task.file().location()).newStream()) {
                            String magic = new String(input.readNBytes(4), StandardCharsets.US_ASCII);
                            if (!magic.equals((mode & 1) != 0 ? "PARE" : "PAR1")) {
                                throw new IllegalStateException("Data payload encryption differs from metadata");
                            }
                        }
                        List<Record> data = records(table, task.file().location(), SCHEMA, (mode & 1) != 0, 'd');
                        Set<Long> positions = new TreeSet<>();
                        Set<Integer> equalities = new TreeSet<>();
                        if (task.deletes().size() != 2) {
                            throw new IllegalStateException("Expected independent position and equality deletes");
                        }
                        for (DeleteFile delete : task.deletes()) {
                            boolean position = delete.content() == org.apache.iceberg.FileContent.POSITION_DELETES;
                            boolean encrypted = (mode & (position ? 2 : 4)) != 0;
                            if (encrypted ? !matchesKey(delete.keyMetadata(), position ? 'p' : 'e', delete.fileSizeInBytes())
                                    : delete.keyMetadata() != null) {
                                throw new IllegalStateException("Delete key metadata differs from fixture mode");
                            }
                            try (InputStream input = table.io().newInputFile(delete.location()).newStream()) {
                                String magic = new String(input.readNBytes(4), StandardCharsets.US_ASCII);
                                if (!magic.equals(encrypted ? "PARE" : "PAR1")) {
                                    throw new IllegalStateException("Delete payload encryption differs from metadata");
                                }
                            }
                            Schema schema = position ? DeleteSchemaUtil.pathPosSchema() : SCHEMA;
                            for (Record row : records(table, delete.location(), schema, encrypted, position ? 'p' : 'e')) {
                                if (position) {
                                    if (!row.get(0, Object.class).toString().equals(task.file().location())) {
                                        throw new IllegalStateException("Position delete references another data file");
                                    }
                                    positions.add(row.get(1, Long.class));
                                } else {
                                    equalities.add(row.get(0, Integer.class));
                                }
                            }
                        }
                        List<Integer> actual = new ArrayList<>();
                        for (int row = 0; row < data.size(); row++) {
                            int value = data.get(row).get(0, Integer.class);
                            if (!positions.contains((long) row) && !equalities.contains(value)) {
                                actual.add(value);
                            }
                        }
                        if (!actual.equals(List.of(1, 3, 4, 6, 8))) {
                            throw new IllegalStateException("Native delete reference rows differ");
                        }
                    }
                    if (files != 1) {
                        throw new IllegalStateException("Expected one data file per fixture");
                    }
                }
                tables++;
            }
        }
        return "{\"tables\":" + tables + ",\"rows\":[1,3,4,6,8],\"independentFileKeys\":true}";
    }

    private static final class SyntheticMetadata implements NativeEncryptionKeyMetadata {
        private final byte[] key = new byte[16];
        private final byte[] aad;

        private SyntheticMetadata(char value) {
            Arrays.fill(key, (byte) value);
            aad = ("synthetic-delete-aad-" + value).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public ByteBuffer encryptionKey() {
            return ByteBuffer.wrap(key.clone());
        }

        @Override
        public ByteBuffer aadPrefix() {
            return ByteBuffer.wrap(aad.clone());
        }

        @Override
        public ByteBuffer buffer() {
            // StandardKeyMetadata V1: Avro bytes(key), optional bytes(AAD), optional long(file length).
            // These small public fixtures fit one-byte Avro lengths and intentionally carry no table key chain.
            ByteBuffer encoded = ByteBuffer.allocate(21 + aad.length);
            encoded.put((byte) 1).put((byte) 32).put(key).put((byte) 2).put((byte) (aad.length * 2)).put(aad).put((byte) 0);
            return encoded.flip();
        }

        @Override
        public NativeEncryptionKeyMetadata copy() {
            return new SyntheticMetadata((char) key[0]);
        }
    }
}
