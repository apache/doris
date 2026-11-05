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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** Generates a small interoperability fixture with synthetic, publicly documented test keys. */
public final class GenerateEncryptedParquet {
    public static void main(String[] args) throws Exception {
        if (!"1.12.0".equals(org.apache.iceberg.IcebergBuild.version())) {
            throw new IllegalArgumentException("This fixture requires Iceberg 1.12.0");
        }
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "label", Types.StringType.get()),
                Types.NestedField.optional(3, "items", Types.ListType.ofOptional(4, Types.IntegerType.get())),
                Types.NestedField.optional(5, "record", Types.StructType.of(
                        Types.NestedField.optional(6, "code", Types.IntegerType.get()))));
        StandardEncryptionManager manager = new StandardEncryptionManager(
                List.of(), "unused-synthetic-master-key", 16, new UnusedKms());
        NativeEncryptionOutputFile output = manager.encrypt(Files.localOutput(args[0]));
        // Replace the SDK-generated random bytes only for this public, reproducible test artifact.
        output.keyMetadata().encryptionKey().duplicate().put(
                "synthetic-key123".getBytes(StandardCharsets.UTF_8));
        output.keyMetadata().aadPrefix().duplicate().put(
                "0123456789abcdef".getBytes(StandardCharsets.UTF_8));
        try (FileAppender<Record> writer = Parquet.write(
                (org.apache.iceberg.encryption.EncryptedOutputFile) output).schema(schema)
                .set(TableProperties.PARQUET_COMPRESSION, "snappy")
                .createWriterFunc(GenericParquetWriter::create).build()) {
            writeRows(writer, schema);
        }
        if (args.length > 1) {
            try (FileAppender<Record> writer = Parquet.write(Files.localOutput(args[1])).schema(schema)
                    .set(TableProperties.PARQUET_COMPRESSION, "snappy")
                    .createWriterFunc(GenericParquetWriter::create).build()) {
                writeRows(writer, schema);
            }
        }
    }

    private static void writeRows(FileAppender<Record> writer, Schema schema) {
        for (int id = 1; id <= 8; id++) {
            GenericRecord row = GenericRecord.create(schema);
            row.setField("id", id);
            int shape = (id - 1) % 4;
            row.setField("label", shape == 1 ? null : "shared");
            row.setField("items", shape == 0 ? List.of(id, id + 1)
                    : shape == 1 ? null : shape == 2 ? List.of() : Arrays.asList(null, id));
            if (shape != 2) {
                GenericRecord nested = GenericRecord.create(
                        schema.findType("record").asStructType());
                nested.setField("code", shape == 1 ? null : id);
                row.setField("record", nested);
            }
            writer.add(row);
        }
    }

    private static class UnusedKms implements KeyManagementClient {
        @Override
        public java.nio.ByteBuffer wrapKey(java.nio.ByteBuffer key, String wrappingKeyId) {
            throw new AssertionError("This standalone file does not need a table KMS");
        }
        @Override
        public java.nio.ByteBuffer unwrapKey(java.nio.ByteBuffer key, String wrappingKeyId) {
            throw new AssertionError("This standalone file does not need a table KMS");
        }
        @Override
        public void initialize(Map<String, String> properties) {
        }
    }
}
