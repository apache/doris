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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Generates independently written page-index and Bloom-filter interoperability fixtures. */
public final class GenerateEncryptedIndexes {
    public static void main(String[] args) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message indexed { required int32 id = 1; optional binary label (UTF8) = 2; }");
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        for (int file = 0; file < args.length; file++) {
            ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(args[file]))
                    .withConf(configuration).withType(schema)
                    .withPageSize(128).withPageRowCountLimit(8).withRowGroupSize(2048)
                    .withMinRowCountForPageSizeCheck(1).withMaxRowCountForPageSizeCheck(8)
                    .withDictionaryEncoding(true).withDictionaryPageSize(128)
                    .withBloomFilterEnabled(true)
                    .withBloomFilterNDV("id", 512).withBloomFilterNDV("label", 512);
            if (file == 0) {
                builder.withEncryption(FileEncryptionProperties.builder(
                        "synthetic-key123".getBytes(StandardCharsets.UTF_8))
                        .withAADPrefix("synthetic-index-aad".getBytes(StandardCharsets.UTF_8))
                        .withoutAADPrefixStorage().build());
            }
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            try (ParquetWriter<Group> writer = builder.build()) {
                for (int row = 0; row < 512; row++) {
                    Group value = factory.newGroup().append("id", row * 2);
                    if (row % 5 != 0) {
                        value.append("label", "value-" + row);
                    }
                    writer.write(value);
                }
            }
        }
    }
}
