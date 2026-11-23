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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergV4FileIOTest {
    @Test
    void serializedReadViewOpensRelativeDataAndDeleteFiles() throws Exception {
        InMemoryFileIO storage = new InMemoryFileIO();
        try (OutputStream output = storage.newOutputFile("s3://example-bucket/table/data/file.parquet").create()) {
            output.write(42);
        }
        IcebergV4FileIO original = new IcebergV4FileIO(storage, "s3://example-bucket/table");
        FileIO restored = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(original));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        InputFile data = restored.newInputFile(DataFiles.builder(spec).withPath("data/file.parquet")
                .withFileSizeInBytes(1).withRecordCount(1).build());
        InputFile delete = restored.newInputFile(FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                .withPath("data/file.parquet").withFileSizeInBytes(1).withRecordCount(1).build());
        for (InputFile input : new InputFile[] {data, delete}) {
            Assertions.assertEquals("s3://example-bucket/table/data/file.parquet", input.location());
            try (SeekableInputStream stream = input.newStream()) {
                Assertions.assertEquals(42, stream.read());
                Assertions.assertEquals(-1, stream.read());
            }
        }
        Assertions.assertThrows(UnsupportedOperationException.class, () -> restored.newOutputFile("data/file.parquet"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> restored.deleteFile("data/file.parquet"));
    }

    @Test
    void borrowedReadViewPreservesDelegatePropertiesAndLifetime() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger typedFactories = new AtomicInteger();
        FileIO delegate = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputFile newInputFile(DataFile file) {
                Assertions.assertNotNull(file.keyMetadata());
                typedFactories.incrementAndGet();
                return new InMemoryFileIO().newInputFile(file.location());
            }

            @Override
            public OutputFile newOutputFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<String, String> properties() {
                return Collections.singletonMap("s3.region", "us-east-1");
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        IcebergV4FileIO view = new IcebergV4FileIO(delegate, "s3://example-bucket/table");
        Assertions.assertSame(delegate, view.delegate());
        Assertions.assertEquals(delegate.properties(), view.properties());
        DataFile encrypted = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://example-bucket/table/data/file.parquet").withFileSizeInBytes(1).withRecordCount(1)
                .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[] {1})).build();
        Assertions.assertEquals(encrypted.location(), view.newInputFile(encrypted).location());
        DataFile relative = DataFiles.builder(PartitionSpec.unpartitioned()).copy(encrypted)
                .withPath("data/file.parquet").build();
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> view.newInputFile(relative));
        Assertions.assertTrue(failure.getMessage().contains("Cannot decrypt data file"));
        Assertions.assertEquals(1, typedFactories.get());
        view.close();
        Assertions.assertEquals(0, closes.get());
    }
}
