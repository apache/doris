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

import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergScanFileIOTest {
    @Test
    public void cancellationGuardsExistingInputAndStreamWithoutClosingCatalogIo() throws Exception {
        AtomicInteger factories = new AtomicInteger();
        AtomicInteger catalogCloses = new AtomicInteger();
        InMemoryFileIO storage = new InMemoryFileIO();
        try (java.io.OutputStream out = storage.newOutputFile("manifest").create()) {
            out.write(new byte[] {1, 2, 3});
        }
        FileIO borrowed = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                factories.incrementAndGet();
                return storage.newInputFile(path);
            }

            @Override
            public Map<String, String> properties() {
                return Collections.singletonMap(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
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
            public void close() {
                catalogCloses.incrementAndGet();
            }
        };
        ConnectorScanContext context = new ConnectorScanContext();
        IcebergScanFileIO io = new IcebergScanFileIO(borrowed, context::checkCancelled);
        InputFile input = io.newInputFile("manifest", 3);
        Assertions.assertEquals("false", io.properties().get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
        Assertions.assertEquals("true", borrowed.properties().get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
        try (SeekableInputStream stream = input.newStream()) {
            Assertions.assertEquals(1, stream.read());
            context.cancel();
            Assertions.assertThrows(CancellationException.class, () -> io.newInputFile("manifest"));
            Assertions.assertThrows(CancellationException.class, input::exists);
            Assertions.assertThrows(CancellationException.class, input::getLength);
            Assertions.assertThrows(CancellationException.class, input::newStream);
            Assertions.assertThrows(CancellationException.class, stream::read);
            Assertions.assertThrows(CancellationException.class, () -> stream.read(new byte[1], 0, 1));
            Assertions.assertThrows(CancellationException.class, () -> stream.seek(0));
            Assertions.assertThrows(CancellationException.class, () -> stream.skip(1));
        }
        io.close();
        Assertions.assertEquals(1, factories.get());
        Assertions.assertEquals(0, catalogCloses.get());
    }

    @Test
    public void manifestOverloadKeepsDelegateEncryptionHandling() throws Exception {
        AtomicInteger manifestFactories = new AtomicInteger();
        InMemoryFileIO storage = new InMemoryFileIO();
        try (java.io.OutputStream out = storage.newOutputFile("decrypted-manifest").create()) {
            out.write(1);
        }
        FileIO encrypted = new FileIO() {
            @Override
            public InputFile newInputFile(ManifestFile file) {
                Assertions.assertNotNull(file.keyMetadata());
                manifestFactories.incrementAndGet();
                return storage.newInputFile("decrypted-manifest");
            }

            @Override
            public InputFile newInputFile(String path) {
                throw new AssertionError("manifest-specific factory must not be flattened to a path");
            }

            @Override
            public OutputFile newOutputFile(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String path) {
                throw new UnsupportedOperationException();
            }
        };
        ManifestFile manifest = (ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> {
                    Assertions.assertEquals("keyMetadata", method.getName());
                    return ByteBuffer.wrap(new byte[] {1});
                });
        ConnectorScanContext context = new ConnectorScanContext();
        IcebergScanFileIO io = new IcebergScanFileIO(encrypted, context::checkCancelled);
        Assertions.assertEquals("decrypted-manifest", io.newInputFile(manifest).location());
        context.cancel();
        Assertions.assertThrows(CancellationException.class, () -> io.newInputFile(manifest));
        Assertions.assertEquals(1, manifestFactories.get());
    }
}
