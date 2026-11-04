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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class DorisHiveCatalogTest {

    @Test
    void closesOwnedFileIOOnceAcrossRepeatedRetirement() throws Exception {
        DorisHiveCatalog catalog = new DorisHiveCatalog();
        RecordingFileIO fileIO = new RecordingFileIO();
        Field ownedFileIO = DorisHiveCatalog.class.getDeclaredField("ownedFileIO");
        ownedFileIO.setAccessible(true);
        ownedFileIO.set(catalog, fileIO);

        catalog.close();
        catalog.close();

        Assertions.assertEquals(1, fileIO.closeCount);
    }

    // HiveCatalog#initialize creates the FileIO before the client pool, and an unknown cache key element fails the
    // client pool after that.
    @Test
    void closesFileIOWhenHiveClientPoolInitializationFails() {
        TrackingFileIO.CLOSE_COUNT.set(0);
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.FILE_IO_IMPL, TrackingFileIO.class.getName());
        properties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "invalid-key-element");
        DorisHiveCatalog catalog = new DorisHiveCatalog();

        Assertions.assertThrows(IllegalArgumentException.class, () -> catalog.initialize("test", properties));

        Assertions.assertEquals(1, TrackingFileIO.CLOSE_COUNT.get());
    }

    /** Loaded by HiveCatalog through {@code io-impl}, so it is public and counts its closes statically. */
    public static class TrackingFileIO implements FileIO {
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
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
            CLOSE_COUNT.incrementAndGet();
        }
    }

    private static final class RecordingFileIO implements FileIO {
        private int closeCount;

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
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
            closeCount++;
        }
    }
}
