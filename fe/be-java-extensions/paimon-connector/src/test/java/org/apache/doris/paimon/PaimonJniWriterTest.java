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

package org.apache.doris.paimon;

import org.apache.doris.thrift.TPaimonCatalogEnvironment;
import org.apache.doris.thrift.TPaimonTableDescriptor;

import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.rest.RESTCatalogLoader;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PaimonJniWriterTest {
    @TempDir
    private Path tempDir;

    @Test
    public void testWriterTableUsesPinnedSchemaWithoutMetadataFiles() throws Exception {
        String location = tempDir.resolve("table-without-schema-files").toUri().toString();
        Map<String, String> options = new HashMap<>();
        options.put("path", location);
        options.put("branch", "audit");
        options.put("scan.snapshot-id", "123");
        for (boolean primaryKey : new boolean[] {false, true}) {
            List<String> keys = primaryKey ? Collections.singletonList("id") : Collections.emptyList();
            TableSchema schema = new TableSchema(7,
                    Arrays.asList(new DataField(4, "id", DataTypes.INT().notNull()),
                            new DataField(9, "value", DataTypes.ROW(new DataField(12, "text", DataTypes.STRING())))),
                    12, Collections.singletonList("id"), keys, options, "pinned table");
            TPaimonTableDescriptor descriptor = new TPaimonTableDescriptor(
                    location, schema.toString(), Collections.emptyMap(), Collections.emptyMap());
            FileStoreTable table = PaimonWriterTable.create(descriptor);
            Assertions.assertEquals(schema, table.schema());
            Assertions.assertNull(table.catalogEnvironment().catalogLoader());
            Assertions.assertEquals(primaryKey ? PrimaryKeyFileStoreTable.class : AppendOnlyFileStoreTable.class,
                    table.getClass());
        }
    }

    @Test
    public void testWriterTableRestAccessUsesSdkLoader() throws Exception {
        String location = "s3://bucket/table";
        TableSchema schema = new TableSchema(7,
                Collections.singletonList(new DataField(4, "id", DataTypes.INT())),
                4, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null);
        TPaimonTableDescriptor descriptor = new TPaimonTableDescriptor(
                location, schema.toString(), Collections.emptyMap(), Collections.singletonMap("metastore", "rest"));
        TPaimonCatalogEnvironment catalog = new TPaimonCatalogEnvironment("db", "table$branch_audit");
        catalog.setSnapshotLoader(true);
        catalog.setVersionManagement(true);
        catalog.setRestTokenEnabled(true);
        descriptor.setCatalogEnvironment(catalog);
        FileStoreTable table = PaimonWriterTable.create(descriptor);
        Assertions.assertInstanceOf(RESTTokenFileIO.class, table.fileIO());
        Assertions.assertEquals("audit", table.catalogEnvironment().identifier().getBranchName());
        Assertions.assertInstanceOf(RESTCatalogLoader.class, table.catalogEnvironment().catalogLoader());
    }

    @Test
    public void testManagedMemoryPoolRequiresAtLeastOnePage() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new DorisMemorySegmentPool(32 * 1024, 64 * 1024, 1L));
        Assertions.assertTrue(exception.getMessage().contains("at least one page"));
        Assertions.assertDoesNotThrow(
                () -> new DorisMemorySegmentPool(64 * 1024, 64 * 1024, 1L));
    }

    @Test
    public void testMergeTreeMemoryPoolRequiresAtLeastThreePages() {
        int pageSize = 64 * 1024;
        long writeBufferSize = 4L * pageSize;
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        writeBufferSize, 2L * pageSize, pageSize, true));
        Assertions.assertTrue(exception.getMessage().contains("requires at least 3 memory pages"));
        Assertions.assertTrue(exception.getMessage().contains("effectivePoolLimit=131072"));

        Assertions.assertEquals(3L * pageSize,
                PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        writeBufferSize, 3L * pageSize, pageSize, true));
        Assertions.assertEquals(pageSize,
                PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        pageSize, pageSize, pageSize, false));
    }

    @Test
    public void testOpenFailureRestoresContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            Assertions.assertThrows(Exception.class, () -> writer.open(
                    new byte[0], new String[0],
                    1L, "test-user", false, false, "UTC", 64L * 1024 * 1024, 1L, 1L));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            try {
                writer.close();
                Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
            } finally {
                thread.setContextClassLoader(originalClassLoader);
                testClassLoader.close();
            }
        }
    }

    @Test
    public void testCloseWaitsForCompactionExecutor() throws Exception {
        PaimonJniWriter writer = new PaimonJniWriter();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        try {
            executor.execute(() -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    stopped.countDown();
                }
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

            Field executorField = PaimonJniWriter.class.getDeclaredField("compactionExecutor");
            executorField.setAccessible(true);
            executorField.set(writer, executor);

            writer.close();
            Assertions.assertTrue(stopped.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(executor.isTerminated());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSpillDirectoryCleanupFailureDoesNotFenceWriter() throws Exception {
        IOManager failingCloseManager = new IOManagerImpl(tempDir.toString()) {
            @Override
            public void close() throws Exception {
                throw new IOException("injected cleanup failure");
            }
        };
        AtomicLong currentBytes = new AtomicLong();
        DorisIOManager.SpillAccountant accountant = new DorisIOManager.SpillAccountant() {
            @Override
            public String[] getSpillDirectories() {
                return new String[] {tempDir.toString()};
            }

            @Override
            public void reserve(String path, long bytes) {
                currentBytes.addAndGet(bytes);
            }

            @Override
            public void rollback(String path, long bytes) {
                currentBytes.addAndGet(-bytes);
            }

            @Override
            public void commitWrite(String path, long bytes) {
            }

            @Override
            public void recordRead(String path, long bytes) {
            }

            @Override
            public void release(String path, long bytes) {
                currentBytes.addAndGet(-bytes);
            }
        };
        DorisIOManager ioManager = new DorisIOManager(failingCloseManager, accountant);
        FileIOChannel.ID channel = ioManager.createChannel();
        BufferFileWriter spillWriter = ioManager.createBufferFileWriter(channel);
        spillWriter.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8));
        spillWriter.close();
        Assertions.assertEquals(12, currentBytes.get());

        PaimonJniWriter writer = new PaimonJniWriter();
        Field ioManagerField = PaimonJniWriter.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(writer, ioManager);

        Assertions.assertDoesNotThrow(writer::close);
        Assertions.assertTrue(channel.getPathFile().exists());
        Assertions.assertEquals(12, currentBytes.get());
        Assertions.assertDoesNotThrow(writer::close);
        spillWriter.deleteChannel();
        Assertions.assertEquals(0, currentBytes.get());
    }
}
