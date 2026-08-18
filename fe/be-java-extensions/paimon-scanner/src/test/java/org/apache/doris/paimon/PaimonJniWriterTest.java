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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class PaimonJniWriterTest {

    @Test
    public void testAbortPreparedCommitAfterWriterClose(@TempDir Path tableDirectory)
            throws Exception {
        org.apache.paimon.fs.Path tablePath =
                new org.apache.paimon.fs.Path(tableDirectory.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        new SchemaManager(fileIO, tablePath).createTable(Schema.newBuilder()
                .column("id", DataTypes.INT())
                .build());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath);
        String commitUser = "doris-report-reject-test";
        List<CommitMessage> messages;
        try (TableWriteImpl<?> writer = table.newWrite(commitUser)) {
            writer.write(GenericRow.of(1));
            messages = writer.prepareCommit(true, 31L);
        }
        byte[][] payloads = new PaimonCommitCodec().encode(messages);
        long filesBeforeAbort = regularFileCount(tableDirectory);
        String serializedTable = Base64.getEncoder().encodeToString(
                InstantiationUtil.serializeObject(table));

        PaimonJniWriter.abortPreparedCommit(
                serializedTable, Collections.emptyMap(), commitUser, payloads);

        Assertions.assertTrue(filesBeforeAbort > regularFileCount(tableDirectory));
    }

    @Test
    public void testMemoryBudgetForFixedBucketTable() {
        long total = 64L * 1024 * 1024;
        int pageSize = 64 * 1024;
        PaimonJniWriter.MemoryBudget budget =
                PaimonJniWriter.createMemoryBudget(total, pageSize, BucketMode.HASH_FIXED);

        Assertions.assertEquals(total, budget.arrowMemoryBytes + budget.writerMemoryBytes);
        Assertions.assertEquals(0, budget.writerMemoryBytes % pageSize);
        Assertions.assertTrue(budget.arrowMemoryBytes > 0);
        Assertions.assertTrue(budget.writerMemoryBytes >= pageSize);
        Assertions.assertEquals(0, budget.globalIndexLookupMemoryBytes);
        Assertions.assertEquals(0, budget.globalIndexWriteBufferBytes);
    }

    @Test
    public void testMemoryBudgetForKeyDynamicTable() {
        long total = 64L * 1024 * 1024;
        int pageSize = 64 * 1024;
        PaimonJniWriter.MemoryBudget budget =
                PaimonJniWriter.createMemoryBudget(total, pageSize, BucketMode.KEY_DYNAMIC);

        Assertions.assertEquals(
                total,
                budget.arrowMemoryBytes
                        + budget.writerMemoryBytes
                        + budget.globalIndexLookupMemoryBytes
                        + budget.globalIndexWriteBufferBytes);
        Assertions.assertEquals(0, budget.writerMemoryBytes % pageSize);
        Assertions.assertEquals(0, budget.globalIndexWriteBufferBytes % pageSize);
        Assertions.assertTrue(budget.arrowMemoryBytes > 0);
        Assertions.assertTrue(budget.writerMemoryBytes >= pageSize);
        Assertions.assertTrue(budget.globalIndexLookupMemoryBytes > 0);
        Assertions.assertTrue(budget.globalIndexWriteBufferBytes >= 2L * pageSize);
    }

    @Test
    public void testMemoryBudgetRejectsInsufficientCapacity() {
        int pageSize = 64 * 1024;
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniWriter.createMemoryBudget(
                        2L * pageSize, pageSize, BucketMode.KEY_DYNAMIC));
    }

    @Test
    public void testManagedMemoryPoolRequiresAtLeastOnePage() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new DorisMemorySegmentPool(32 * 1024, 64 * 1024, 1L));
        Assertions.assertTrue(exception.getMessage().contains("at least one page"));
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
                    "not-a-serialized-table", Collections.emptyMap(), new String[0],
                    1L, "test-user", false, false, false, "UTC",
                    System.getProperty("java.io.tmpdir"),
                    64L * 1024 * 1024, 1L));
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
    public void testAbortRestoresContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            writer.abort();
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            try {
                writer.close();
            } finally {
                thread.setContextClassLoader(originalClassLoader);
                testClassLoader.close();
            }
        }
    }

    @Test
    public void testDataEntryPointFailuresRestoreContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            Assertions.assertThrows(Exception.class,
                    () -> writer.write(ByteBuffer.allocateDirect(0)));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());

            Assertions.assertThrows(Exception.class, writer::prepareCommit);
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
            testClassLoader.close();
        }
    }

    private static long regularFileCount(Path directory) throws Exception {
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }
}
