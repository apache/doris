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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonJniWriterTest {

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
                    "not-a-serialized-table", Collections.emptyMap(), new String[0],
                    1L, "test-user", false, false, "UTC", System.getProperty("java.io.tmpdir"),
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
                    () -> writer.writeArrow(0L, 0L));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());

            Assertions.assertThrows(Exception.class, writer::prepareCommit);
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
            testClassLoader.close();
        }
    }

    @Test
    public void testCloseResourceCanRetryAfterOutOfMemoryError() {
        AtomicInteger closeAttempts = new AtomicInteger();
        OutOfMemoryError firstFailure = new OutOfMemoryError("test OOM during close");
        AutoCloseable resource = () -> {
            if (closeAttempts.getAndIncrement() == 0) {
                throw firstFailure;
            }
        };

        Assertions.assertSame(firstFailure, PaimonJniWriter.closeResource(resource));
        Assertions.assertNull(PaimonJniWriter.closeResource(resource));
        Assertions.assertEquals(2, closeAttempts.get());
    }

    @Test
    public void testDetectNestedOutOfMemoryFailure() {
        RuntimeException wrapped = new RuntimeException(
                "write failed", new IllegalStateException(new OutOfMemoryError("test OOM")));
        Assertions.assertTrue(PaimonJniWriter.containsOutOfMemory(wrapped));
        Assertions.assertFalse(PaimonJniWriter.containsOutOfMemory(
                new RuntimeException("not an OOM")));
    }

    @Test
    public void testAllocatorCloseWaitsForOutstandingBuffers() {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try {
            try (ArrowBuf ignored = allocator.buffer(8)) {
                IllegalStateException exception = Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> PaimonJniWriter.verifyAllocatorHasNoOutstandingMemory(allocator));
                Assertions.assertTrue(exception.getMessage().contains("still owns 8 bytes"));
            }

            Assertions.assertDoesNotThrow(
                    () -> PaimonJniWriter.verifyAllocatorHasNoOutstandingMemory(allocator));
        } finally {
            allocator.close();
        }
    }

    @Test
    public void testWriterCloseFailureKeepsIoManagerForRetry() throws Exception {
        AtomicInteger writerCloseAttempts = new AtomicInteger();
        AtomicInteger ioManagerCloseAttempts = new AtomicInteger();
        FileStoreWrite<?> fileStoreWrite = (FileStoreWrite<?>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {FileStoreWrite.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")
                            && writerCloseAttempts.getAndIncrement() == 0) {
                        throw new OutOfMemoryError("test writer close OOM");
                    }
                    return null;
                });
        TableWriteImpl<?> tableWrite = new TableWriteImpl<>(
                RowType.of(), fileStoreWrite, null, null, null, null);
        IOManager ioManager = (IOManager) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {IOManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        ioManagerCloseAttempts.incrementAndGet();
                    }
                    return null;
                });

        PaimonJniWriter writer = new PaimonJniWriter();
        setField(writer, "writer", tableWrite);
        setField(writer, "ioManager", ioManager);
        try {
            Assertions.assertThrows(OutOfMemoryError.class, writer::closeWriter);
            Assertions.assertEquals(1, writerCloseAttempts.get());
            Assertions.assertEquals(0, ioManagerCloseAttempts.get());

            Assertions.assertDoesNotThrow(writer::closeWriter);
            Assertions.assertEquals(2, writerCloseAttempts.get());
            Assertions.assertEquals(1, ioManagerCloseAttempts.get());
        } finally {
            writer.close();
        }
    }

    @Test
    public void testOutOfMemoryAbortDefersPrepareUntilRecovery() throws Exception {
        AtomicInteger prepareAttempts = new AtomicInteger();
        AtomicInteger writerCloseAttempts = new AtomicInteger();
        FileStoreWrite<?> fileStoreWrite = (FileStoreWrite<?>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {FileStoreWrite.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareCommit")) {
                        prepareAttempts.incrementAndGet();
                        return Collections.emptyList();
                    }
                    if (method.getName().equals("close")) {
                        writerCloseAttempts.incrementAndGet();
                    }
                    return null;
                });
        TableWriteImpl<?> tableWrite = new TableWriteImpl<>(
                RowType.of(), fileStoreWrite, null, null, null, null);

        PaimonJniWriter writer = new PaimonJniWriter();
        setField(writer, "writer", tableWrite);
        setField(writer, "deferAbortAfterOutOfMemory", true);

        writer.abort();
        Assertions.assertEquals(0, prepareAttempts.get());
        Assertions.assertEquals(0, writerCloseAttempts.get());
        Assertions.assertThrows(IllegalStateException.class, writer::close);

        writer.recoverAndClose();
        Assertions.assertEquals(1, prepareAttempts.get());
        Assertions.assertEquals(1, writerCloseAttempts.get());
        Assertions.assertDoesNotThrow(writer::close);
    }

    @Test
    public void testOrdinaryAbortPreparesAndClosesWriterImmediately() throws Exception {
        AtomicInteger prepareAttempts = new AtomicInteger();
        AtomicInteger writerCloseAttempts = new AtomicInteger();
        FileStoreWrite<?> fileStoreWrite = (FileStoreWrite<?>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {FileStoreWrite.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareCommit")) {
                        prepareAttempts.incrementAndGet();
                        return Collections.emptyList();
                    }
                    if (method.getName().equals("close")) {
                        writerCloseAttempts.incrementAndGet();
                    }
                    return null;
                });
        TableWriteImpl<?> tableWrite = new TableWriteImpl<>(
                RowType.of(), fileStoreWrite, null, null, null, null);

        PaimonJniWriter writer = new PaimonJniWriter();
        setField(writer, "writer", tableWrite);

        writer.abort();
        Assertions.assertEquals(1, prepareAttempts.get());
        Assertions.assertEquals(1, writerCloseAttempts.get());
        Assertions.assertDoesNotThrow(writer::close);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = PaimonJniWriter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
