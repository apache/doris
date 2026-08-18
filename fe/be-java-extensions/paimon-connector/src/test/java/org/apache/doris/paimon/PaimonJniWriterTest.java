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

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

public class PaimonJniWriterTest {

    @Test
    public void testClassifyWriterMemoryErrors() {
        Assertions.assertEquals(
                PaimonJniWriter.MEMORY_ERROR_ARROW,
                PaimonJniWriter.classifyMemoryError(
                        new RuntimeException(new OutOfMemoryException("Arrow limit"))));
        Assertions.assertEquals(
                PaimonJniWriter.MEMORY_ERROR_PAIMON_PAGE,
                PaimonJniWriter.classifyMemoryError(
                        new OutOfMemoryError(
                                "Paimon JNI native page allocation failed: query limit")));
        Assertions.assertEquals(
                PaimonJniWriter.MEMORY_ERROR_JVM_HEAP,
                PaimonJniWriter.classifyMemoryError(new OutOfMemoryError("Java heap space")));
        Assertions.assertEquals(
                PaimonJniWriter.MEMORY_ERROR_NONE,
                PaimonJniWriter.classifyMemoryError(new IllegalArgumentException("not memory")));
        Assertions.assertEquals(
                PaimonJniWriter.MEMORY_ERROR_CANCELLED,
                PaimonJniWriter.classifyMemoryError(
                        new RuntimeException(new CancellationException("query cancelled"))));
    }

    @Test
    public void testWriterMemoryBudgetIncludesArrowAndPaimonPages() {
        int pageSize = 64 * 1024;
        PaimonJniWriter.WriterMemoryBudget budget = PaimonJniWriter.splitWriterMemoryBudget(
                512L * 1024 * 1024, 512L * 1024 * 1024, pageSize, 3);

        Assertions.assertEquals(128L * 1024 * 1024, budget.arrowHeadroomBytes);
        Assertions.assertEquals(384L * 1024 * 1024, budget.paimonPageBudgetBytes);
        Assertions.assertEquals(
                512L * 1024 * 1024,
                budget.arrowHeadroomBytes + budget.paimonPageBudgetBytes);
    }

    @Test
    public void testWriterMemoryBudgetKeepsConfiguredWriteBufferCap() {
        int pageSize = 64 * 1024;
        PaimonJniWriter.WriterMemoryBudget budget = PaimonJniWriter.splitWriterMemoryBudget(
                512L * 1024 * 1024, 64L * 1024 * 1024, pageSize, 3);

        Assertions.assertEquals(128L * 1024 * 1024, budget.arrowHeadroomBytes);
        Assertions.assertEquals(64L * 1024 * 1024, budget.paimonPageBudgetBytes);
    }

    @Test
    public void testWriterMemoryBudgetRejectsMissingArrowHeadroom() {
        int pageSize = 64 * 1024;
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniWriter.splitWriterMemoryBudget(
                        16L * 1024 * 1024, 16L * 1024 * 1024, pageSize, 1));

        Assertions.assertTrue(
                exception.getMessage().contains("Arrow headroom and 1 required Paimon page(s)"));
    }

    @Test
    public void testWriterMemoryBudgetPreservesMergeTreeMinimumPages() {
        int pageSize = 8 * 1024 * 1024;
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniWriter.splitWriterMemoryBudget(
                        32L * 1024 * 1024, 32L * 1024 * 1024, pageSize, 3));

        Assertions.assertTrue(exception.getMessage().contains("3 required Paimon page(s)"));

        PaimonJniWriter.WriterMemoryBudget budget = PaimonJniWriter.splitWriterMemoryBudget(
                40L * 1024 * 1024, 40L * 1024 * 1024, pageSize, 3);
        Assertions.assertEquals(16L * 1024 * 1024, budget.arrowHeadroomBytes);
        Assertions.assertEquals(24L * 1024 * 1024, budget.paimonPageBudgetBytes);
    }

    @Test
    public void testManagedMemoryPoolRequiresAtLeastOnePage() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new DorisMemorySegmentPool(32 * 1024, 64 * 1024, 1L));
        Assertions.assertTrue(exception.getMessage().contains("at least one page"));
    }

    @Test
    public void testManagedMemoryPoolWaitsOnlyAfterPaimonPreemption() {
        int pageSize = 64 * 1024;
        List<Boolean> waitModes = new ArrayList<>();
        DorisMemorySegmentPool pool = new DorisMemorySegmentPool(
                2L * pageSize, pageSize, 1L,
                (manager, bytes, waitForMemory) -> {
                    waitModes.add(waitForMemory);
                    return waitForMemory ? ByteBuffer.allocateDirect(bytes) : null;
                });
        MemoryPoolFactory factory = new MemoryPoolFactory(pool);

        class TestMemoryOwner implements MemoryOwner {
            private final long occupancy;
            private MemorySegmentPool memoryPool;
            private boolean flushed;

            private TestMemoryOwner(long occupancy) {
                this.occupancy = occupancy;
            }

            @Override
            public void setMemoryPool(MemorySegmentPool memoryPool) {
                this.memoryPool = memoryPool;
            }

            @Override
            public long memoryOccupancy() {
                return occupancy;
            }

            @Override
            public void flushMemory() {
                flushed = true;
            }
        }

        TestMemoryOwner requestor = new TestMemoryOwner(0);
        TestMemoryOwner preempted = new TestMemoryOwner(pageSize);
        factory.addOwners(List.of(requestor, preempted));
        factory.notifyNewOwner(requestor);

        MemorySegment segment = requestor.memoryPool.nextSegment();
        Assertions.assertNull(segment);
        Assertions.assertTrue(preempted.flushed);
        Assertions.assertEquals(1, factory.bufferPreemptCount());
        Assertions.assertEquals(List.of(false, false), waitModes);

        pool.waitForMemoryIfNeeded();
        Assertions.assertEquals(List.of(false, false, true), waitModes);

        segment = requestor.memoryPool.nextSegment();
        Assertions.assertNotNull(segment);
        Assertions.assertEquals(1, requestor.memoryPool.freePages());

        requestor.memoryPool.returnAll(Collections.singletonList(segment));
        Assertions.assertEquals(2, requestor.memoryPool.freePages());
        Assertions.assertSame(segment, requestor.memoryPool.nextSegment());
        Assertions.assertEquals(List.of(false, false, true), waitModes);
    }

    @Test
    public void testManagedMemoryPoolPreallocatesRequiredOwnerPagesInBlockingMode() {
        int pageSize = 64 * 1024;
        List<Boolean> waitModes = new ArrayList<>();
        DorisMemorySegmentPool pool = new DorisMemorySegmentPool(
                3L * pageSize, pageSize, 1L,
                (manager, bytes, waitForMemory) -> {
                    waitModes.add(waitForMemory);
                    return ByteBuffer.allocateDirect(bytes);
                });

        pool.preallocate(3);

        Assertions.assertEquals(List.of(true, true, true), waitModes);
        Assertions.assertEquals(3, pool.freePages());
        Assertions.assertNotNull(pool.nextSegment());
        Assertions.assertEquals(List.of(true, true, true), waitModes);
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
                    () -> writer.write(ByteBuffer.allocateDirect(0)));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());

            Assertions.assertThrows(Exception.class, writer::prepareCommit);
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
            testClassLoader.close();
        }
    }
}
