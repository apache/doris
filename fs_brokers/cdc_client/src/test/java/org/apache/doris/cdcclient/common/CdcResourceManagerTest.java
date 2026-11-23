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

package org.apache.doris.cdcclient.common;

import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_INCREMENTAL;
import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_SNAPSHOT;
import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.STANDALONE_TVF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class CdcResourceManagerTest {
    private static final long MIB = 1024L * 1024;

    @Test
    void readerReservationUsesConfiguredQueueAndHeap() {
        Properties config = new Properties();
        assertEquals(
                96 * MIB, new CdcResourceManager(1024 * MIB, config).getReaderReservationBytes());
        assertEquals(
                384 * MIB, new CdcResourceManager(4096 * MIB, config).getReaderReservationBytes());
        config.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, "1025");
        assertEquals(1538, new CdcResourceManager(4096 * MIB, config).getReaderReservationBytes());
        config.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, "0");
        assertEquals(
                384 * MIB, new CdcResourceManager(4096 * MIB, config).getReaderReservationBytes());
    }

    @Test
    void changingEitherRatioLeavesTheOtherQuotaUnchanged() {
        Properties config = new Properties();
        long heap = 4096 * MIB;
        config.setProperty("cdc.sink.memory.ratio", "0.25");
        CdcResourceManager resources = new CdcResourceManager(heap, config);
        assertEquals(heap / 4, resources.getSinkByteLimit());
        assertEquals(heap / 3, resources.getReaderMemoryQuotaBytes());
        config.setProperty("cdc.reader.memory.ratio", "0.375");
        resources = new CdcResourceManager(heap, config);
        assertEquals(heap / 4, resources.getSinkByteLimit());
        assertEquals(1536 * MIB, resources.getReaderMemoryQuotaBytes());
    }

    @Test
    void overflowingQueueEstimateFailsAtInitialization() {
        Properties config = new Properties();
        config.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, Long.toString(Long.MAX_VALUE));
        assertThrows(ArithmeticException.class, () -> new CdcResourceManager(4096 * MIB, config));
    }

    @ParameterizedTest
    @CsvSource({
        "0,0.25",
        "0.25,-0.1",
        "0.5,0.5",
        "0.5,0.6",
        "NaN,0.25",
        "0.25,Infinity",
        "bad,0.25"
    })
    void rejectsInvalidHeapRatios(String sinkRatio, String readerRatio) {
        Properties config = new Properties();
        config.setProperty("cdc.sink.memory.ratio", sinkRatio);
        config.setProperty("cdc.reader.memory.ratio", readerRatio);
        assertThrows(
                IllegalArgumentException.class, () -> new CdcResourceManager(4096 * MIB, config));
    }

    @Test
    void fourGiBHeapSharesAllThreeReaderSlotsAcrossGroups() {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        try (CdcResourceManager.SnapshotQuota quota =
                resources.newSnapshotQuota("snapshot", JOB_SNAPSHOT)) {
            assertEquals(3, quota.tryReserveSlots(16));
            assertNull(
                    resources.createIncrementalReader(
                            "incremental",
                            JOB_INCREMENTAL,
                            Object::new,
                            ignored -> terminatedWorker()));
        }
        assertEquals(
                resources.getReaderMemoryQuotaBytes(),
                resources.getAvailableReaderBytes(STANDALONE_TVF));
    }

    @Test
    void taskReusesItsSlotOnlyAfterTheLastBatchCloses() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(1024 * MIB, new Properties());
        try (CdcResourceManager.SnapshotQuota quota =
                resources.newSnapshotQuota("snapshot", JOB_SNAPSHOT)) {
            assertEquals(1, quota.tryReserveSlots(1));
            try (ManagedReader<Object> reader =
                            quota.createSnapshotReader(Object::new, ignored -> terminatedWorker());
                    ReaderBatch<String> batch =
                            reader.pollBatch(ignored -> List.of("row").iterator())) {
                reader.close();
                assertNull(quota.createSnapshotReader(Object::new, ignored -> terminatedWorker()));
                batch.close();
                resources.setWaitingIncrementalReaders(10);
                try (ManagedReader<Object> next =
                        quota.createSnapshotReader(Object::new, ignored -> terminatedWorker())) {
                    assertNotNull(next);
                    quota.close();
                    assertEquals(
                            resources.getReaderReservationBytes(),
                            resources.getReaderBytes(JOB_SNAPSHOT));
                }
            }
        }
        assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
    }

    @Test
    void rejectsReaderLargerThanCapacityBeforeCallingFactory() {
        Properties config = new Properties();
        config.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, Long.toString(4096 * MIB));
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, config);
        AtomicInteger creations = new AtomicInteger();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        resources.createIncrementalReader(
                                "snapshot",
                                JOB_SNAPSHOT,
                                creations::incrementAndGet,
                                ignored -> terminatedWorker()));
        assertEquals(0, creations.get());
        assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
    }

    @Test
    void reservesBeforeCreationAndReleasesOnFailure() {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        IllegalStateException failure = new IllegalStateException("creation failed");
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                resources.createIncrementalReader(
                                        "incremental",
                                        JOB_INCREMENTAL,
                                        () -> {
                                            assertEquals(
                                                    resources.getReaderReservationBytes(),
                                                    resources.getReaderBytes(JOB_INCREMENTAL));
                                            throw failure;
                                        },
                                        ignored -> terminatedWorker())));
        assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
    }

    @Test
    void closingKeepsReservationUntilCloseReturnsAndReleasesOnlyOnce() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch finishClose = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        ManagedReader<Object> reader =
                resources.createIncrementalReader(
                        "tvf",
                        STANDALONE_TVF,
                        Object::new,
                        ignored -> {
                            closes.incrementAndGet();
                            closing.countDown();
                            try {
                                assertTrue(finishClose.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                            return terminatedWorker();
                        });
        FutureTask<Void> first =
                new FutureTask<>(
                        () -> {
                            reader.close();
                            return null;
                        });
        FutureTask<Void> second =
                new FutureTask<>(
                        () -> {
                            reader.close();
                            return null;
                        });
        new Thread(first).start();
        try {
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            new Thread(second).start();
            assertEquals(
                    resources.getReaderReservationBytes(),
                    resources.getReaderBytes(STANDALONE_TVF));
            // Closing a different reader must not need the slow connector's lock.
            managedReader(resources, JOB_SNAPSHOT).close();
        } finally {
            finishClose.countDown();
        }
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(1, closes.get());
        assertEquals(0, resources.getReaderBytes(STANDALONE_TVF));
    }

    @Test
    void failedCloseRetainsReservationForRetry() {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        AtomicInteger attempts = new AtomicInteger();
        ManagedReader<Object> reader =
                resources.createIncrementalReader(
                        "tvf",
                        STANDALONE_TVF,
                        Object::new,
                        ignored -> {
                            if (attempts.getAndIncrement() == 0) {
                                throw new IllegalStateException("close failed");
                            }
                            return terminatedWorker();
                        });
        assertThrows(IllegalStateException.class, reader::close);
        assertEquals(
                resources.getReaderReservationBytes(), resources.getReaderBytes(STANDALONE_TVF));
        resources.retryClosingReaders();
        assertEquals(0, resources.getReaderBytes(STANDALONE_TVF));
    }

    @Test
    void onlyOneConcurrentReaderCanAcquireTheLastReservation() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(288 * MIB, new Properties());
        ExecutorService callers = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger creations = new AtomicInteger();
        Callable<ManagedReader<Integer>> create =
                () -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return resources.createIncrementalReader(
                            "snapshot",
                            JOB_SNAPSHOT,
                            creations::incrementAndGet,
                            ignored -> terminatedWorker());
                };
        try {
            Future<ManagedReader<Integer>> first = callers.submit(create);
            Future<ManagedReader<Integer>> second = callers.submit(create);
            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }
            try (ManagedReader<Integer> one = first.get(5, TimeUnit.SECONDS);
                    ManagedReader<Integer> two = second.get(5, TimeUnit.SECONDS)) {
                assertTrue((one == null) != (two == null));
                assertEquals(1, creations.get());
                assertEquals(
                        resources.getReaderReservationBytes(),
                        resources.getReaderBytes(JOB_SNAPSHOT));
            }
        } finally {
            start.countDown();
            callers.shutdownNow();
        }
        assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
    }

    @Test
    void waitingIncrementalReaderReservesOneAdmissionWithoutBlockingOtherSnapshots() {
        Properties config = new Properties();
        // One third of this heap fits three 96 MiB readers.
        CdcResourceManager resources = new CdcResourceManager(864 * MIB, config);
        resources.setWaitingIncrementalReaders(1);
        try (ManagedReader<Object> first = managedReader(resources, JOB_SNAPSHOT);
                ManagedReader<Object> second = managedReader(resources, JOB_SNAPSHOT)) {
            assertNotNull(first);
            assertNotNull(second);
            assertNull(managedReader(resources, JOB_SNAPSHOT));
            assertNull(managedReader(resources, STANDALONE_TVF));
            try (ManagedReader<Object> incremental = managedReader(resources, JOB_INCREMENTAL)) {
                assertNotNull(incremental);
                resources.setWaitingIncrementalReaders(0);
                assertEquals(0, resources.getAvailableReaderBytes(JOB_SNAPSHOT));
            }
        }
        assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
        assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
    }

    @Test
    void polledBatchRetainsReservationAfterReaderCloseUntilBatchClose() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        try (ManagedReader<Object> reader = managedReader(resources, JOB_SNAPSHOT);
                ReaderBatch<String> batch =
                        reader.pollBatch(
                                ignored -> Collections.singletonList("record").iterator())) {
            reader.close();
            resources.refreshClosingReaders();
            assertNull(reader.getReader());
            assertEquals(
                    resources.getReaderReservationBytes(), resources.getReaderBytes(JOB_SNAPSHOT));
            assertEquals("record", batch.next());
            assertFalse(batch.hasNext());
            assertEquals(
                    resources.getReaderReservationBytes(), resources.getReaderBytes(JOB_SNAPSHOT));
            batch.close();
            batch.close();
            resources.refreshClosingReaders();
            assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
        }
    }

    @Test
    void retainsReservationAcrossCloseTimeoutUntilWorkerAndPollExit() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(4096 * MIB, new Properties());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch finishWorker = new CountDownLatch(1);
        worker.submit(
                () -> {
                    finishWorker.await();
                    return null;
                });
        AtomicInteger closes = new AtomicInteger();
        ManagedReader<Object> reader =
                resources.createIncrementalReader(
                        "snapshot",
                        JOB_SNAPSHOT,
                        Object::new,
                        ignored -> {
                            closes.incrementAndGet();
                            worker.shutdown();
                            return worker;
                        });
        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch finishPoll = new CountDownLatch(1);
        FutureTask<Void> poll =
                new FutureTask<>(
                        () -> {
                            try (ReaderBatch<Object> ignored =
                                    reader.pollBatch(
                                            unused -> {
                                                polling.countDown();
                                                finishPoll.await();
                                                return Collections.emptyIterator();
                                            })) {
                                return null;
                            }
                        });
        new Thread(poll).start();
        try {
            assertTrue(polling.await(5, TimeUnit.SECONDS));
            reader.close();
            resources.retryClosingReaders();
            assertEquals(
                    resources.getReaderReservationBytes(), resources.getReaderBytes(JOB_SNAPSHOT));
            try (ReaderBatch<Object> batch =
                    reader.pollBatch(
                            ignored -> {
                                throw new AssertionError("poll after close");
                            })) {
                assertFalse(batch.hasNext());
            }
            finishWorker.countDown();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            resources.retryClosingReaders();
            assertEquals(
                    resources.getReaderReservationBytes(), resources.getReaderBytes(JOB_SNAPSHOT));
            finishPoll.countDown();
            poll.get(5, TimeUnit.SECONDS);
            assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
            resources.retryClosingReaders();
            assertEquals(1, closes.get());
        } finally {
            finishWorker.countDown();
            finishPoll.countDown();
            worker.shutdownNow();
        }
    }

    private static ExecutorService terminatedWorker() {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        worker.shutdown();
        return worker;
    }

    private static ManagedReader<Object> managedReader(
            CdcResourceManager resources, CdcResourceManager.ReaderGroup group) {
        return resources.createIncrementalReader(
                "job", group, Object::new, ignored -> terminatedWorker());
    }
}
