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

package org.apache.doris.datasource.split;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SplitAssignmentTest {

    private FederationBackendPolicy mockBackendPolicy;

    private SplitGenerator mockSplitGenerator;

    private SplitToScanRange mockSplitToScanRange;

    private Split mockSplit;

    private Backend mockBackend;

    private TScanRangeLocations mockScanRangeLocations;

    private SplitAssignment splitAssignment;
    private Map<String, String> locationProperties;
    private List<String> pathPartitionKeys;

    @BeforeEach
    void setUp() {
        mockBackendPolicy = Mockito.mock(FederationBackendPolicy.class);
        mockSplitGenerator = Mockito.mock(SplitGenerator.class);
        mockSplitToScanRange = Mockito.mock(SplitToScanRange.class);
        mockSplit = Mockito.mock(Split.class);
        mockBackend = Mockito.mock(Backend.class);
        mockScanRangeLocations = Mockito.mock(TScanRangeLocations.class);

        locationProperties = new HashMap<>();
        pathPartitionKeys = new ArrayList<>();

        splitAssignment = new SplitAssignment(
                mockBackendPolicy,
                mockSplitGenerator,
                mockSplitToScanRange,
                locationProperties,
                pathPartitionKeys,
                true,
                128,
                32
        );
    }

    @Test
    void firstConvertedSplitDoesNotWaitForAnotherBackendAssignment() throws Exception {
        CountDownLatch assignmentEntered = new CountDownLatch(1);
        CountDownLatch releaseAssignment = new CountDownLatch(1);
        CountDownLatch initStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            initStarted.countDown();
            return null;
        }).when(mockSplitGenerator).startSplit(Mockito.anyInt());
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenAnswer(invocation -> {
            assignmentEntered.countDown();
            Assertions.assertTrue(releaseAssignment.await(30, TimeUnit.SECONDS));
            return ArrayListMultimap.create();
        });
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            Future<?> init = workers.submit(() -> {
                splitAssignment.init();
                return null;
            });
            Assertions.assertTrue(initStarted.await(5, TimeUnit.SECONDS));
            Future<?> assigning = workers.submit(() -> splitAssignment.assignBatch(
                    Collections.singletonList(mockSplit)));
            Assertions.assertTrue(assignmentEntered.await(5, TimeUnit.SECONDS));
            Future<?> converted = workers.submit(() -> splitAssignment.createScanRanges(
                    mockBackend, Collections.singletonList(mockSplit)));
            converted.get(5, TimeUnit.SECONDS);
            init.get(5, TimeUnit.SECONDS);
            Assertions.assertSame(mockSplit, splitAssignment.getSampleSplit());
            Assertions.assertFalse(assigning.isDone(), "readiness must not wait for unrelated assignment work");
        } finally {
            releaseAssignment.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void demandCallbackStopsWithTheAssignment() {
        java.util.concurrent.atomic.AtomicInteger requested = new java.util.concurrent.atomic.AtomicInteger();
        splitAssignment.setMoreSplitsRequestedCallback(requested::incrementAndGet);
        splitAssignment.onMoreSplitsRequested();
        Assertions.assertEquals(1, requested.get());
        splitAssignment.stop();
        splitAssignment.onMoreSplitsRequested();
        splitAssignment.setMoreSplitsRequestedCallback(requested::incrementAndGet);
        splitAssignment.onMoreSplitsRequested();
        Assertions.assertEquals(1, requested.get());
    }

    @Test
    void asyncResourceRegisteredAfterStopIsClosedImmediately() {
        java.util.concurrent.atomic.AtomicInteger closes = new java.util.concurrent.atomic.AtomicInteger();
        splitAssignment.stop();
        splitAssignment.addCloseable(closes::incrementAndGet);
        Assertions.assertEquals(1, closes.get());
        splitAssignment.stop();
        Assertions.assertEquals(1, closes.get());
    }

    @Test
    void stopClosesRegisteredResourceOutsideRegistryLock() {
        java.util.concurrent.atomic.AtomicInteger closes = new java.util.concurrent.atomic.AtomicInteger();
        splitAssignment.addCloseable(() -> {
            closes.incrementAndGet();
            splitAssignment.addCloseable(closes::incrementAndGet);
        });
        splitAssignment.stop();
        Assertions.assertEquals(2, closes.get());
    }

    @Test
    void closingActiveProducerDoesNotTurnSuccessfulStopIntoFailure() throws Exception {
        splitAssignment.addCloseable(() -> splitAssignment.setException(new UserException("source closed")));
        Assertions.assertDoesNotThrow(() -> splitAssignment.stop());
        splitAssignment.setException(new UserException("producer interrupted after stop"));
        Assertions.assertNull(splitAssignment.getAssignedSplits(mockBackend));
    }

    @Test
    void stopPreservesFailurePublishedBeforeCancellation() {
        UserException failure = new UserException("manifest read failed");
        splitAssignment.setException(failure);
        splitAssignment.addCloseable(() -> splitAssignment.setException(new UserException("source closed")));
        RuntimeException actual = Assertions.assertThrows(RuntimeException.class, () -> splitAssignment.stop());
        Assertions.assertSame(failure, actual.getCause());
        Assertions.assertEquals(0, failure.getSuppressed().length);
    }

    // ==================== init() method tests ====================

    @Test
    void testInitSuccess() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        // Start a thread to simulate split generation after a short delay
        Thread splitGeneratorThread = new Thread(() -> {
            try {
                Thread.sleep(50); // Short delay to simulate async split generation
                List<Split> splits = Collections.singletonList(mockSplit);
                splitAssignment.addToQueue(splits);
            } catch (Exception e) {
                // Ignore for test
            }
        });

        splitGeneratorThread.start();

        // Test
        Assertions.assertDoesNotThrow(() -> splitAssignment.init());

        // Verify sample split is set
        Assertions.assertNotNull(splitAssignment.getSampleSplit());

        splitGeneratorThread.join(1000); // Wait for thread to complete
    }

    @Test
    void testInitInterrupted() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<UserException> result = executor.submit(() -> {
                Thread.currentThread().interrupt();
                return Assertions.assertThrows(UserException.class, splitAssignment::init);
            });
            Assertions.assertInstanceOf(InterruptedException.class, result.get(5, TimeUnit.SECONDS).getCause());
            Mockito.verify(mockSplitGenerator).startSplit(mockBackendPolicy.numBackends());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void testInitWithPreExistingException() throws Exception {
        UserException preException = new UserException("Pre-existing error");
        splitAssignment.setException(preException);

        // Test & Verify
        UserException exception = Assertions.assertThrows(UserException.class, () -> splitAssignment.init());
        Assertions.assertTrue(exception.getMessage().contains(" Pre-existing error"), exception.getMessage());
    }

    // ==================== addToQueue() method tests ====================

    @Test
    void testAddToQueueWithEmptyList() throws Exception {
        // Test
        Assertions.assertDoesNotThrow(() -> splitAssignment.addToQueue(Collections.emptyList()));
    }

    @Test
    void testAddToQueueSuccess() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        // Mock setup
        List<Split> splits = Collections.singletonList(mockSplit);

        // Test
        Assertions.assertDoesNotThrow(() -> splitAssignment.addToQueue(splits));

        // Verify sample split is set
        Assertions.assertEquals(mockSplit, splitAssignment.getSampleSplit());

        // Verify assignment queue is created and contains data
        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertNotNull(queue);
    }

    @Test
    void testConsumerCreatesBoundedQueueFirst() throws Exception {
        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertEquals(32, queue.remainingCapacity());
        Assertions.assertSame(queue, splitAssignment.getAssignedSplits(mockBackend));
    }

    @Test
    void testSamplePublishedAfterScanParametersAreInitialized() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        CountDownLatch converting = new CountDownLatch(1);
        CountDownLatch converted = new CountDownLatch(1);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenAnswer(invocation -> {
                    converting.countDown();
                    Assertions.assertTrue(converted.await(5, TimeUnit.SECONDS));
                    return mockScanRangeLocations;
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                splitAssignment.addToQueue(Collections.singletonList(mockSplit));
                return null;
            });
            Assertions.assertTrue(converting.await(5, TimeUnit.SECONDS));
            Assertions.assertNull(splitAssignment.getSampleSplit());
            converted.countDown();
            producer.get(5, TimeUnit.SECONDS);
            Assertions.assertSame(mockSplit, splitAssignment.getSampleSplit());
        } finally {
            converted.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void testProducerCreatesBoundedQueueFirst() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        splitAssignment.addToQueue(Collections.singletonList(mockSplit));

        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertEquals(1, queue.size());
        Assertions.assertEquals(31, queue.remainingCapacity());
        Assertions.assertSame(mockScanRangeLocations, queue.element().iterator().next());
    }

    @Test
    void testConsumerCreatedQueueBlocksProducerUntilSpaceAvailable() throws Exception {
        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Collection<TScanRangeLocations> queuedBatch = Collections.singletonList(mockScanRangeLocations);
        for (int i = 0; i < 32; i++) {
            Assertions.assertTrue(queue.offer(queuedBatch));
        }
        Assertions.assertFalse(queue.offer(queuedBatch));

        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        CountDownLatch scanRangeCreated = new CountDownLatch(1);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenAnswer(invocation -> {
                    scanRangeCreated.countDown();
                    return mockScanRangeLocations;
                });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                splitAssignment.addToQueue(Collections.singletonList(mockSplit));
                return null;
            });
            Assertions.assertTrue(scanRangeCreated.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> producer.get(200, TimeUnit.MILLISECONDS));
            Assertions.assertEquals(32, queue.size());

            Assertions.assertSame(queuedBatch, queue.poll());
            producer.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(32, queue.size());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void testAddToQueueSampleSplitAlreadySet() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Split secondSplit = Mockito.mock(Split.class);
        Multimap<Backend, Split> secondBatch = ArrayListMultimap.create();
        secondBatch.put(mockBackend, secondSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch, secondBatch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        splitAssignment.addToQueue(Collections.singletonList(mockSplit));
        splitAssignment.addToQueue(Collections.singletonList(secondSplit));
        Assertions.assertSame(mockSplit, splitAssignment.getSampleSplit());
    }

    @Test
    void testAddToQueueConcurrentAccess() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        // Test concurrent access to addToQueue method
        List<Split> splits = Collections.singletonList(mockSplit);

        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();
                    splitAssignment.addToQueue(splits);
                } catch (Exception e) {
                    // Log but don't fail test for concurrency issues
                } finally {
                    doneLatch.countDown();
                }
            });
            threads.add(thread);
            thread.start();
        }

        startLatch.countDown(); // Start all threads
        Assertions.assertTrue(doneLatch.await(5, TimeUnit.SECONDS)); // Wait for completion

        // Verify sample split is set
        Assertions.assertNotNull(splitAssignment.getSampleSplit());

        // Cleanup
        for (Thread thread : threads) {
            thread.join(1000);
        }
    }

    // ==================== Integration tests for init() and addToQueue() ====================

    @Test
    void testInitAndAddToQueueIntegration() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

        List<Split> splits = Collections.singletonList(mockSplit);

        // Start background thread to add splits after init starts
        Thread splitsProvider = new Thread(() -> {
            try {
                Thread.sleep(100); // Small delay to ensure init is waiting
                splitAssignment.addToQueue(splits);
            } catch (Exception e) {
                // Ignore
            }
        });

        splitsProvider.start();

        // Test init - should succeed once splits are added
        Assertions.assertDoesNotThrow(() -> splitAssignment.init());

        // Verify
        Assertions.assertNotNull(splitAssignment.getSampleSplit());
        Assertions.assertEquals(mockSplit, splitAssignment.getSampleSplit());

        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertNotNull(queue);

        splitsProvider.join(1000);
    }

    @Test
    void testInitWhenNeedMoreSplitReturnsFalse() throws Exception {
        // Test init behavior when needMoreSplit() returns false
        splitAssignment.stop(); // This should make needMoreSplit() return false

        // Init should complete immediately without waiting
        Assertions.assertDoesNotThrow(() -> splitAssignment.init());
    }

    @Test
    void testInitWithScheduleFinished() throws Exception {
        // Test init behavior when schedule is already finished
        splitAssignment.finishSchedule();

        // Init should complete immediately without waiting
        Assertions.assertDoesNotThrow(() -> splitAssignment.init());
    }
}
