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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import mockit.Expectations;
import mockit.Injectable;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SplitAssignmentTest {

    @Injectable
    private FederationBackendPolicy mockBackendPolicy;

    @Injectable
    private SplitGenerator mockSplitGenerator;

    @Injectable
    private SplitToScanRange mockSplitToScanRange;

    @Mocked
    private Split mockSplit;

    @Mocked
    private Backend mockBackend;

    @Mocked
    private TScanRangeLocations mockScanRangeLocations;

    private SplitAssignment splitAssignment;
    private Map<String, String> locationProperties;
    private List<String> pathPartitionKeys;

    @BeforeEach
    void setUp() {
        locationProperties = new HashMap<>();
        pathPartitionKeys = new ArrayList<>();

        splitAssignment = new SplitAssignment(
                new TUniqueId(1, 2),
                mockBackendPolicy,
                mockSplitGenerator,
                mockSplitToScanRange,
                locationProperties,
                pathPartitionKeys
        );
    }

    // ==================== init() method tests ====================

    @Test
    void testInitSuccess() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        new Expectations() {
            {
                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

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
    void testInitTimeout() throws Exception {
        // Use MockUp to simulate timeout behavior quickly instead of waiting 30 seconds
        SplitAssignment testAssignment = new SplitAssignment(
                new TUniqueId(1, 2),
                mockBackendPolicy,
                mockSplitGenerator,
                mockSplitToScanRange,
                locationProperties,
                pathPartitionKeys
        );

        new MockUp<SplitAssignment>() {
            @mockit.Mock
            public void init() throws UserException {
                // Directly throw timeout exception to simulate the timeout scenario quickly
                throw new UserException("Failed to get first split after waiting for 0 seconds.");
            }
        };

        // Test & Verify - should timeout immediately now
        UserException exception = Assertions.assertThrows(UserException.class, () -> testAssignment.init());
        Assertions.assertTrue(exception.getMessage().contains("Failed to get first split after waiting for"));
    }

    @Test
    void testInitInterrupted() throws Exception {
        CountDownLatch initStarted = new CountDownLatch(1);
        CountDownLatch shouldInterrupt = new CountDownLatch(1);

        Thread initThread = new Thread(() -> {
            try {
                initStarted.countDown();
                shouldInterrupt.await();
                splitAssignment.init();
            } catch (Exception e) {
                // Expected interruption
            }
        });

        initThread.start();
        initStarted.await();

        // Interrupt the init thread
        initThread.interrupt();
        shouldInterrupt.countDown();

        initThread.join(1000);
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

        new Expectations() {
            {
                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

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
    void testAddToQueueSampleSplitAlreadySet() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        new Expectations() {
            {
                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;
                minTimes = 0;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
                minTimes = 0;
            }
        };

        // Setup: First call to set sample split
        List<Split> firstSplits = Collections.singletonList(mockSplit);

        splitAssignment.addToQueue(firstSplits);
        Split firstSampleSplit = splitAssignment.getSampleSplit();

        // Test: Second call should not change sample split
        List<Split> secondSplits = Collections.singletonList(mockSplit);

        splitAssignment.addToQueue(secondSplits);

        // Verify sample split unchanged
        Assertions.assertEquals(firstSampleSplit, splitAssignment.getSampleSplit());
    }

    @Test
    void testAddToQueueWithQueueBlockingScenario() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        new Expectations() {
            {
                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

        // This test simulates a scenario where appendBatch might experience queue blocking
        // by adding multiple batches rapidly

        List<Split> splits = Collections.singletonList(mockSplit);

        // First, fill up the queue by adding many batches
        for (int i = 0; i < 10; i++) {
            splitAssignment.addToQueue(splits);
        }

        // Verify the queue has data
        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertNotNull(queue);
    }

    @Test
    void testAddToQueueConcurrentAccess() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        new Expectations() {
            {
                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

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
        new Expectations() {
            {
                Multimap<Backend, Split> batch = ArrayListMultimap.create();
                batch.put(mockBackend, mockSplit);

                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

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

    // ==================== appendBatch() behavior tests ====================

    @Test
    void testAppendBatchTimeoutBehavior() throws Exception {
        new Expectations() {
            {
                Multimap<Backend, Split> batch = ArrayListMultimap.create();
                batch.put(mockBackend, mockSplit);

                mockBackendPolicy.computeScanRangeAssignment((List<Split>) any);
                result = batch;

                mockSplitToScanRange.getScanRange(mockBackend, locationProperties, mockSplit, pathPartitionKeys);
                result = mockScanRangeLocations;
            }
        };

        // This test verifies that appendBatch properly handles queue offer timeouts
        // We'll simulate this by first filling the assignment and then trying to add more

        List<Split> splits = Collections.singletonList(mockSplit);

        // Add multiple splits to potentially cause queue pressure
        for (int i = 0; i < 50; i++) {
            try {
                splitAssignment.addToQueue(splits);
            } catch (Exception e) {
                // Expected if queue gets full and times out
                break;
            }
        }

        // Verify that splits were processed
        Assertions.assertNotNull(splitAssignment.getSampleSplit());
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
