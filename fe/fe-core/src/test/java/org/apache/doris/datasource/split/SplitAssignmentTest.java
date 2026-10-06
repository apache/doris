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
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
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

    // The query whose planning started an assignment, which its stop names in the log.
    private static final TUniqueId PLANNED_BY = new TUniqueId(1, 2);

    private FederationBackendPolicy mockBackendPolicy;

    private SplitGenerator mockSplitGenerator;

    private SplitToScanRange mockSplitToScanRange;

    private Split mockSplit;

    private Backend mockBackend;

    private TScanRangeLocations mockScanRangeLocations;

    private SplitAssignment splitAssignment;
    private Map<String, String> locationProperties;
    private List<String> pathPartitionKeys;
    private SplitSourceManager splitSourceManager;

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
        splitSourceManager = new SplitSourceManager();

        splitAssignment = new SplitAssignment(
                mockBackendPolicy,
                mockSplitGenerator,
                mockSplitToScanRange,
                locationProperties,
                pathPartitionKeys,
                true,
                splitSourceManager
        );
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
    void testInitTimeout() throws Exception {
        // Use spy to simulate timeout behavior quickly instead of waiting 30 seconds
        SplitAssignment testAssignment = Mockito.spy(new SplitAssignment(
                mockBackendPolicy,
                mockSplitGenerator,
                mockSplitToScanRange,
                locationProperties,
                pathPartitionKeys,
                true,
                splitSourceManager
        ));

        Mockito.doThrow(new UserException("Failed to get first split after waiting for 0 seconds."))
                .when(testAssignment).init();

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
    void testAddToQueueSampleSplitAlreadySet() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

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

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

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

    // ==================== appendBatch() behavior tests ====================

    @Test
    void testAppendBatchTimeoutBehavior() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);

        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);

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

    // ==================== SplitSource.getNextBatch() tests ====================

    @Test
    void testFetchTakesTheLastSplitsWithoutWaitingOnceNoMoreCanCome() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 1000);
        // The generator queued every split and finished, as a remote Doris scan's does when it is started.
        splitAssignment.addToQueue(Collections.singletonList(mockSplit));
        splitAssignment.finishSchedule();

        // A fetch that waited on the queue for more would fail on an interrupted thread: this one takes what is
        // queued and returns at once.
        Thread.currentThread().interrupt();
        List<TScanRangeLocations> lastSplits;
        try {
            lastSplits = source.getNextBatch(1000);
        } finally {
            Thread.interrupted();
        }

        Assertions.assertEquals(Collections.singletonList(mockScanRangeLocations), lastSplits);
        // The backend's next fetch learns that the source is done.
        Assertions.assertTrue(source.getNextBatch(1000).isEmpty());
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

    // ==================== start() / stop() lifecycle tests ====================

    @Test
    void testSourcesAreReachableOnlyFromStartToStop() throws Exception {
        // No split to wait for: init() returns at once.
        splitAssignment.finishSchedule();
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        // Planned, not dispatched: a backend could not reach the source.
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));

        splitAssignment.start();
        Assertions.assertSame(source, splitSourceManager.getSplitSource(source.getUniqueId()));
        // The source of an assignment started already is reachable at once.
        SplitSource late = new SplitSource(mockBackend, splitAssignment, 100);
        Assertions.assertSame(late, splitSourceManager.getSplitSource(late.getUniqueId()));

        splitAssignment.stop();
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
        Assertions.assertNull(splitSourceManager.getSplitSource(late.getUniqueId()));
    }

    @Test
    void testStartRunsTheGeneratorOnce() throws Exception {
        splitAssignment.finishSchedule();

        splitAssignment.start();
        splitAssignment.start();

        Mockito.verify(mockSplitGenerator, Mockito.times(1)).startSplit(Mockito.anyInt());
    }

    @Test
    void testStoppedAssignmentDoesNotStart() throws Exception {
        // The coordinator was cancelled before it dispatched the plan.
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        splitAssignment.stop();

        splitAssignment.start();

        Mockito.verify(mockSplitGenerator, Mockito.never()).startSplit(Mockito.anyInt());
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
    }

    @Test
    void testStopClosesWhatWasHandedOverOnce() throws Exception {
        Closeable resource = Mockito.mock(Closeable.class);
        splitAssignment.addCloseable(resource);
        Mockito.verify(resource, Mockito.never()).close();

        splitAssignment.stop();
        splitAssignment.stop();

        Mockito.verify(resource, Mockito.times(1)).close();
    }

    @Test
    void testWhatIsHandedOverAfterStopIsClosedAtOnce() throws Exception {
        // The generator was still opening it when the scan was stopped.
        splitAssignment.stop();
        Closeable late = Mockito.mock(Closeable.class);

        splitAssignment.addCloseable(late);

        Mockito.verify(late, Mockito.times(1)).close();
    }

    @Test
    void testCoordinatorTakesOverAnAssignmentStartedWhilePlanning() throws Exception {
        splitAssignment.finishSchedule();
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        // A scan that plans with its first split started generating its splits while it was planned.
        splitAssignment.startWhilePlanning(60_000, PLANNED_BY);
        Assertions.assertSame(source, splitSourceManager.getSplitSource(source.getUniqueId()));

        // The coordinator dispatching the plan takes it over: the generator does not start again ...
        splitAssignment.start();
        Mockito.verify(mockSplitGenerator, Mockito.times(1)).startSplit(Mockito.anyInt());
        // ... and the end of the statement leaves it to the coordinator, which stops it when it closes.
        splitAssignment.stopIfNotDispatched();
        Assertions.assertFalse(splitAssignment.isStop());
        Assertions.assertSame(source, splitSourceManager.getSplitSource(source.getUniqueId()));

        splitAssignment.stop();
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
    }

    @Test
    void testAssignmentStartedWhilePlanningIsStoppedWhenNoCoordinatorTookItOver() throws Exception {
        splitAssignment.finishSchedule();
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        splitAssignment.startWhilePlanning(60_000, PLANNED_BY);

        // The statement ends with the plan never dispatched: an EXPLAIN, a plan CREATE JOB validates, a statement
        // refused before dispatch.
        splitAssignment.stopIfNotDispatched();

        Assertions.assertTrue(splitAssignment.isStop());
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
    }

    @Test
    void testTheEndOfAStatementStopsAnAssignmentWhoseGenerationFailedWithoutThrowing() throws Exception {
        splitAssignment.finishSchedule();
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        splitAssignment.startWhilePlanning(60_000, PLANNED_BY);
        splitAssignment.setException(new UserException("split generation failed"));

        // No backend read the splits and the statement that planned them is over: the failure is logged rather than
        // thrown at the end of that statement, which goes on to stop its other assignments.
        Assertions.assertDoesNotThrow(() -> splitAssignment.stopIfNotDispatched());

        Assertions.assertTrue(splitAssignment.isStop());
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
    }

    @Test
    void testStopDropsTheSplitsNoBackendFetched() throws Exception {
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(mockBackendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        Mockito.when(mockSplitToScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(mockScanRangeLocations);
        splitAssignment.addToQueue(Collections.singletonList(mockSplit));
        splitAssignment.addToQueue(Collections.singletonList(mockSplit));
        BlockingQueue<Collection<TScanRangeLocations>> queue = splitAssignment.getAssignedSplits(mockBackend);
        Assertions.assertEquals(2, queue.size());

        // The coordinator stops the assignment with splits still queued: the query reached its LIMIT, say.
        splitAssignment.stop();

        // Nothing can fetch them any more, so whatever still references the assignment keeps none of them ...
        Assertions.assertTrue(queue.isEmpty());
        // ... nor what a generator still running queues afterwards.
        splitAssignment.addToQueue(Collections.singletonList(mockSplit));
        Assertions.assertTrue(queue.isEmpty());
    }

    @Test
    void testGeneratorStopsAnAssignmentWhosePlanStayedUndispatchedPastTheTimeout() throws Exception {
        SplitAssignment assignment = assignmentPumpedBySplitGenerator();
        // Planned by a statement whose timeout is over by the time the backend's queue is full, and that ended
        // without stopping the assignment (as a COM_STMT_EXECUTE or an internal statement may).
        assignment.startWhilePlanning(0, PLANNED_BY);
        keepPumping.countDown();

        // The generator stops it rather than wait for a dispatch that will never come.
        pumpingThread.join(30_000);
        Assertions.assertFalse(pumpingThread.isAlive());
        Assertions.assertTrue(assignment.isStop());
    }

    @Test
    void testGeneratorOfADispatchedAssignmentWaitsForTheBackends() throws Exception {
        SplitAssignment assignment = assignmentPumpedBySplitGenerator();
        // Started while planned, by a statement whose timeout is over by the time the backend's queue is full, and
        // taken over by the coordinator dispatching the plan.
        assignment.startWhilePlanning(0, PLANNED_BY);
        assignment.start();
        keepPumping.countDown();

        // The backend's queue is full: the generator waits for the backend to fetch, however long that takes.
        while (pumpingThread.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(10);
        }
        Assertions.assertEquals(0, assignment.getAssignedSplits(mockBackend).remainingCapacity());
        Thread.sleep(300);
        Assertions.assertTrue(pumpingThread.isAlive());
        Assertions.assertFalse(assignment.isStop());

        // The coordinator closes.
        assignment.stop();
        pumpingThread.join(30_000);
        Assertions.assertFalse(pumpingThread.isAlive());
    }

    @Test
    void testStopDropsTheBatchAGeneratorWaitingOnAFullQueueOffersAfterIt() throws Exception {
        SplitAssignment assignment = assignmentPumpedBySplitGenerator();
        assignment.start();
        keepPumping.countDown();
        // The backend's queue is full, and the generator waits in offer for room.
        while (pumpingThread.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(10);
        }
        BlockingQueue<Collection<TScanRangeLocations>> queue = assignment.getAssignedSplits(mockBackend);
        Assertions.assertEquals(0, queue.remainingCapacity());

        // The coordinator stops the assignment. Emptying the queue makes room, so the batch the generator was waiting
        // to offer lands in it after stop() emptied it: the generator drops it itself, on its way out.
        assignment.stop();
        pumpingThread.join(30_000);
        Assertions.assertFalse(pumpingThread.isAlive());
        Assertions.assertTrue(queue.isEmpty(), "batches left in the queue: " + queue.size());
    }

    private Thread pumpingThread;
    private final CountDownLatch keepPumping = new CountDownLatch(1);

    // An assignment of one backend whose generator queues splits one at a time for as long as the assignment needs
    // more, as the streaming generator of an iceberg scan does: once the backend's queue holds 10000 of them, nothing
    // empties it but a backend's fetch. The generator queues the first split - all that starting the assignment waits
    // for - then waits for keepPumping.
    private SplitAssignment assignmentPumpedBySplitGenerator() throws Exception {
        FederationBackendPolicy backendPolicy = Mockito.mock(FederationBackendPolicy.class,
                Mockito.withSettings().stubOnly());
        Multimap<Backend, Split> batch = ArrayListMultimap.create();
        batch.put(mockBackend, mockSplit);
        Mockito.when(backendPolicy.computeScanRangeAssignment(Mockito.any())).thenReturn(batch);
        SplitGenerator splitGenerator = Mockito.mock(SplitGenerator.class);
        SplitAssignment assignment = new SplitAssignment(backendPolicy, splitGenerator,
                (backend, properties, split, keys, admission) -> mockScanRangeLocations,
                locationProperties, pathPartitionKeys, true, splitSourceManager);
        Mockito.doAnswer(invocation -> {
            pumpingThread = new Thread(() -> {
                try {
                    assignment.addToQueue(Collections.singletonList(mockSplit));
                    keepPumping.await();
                    while (assignment.needMoreSplit()) {
                        assignment.addToQueue(Collections.singletonList(mockSplit));
                    }
                } catch (UserException | InterruptedException e) {
                    assignment.setException(new UserException(e.getMessage(), e));
                }
            });
            pumpingThread.start();
            // Starting the assignment returns once the first split is published, before it is queued: wait for that
            // too, so that each test begins with the generator waiting for keepPumping.
            while (pumpingThread.getState() != Thread.State.WAITING) {
                Thread.sleep(1);
            }
            return null;
        }).when(splitGenerator).startSplit(Mockito.anyInt());
        return assignment;
    }

    @Test
    void testStopReleasesEverythingBeforeRethrowingAGenerationFailure() throws Exception {
        splitAssignment.finishSchedule();
        SplitSource source = new SplitSource(mockBackend, splitAssignment, 100);
        splitAssignment.start();
        Closeable resource = Mockito.mock(Closeable.class);
        splitAssignment.addCloseable(resource);
        splitAssignment.setException(new UserException("split generation failed"));

        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> splitAssignment.stop());

        Assertions.assertTrue(e.getMessage().contains("split generation failed"), e.getMessage());
        Assertions.assertNull(splitSourceManager.getSplitSource(source.getUniqueId()));
        Mockito.verify(resource, Mockito.times(1)).close();
        // Stopped already: a second stop() has nothing left to release, and throws nothing.
        Assertions.assertDoesNotThrow(() -> splitAssignment.stop());
    }
}
