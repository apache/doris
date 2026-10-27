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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VarAttrDef;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SplitSourceTest {
    private Backend backend;
    private SplitAssignment assignment;
    private List<Split> splits;
    private List<TScanRangeLocations> ranges;

    @BeforeEach
    void setUp() throws Exception {
        backend = Mockito.mock(Backend.class);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(invocation -> {
            Multimap<Backend, Split> batch = ArrayListMultimap.create();
            for (Split split : invocation.<List<Split>>getArgument(0)) {
                batch.put(backend, split);
            }
            return batch;
        });
        splits = new ArrayList<>();
        ranges = new ArrayList<>();
        IdentityHashMap<Split, TScanRangeLocations> locations = new IdentityHashMap<>();
        for (int i = 0; i < 8; i++) {
            Split split = Mockito.mock(Split.class);
            // Keep envelopes incompatible so the queue tests still observe one envelope per split.
            TScanRangeLocations range = new TScanRangeLocations().setScanRange(new TScanRange()
                    .setExtScanRange(new TExternalScanRange().setFileScanRange(new TFileScanRange()
                            .setRanges(new ArrayList<>(Collections.singletonList(
                                    new TFileRangeDesc().setPath("file-" + i))))
                            .setParams(new TFileScanRangeParams().setSrcTupleId(i)))));
            splits.add(split);
            ranges.add(range);
            locations.put(split, range);
        }
        assignment = new SplitAssignment(policy, Mockito.mock(SplitGenerator.class),
                (be, properties, split, keys, admission) -> locations.get(split),
                Collections.emptyMap(), Collections.emptyList(), true, 3, 8);
    }

    private Object capacityMonitor() throws Exception {
        Field field = SplitAssignment.class.getDeclaredField("queueSpaceListeners");
        field.setAccessible(true);
        return field.get(assignment);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uncontendedChannelDoesNotAcquireCapacityMonitor(boolean produce) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            synchronized (capacityMonitor()) {
                Future<?> task = executor.submit(() -> {
                    if (produce) {
                        Assertions.assertEquals(1, assignment.offerBatch(backend,
                                Collections.singletonList(ranges.get(0)), 0, () -> {}));
                    } else {
                        assignment.notifyQueueSpace(backend);
                    }
                });
                task.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void capacityReleasedBeforeWaiterRegistrationIsRetried() throws Exception {
        SplitAssignment.AssignmentQueue queue = assignment.getAssignedSplits(backend);
        while (queue.remainingCapacity() > 0) {
            Assertions.assertEquals(1, queue.offer(Collections.singletonList(ranges.get(0)), 0));
        }
        AtomicInteger resumed = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Integer> offered = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                offered.set(assignment.offerBatch(backend, Collections.singletonList(ranges.get(1)), 0,
                        resumed::incrementAndGet));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        Object monitor = capacityMonitor();
        synchronized (monitor) {
            producer.start();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                java.lang.management.ThreadInfo info = ManagementFactory.getThreadMXBean()
                        .getThreadInfo(producer.getId());
                return info != null && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(monitor);
            });
            Assertions.assertNotNull(queue.poll());
            assignment.notifyQueueSpace(backend);
        }
        producer.join(5000);
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertNull(failure.get());
        Assertions.assertEquals(1, offered.get());
        Assertions.assertEquals(0, queue.remainingCapacity());
        assignment.notifyQueueSpace(backend);
        Assertions.assertEquals(0, resumed.get(), "the successful retry must remove its capacity callback");
    }

    @Test
    void partialOfferFillsRemainingCapacity() throws Exception {
        SplitAssignment.AssignmentQueue queue = assignment.getAssignedSplits(backend);
        AtomicInteger resumed = new AtomicInteger();
        Runnable callback = resumed::incrementAndGet;
        Assertions.assertEquals(3, assignment.offerBatch(backend, ranges.subList(0, 3), 0, callback));
        Assertions.assertEquals(3, assignment.offerBatch(backend, ranges.subList(3, 6), 0, callback));
        Assertions.assertEquals(6, queue.size());
        Assertions.assertEquals(2, queue.remainingCapacity());
        Assertions.assertEquals(2, assignment.offerBatch(backend, ranges.subList(0, 3), 0, callback));
        Assertions.assertEquals(0, queue.remainingCapacity());
        Assertions.assertEquals(0, assignment.offerBatch(backend, ranges.subList(0, 3), 2, callback));
        Assertions.assertEquals(ranges.get(0), queue.poll());
        assignment.notifyQueueSpace(backend);
        Assertions.assertEquals(1, resumed.get());
        Assertions.assertEquals(1, assignment.offerBatch(backend, ranges.subList(0, 3), 2, callback));
        Assertions.assertEquals(0, queue.remainingCapacity());
    }

    @Test
    void backendChannelsUseStableIdsAcrossEndpointChanges() throws Exception {
        Backend first = new Backend(11L, "127.0.0.1", 9050);
        Backend second = new Backend(12L, "127.0.0.2", 9050);
        SplitAssignment.AssignmentQueue firstQueue = assignment.getAssignedSplits(first);
        SplitAssignment.AssignmentQueue secondQueue = assignment.getAssignedSplits(second);
        AtomicInteger resumed = new AtomicInteger();
        Runnable callback = resumed::incrementAndGet;
        int capacity = firstQueue.remainingCapacity();
        for (int i = 0; i < capacity; i++) {
            Assertions.assertEquals(1, assignment.offerBatch(first,
                    Collections.singletonList(ranges.get(0)), 0, callback));
        }
        Assertions.assertEquals(0, assignment.offerBatch(first,
                Collections.singletonList(ranges.get(0)), 0, callback));
        first.setAlive(true);
        first.setBePort(9061);
        Backend sameId = new Backend(11L, "127.0.0.3", 9150);
        Assertions.assertSame(firstQueue, assignment.getAssignedSplits(sameId));
        Assertions.assertSame(firstQueue, assignment.getAssignedSplits(first));
        Assertions.assertNotSame(firstQueue, secondQueue);
        assignment.notifyQueueSpace(second);
        Assertions.assertEquals(0, resumed.get());
        Assertions.assertNotNull(firstQueue.poll());
        assignment.notifyQueueSpace(sameId);
        Assertions.assertEquals(1, resumed.get(), "capacity wakeups must find the same backend by ID");
        Assertions.assertEquals(capacity - 1, firstQueue.size());
        assignment.notifyQueueSpace(first);
        Assertions.assertEquals(1, resumed.get(), "a registered callback is removed after notification");
    }

    @Test
    void bulkDrainRespectsRequestAndResumesBackpressuredProducer() throws Exception {
        assignment.addToQueue(splits);
        AtomicInteger resumed = new AtomicInteger();
        Assertions.assertEquals(0, assignment.offerBatch(backend, Collections.singletonList(ranges.get(0)), 0,
                resumed::incrementAndGet));
        SplitSource source = new SplitSource(backend, assignment, 1000);
        Assertions.assertEquals(ranges.subList(0, 6), source.getNextBatch(6));
        Assertions.assertEquals(1, resumed.get());
        Assertions.assertEquals(2, assignment.getAssignedSplits(backend).size(),
                "splits beyond the BE request must stay in the bounded queue");
        assignment.finishSchedule();
        Assertions.assertEquals(ranges.subList(6, 8), source.getNextBatch(6));
        Assertions.assertTrue(source.getNextBatch(6).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bulkDrainObservesTerminalStatePublishedByCapacityCallback(boolean fail) throws Exception {
        assignment.addToQueue(splits);
        Assertions.assertEquals(0, assignment.offerBatch(backend, Collections.singletonList(ranges.get(0)), 0, () -> {
            if (fail) {
                assignment.setException(new UserException("bulk drain failure"));
            } else {
                assignment.stop();
            }
        }));
        SplitSource source = new SplitSource(backend, assignment, 1000);
        if (fail) {
            UserException error = Assertions.assertThrows(UserException.class, () -> source.getNextBatch(6));
            Assertions.assertTrue(error.getMessage().contains("bulk drain failure"));
        } else {
            Assertions.assertTrue(source.getNextBatch(6).isEmpty());
            Assertions.assertTrue(source.getNextBatch(6).isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void awaitedSingleSplitObservesTerminalStateBeforeReturning(boolean fail) throws Exception {
        assignment = Mockito.spy(assignment);
        CountDownLatch waiting = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            waiting.countDown();
            TScanRangeLocations split = (TScanRangeLocations) invocation.callRealMethod();
            if (fail) {
                assignment.setException(new UserException("awaited split failure"));
            } else {
                assignment.stop();
            }
            return split;
        }).when(assignment).awaitSplit(Mockito.eq(backend), Mockito.anyLong());
        SplitSource source = new SplitSource(backend, assignment, 1000, 1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<TScanRangeLocations>> fetch = executor.submit(() -> source.getNextBatch(1));
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, assignment.offerBatch(backend,
                    Collections.singletonList(ranges.get(0)), 0, () -> {}));
            if (fail) {
                ExecutionException error = Assertions.assertThrows(ExecutionException.class,
                        () -> fetch.get(5, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(UserException.class, error.getCause());
                Assertions.assertTrue(error.getCause().getMessage().contains("awaited split failure"));
            } else {
                Assertions.assertTrue(fetch.get(5, TimeUnit.SECONDS).isEmpty());
                Assertions.assertTrue(source.getNextBatch(1).isEmpty());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void initialFetchLimitIsIndependentOfProductionAndSubsequentFetches() throws Exception {
        // All eight splits are already produced without a BE request, including a partially drained batch.
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000, 2);
        Assertions.assertEquals(ranges.subList(0, 2), source.getNextBatch(100));
        Assertions.assertEquals(ranges.subList(2, 8), source.getNextBatch(100));
        Assertions.assertTrue(source.getNextBatch(100).isEmpty());
    }

    @Test
    void firstFetchNeverExceedsBackendRequest() throws Exception {
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000, 5);
        Assertions.assertEquals(ranges.subList(0, 1), source.getNextBatch(1));
        Assertions.assertEquals(ranges.subList(1, 8), source.getNextBatch(100));
    }

    @Test
    void initialLimitIsAppliedBeforeRangeCompaction() throws Exception {
        for (TScanRangeLocations range : ranges) {
            range.getScanRange().getExtScanRange().getFileScanRange().setParams(new TFileScanRangeParams());
        }
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000, 2);
        List<TScanRangeLocations> first = source.getNextBatch(100);
        Assertions.assertEquals(1, first.size());
        Assertions.assertEquals(2, first.get(0).getScanRange().getExtScanRange().getFileScanRange().getRangesSize());
        List<TScanRangeLocations> rest = source.getNextBatch(100);
        Assertions.assertEquals(1, rest.size());
        Assertions.assertEquals(6, rest.get(0).getScanRange().getExtScanRange().getFileScanRange().getRangesSize());
    }

    @Test
    void initialFetchSizeValidationAndForwarding() throws Exception {
        SessionVariable variables = new SessionVariable();
        Assertions.assertEquals(64, variables.getSplitInitialBatchSize());
        variables.checkSplitInitialBatchSize("1");
        Assertions.assertThrows(Exception.class, () -> variables.checkSplitInitialBatchSize("0"));
        Assertions.assertThrows(Exception.class, () -> variables.checkSplitInitialBatchSize("-1"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SplitSource(backend, assignment, 1000, 0));
        VarAttrDef.VarAttr annotation = SessionVariable.class
                .getField("splitInitialBatchSize").getAnnotation(VarAttrDef.VarAttr.class);
        Assertions.assertTrue(annotation.needForward());
    }

    @Test
    void finishedQueueDrainsRemainingSplitsBeforeEof() throws Exception {
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000);

        List<TScanRangeLocations> actual = new ArrayList<>();
        for (int requested : Arrays.asList(2, 1, 4, 1)) {
            List<TScanRangeLocations> batch = source.getNextBatch(requested);
            Assertions.assertEquals(requested, batch.size());
            actual.addAll(batch);
        }
        Assertions.assertEquals(ranges, actual);
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
    }

    @Test
    void producerBatchesAndConsumerCreatedQueueRespectConfiguredLimits() throws Exception {
        SplitAssignment.AssignmentQueue queue = assignment.getAssignedSplits(backend);
        Assertions.assertEquals(8, queue.remainingCapacity());
        assignment.addToQueue(splits);
        Assertions.assertEquals(8, queue.size());
        Assertions.assertEquals(ranges, Arrays.asList(queue.poll(), queue.poll(), queue.poll(), queue.poll(),
                queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        Assertions.assertEquals(8, queue.remainingCapacity());
    }

    @Test
    void fullQueueResumesAfterConsumerFetch() throws Exception {
        assignment.addToQueue(splits);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                started.countDown();
                assignment.addToQueue(Collections.singletonList(splits.get(0)));
                return null;
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> producer.get(200, TimeUnit.MILLISECONDS));
            SplitSource source = new SplitSource(backend, assignment, 1000);
            Assertions.assertEquals(ranges.subList(0, 1), source.getNextBatch(1));
            producer.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(8, assignment.getAssignedSplits(backend).size());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void availablePartialBatchDoesNotWaitForProducer() throws Exception {
        assignment.addToQueue(splits.subList(0, 1));
        SplitSource source = new SplitSource(backend, assignment, 5000);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                Assertions.assertEquals(ranges.subList(0, 1), source.getNextBatch(100)));
    }

    @Test
    void timeoutIsAnErrorRatherThanEof() {
        SplitSource source = new SplitSource(backend, assignment, 20);
        UserException failure = Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1));
        Assertions.assertTrue(failure.getMessage().contains("Timeout"));
    }

    @Test
    void errorIsCheckedWhilePendingSplitsRemain() throws Exception {
        assignment.addToQueue(splits.subList(0, 3));
        SplitSource source = new SplitSource(backend, assignment, 1000);
        source.getNextBatch(1);
        UserException failure = new UserException("failed to generate splits");
        assignment.setException(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1)));
    }

    @Test
    void concurrentRequestsDoNotDuplicateOrLosePendingSplits() throws Exception {
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<TScanRangeLocations>> first = executor.submit(() -> source.getNextBatch(2));
            Future<List<TScanRangeLocations>> second = executor.submit(() -> source.getNextBatch(2));
            List<TScanRangeLocations> actual = new ArrayList<>(first.get(5, TimeUnit.SECONDS));
            actual.addAll(second.get(5, TimeUnit.SECONDS));
            actual.addAll(source.getNextBatch(8));
            Set<TScanRangeLocations> unique = Collections.newSetFromMap(new IdentityHashMap<>());
            unique.addAll(actual);
            Assertions.assertEquals(8, actual.size());
            Assertions.assertEquals(8, unique.size());
            Assertions.assertTrue(unique.containsAll(ranges));
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationUnblocksProducerAndClosesLateResources() throws Exception {
        assignment.addToQueue(splits);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                assignment.addToQueue(Collections.singletonList(splits.get(0)));
                return null;
            });
            Assertions.assertThrows(TimeoutException.class, () -> producer.get(200, TimeUnit.MILLISECONDS));
            assignment.stop();
            producer.get(5, TimeUnit.SECONDS);
            CountDownLatch closed = new CountDownLatch(1);
            assignment.addCloseable(closed::countDown);
            Assertions.assertEquals(0, closed.getCount());
            Assertions.assertTrue(new SplitSource(backend, assignment, 1000).getNextBatch(1).isEmpty());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"finish", "stop", "failure", "data"})
    void queueStateChangesWakeBlockedConsumer(String change) throws Exception {
        SplitSource source = new SplitSource(backend, assignment, 30000);
        AtomicReference<Thread> waiting = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        UserException failure = new UserException("manifest read failed");
        try {
            Future<List<TScanRangeLocations>> result = executor.submit(() -> {
                waiting.set(Thread.currentThread());
                started.countDown();
                return source.getNextBatch(1);
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            awaitConsumerWait(waiting.get());
            switch (change) {
                case "finish":
                    assignment.finishSchedule();
                    break;
                case "stop":
                    assignment.stop();
                    break;
                case "failure":
                    assignment.setException(failure);
                    break;
                case "data":
                    assignment.addToQueue(splits.subList(0, 1));
                    break;
                default:
                    throw new AssertionError(change);
            }
            if ("failure".equals(change)) {
                ExecutionException actual = Assertions.assertThrows(ExecutionException.class,
                        () -> result.get(5, TimeUnit.SECONDS));
                Assertions.assertSame(failure, actual.getCause());
            } else {
                Assertions.assertEquals("data".equals(change) ? ranges.subList(0, 1) : Collections.emptyList(),
                        result.get(5, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void backendWaitChannelsAreIndependentAndCompletionWakesBoth() throws Exception {
        Backend otherBackend = new Backend(2L, "127.0.0.2", 9050);
        SplitSource first = new SplitSource(backend, assignment, 30000);
        SplitSource second = new SplitSource(otherBackend, assignment, 30000);
        AtomicReference<Thread> firstThread = new AtomicReference<>();
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<TScanRangeLocations>> firstResult = executor.submit(() -> {
                firstThread.set(Thread.currentThread());
                started.countDown();
                return first.getNextBatch(1);
            });
            Future<List<TScanRangeLocations>> secondResult = executor.submit(() -> {
                secondThread.set(Thread.currentThread());
                started.countDown();
                return second.getNextBatch(1);
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            awaitConsumerWait(firstThread.get());
            awaitConsumerWait(secondThread.get());
            Assertions.assertEquals(1, assignment.offerBatch(backend, ranges.subList(0, 1), 0, () -> {}));
            Assertions.assertEquals(ranges.subList(0, 1), firstResult.get(5, TimeUnit.SECONDS));
            Assertions.assertFalse(secondResult.isDone());
            assignment.finishSchedule();
            Assertions.assertTrue(secondResult.get(5, TimeUnit.SECONDS).isEmpty());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void completionRacingConsumerRegistrationDoesNotLoseWakeup() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 200; round++) {
                SplitAssignment current = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class), Mockito.mock(SplitGenerator.class),
                        (be, properties, split, keys, admission) -> ranges.get(0),
                        Collections.emptyMap(), Collections.emptyList(), true, 1, 1);
                CountDownLatch start = new CountDownLatch(1);
                Future<List<TScanRangeLocations>> result = executor.submit(() -> {
                    start.await();
                    return new SplitSource(backend, current, 30000).getNextBatch(1);
                });
                Future<?> finished = executor.submit(() -> {
                    start.await();
                    current.finishSchedule();
                    return null;
                });
                start.countDown();
                finished.get(5, TimeUnit.SECONDS);
                Assertions.assertTrue(result.get(5, TimeUnit.SECONDS).isEmpty());
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedProducerPublishesFailureToWaitingConsumer() throws Exception {
        assignment.addToQueue(splits);
        SplitSource source = new SplitSource(new Backend(2L, "127.0.0.2", 9050), assignment, 30000);
        AtomicReference<Thread> producerThread = new AtomicReference<>();
        AtomicReference<Thread> consumerThread = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> producer = executor.submit(() -> {
                producerThread.set(Thread.currentThread());
                started.countDown();
                assignment.addToQueue(splits.subList(0, 1));
                return null;
            });
            Future<?> consumer = executor.submit(() -> {
                consumerThread.set(Thread.currentThread());
                started.countDown();
                return source.getNextBatch(1);
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            awaitConsumerWait(producerThread.get());
            awaitConsumerWait(consumerThread.get());
            producerThread.get().interrupt();
            producer.get(5, TimeUnit.SECONDS);
            ExecutionException actual = Assertions.assertThrows(ExecutionException.class,
                    () -> consumer.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(UserException.class, actual.getCause());
            Assertions.assertTrue(actual.getCause().getMessage().contains("interrupted"));
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationWakesConsumerBeforeClosingResources() throws Exception {
        java.util.concurrent.CompletableFuture<Void> allowClose = new java.util.concurrent.CompletableFuture<>();
        CountDownLatch closing = new CountDownLatch(1);
        assignment.addCloseable(() -> {
            closing.countDown();
            allowClose.join();
        });
        AtomicReference<Thread> consumerThread = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<TScanRangeLocations>> consumer = executor.submit(() -> {
                consumerThread.set(Thread.currentThread());
                started.countDown();
                return new SplitSource(backend, assignment, 30000).getNextBatch(1);
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            awaitConsumerWait(consumerThread.get());
            Future<?> stopping = executor.submit(assignment::stop);
            Assertions.assertTrue(closing.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(consumer.get(5, TimeUnit.SECONDS).isEmpty());
            Assertions.assertFalse(stopping.isDone());
            allowClose.complete(null);
            stopping.get(5, TimeUnit.SECONDS);
        } finally {
            allowClose.complete(null);
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void awaitConsumerWait(Thread thread) {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            while (thread.getState() != Thread.State.TIMED_WAITING) {
                Assertions.assertTrue(thread.isAlive());
                Thread.yield();
            }
        });
    }

    @Test
    void invalidRequestSizeDoesNotReturnFalseEof() {
        SplitSource source = new SplitSource(backend, assignment, 1000);
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.getNextBatch(0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.getNextBatch(-1));
    }

    @Test
    void demandHintStartsWithTheSecondRequestAndStopsOnCancellation() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        assignment.setMoreSplitsRequestedCallback(requests::incrementAndGet);
        assignment.addToQueue(splits);
        SplitSource source = new SplitSource(backend, assignment, 1000);
        source.getNextBatch(2);
        Assertions.assertEquals(0, requests.get());
        source.getNextBatch(2);
        Assertions.assertEquals(1, requests.get());
        assignment.stop();
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    void compactedResponsesPreservePendingSplitsAndRequestLimits() throws Exception {
        ranges.forEach(range -> range.getScanRange().getExtScanRange().getFileScanRange()
                .setParams(new TFileScanRangeParams().setSrcTupleId(1)));
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000);
        List<TFileRangeDesc> actual = new ArrayList<>();
        for (int requested : Arrays.asList(2, 3, 3)) {
            List<TScanRangeLocations> response = source.getNextBatch(requested);
            Assertions.assertEquals(1, response.size());
            List<TFileRangeDesc> files = response.get(0).getScanRange().getExtScanRange()
                    .getFileScanRange().getRanges();
            Assertions.assertEquals(requested, files.size());
            actual.addAll(files);
        }
        List<TFileRangeDesc> expected = new ArrayList<>();
        ranges.forEach(range -> expected.addAll(range.getScanRange().getExtScanRange()
                .getFileScanRange().getRanges()));
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(8, expected.size(), "compaction must not mutate queued envelopes");
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
    }
}
