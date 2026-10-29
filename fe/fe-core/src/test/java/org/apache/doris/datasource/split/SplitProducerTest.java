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
import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

class SplitProducerTest {
    private ConnectorTaskScheduler scheduler(FairTaskExecutor.Group group) {
        return new ConnectorTaskScheduler() {
            @Override
            public boolean tryExecute(Runnable task, java.util.function.Consumer<Throwable> onFailure,
                    Runnable onSpace) {
                return group.tryExecute(task, onFailure, onSpace);
            }

            @Override
            public boolean tryExecuteContinuation(Runnable task, java.util.function.Consumer<Throwable> onFailure,
                    Runnable onSpace) {
                return group.tryExecuteContinuation(task, onFailure, onSpace);
            }

            @Override
            public void removeSpaceListener(Runnable onSpace) {
                group.removeSpaceListener(onSpace);
            }

            @Override
            public void execute(Runnable task) {
                group.execute(task);
            }
        };
    }

    @Test
    void producersFillConfiguredBufferBeforeAnyBackendFetch() throws Exception {
        Fixture fixture = new Fixture(1, 3);
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 0, 2, 4, 6, 8);
        fixture.start(executor, source);
        executor.runUntilIdle();
        Assertions.assertEquals(3, fixture.assignment.getAssignedSplits(fixture.backend).size());
        Assertions.assertEquals(0, fixture.completions.get());
        SplitSource consumer = fixture.source();
        List<Long> actual = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            actual.addAll(identities(consumer.getNextBatch(1)));
            executor.runUntilIdle();
        }
        Assertions.assertEquals(Arrays.asList(0L, 2L, 4L, 6L, 8L), actual);
        Assertions.assertTrue(consumer.getNextBatch(1).isEmpty());
        Assertions.assertEquals(1, fixture.completions.get());
    }

    @Test
    void partiallyOfferedBatchResumesWithoutLosingOrRepeatingSplits() throws Exception {
        Fixture fixture = new Fixture(3, 3);
        fixture.assignment.addToQueue(Arrays.asList(split(0), split(2)));
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(3, 4, 6, 8);
        fixture.start(executor, source);
        executor.runUntilIdle();

        Assertions.assertEquals(3, fixture.assignment.getAssignedSplits(fixture.backend).size());
        Assertions.assertEquals(0, fixture.completions.get());
        SplitSource consumer = fixture.source();
        Assertions.assertEquals(Arrays.asList(0L, 2L, 4L), identities(consumer.getNextBatch(3)));
        executor.runUntilIdle();
        Assertions.assertEquals(Arrays.asList(6L, 8L), identities(consumer.getNextBatch(3)));
        Assertions.assertTrue(consumer.getNextBatch(1).isEmpty());
        Assertions.assertEquals(1, fixture.conversions.get(4L));
        Assertions.assertEquals(1, fixture.conversions.get(6L));
        Assertions.assertEquals(1, fixture.conversions.get(8L));
        Assertions.assertEquals(1, fixture.completions.get());
    }

    @Test
    void queryCloseReleasesAProducerWaitingForTaskQueueCapacity() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        RecordingSource source = new RecordingSource(1, 0, 2, 4);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (FairTaskExecutor pool = new FairTaskExecutor(1, Thread::new)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            group.execute(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            try {
                Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
                group.execute(() -> { });
                fixture.start(scheduler(group), source);
                fixture.assignment.stop();
                group.close();
                Assertions.assertTrue(source.closed.await(5, TimeUnit.SECONDS));
                Assertions.assertEquals(1, source.closeCalls.get());
                Assertions.assertEquals(1, fixture.completions.get());
            } finally {
                release.countDown();
                pool.close();
                Assertions.assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void fairExecutorRetainsBackendBackpressureAndProducesEverySplitOnce() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        RecordingSource source = new RecordingSource(1, 0, 2, 4, 6, 8);
        try (FairTaskExecutor pool = new FairTaskExecutor(2, Thread::new)) {
            fixture.start(scheduler(pool.newGroup(1)), source);
            Assertions.assertEquals(Arrays.asList(0L, 2L, 4L, 6L, 8L), drain(fixture.source(), 1));
            Assertions.assertTrue(source.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, fixture.completions.get());
            pool.close();
            Assertions.assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            fixture.assignment.stop();
        }
    }

    @Test
    void fairContinuationYieldsToAnotherQueryAndPreservesAllSplits() throws Exception {
        Fixture first = new Fixture(1, 512);
        Fixture second = new Fixture(1, 512);
        long[] ids = new long[200];
        List<Long> expected = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i * 2L;
            expected.add(ids[i]);
        }
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordingSource firstSource = new RecordingSource(1, ids) {
            boolean firstRead = true;

            @Override
            public List<Split> getNextBatch() throws IOException {
                if (firstRead) {
                    firstRead = false;
                    started.countDown();
                    try {
                        Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
                return super.getNextBatch();
            }
        };
        AtomicInteger firstReadsWhenSecondStarts = new AtomicInteger();
        RecordingSource secondSource = new RecordingSource(1, 1000) {
            @Override
            public List<Split> getNextBatch() throws IOException {
                firstReadsWhenSecondStarts.compareAndSet(0, firstSource.next);
                return super.getNextBatch();
            }
        };
        try (FairTaskExecutor pool = new FairTaskExecutor(1, Thread::new)) {
            first.start(scheduler(pool.newGroup(1)), firstSource, () -> 0L);
            try {
                Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
                second.start(scheduler(pool.newGroup(1)), secondSource);
            } finally {
                release.countDown();
            }
            Assertions.assertTrue(firstSource.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(secondSource.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(SplitProducer.MAX_BATCHES_PER_TASK, firstReadsWhenSecondStarts.get());
            Assertions.assertEquals(expected, drain(first.source(), 1));
            Assertions.assertEquals(Collections.singletonList(1000L), drain(second.source(), 1));
            Assertions.assertEquals(1, firstSource.closeCalls.get());
            Assertions.assertEquals(1, secondSource.closeCalls.get());
        } finally {
            first.assignment.stop();
            second.assignment.stop();
        }
    }

    @Test
    void twoScansInitializeOnOneWorkerBeforeAnyConsumerStarts() throws Exception {
        Fixture first = new Fixture(1, 1);
        Fixture second = new Fixture(1, 1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService planner = Executors.newSingleThreadExecutor();
        RecordingSource firstSource = new RecordingSource(1, 0, 2, 4, 6, 8);
        RecordingSource secondSource = new RecordingSource(1, 10, 12, 14, 16, 18);
        Mockito.doAnswer(call -> {
            first.start(worker, firstSource);
            return null;
        }).when(first.generator).startSplit(Mockito.anyInt());
        Mockito.doAnswer(call -> {
            second.start(worker, secondSource);
            return null;
        }).when(second.generator).startSplit(Mockito.anyInt());
        try {
            Future<?> initialized = planner.submit(() -> {
                first.assignment.init(1000);
                second.assignment.init(1000);
                return null;
            });
            initialized.get(5, TimeUnit.SECONDS);
            worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, first.assignment.getAssignedSplits(first.backend).size());
            Assertions.assertEquals(1, second.assignment.getAssignedSplits(second.backend).size());
            Assertions.assertEquals(Arrays.asList(0L, 2L, 4L, 6L, 8L), drain(first.source(), 2));
            Assertions.assertEquals(Arrays.asList(10L, 12L, 14L, 16L, 18L), drain(second.source(), 1));
            Assertions.assertTrue(firstSource.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(secondSource.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, firstSource.closeCalls.get());
            Assertions.assertEquals(1, secondSource.closeCalls.get());
        } finally {
            first.assignment.stop();
            second.assignment.stop();
            planner.shutdownNow();
            worker.shutdownNow();
            Assertions.assertTrue(planner.awaitTermination(5, TimeUnit.SECONDS));
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void partialMultiBackendSubmissionDoesNotReplayOrReassignTheBatch() throws Exception {
        Fixture fixture = new Fixture(2, 2);
        ManualExecutor executor = new ManualExecutor();
        fixture.assignment.addToQueue(Arrays.asList(split(1), split(5)));
        RecordingSource source = new RecordingSource(2, 0, 3);
        fixture.start(executor, source);
        executor.runNext();
        Assertions.assertTrue(executor.tasks.isEmpty());
        Assertions.assertEquals(0, fixture.completions.get());
        Assertions.assertEquals(Collections.singletonList(0L), identities(fixture.source().getNextBatch(1)));
        Assertions.assertTrue(executor.tasks.isEmpty());
        SplitSource other = new SplitSource(fixture.otherBackend, fixture.assignment, 1000);
        Assertions.assertEquals(Collections.singletonList(1L), identities(other.getNextBatch(1)));
        executor.runUntilIdle();
        Assertions.assertEquals(Collections.singletonList(5L), identities(other.getNextBatch(1)));
        Assertions.assertEquals(Collections.singletonList(3L), identities(other.getNextBatch(1)));
        Assertions.assertTrue(other.getNextBatch(1).isEmpty());
        Assertions.assertTrue(fixture.source().getNextBatch(1).isEmpty());
        Mockito.verify(fixture.policy, Mockito.times(2)).computeScanRangeAssignment(Mockito.any());
        Assertions.assertEquals(1, fixture.conversions.get(0L));
        Assertions.assertEquals(1, fixture.conversions.get(3L));
        Assertions.assertEquals(1, fixture.completions.get());
        Assertions.assertEquals(1, source.closeCalls.get());
    }

    @Test
    void capacityReleasedBeforeTheProducerSuspendsIsNotLost() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        fixture.assignment = Mockito.spy(fixture.assignment);
        fixture.assignment.addToQueue(Collections.singletonList(split(0)));
        SplitSource consumer = fixture.source();
        Mockito.doAnswer(call -> {
            int accepted = (int) call.callRealMethod();
            if (accepted == 0) {
                Assertions.assertEquals(Collections.singletonList(0L), identities(consumer.getNextBatch(1)));
            }
            return accepted;
        }).when(fixture.assignment).offerBatch(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 2);
        fixture.start(executor, source);
        executor.runNext();
        Assertions.assertEquals(1, executor.tasks.size());
        executor.runUntilIdle();
        Assertions.assertEquals(Collections.singletonList(2L), identities(consumer.getNextBatch(1)));
        Assertions.assertTrue(consumer.getNextBatch(1).isEmpty());
        Assertions.assertEquals(1, source.closeCalls.get());
    }

    @Test
    void cancellationClosesASuspendedProducerWithoutAConsumer() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        fixture.assignment.addToQueue(Collections.singletonList(split(0)));
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 2);
        fixture.start(executor, source);
        executor.runNext();
        Assertions.assertTrue(executor.tasks.isEmpty());
        Assertions.assertEquals(0, source.closeCalls.get());
        fixture.assignment.stop();
        executor.runUntilIdle();
        Assertions.assertEquals(1, source.closeCalls.get());
        Assertions.assertEquals(1, fixture.completions.get());
        Assertions.assertTrue(fixture.source().getNextBatch(1).isEmpty());
        fixture.assignment.stop();
        Assertions.assertTrue(executor.tasks.isEmpty());
    }

    @Test
    void cancellationDoesNotCloseTheSourceDuringAnActiveRead() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordingSource source = new RecordingSource(1, 0) {
            @Override
            public List<Split> getNextBatch() throws IOException {
                reading.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                return super.getNextBatch();
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            fixture.start(executor, source);
            Assertions.assertTrue(reading.await(5, TimeUnit.SECONDS));
            fixture.assignment.stop();
            Assertions.assertEquals(0, source.closeCalls.get());
            release.countDown();
            Assertions.assertTrue(source.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, source.closeCalls.get());
            Assertions.assertEquals(1, fixture.completions.get());
            Mockito.verify(fixture.policy, Mockito.never()).computeScanRangeAssignment(Mockito.any());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void readThatOutlivesItsStatementLoadsNothingIntoTheClosedScope() throws Exception {
        // Stopping the scan only flags a producer that is already inside getNextBatch, and the statement then
        // closes its connector scope. When that read goes on to resolve its table (Iceberg's streamSplits borrows
        // one through the scope), the closed scope refuses it, so it borrows nothing that would never be returned.
        // MUTATION: a scope that still loads after closeAll -> loads==1 -> red.
        Fixture fixture = new Fixture(1, 1);
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordingSource source = new RecordingSource(1, 0) {
            @Override
            public List<Split> getNextBatch() throws IOException {
                reading.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                scope.computeIfAbsent("iceberg:table:1:db:tbl", () -> {
                    loads.incrementAndGet();
                    return (AutoCloseable) () -> { };
                });
                return super.getNextBatch();
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            fixture.start(executor, source);
            Assertions.assertTrue(reading.await(5, TimeUnit.SECONDS));
            fixture.assignment.stop();
            scope.closeAll();
            release.countDown();
            Assertions.assertTrue(source.closed.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(0, loads.get(), "the closed scope refuses the late read's load");
            Assertions.assertEquals(1, source.closeCalls.get());
            Assertions.assertEquals(1, fixture.completions.get());
            Mockito.verify(fixture.policy, Mockito.never()).computeScanRangeAssignment(Mockito.any());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectedCancellationCleanupStillClosesWithoutAddingAnError() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        fixture.assignment.addToQueue(Collections.singletonList(split(0)));
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 2);
        fixture.start(executor, source);
        executor.runNext();
        executor.reject = true;
        fixture.assignment.stop();
        Assertions.assertEquals(1, source.closeCalls.get());
        Assertions.assertEquals(1, fixture.completions.get());
        Assertions.assertTrue(fixture.source().getNextBatch(1).isEmpty());
    }

    @Test
    void rejectedInitialSubmissionPublishesFailureAndClosesTheSource() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        ManualExecutor executor = new ManualExecutor();
        executor.reject = true;
        RecordingSource source = new RecordingSource(1, 0);
        fixture.start(executor, source);
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> fixture.assignment.getAssignedSplits(fixture.backend));
        Assertions.assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        Assertions.assertEquals(1, source.closeCalls.get());
        Assertions.assertEquals(1, fixture.completions.get());
    }

    @Test
    void rejectedResumptionClosesThePendingSource() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        fixture.assignment.addToQueue(Collections.singletonList(split(0)));
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 2);
        fixture.start(executor, source);
        executor.runNext();
        executor.reject = true;
        UserException failure = Assertions.assertThrows(UserException.class, () -> fixture.source().getNextBatch(1));
        Assertions.assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        Assertions.assertEquals(1, source.closeCalls.get());
        Assertions.assertEquals(1, fixture.completions.get());
    }

    @Test
    void sourceFailureWakesOtherSuspendedProducers() throws Exception {
        Fixture fixture = new Fixture(1, 1);
        fixture.assignment.addToQueue(Collections.singletonList(split(0)));
        ManualExecutor executor = new ManualExecutor();
        RecordingSource pending = new RecordingSource(1, 2);
        fixture.start(executor, pending);
        executor.runNext();
        RecordingSource failed = new RecordingSource(1) {
            @Override
            public List<Split> getNextBatch() throws IOException {
                throw new IOException("listing failed");
            }
        };
        fixture.start(executor, failed);
        executor.runUntilIdle();
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> fixture.assignment.getAssignedSplits(fixture.backend));
        Assertions.assertInstanceOf(IOException.class, failure.getCause());
        Assertions.assertEquals(1, pending.closeCalls.get());
        Assertions.assertEquals(1, failed.closeCalls.get());
        Assertions.assertEquals(2, fixture.completions.get());
    }

    @Test
    void failureRacingWaiterRegistrationDoesNotLeaveTheSourceSuspended() throws Exception {
        for (boolean beforeOffer : Arrays.asList(true, false)) {
            Fixture fixture = new Fixture(1, 1);
            fixture.assignment = Mockito.spy(fixture.assignment);
            fixture.assignment.addToQueue(Collections.singletonList(split(0)));
            UserException expected = new UserException("another producer failed");
            Mockito.doAnswer(call -> {
                if (beforeOffer) {
                    fixture.assignment.setException(expected);
                }
                int accepted = (int) call.callRealMethod();
                if (!beforeOffer) {
                    fixture.assignment.setException(expected);
                }
                return accepted;
            }).when(fixture.assignment).offerBatch(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
            ManualExecutor executor = new ManualExecutor();
            RecordingSource source = new RecordingSource(1, 2);
            fixture.start(executor, source);
            executor.runUntilIdle();
            Assertions.assertSame(expected, Assertions.assertThrows(UserException.class,
                    () -> fixture.assignment.getAssignedSplits(fixture.backend)));
            Assertions.assertEquals(1, source.closeCalls.get());
            Assertions.assertEquals(1, fixture.completions.get());
        }
    }

    @Test
    void productiveTaskYieldsToAnotherQueryBeforeFinishing() throws Exception {
        Fixture first = new Fixture(1, 1000);
        Fixture second = new Fixture(1, 1000);
        ManualExecutor executor = new ManualExecutor();
        long[] ids = new long[200];
        List<Long> expected = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i * 2L;
            expected.add(ids[i]);
        }
        RecordingSource source = new RecordingSource(1, ids);
        first.start(executor, source, () -> 0L);
        second.start(executor, new RecordingSource(1, 1000));
        executor.runNext();
        Assertions.assertEquals(SplitProducer.MAX_BATCHES_PER_TASK, source.next);
        Assertions.assertEquals(0, first.completions.get());
        executor.runNext();
        Assertions.assertEquals(1, second.assignment.getAssignedSplits(second.backend).size());
        executor.runUntilIdle();
        Assertions.assertEquals(expected, drain(first.source(), 1));
        Assertions.assertEquals(1, source.closeCalls.get());
    }

    @Test
    void slowBatchYieldsEvenBeforeBatchBudgetIsExhausted() throws Exception {
        Fixture fixture = new Fixture(1, 100);
        ManualExecutor executor = new ManualExecutor();
        AtomicLong clock = new AtomicLong();
        RecordingSource source = new RecordingSource(1, 0, 2) {
            @Override
            public List<Split> getNextBatch() throws IOException {
                clock.addAndGet(SplitProducer.MAX_TASK_NANOS);
                return super.getNextBatch();
            }
        };
        fixture.start(executor, source, clock::get);
        executor.runNext();
        Assertions.assertEquals(1, source.next);
        Assertions.assertEquals(1, executor.tasks.size());
        fixture.assignment.stop();
        executor.runUntilIdle();
        Assertions.assertEquals(1, source.next);
        Assertions.assertEquals(1, source.closeCalls.get());
    }

    @Test
    void cancellationBetweenInlineBatchesStopsFurtherReads() throws Exception {
        Fixture fixture = new Fixture(1, 100);
        fixture.assignment = Mockito.spy(fixture.assignment);
        Mockito.doAnswer(call -> {
            int result = (int) call.callRealMethod();
            fixture.assignment.stop();
            return result;
        }).when(fixture.assignment).offerBatch(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
        ManualExecutor executor = new ManualExecutor();
        RecordingSource source = new RecordingSource(1, 0, 2, 4);
        fixture.start(executor, source);
        executor.runUntilIdle();
        Assertions.assertEquals(1, source.next);
        Assertions.assertEquals(1, source.closeCalls.get());
        Assertions.assertEquals(1, fixture.completions.get());
    }

    private static List<Long> drain(SplitSource source, int batchSize) throws Exception {
        List<Long> result = new ArrayList<>();
        while (true) {
            List<TScanRangeLocations> batch = source.getNextBatch(batchSize);
            Assertions.assertTrue(batch.size() <= batchSize);
            if (batch.isEmpty()) {
                return result;
            }
            result.addAll(identities(batch));
        }
    }

    private static List<Long> identities(List<TScanRangeLocations> ranges) {
        List<Long> result = new ArrayList<>();
        for (TScanRangeLocations range : ranges) {
            result.add(range.getLocations().get(0).getBackendId());
        }
        return result;
    }

    private static Split split(long identity) {
        return new FileSplit(LocationPath.of("s3://bucket/file.parquet"), identity, 1, 100, 0,
                null, Collections.emptyList());
    }

    private static class RecordingSource implements SplitProducer.BatchSource {
        private final List<Split> splits = new ArrayList<>();
        private final int batchSize;
        private int next;
        final AtomicInteger closeCalls = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);

        RecordingSource(int batchSize, long... identities) {
            this.batchSize = batchSize;
            for (long identity : identities) {
                splits.add(split(identity));
            }
        }

        @Override
        public List<Split> getNextBatch() throws IOException {
            int end = Math.min(splits.size(), next + batchSize);
            List<Split> batch = splits.subList(next, end);
            next = end;
            return batch;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            closed.countDown();
        }
    }

    private static class ManualExecutor implements Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();
        boolean reject;

        @Override
        public void execute(Runnable task) {
            if (reject) {
                throw new RejectedExecutionException("executor stopped");
            }
            tasks.add(task);
        }

        void runNext() {
            Assertions.assertFalse(tasks.isEmpty());
            tasks.remove().run();
        }

        void runUntilIdle() {
            for (int i = 0; i < 100 && !tasks.isEmpty(); i++) {
                runNext();
            }
            Assertions.assertTrue(tasks.isEmpty(), "Producer must yield on a full queue or terminate at EOF");
        }
    }

    private static class Fixture {
        final Backend backend = new Backend(0, "127.0.0.1", 9050);
        final Backend otherBackend = new Backend(1, "127.0.0.1", 9051);
        final SplitGenerator generator = Mockito.mock(SplitGenerator.class);
        final FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        final AtomicInteger completions = new AtomicInteger();
        final Map<Long, Integer> conversions = new HashMap<>();
        SplitAssignment assignment;

        Fixture(int batchSize, int queueCapacity) throws Exception {
            Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(call -> {
                Multimap<Backend, Split> result = LinkedListMultimap.create();
                for (Split split : call.<List<Split>>getArgument(0)) {
                    result.put(split.getStart() % 2 == 0 ? backend : otherBackend, split);
                }
                return result;
            });
            assignment = new SplitAssignment(policy, generator, (be, properties, split, keys, admission) -> {
                conversions.merge(split.getStart(), 1, Integer::sum);
                TFileScanRange range = new TFileScanRange().setParams(new TFileScanRangeParams())
                        .setRanges(Collections.singletonList(new TFileRangeDesc()));
                return new TScanRangeLocations().setScanRange(new TScanRange().setExtScanRange(
                        new TExternalScanRange().setFileScanRange(range)))
                        .setLocations(Collections.singletonList(
                                new TScanRangeLocation().setBackendId(split.getStart())));
            }, Collections.emptyMap(), Collections.emptyList(), true, batchSize, queueCapacity);
        }

        void start(Executor executor, RecordingSource source) {
            start(executor, source, System::nanoTime);
        }

        void start(Executor executor, RecordingSource source, LongSupplier nanoTime) {
            new SplitProducer(assignment, executor, source, failure -> {
                if (failure != null) {
                    assignment.setException(new UserException(failure.getMessage(), failure));
                }
                completions.incrementAndGet();
                assignment.finishSchedule();
            }, nanoTime).start();
        }

        SplitSource source() {
            return new SplitSource(backend, assignment, 1000);
        }
    }
}
