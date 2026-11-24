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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class SplitProducerTest {
    @Test
    void rejectedSubmissionPublishesFailureAndReleasesItsSource() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        SplitAssignment assignment = assignment(new SplitGenerator() {});
        new SplitProducer(task -> {
            throw new RejectedExecutionException("split executor full");
        }, assignment, () -> Assertions.fail("rejected task ran"), closes::incrementAndGet,
                Runnable::run).submit();
        assignment.getTerminationFuture().get(5, TimeUnit.SECONDS);
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> assignment.getAssignedSplits(Mockito.mock(org.apache.doris.system.Backend.class)));
        Assertions.assertTrue(failure.getMessage().contains("split executor full"));
        Assertions.assertEquals(1, closes.get());
    }

    @Test
    void firstSplitDeadlineStopsQueuedWorkAndReportsRealProgress() throws Exception {
        ExecutorService workers = Executors.newSingleThreadExecutor();
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<SplitAssignment> owner = new AtomicReference<>();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        SplitAssignment assignment = assignment(new SplitGenerator() {
            @Override
            public void startSplit(int backends) {
                SplitScanContext context = owner.get().getScanContext();
                context.setTotalPartitions(7);
                context.partitionProcessed();
                context.filesEnumerated(3);
                new SplitProducer(workers, owner.get(), reads::incrementAndGet,
                        closes::incrementAndGet, Runnable::run).submit();
            }
        });
        owner.set(assignment);
        try {
            workers.execute(() -> {
                occupied.countDown();
                await(release);
            });
            await(occupied);
            UserException timeout = Assertions.assertThrows(UserException.class, () -> assignment.init(1));
            Assertions.assertTrue(timeout.getMessage().contains("partitions_processed=1/7"));
            Assertions.assertTrue(timeout.getMessage().contains("files_enumerated=3"));
            Assertions.assertTrue(timeout.getMessage().contains("splits_generated=0"));
            assignment.getTerminationFuture().get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(assignment.isStop());
            Assertions.assertEquals(0, reads.get());
            Assertions.assertEquals(1, closes.get());
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void cancellationStopsSplittingInsideOneLargeFile() {
        for (long specifiedSize : new long[] {0, 1}) {
            SplitScanContext context = new SplitScanContext();
            AtomicInteger generated = new AtomicInteger();
            SplitCreator creator = (path, offset, length, fileLength, splitSize, time, hosts, partitions) -> {
                if (generated.incrementAndGet() == 3) {
                    context.cancel();
                }
                return new FileSplit(path, offset, length, fileLength, time, hosts, partitions);
            };
            FileSplitter splitter = new FileSplitter(1, 1, 0);
            Assertions.assertThrows(java.util.concurrent.CancellationException.class,
                    () -> splitter.splitFile(org.apache.doris.common.util.LocationPath.of("file:///tmp/large.parquet"),
                            specifiedSize, null, 1_000_000, 0, true, Collections.emptyList(), creator,
                            context::checkCancelled));
            Assertions.assertEquals(3, generated.get());
        }
    }

    @Test
    void producerFailureWhileDrainingABatchCannotBecomeSuccessfulEof() throws Exception {
        SplitAssignment assignment = assignment(new SplitGenerator() {});
        org.apache.doris.system.Backend backend = Mockito.mock(org.apache.doris.system.Backend.class);
        CountDownLatch draining = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        java.util.Collection<org.apache.doris.thrift.TScanRangeLocations> batch = new java.util.AbstractCollection<org.apache.doris.thrift.TScanRangeLocations>() {
            @Override
            public java.util.Iterator<org.apache.doris.thrift.TScanRangeLocations> iterator() {
                draining.countDown();
                await(release);
                return Collections.singletonList(new org.apache.doris.thrift.TScanRangeLocations()).iterator();
            }

            @Override
            public int size() {
                return 1;
            }
        };
        assignment.getAssignedSplits(backend).add(batch);
        SplitSource source = new SplitSource(backend, assignment, 1000);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<?> result = consumer.submit(() -> source.getNextBatch(1));
            await(draining);
            assignment.setException(new UserException("file listing failed"));
            release.countDown();
            java.util.concurrent.ExecutionException error = Assertions.assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
            Assertions.assertTrue(error.getCause().getMessage().contains("file listing failed"));
        } finally {
            release.countDown();
            consumer.shutdownNow();
        }
    }

    @Test
    void failureBecomingVisibleWithStopCannotBecomeSuccessfulEof() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean failOnStopRead = new java.util.concurrent.atomic.AtomicBoolean();
        SplitAssignment assignment = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class),
                new SplitGenerator() {}, Mockito.mock(SplitToScanRange.class),
                Collections.emptyMap(), Collections.emptyList(), false) {
            @Override
            public boolean isStop() {
                if (failOnStopRead.compareAndSet(true, false)) {
                    setException(new UserException("producer failed before EOF"));
                }
                return super.isStop();
            }
        };
        org.apache.doris.system.Backend backend = Mockito.mock(org.apache.doris.system.Backend.class);
        assignment.getAssignedSplits(backend).add(
                new java.util.AbstractCollection<org.apache.doris.thrift.TScanRangeLocations>() {
                    @Override
                    public java.util.Iterator<org.apache.doris.thrift.TScanRangeLocations> iterator() {
                        failOnStopRead.set(true);
                        return Collections.singletonList(new org.apache.doris.thrift.TScanRangeLocations()).iterator();
                    }

                    @Override
                    public int size() {
                        return 1;
                    }
                });
        SplitSource source = new SplitSource(backend, assignment, 1000);
        UserException failure = Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1));
        Assertions.assertTrue(failure.getMessage().contains("producer failed before EOF"));
    }

    private static SplitAssignment assignment(SplitGenerator generator) {
        return new SplitAssignment(Mockito.mock(FederationBackendPolicy.class), generator,
                Mockito.mock(SplitToScanRange.class), Collections.emptyMap(), Collections.emptyList(), false);
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
