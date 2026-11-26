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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

class HivePartitionBatchLoaderTest {
    @Test
    void firstBatchStartsSmallAndRemainingBatchIsSharedWithoutDuplicates() {
        QueuedScheduler scheduler = new QueuedScheduler();
        List<Integer> requests = new ArrayList<>();
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 1000, 1024, batch -> {
            requests.add(batch.size());
            return partitions(batch);
        });
        Assertions.assertTrue(requests.isEmpty());
        CompletableFuture<Void> initial = loader.isBlocked();
        Assertions.assertFalse(initial.isDone());
        scheduler.runNext();
        Assertions.assertTrue(initial.isDone());
        List<HmsPartitionInfo> all = new ArrayList<>(loader.take(16));
        Assertions.assertEquals(1, scheduler.tasks.size(), "next HMS batch overlaps the first directory batch");
        scheduler.runAll();
        Assertions.assertEquals(List.of(16, 984), requests);
        while (!loader.isFinished()) {
            List<HmsPartitionInfo> batch = loader.take(16);
            Assertions.assertTrue(batch.size() <= 16);
            all.addAll(batch);
        }
        Assertions.assertEquals(1000, all.size());
        Assertions.assertEquals(1000, new HashSet<>(all.stream().map(HmsPartitionInfo::getValues)
                .collect(java.util.stream.Collectors.toList())).size());
    }

    @Test
    void takingFirstDirectoryBatchDoesNotWalkTheNextLargeCachedBatch() {
        QueuedScheduler scheduler = new QueuedScheduler();
        List<Integer> probes = new ArrayList<>();
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, new ConnectorScanContext(),
                names(1000), 1024, 3, batch -> {
                    probes.add(batch.size());
                    return Optional.of(partitions(batch));
                }, batch -> {
                    throw new AssertionError("cached metadata must not issue an HMS request");
                });
        Assertions.assertNull(loader.isBlocked());
        Assertions.assertEquals(16, loader.take(16).size());
        Assertions.assertEquals(List.of(16), probes,
                "claiming the first slice must not prepare the following 984 cached partitions inline");
        Assertions.assertEquals(1, scheduler.tasks.size());
        Runnable queued = scheduler.tasks.element();
        Assertions.assertNull(loader.isBlocked(), "an idle reader can help the cache probe");
        queued.run();
        Assertions.assertTrue(scheduler.tasks.isEmpty(), "the helped task must be removed from the queue");
        Assertions.assertEquals(List.of(16, 984), probes);
        int count = 16;
        while (!loader.isFinished()) {
            count += loader.take(16).size();
        }
        Assertions.assertEquals(1000, count);
    }

    @Test
    void helperAndMetadataWorkerCannotRunTheSameCacheProbeTwice() throws Exception {
        QueuedScheduler scheduler = new QueuedScheduler();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger probes = new AtomicInteger();
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, new ConnectorScanContext(),
                names(1000), 1024, 3, batch -> {
                    probes.incrementAndGet();
                    if (batch.size() > 16) {
                        entered.countDown();
                        try {
                            Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                    }
                    return Optional.of(partitions(batch));
                }, batch -> {
                    throw new AssertionError("cache hit must not issue an HMS request");
                });
        loader.isBlocked();
        loader.take(16);
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<CompletableFuture<Void>> helper = reader.submit(loader::isBlocked);
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            scheduler.runNext();
            Assertions.assertFalse(helper.isDone(), "the metadata worker must not duplicate the helper's probe");
            release.countDown();
            Assertions.assertNull(helper.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(2, probes.get());
            Assertions.assertTrue(scheduler.tasks.isEmpty());
        } finally {
            release.countDown();
            reader.shutdownNow();
        }
    }

    @Test
    void helpedCacheMissHandsOneQueueSlotToRemoteLoadWithoutWaiting() {
        QueuedScheduler scheduler = new QueuedScheduler();
        scheduler.capacity = 1;
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, new ConnectorScanContext(),
                names(1000), 1024, 3,
                batch -> batch.size() == 16 ? Optional.of(partitions(batch)) : Optional.empty(), batch -> {
                    requests.incrementAndGet();
                    return partitions(batch);
                });
        loader.isBlocked();
        Assertions.assertEquals(16, loader.take(16).size());
        CompletableFuture<Void> pending = loader.isBlocked();
        Assertions.assertFalse(pending.isDone());
        Assertions.assertEquals(0, requests.get(), "helping a cache miss must not perform the RPC");
        Assertions.assertEquals(1, scheduler.tasks.size());
        Assertions.assertNull(scheduler.onSpace, "removing the helped cache task must admit the remote task");
        scheduler.runNext();
        Assertions.assertTrue(pending.isDone());
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    void failureInLargeCachedProbeWakesReaders() {
        QueuedScheduler scheduler = new QueuedScheduler();
        IllegalStateException failure = new IllegalStateException("cache probe failed");
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, new ConnectorScanContext(),
                names(1000), 1024, 3, batch -> {
                    if (batch.size() > 16) {
                        throw failure;
                    }
                    return Optional.of(partitions(batch));
                }, batch -> {
                    throw new AssertionError("failed cache probe must not issue an HMS request");
                });
        Assertions.assertNull(loader.isBlocked());
        Assertions.assertEquals(16, loader.take(16).size());
        scheduler.runNext();
        CompletableFuture<Void> pending = loader.isBlocked();
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, pending::join).getCause());
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class,
                () -> loader.take(16)).getCause());
    }

    @Test
    void cancellingQueuedLargeCachedProbePreventsItsExecution() {
        QueuedScheduler scheduler = new QueuedScheduler();
        ConnectorScanContext context = new ConnectorScanContext();
        List<Integer> probes = new ArrayList<>();
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, context,
                names(1000), 1024, 3, batch -> {
                    probes.add(batch.size());
                    return Optional.of(partitions(batch));
                }, batch -> {
                    throw new AssertionError("stopped cached scan must not issue an HMS request");
                });
        loader.isBlocked();
        loader.take(16);
        Runnable stale = scheduler.tasks.element();
        context.cancel();
        Assertions.assertTrue(loader.isBlocked().isCompletedExceptionally());
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        stale.run();
        Assertions.assertEquals(List.of(16), probes);
    }

    @Test
    void cancellationDuringLargeCacheMissDoesNotStartAnHmsRequest() {
        QueuedScheduler scheduler = new QueuedScheduler();
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, context,
                names(1000), 1024, 3, batch -> {
                    if (batch.size() > 16) {
                        context.cancel();
                        return Optional.empty();
                    }
                    return Optional.of(partitions(batch));
                }, batch -> {
                    requests.incrementAndGet();
                    return partitions(batch);
                });
        loader.isBlocked();
        loader.take(16);
        scheduler.runNext();
        Assertions.assertTrue(loader.isBlocked().isCompletedExceptionally());
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    void maximumRpcLimitDoesNotOverflowAfterTheFirstBatch() {
        List<Integer> requests = new ArrayList<>();
        HivePartitionBatchLoader loader = loader(ConnectorTaskScheduler.DIRECT, new ConnectorScanContext(),
                17, Integer.MAX_VALUE, batch -> {
                    requests.add(batch.size());
                    return partitions(batch);
                });
        Assertions.assertNull(loader.isBlocked());
        List<HmsPartitionInfo> all = new ArrayList<>(loader.take(16));
        all.addAll(loader.take(16));
        Assertions.assertTrue(loader.isFinished());
        Assertions.assertEquals(17, all.size());
        Assertions.assertEquals(List.of(16, 1), requests);
    }

    @Test
    void configuredRpcLimitAlsoBoundsFirstBatch() {
        List<Integer> requests = new ArrayList<>();
        HivePartitionBatchLoader loader = loader(ConnectorTaskScheduler.DIRECT, new ConnectorScanContext(),
                10, 4, batch -> {
                    requests.add(batch.size());
                    return partitions(batch);
                });
        int count = 0;
        while (!loader.isFinished()) {
            Assertions.assertNull(loader.isBlocked());
            count += loader.take(16).size();
        }
        Assertions.assertEquals(10, count);
        Assertions.assertEquals(List.of(4, 4, 2), requests);
    }

    @Test
    void oneReadyListBoundsMetadataAheadOfDirectoryConsumers() {
        QueuedScheduler scheduler = new QueuedScheduler();
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 200, 32,
                HivePartitionBatchLoaderTest::partitions);
        loader.isBlocked();
        scheduler.runNext();
        loader.take(16);
        scheduler.runAll();
        Assertions.assertNull(loader.isBlocked());
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        loader.take(16);
        Assertions.assertTrue(scheduler.tasks.isEmpty(), "half-consumed batch must stop the parent");
        loader.take(16);
        Assertions.assertEquals(1, scheduler.tasks.size());
    }

    @Test
    void fullyCachedMetadataDoesNotNeedMetadataCapacity() {
        QueuedScheduler scheduler = new QueuedScheduler();
        scheduler.full = true;
        HivePartitionBatchLoader loader = new HivePartitionBatchLoader(scheduler, new ConnectorScanContext(),
                names(100), 32, 3, batch -> Optional.of(partitions(batch)), batch -> {
                    throw new AssertionError("cache hit must not load remotely");
                });
        int count = 0;
        while (!loader.isFinished()) {
            Assertions.assertNull(loader.isBlocked());
            count += loader.take(16).size();
        }
        Assertions.assertEquals(100, count);
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        Assertions.assertNull(scheduler.onSpace);
    }

    @Test
    void fullSchedulerReturnsImmediatelyAndCancellationRemovesSpaceWaiter() {
        QueuedScheduler scheduler = new QueuedScheduler();
        scheduler.full = true;
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = loader(scheduler, context, 100, 32, batch -> {
            requests.incrementAndGet();
            return partitions(batch);
        });
        CompletableFuture<Void> pending = loader.isBlocked();
        Assertions.assertFalse(pending.isDone());
        Runnable stale = scheduler.onSpace;
        context.cancel();
        Assertions.assertTrue(pending.isCompletedExceptionally());
        Assertions.assertNull(scheduler.onSpace);
        scheduler.full = false;
        stale.run();
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    void admissionResumesAfterSpaceWithoutDuplicateRpc() {
        QueuedScheduler scheduler = new QueuedScheduler();
        scheduler.full = true;
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 10, 32, batch -> {
            requests.incrementAndGet();
            return partitions(batch);
        });
        CompletableFuture<Void> pending = loader.isBlocked();
        scheduler.full = false;
        scheduler.onSpace.run();
        scheduler.runNext();
        Assertions.assertTrue(pending.isDone());
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    void spaceCallbackBeforeAdmissionReturnsCannotEnqueueTwice() {
        Queue<Runnable> tasks = new ArrayDeque<>();
        AtomicInteger admissions = new AtomicInteger();
        List<Runnable> callbacks = new ArrayList<>();
        ConnectorTaskScheduler scheduler = new ConnectorTaskScheduler() {
            @Override
            public boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
                callbacks.add(onSpace);
                if (admissions.incrementAndGet() == 1) {
                    onSpace.run();
                    return false;
                }
                tasks.add(task);
                return true;
            }

            @Override
            public void removeSpaceListener(Runnable callback) {
            }

            @Override
            public void execute(Runnable task) {
                throw new UnsupportedOperationException();
            }
        };
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 10, 32,
                HivePartitionBatchLoaderTest::partitions);
        CompletableFuture<Void> pending = loader.isBlocked();
        callbacks.get(0).run();
        Assertions.assertEquals(2, admissions.get());
        Assertions.assertEquals(1, tasks.size());
        tasks.remove().run();
        Assertions.assertTrue(pending.isDone());
        Assertions.assertEquals(10, loader.take(16).size());
    }

    @Test
    void closingLastReaderCancelsQueuedParentAndWakesWaiters() {
        QueuedScheduler scheduler = new QueuedScheduler();
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 100, 32, batch -> {
            requests.incrementAndGet();
            return partitions(batch);
        });
        CompletableFuture<Void> pending = loader.isBlocked();
        Runnable stale = scheduler.tasks.element();
        loader.readerClosed();
        loader.readerClosed();
        Assertions.assertFalse(pending.isDone());
        loader.readerClosed();
        Assertions.assertTrue(pending.isCompletedExceptionally());
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        stale.run();
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    void failedAdmissionWakesReadersAndDoesNotRunAStaleTask() {
        QueuedScheduler scheduler = new QueuedScheduler();
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 100, 32, batch -> {
            requests.incrementAndGet();
            return partitions(batch);
        });
        CompletableFuture<Void> pending = loader.isBlocked();
        Runnable stale = scheduler.tasks.element();
        IllegalStateException failure = new IllegalStateException("metadata scheduler closed");
        scheduler.onFailure.accept(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, pending::join).getCause());
        Assertions.assertTrue(scheduler.tasks.isEmpty());
        stale.run();
        Assertions.assertEquals(0, requests.get());
    }

    @Test
    void loadingFailureIsVisibleToEveryReader() {
        QueuedScheduler scheduler = new QueuedScheduler();
        IllegalStateException failure = new IllegalStateException("HMS failed");
        HivePartitionBatchLoader loader = loader(scheduler, new ConnectorScanContext(), 100, 32, batch -> {
            throw failure;
        });
        CompletableFuture<Void> a = loader.isBlocked();
        CompletableFuture<Void> b = loader.isBlocked();
        scheduler.runNext();
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, a::join).getCause());
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, b::join).getCause());
        Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class,
                () -> loader.take(16)).getCause());
    }

    @Test
    void cancellationDiscardsUninterruptibleRpcResultAndStopsFurtherCalls() throws Exception {
        QueuedScheduler scheduler = new QueuedScheduler();
        ConnectorScanContext context = new ConnectorScanContext();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        HivePartitionBatchLoader loader = loader(scheduler, context, 100, 32, batch -> {
            requests.incrementAndGet();
            entered.countDown();
            try {
                Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return partitions(batch);
        });
        CompletableFuture<Void> pending = loader.isBlocked();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> running = worker.submit(scheduler::runNext);
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            context.cancel();
            Assertions.assertTrue(pending.isCompletedExceptionally());
            release.countDown();
            running.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, requests.get());
            Assertions.assertTrue(scheduler.tasks.isEmpty());
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void droppedPartitionsDoNotRecurseOrPrematurelyFinish() {
        HivePartitionBatchLoader loader = loader(ConnectorTaskScheduler.DIRECT, new ConnectorScanContext(),
                20000, 1, batch -> Collections.emptyList());
        Assertions.assertNull(loader.isBlocked());
        Assertions.assertTrue(loader.isFinished());
    }

    private static HivePartitionBatchLoader loader(ConnectorTaskScheduler scheduler, ConnectorScanContext context,
            int count, int batchSize, Function<List<String>, List<HmsPartitionInfo>> fetch) {
        return new HivePartitionBatchLoader(scheduler, context, names(count), batchSize, 3,
                batch -> Optional.empty(), fetch);
    }

    private static List<String> names(int count) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add("p=" + i);
        }
        return result;
    }

    private static List<HmsPartitionInfo> partitions(List<String> names) {
        List<HmsPartitionInfo> result = new ArrayList<>();
        for (String name : names) {
            result.add(new HmsPartitionInfo(List.of(name.substring(2)), name,
                    null, null, null, Collections.emptyMap()));
        }
        return result;
    }

    private static final class QueuedScheduler implements ConnectorTaskScheduler {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private boolean full;
        private int capacity = Integer.MAX_VALUE;
        private Runnable onSpace;
        private Consumer<Throwable> onFailure;

        @Override
        public boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable callback) {
            if (full || tasks.size() >= capacity) {
                onSpace = callback;
                return false;
            }
            this.onFailure = onFailure;
            tasks.add(task);
            return true;
        }

        @Override
        public void removeSpaceListener(Runnable callback) {
            if (onSpace == callback) {
                onSpace = null;
            }
        }

        @Override
        public void remove(Runnable task) {
            if (tasks.remove(task) && !full && tasks.size() < capacity && onSpace != null) {
                Runnable callback = onSpace;
                onSpace = null;
                callback.run();
            }
        }

        @Override
        public void execute(Runnable task) {
            throw new UnsupportedOperationException();
        }

        private void runAll() {
            while (!tasks.isEmpty()) {
                runNext();
            }
        }

        private void runNext() {
            tasks.remove().run();
        }
    }
}
