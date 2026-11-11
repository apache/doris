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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorTaskScheduler;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

@Timeout(30)
class IcebergScheduledManifestTest {
    private static final ManifestCacheValue EMPTY = ManifestCacheValue.forDataFiles(Collections.emptyList());

    private static final class Scheduler implements ConnectorTaskScheduler {
        final Queue<Runnable> tasks = new ArrayDeque<>();
        final Set<Runnable> listeners = new LinkedHashSet<>();
        final CountDownLatch full = new CountDownLatch(1);
        final CountDownLatch admitted = new CountDownLatch(1);
        volatile boolean closed;

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public synchronized boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
            if (closed) {
                throw new RejectedExecutionException("query closed");
            }
            if (!tasks.isEmpty()) {
                listeners.add(onSpace);
                full.countDown();
                return false;
            }
            tasks.add(task);
            admitted.countDown();
            return true;
        }

        @Override
        public synchronized void removeSpaceListener(Runnable onSpace) {
            listeners.remove(onSpace);
        }

        @Override
        public void execute(Runnable task) {
            Assertions.fail("must use bounded admission");
        }

        void runOne() {
            Runnable task;
            List<Runnable> notifications;
            synchronized (this) {
                task = tasks.remove();
                notifications = new ArrayList<>(listeners);
                listeners.clear();
            }
            notifications.forEach(Runnable::run);
            task.run();
        }
    }

    private IcebergManifestPrefetcher source(Scheduler scheduler, AtomicInteger loads) {
        List<ManifestFile> manifests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            manifests.add(sourceManifest("m-" + i));
        }
        return new IcebergManifestPrefetcher(CloseableIterable.withNoopClose(manifests).iterator(), 3,
                manifest -> null, manifest -> {
                    loads.incrementAndGet();
                    return EMPTY;
                },
                null, new Semaphore(3), () -> true, scheduler);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void queryCancellationStopsAReadWaitingForItsSharedPermit(boolean releasePermit) throws Exception {
        Scheduler scheduler = new Scheduler();
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch waiting = new CountDownLatch(1);
        Semaphore permits = new Semaphore(0) {
            @Override
            public void acquire() throws InterruptedException {
                waiting.countDown();
                super.acquire();
            }

            @Override
            public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
                waiting.countDown();
                return super.tryAcquire(timeout, unit);
            }
        };
        ExecutorService parent = Executors.newSingleThreadExecutor();
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(
                CloseableIterable.withNoopClose(Collections.singletonList(
                        sourceManifest("waiting"))).iterator(), 1, manifest -> null,
                manifest -> {
                    loads.incrementAndGet();
                    return EMPTY;
                }, null, permits, () -> true, scheduler)) {
            Future<?> read = parent.submit(source::next);
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            scheduler.closed = true;
            if (releasePermit) {
                permits.release();
            }
            ExecutionException failure = Assertions.assertThrows(
                    ExecutionException.class, () -> read.get(2, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, failure.getCause());
            Assertions.assertEquals(0, loads.get(), "cancellation must not start another remote read");
            Assertions.assertEquals(releasePermit ? 1 : 0, permits.availablePermits(),
                    "a waiter must not release another scan's permit");
        } finally {
            permits.release();
            parent.shutdownNow();
            Assertions.assertTrue(parent.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static ManifestFile sourceManifest(String path) {
        return (ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> {
                    if (method.getName().equals("path")) {
                        return path;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @Test
    void fullMetadataQueueDoesNotBlockSubmissionAndResumesWithinTheWindow() throws Exception {
        Scheduler scheduler = new Scheduler();
        AtomicInteger loads = new AtomicInteger();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        try (IcebergManifestPrefetcher source = source(scheduler, loads)) {
            Assertions.assertEquals("m-0", source.next().manifest.path());
            Future<IcebergManifestPrefetcher.LoadedManifest> next = parent.submit(source::next);
            Assertions.assertTrue(scheduler.full.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loads.get(), "a waiting parent must not bypass query scheduling");
            scheduler.runOne();
            Assertions.assertEquals("m-1", next.get(10, TimeUnit.SECONDS).manifest.path());
            Assertions.assertEquals(2, loads.get());
            source.close();
            Assertions.assertTrue(scheduler.listeners.isEmpty());
            while (!scheduler.tasks.isEmpty()) {
                scheduler.runOne();
            }
            Assertions.assertEquals(2, loads.get(), "closed window must not issue another IO");
        } finally {
            parent.shutdownNow();
        }
    }

    @Test
    void rejectionWakesParentAndPreservesTheCause() throws Exception {
        Scheduler scheduler = new Scheduler();
        AtomicInteger loads = new AtomicInteger();
        try (IcebergManifestPrefetcher source = source(scheduler, loads)) {
            source.next();
            scheduler.closed = true;
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, source::next);
            Assertions.assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            Assertions.assertEquals(1, loads.get());
            Assertions.assertTrue(scheduler.listeners.isEmpty());
        }
    }

    @Test
    void cancelWakesAParentEvenWhenTheMetadataWorkerIsUnavailable() throws Exception {
        Scheduler scheduler = new Scheduler();
        AtomicInteger loads = new AtomicInteger();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        try (IcebergManifestPrefetcher source = source(scheduler, loads)) {
            source.next();
            Future<?> next = parent.submit(source::next);
            Assertions.assertTrue(scheduler.full.await(10, TimeUnit.SECONDS));
            source.close();
            Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> next.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loads.get());
            Assertions.assertTrue(scheduler.listeners.isEmpty());
        } finally {
            parent.shutdownNow();
        }
    }

    private IcebergManifestPrefetcher sharedSource(Scheduler scheduler, int count,
            Function<ManifestFile, CompletionStage<ManifestCacheValue>> loader) {
        List<ManifestFile> manifests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            manifests.add(sourceManifest("m-" + i));
        }
        return new IcebergManifestPrefetcher(CloseableIterable.withNoopClose(manifests).iterator(), 3,
                manifest -> null, manifest -> EMPTY, null, new Semaphore(3), () -> true, scheduler, loader);
    }

    @Test
    void cancelledFutureRetainsLoadOwnerUntilCallableActuallyExits() throws Exception {
        Scheduler scheduler = new Scheduler();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch finishRead = new CountDownLatch(1);
        AtomicInteger retained = new AtomicInteger();
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(
                CloseableIterable.withNoopClose(List.of(sourceManifest("first"), sourceManifest("second"))).iterator(),
                2, manifest -> null, manifest -> EMPTY, null, new Semaphore(2), () -> true, scheduler,
                manifest -> {
                    reading.countDown();
                    try {
                        Assertions.assertTrue(finishRead.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return CompletableFuture.completedFuture(EMPTY);
                }, () -> {
                    retained.incrementAndGet();
                    return retained::decrementAndGet;
                })) {
            source.next();
            Future<?> next = parent.submit(source::next);
            Assertions.assertTrue(scheduler.admitted.await(5, TimeUnit.SECONDS));
            Future<?> running = worker.submit(scheduler::runOne);
            Assertions.assertTrue(reading.await(5, TimeUnit.SECONDS));
            source.close();
            Assertions.assertEquals(1, retained.get(), "cancel(false) must not close a running load's FileIO");
            Assertions.assertThrows(ExecutionException.class, () -> next.get(5, TimeUnit.SECONDS));
            finishRead.countDown();
            running.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(0, retained.get());
        } finally {
            finishRead.countDown();
            parent.shutdownNow();
            worker.shutdownNow();
        }
    }

    @Test
    void sharedFutureReturnsWorkerAndCompletesInReadinessOrder() throws Exception {
        Scheduler scheduler = new Scheduler();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        CompletableFuture<ManifestCacheValue> slow = new CompletableFuture<>();
        CompletableFuture<ManifestCacheValue> fast = new CompletableFuture<>();
        AtomicInteger registered = new AtomicInteger();
        try (IcebergManifestPrefetcher source = sharedSource(scheduler, 3, manifest -> {
            registered.incrementAndGet();
            return manifest.path().equals("m-1") ? slow : fast;
        })) {
            Assertions.assertEquals("m-0", source.next().manifest.path());
            Future<?> next = parent.submit(() -> source.next().manifest.path());
            Assertions.assertTrue(scheduler.full.await(5, TimeUnit.SECONDS));
            scheduler.runOne();
            Assertions.assertEquals(1, registered.get(), "worker must return before a shared load completes");
            scheduler.runOne();
            Assertions.assertEquals(2, registered.get(), "other scheduled work must be able to run");
            Assertions.assertFalse(next.isDone());
            fast.complete(EMPTY);
            Assertions.assertEquals("m-2", next.get(5, TimeUnit.SECONDS));
            slow.complete(EMPTY);
            Assertions.assertEquals("m-1", source.next().manifest.path());
            Assertions.assertFalse(source.hasNext());
        } finally {
            slow.complete(EMPTY);
            fast.complete(EMPTY);
            parent.shutdownNow();
        }
    }

    @Test
    void closeDetachesPendingObserverWithoutCancellingSharedRead() throws Exception {
        Scheduler scheduler = new Scheduler();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        CompletableFuture<ManifestCacheValue> shared = new CompletableFuture<>();
        AtomicInteger registered = new AtomicInteger();
        try (IcebergManifestPrefetcher source = sharedSource(scheduler, 10, manifest -> {
            registered.incrementAndGet();
            return shared;
        })) {
            source.next();
            Future<?> next = parent.submit(source::next);
            Assertions.assertTrue(scheduler.full.await(5, TimeUnit.SECONDS));
            scheduler.runOne();
            Assertions.assertEquals(1, registered.get());
            source.close();
            Assertions.assertThrows(ExecutionException.class, () -> next.get(5, TimeUnit.SECONDS));
            Assertions.assertFalse(shared.isDone());
            Assertions.assertTrue(scheduler.listeners.isEmpty());
            while (!scheduler.tasks.isEmpty()) {
                scheduler.runOne();
            }
            Assertions.assertEquals(1, registered.get(), "early stop must prevent queued remote requests");
            shared.complete(EMPTY);
            Assertions.assertFalse(source.hasNext());
        } finally {
            shared.complete(EMPTY);
            parent.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sharedFailurePreservesCauseBeforeOrAfterObserverRegistration(boolean alreadyCompleted) throws Exception {
        Scheduler scheduler = new Scheduler();
        ExecutorService parent = Executors.newSingleThreadExecutor();
        IllegalStateException expected = new IllegalStateException("shared manifest failed");
        CompletableFuture<ManifestCacheValue> shared = new CompletableFuture<>();
        if (alreadyCompleted) {
            shared.completeExceptionally(expected);
        }
        try (IcebergManifestPrefetcher source = sharedSource(scheduler, 10, manifest -> shared)) {
            source.next();
            Future<?> next = parent.submit(source::next);
            Assertions.assertTrue(scheduler.full.await(5, TimeUnit.SECONDS));
            scheduler.runOne();
            shared.completeExceptionally(expected);
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> next.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(expected, failure.getCause().getCause());
            Assertions.assertFalse(source.hasNext());
            Assertions.assertTrue(scheduler.listeners.isEmpty());
            while (!scheduler.tasks.isEmpty()) {
                scheduler.runOne();
            }
        } finally {
            shared.completeExceptionally(expected);
            parent.shutdownNow();
        }
    }

    @Test
    void completionRegistrationAndCloseCannotLoseWakeup() throws Exception {
        ExecutorService parent = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 100; i++) {
                Scheduler scheduler = new Scheduler();
                CompletableFuture<ManifestCacheValue> shared = new CompletableFuture<>();
                try (IcebergManifestPrefetcher source = sharedSource(scheduler, 2, manifest -> shared)) {
                    source.next();
                    Future<?> next = parent.submit(source::next);
                    Assertions.assertTrue(scheduler.admitted.await(5, TimeUnit.SECONDS));
                    if ((i & 1) == 0) {
                        shared.complete(EMPTY);
                    }
                    scheduler.runOne();
                    source.close();
                    shared.complete(EMPTY);
                    try {
                        next.get(5, TimeUnit.SECONDS);
                    } catch (ExecutionException failure) {
                        Assertions.assertInstanceOf(CancellationException.class, failure.getCause());
                    }
                    Assertions.assertTrue(scheduler.listeners.isEmpty());
                }
            }
        } finally {
            parent.shutdownNow();
        }
    }

}
