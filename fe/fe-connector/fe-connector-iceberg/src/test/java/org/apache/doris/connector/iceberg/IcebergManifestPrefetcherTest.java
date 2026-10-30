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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class IcebergManifestPrefetcherTest {
    private static final ManifestCacheValue EMPTY = ManifestCacheValue.forDataFiles(Collections.emptyList());

    private static CloseableIterator<ManifestFile> manifests(int count) {
        List<ManifestFile> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String path = "manifest-" + i;
            files.add((ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                    new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> {
                        if (method.getName().equals("path")) {
                            return path;
                        }
                        throw new UnsupportedOperationException(method.getName());
                    }));
        }
        return CloseableIterable.withNoopClose(files).iterator();
    }

    private static class Context implements ConnectorContext {
        final List<RunnableFuture<?>> submitted = new ArrayList<>();
        final ExecutorService executor;
        final AtomicInteger authenticated = new AtomicInteger();

        Context(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public long getCatalogId() {
            return 1;
        }

        @Override
        public synchronized <T> RunnableFuture<T> submitMetadataTask(Callable<T> callable) {
            FutureTask<T> future = new FutureTask<>(callable);
            submitted.add(future);
            if (executor != null) {
                executor.execute(future);
            }
            return future;
        }

        @Override
        public <T> T executeAuthenticated(Callable<T> callable) throws Exception {
            Assertions.assertSame(IcebergManifestPrefetcher.class.getClassLoader(),
                    Thread.currentThread().getContextClassLoader());
            authenticated.incrementAndGet();
            return callable.call();
        }
    }

    @Test
    public void warmCacheAvoidsSubmissionAndSpeculativeTraversal() throws Exception {
        Context context = new Context(null);
        AtomicInteger probes = new AtomicInteger();
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(20), 8,
                manifest -> {
                    probes.incrementAndGet();
                    return EMPTY;
                },
                manifest -> {
                    throw new AssertionError("warm cache loaded remotely");
                }, context, new Semaphore(8))) {
            Assertions.assertEquals("manifest-0", source.next().manifest.path());
            Assertions.assertEquals(1, probes.get());
            Assertions.assertTrue(context.submitted.isEmpty());
        }
    }

    @Test
    public void saturatedExecutorDegradesToOwnTaskAndCloseCancelsUnusedWindow() throws Exception {
        Context context = new Context(null); // accepts tasks but has no worker available
        AtomicInteger loads = new AtomicInteger();
        IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(100), 4,
                manifest -> null, manifest -> {
                    loads.incrementAndGet();
                    return EMPTY;
                }, context, new Semaphore(4));
        Assertions.assertEquals("manifest-0", source.next().manifest.path());
        Assertions.assertEquals(0, context.submitted.size(), "first manifest must not submit executor work");
        Assertions.assertEquals(1, loads.get());
        Assertions.assertEquals("manifest-1", source.next().manifest.path());
        Assertions.assertEquals(4, context.submitted.size(), "subsequent demand expands the bounded window");
        Assertions.assertEquals(2, loads.get());
        source.close();
        source.close();
        Assertions.assertFalse(source.hasNext());
        for (RunnableFuture<?> task : context.submitted) {
            task.run();
        }
        Assertions.assertEquals(2, loads.get(), "cancelled queued tasks must never start a read");
        Assertions.assertEquals(2, context.authenticated.get());
    }

    @Test
    public void demandControlsPrefetchWithoutBlockingSerialProgress() throws Exception {
        Context context = new Context(null);
        java.util.concurrent.atomic.AtomicBoolean demand = new java.util.concurrent.atomic.AtomicBoolean();
        AtomicInteger loads = new AtomicInteger();
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(20), 4,
                manifest -> null, manifest -> {
                    loads.incrementAndGet();
                    return EMPTY;
                }, context, new Semaphore(4), demand::get)) {
            Assertions.assertEquals("manifest-0", source.next().manifest.path());
            Assertions.assertEquals("manifest-1", source.next().manifest.path());
            Assertions.assertTrue(context.submitted.isEmpty());
            demand.set(true);
            Assertions.assertEquals("manifest-2", source.next().manifest.path());
            Assertions.assertEquals(4, context.submitted.size());
            demand.set(false);
            Assertions.assertEquals("manifest-3", source.next().manifest.path());
            Assertions.assertEquals(4, context.submitted.size(), "paused demand must not refill the window");
            Assertions.assertEquals(4, loads.get());
        }
        Assertions.assertTrue(context.submitted.stream().allMatch(Future::isDone));
    }

    @Test
    public void sequentialSettingDoesNotSubmitAndKeepsOrder() throws Exception {
        Context context = new Context(null);
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(3), 1,
                manifest -> null, manifest -> EMPTY, context, new Semaphore(1))) {
            for (int i = 0; i < 3; i++) {
                Assertions.assertEquals("manifest-" + i, source.next().manifest.path());
            }
            Assertions.assertFalse(source.hasNext());
            Assertions.assertTrue(context.submitted.isEmpty());
        }
    }

    @Test
    public void loadFailurePropagatesAndCancelsTheRemainingWindow() throws Exception {
        Context context = new Context(null);
        IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(10), 4,
                manifest -> null, manifest -> {
                    if (manifest.path().equals("manifest-0")) {
                        return EMPTY;
                    }
                    throw new IllegalStateException("manifest read failed");
                },
                context, new Semaphore(4));
        Assertions.assertEquals("manifest-0", source.next().manifest.path());
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, source::next);
        Assertions.assertEquals("manifest read failed", failure.getCause().getMessage());
        Assertions.assertFalse(source.hasNext());
        Assertions.assertTrue(context.submitted.stream().allMatch(Future::isDone));
    }

    @Test
    public void multipleScansShareReadLimitAndReturnEveryManifestOnce() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(6);
        ExecutorService parents = Executors.newFixedThreadPool(2);
        Context context = new Context(workers);
        Semaphore permits = new Semaphore(2);
        CountDownLatch twoReading = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Function<ManifestFile, ManifestCacheValue> load = manifest -> {
            int current = active.incrementAndGet();
            peak.accumulateAndGet(current, Math::max);
            twoReading.countDown();
            try {
                Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                return EMPTY;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                active.decrementAndGet();
            }
        };
        try (IcebergManifestPrefetcher first = new IcebergManifestPrefetcher(manifests(12), 4,
                manifest -> null, load, context, permits);
                IcebergManifestPrefetcher second = new IcebergManifestPrefetcher(manifests(12), 4,
                        manifest -> null, load, context, permits)) {
            List<Future<List<String>>> results = new ArrayList<>();
            for (IcebergManifestPrefetcher source : List.of(first, second)) {
                results.add(parents.submit(() -> {
                    List<String> paths = new ArrayList<>();
                    while (source.hasNext()) {
                        paths.add(source.next().manifest.path());
                    }
                    return paths;
                }));
            }
            Assertions.assertTrue(twoReading.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(2, peak.get());
            release.countDown();
            for (Future<List<String>> result : results) {
                List<String> paths = result.get(5, TimeUnit.SECONDS);
                Set<String> expected = new HashSet<>();
                for (int i = 0; i < 12; i++) {
                    expected.add("manifest-" + i);
                }
                Assertions.assertEquals(expected, new HashSet<>(paths));
                Assertions.assertEquals(12, paths.size());
            }
            Assertions.assertEquals(2, peak.get());
            Assertions.assertEquals(2, permits.availablePermits());
            Assertions.assertEquals(24, context.authenticated.get());
        } finally {
            release.countDown();
            workers.shutdownNow();
            parents.shutdownNow();
        }
    }

    @Test
    public void cancellationPreservesSharedLoadAndPreventsFurtherRequests() throws Exception {
        ExecutorService parents = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch secondReading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger interrupted = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, ManifestCacheValue> cache = owner.create(MetaCacheDefinition
                    .<String, ManifestCacheValue>builder("shared-manifest", CacheSpec.of(true, -1, 10),
                            ignored -> ScopePath.catalog()).build());
            Function<ManifestFile, ManifestCacheValue> loader = manifest -> cache.get(manifest.path(), key -> {
                loads.incrementAndGet();
                entered.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                    return EMPTY;
                } catch (InterruptedException e) {
                    interrupted.incrementAndGet();
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
            try (IcebergManifestPrefetcher first = new IcebergManifestPrefetcher(manifests(30), 4,
                    manifest -> null, loader, null, new Semaphore(4));
                    IcebergManifestPrefetcher second = new IcebergManifestPrefetcher(manifests(1), 4,
                            manifest -> null, manifest -> {
                                secondReading.countDown();
                                return loader.apply(manifest);
                            }, null, new Semaphore(4))) {
                Future<?> cancelled = parents.submit(first::next);
                Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
                Future<String> survivor = parents.submit(() ->
                        second.next().manifest.path());
                Assertions.assertTrue(secondReading.await(5, TimeUnit.SECONDS));
                first.close();
                release.countDown();
                Assertions.assertEquals("manifest-0", survivor.get(5, TimeUnit.SECONDS));
                Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> cancelled.get(5, TimeUnit.SECONDS));
                Assertions.assertFalse(first.hasNext());
                Assertions.assertEquals(0, interrupted.get(), "one query must not interrupt a shared cache loader");
                Assertions.assertEquals(1, loads.get(), "cancelled queued manifests must not start new requests");
            }
        } finally {
            release.countDown();
            parents.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Context startedHeadContext(ExecutorService workers, CountDownLatch started) {
        return new Context(workers) {
            @Override
            public synchronized <T> RunnableFuture<T> submitMetadataTask(Callable<T> callable) {
                RunnableFuture<T> task = super.submitMetadataTask(callable);
                if (submitted.size() == 1) {
                    await(started);
                }
                return task;
            }
        };
    }

    @Test
    public void completedManifestBypassesSlowHeadWithoutGrowingPausedWindow() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(3);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Context context = startedHeadContext(workers, started);
        AtomicInteger loads = new AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean demand = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(20), 3,
                manifest -> null, manifest -> {
                    loads.incrementAndGet();
                    if (manifest.path().equals("manifest-1")) {
                        started.countDown();
                        await(release);
                    }
                    return EMPTY;
                }, context, new Semaphore(3), demand::get)) {
            Assertions.assertEquals("manifest-0", source.next().manifest.path());
            String first = consumer.submit(() -> source.next().manifest.path()).get(5, TimeUnit.SECONDS);
            Assertions.assertNotEquals("manifest-1", first, "a completed manifest must bypass the slow head");
            Assertions.assertEquals(3, context.submitted.size());
            demand.set(false);
            String second = consumer.submit(() -> source.next().manifest.path()).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(Set.of("manifest-2", "manifest-3"), Set.of(first, second));
            Assertions.assertEquals(3, context.submitted.size(), "paused consumption must not refill the window");
            Assertions.assertEquals(4, loads.get(), "completed and running results share one bounded window");
            release.countDown();
            Assertions.assertEquals("manifest-1", source.next().manifest.path());
            demand.set(true);
            Set<String> seen = new HashSet<>(Set.of("manifest-0", first, second, "manifest-1"));
            while (source.hasNext()) {
                Assertions.assertTrue(seen.add(source.next().manifest.path()), "duplicate manifest");
            }
            Assertions.assertEquals(20, seen.size());
        } finally {
            release.countDown();
            workers.shutdownNow();
            consumer.shutdownNow();
        }
    }

    @Test
    public void completedFailureBypassesSlowHeadAndClosesWindow() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Context context = startedHeadContext(workers, started);
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(3), 2,
                manifest -> null, manifest -> {
                    if (manifest.path().equals("manifest-1")) {
                        started.countDown();
                        await(release);
                    } else if (manifest.path().equals("manifest-2")) {
                        throw new IllegalStateException("later manifest failed");
                    }
                    return EMPTY;
                }, context, new Semaphore(2))) {
            source.next();
            Future<?> next = consumer.submit(source::next);
            java.util.concurrent.ExecutionException failure = Assertions.assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> next.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals("later manifest failed", failure.getCause().getCause().getMessage());
            Assertions.assertFalse(source.hasNext());
            Assertions.assertTrue(context.submitted.stream().allMatch(Future::isDone));
        } finally {
            release.countDown();
            workers.shutdownNow();
            consumer.shutdownNow();
        }
    }

    @Test
    public void closeWakesCompletionWaitWithoutInterruptingSharedRead() throws Exception {
        ExecutorService workers = Executors.newSingleThreadExecutor();
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Context context = startedHeadContext(workers, started);
        AtomicInteger loads = new AtomicInteger();
        try (IcebergManifestPrefetcher source = new IcebergManifestPrefetcher(manifests(10), 3,
                manifest -> null, manifest -> {
                    loads.incrementAndGet();
                    if (manifest.path().equals("manifest-1")) {
                        started.countDown();
                        await(release);
                    }
                    return EMPTY;
                }, context, new Semaphore(3))) {
            source.next();
            Future<?> next = consumer.submit(source::next);
            await(started);
            source.close();
            java.util.concurrent.ExecutionException failure = Assertions.assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> next.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(java.util.concurrent.CancellationException.class, failure.getCause());
            release.countDown();
            for (RunnableFuture<?> task : context.submitted) {
                task.run();
            }
            Assertions.assertEquals(2, loads.get(), "closing must cancel queued reads");
            Assertions.assertFalse(source.hasNext());
        } finally {
            release.countDown();
            workers.shutdownNow();
            consumer.shutdownNow();
        }
    }

}
