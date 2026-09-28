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

import org.apache.doris.common.Pair;
import org.apache.doris.datasource.metacache.MetaCache;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MetaCacheTest {

    private MetaCache<String> metaCache;

    @Before
    public void setUp() {
        CacheLoader<String, List<Pair<String, String>>> namesCacheLoader = key -> Lists.newArrayList();
        CacheLoader<String, Optional<String>> metaObjCacheLoader = key -> Optional.empty();
        RemovalListener<String, Optional<String>> removalListener = (key, value, cause) -> {};

        metaCache = new MetaCache<>(
                "testCache",
                Executors.newCachedThreadPool(),
                OptionalLong.of(1),
                OptionalLong.of(1),
                100, // max size
                namesCacheLoader,
                metaObjCacheLoader,
                removalListener
        );
    }

    @Test
    public void testListNames() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);

        List<String> names = metaCache.listNames();
        Assert.assertEquals(2, names.size());
        Assert.assertTrue(names.contains("local1"));
        Assert.assertTrue(names.contains("local2"));
    }

    @Test
    public void testRefreshNamesReloadsCompleteSnapshot() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loadCount = new AtomicInteger();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            Assert.assertEquals(Lists.newArrayList("local-2"), cache.refreshNames());
            Assert.assertEquals(2, loadCount.get());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshNamesDoesNotLoadAfterCallerIsInterrupted() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadCount.incrementAndGet();
                    return Lists.newArrayList(Pair.of("remote", "local"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        Thread caller = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                cache.refreshNames();
                failure.set(new AssertionError("interrupted refresh unexpectedly loaded names"));
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof InterruptedException)) {
                    failure.set(e);
                }
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }
        }, "interrupted-names-refresh");

        try {
            caller.start();
            caller.join(TimeUnit.SECONDS.toMillis(3));
            Assert.assertFalse(caller.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertTrue(interruptPreserved.get());
            Assert.assertEquals(0, loadCount.get());
        } finally {
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRetiredInterruptedRefreshDoesNotStartReplacementLoad() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch interruptLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        loadStarted.countDown();
                        Assert.assertTrue(interruptLoad.await(3, TimeUnit.SECONDS));
                        throw new InterruptedException("cancelled obsolete connector load");
                    }
                    return Lists.newArrayList(Pair.of("remote-2", "local-2"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        Thread caller = new Thread(() -> {
            try {
                cache.refreshNames();
                failure.set(new AssertionError("interrupted refresh unexpectedly retried"));
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof InterruptedException)) {
                    failure.set(e);
                }
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }
        }, "retired-interrupted-names-refresh");

        try {
            caller.start();
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            cache.invalidateNames();
            interruptLoad.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(3));
            Assert.assertFalse(caller.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertTrue(interruptPreserved.get());
            Assert.assertEquals(1, loadCount.get());
        } finally {
            interruptLoad.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(3));
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshNamesStartsLoadAfterExistingRefresh() throws Exception {
        CountDownLatch oldRefreshLoaded = new CountDownLatch(1);
        CountDownLatch publishOldRefresh = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 2) {
                        oldRefreshLoaded.countDown();
                        Assert.assertTrue(publishOldRefresh.await(3, TimeUnit.SECONDS));
                    }
                    return Lists.newArrayList(Pair.of(
                            "remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            cache.refreshNamesForTest();
            Assert.assertTrue(oldRefreshLoaded.await(3, TimeUnit.SECONDS));
            Future<List<String>> forcedRefresh = caller.submit(cache::refreshNames);
            Assert.assertFalse(forcedRefresh.isDone());
            publishOldRefresh.countDown();
            Assert.assertEquals(Lists.newArrayList("local-3"), forcedRefresh.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(3, loadCount.get());
        } finally {
            publishOldRefresh.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshNamesKeepsRetiredPhysicalLoadsBounded() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch secondLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoads = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(4);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        firstLoadStarted.countDown();
                    } else if (currentLoad == 2) {
                        secondLoadStarted.countDown();
                    }
                    Assert.assertTrue(releaseLoads.await(3, TimeUnit.SECONDS));
                    return Lists.newArrayList(Pair.of(
                            "remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            callers.submit(cache::refreshNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));
            callers.submit(cache::refreshNames);
            Assert.assertTrue(secondLoadStarted.await(3, TimeUnit.SECONDS));

            for (int i = 0; i < 2; i++) {
                Future<List<String>> rejectedRefresh = callers.submit(cache::refreshNames);
                try {
                    rejectedRefresh.get(1, TimeUnit.SECONDS);
                    Assert.fail("refresh should fail while two physical loads are still running");
                } catch (java.util.concurrent.ExecutionException e) {
                    Assert.assertTrue(e.getCause() instanceof IllegalStateException);
                }
            }
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseLoads.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFailedForcedRefreshPreservesWarmSnapshot() throws Exception {
        CountDownLatch backgroundLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseBackgroundLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        return Lists.newArrayList(Pair.of("remote-warm", "local-warm"));
                    }
                    if (currentLoad == 2) {
                        backgroundLoadStarted.countDown();
                        Assert.assertTrue(releaseBackgroundLoad.await(3, TimeUnit.SECONDS));
                        return Lists.newArrayList(Pair.of("remote-background", "local-background"));
                    }
                    throw new RuntimeException("connector unavailable");
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-warm"), cache.listNames());
            cache.refreshNamesForTest();
            Assert.assertTrue(backgroundLoadStarted.await(3, TimeUnit.SECONDS));

            try {
                cache.refreshNames();
                Assert.fail("forced refresh should fail while the connector is unavailable");
            } catch (RuntimeException e) {
                Assert.assertEquals("connector unavailable", e.getMessage());
            }

            Assert.assertEquals(Lists.newArrayList("local-warm"), cache.listNames());
            Assert.assertEquals(3, loadCount.get());
        } finally {
            releaseBackgroundLoad.countDown();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshNamesDoesNotRejoinCompletedActiveLoad() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        CountDownLatch completionCallbackFinished = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<List<String>> refreshedNames = new AtomicReference<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        firstLoadStarted.countDown();
                        Assert.assertTrue(releaseFirstLoad.await(3, TimeUnit.SECONDS));
                    }
                    return Lists.newArrayList(Pair.of(
                            "remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> firstLoad = caller.submit(cache::listNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));

            CompletableFuture<?> activeLoad = getActiveNamesLoadFuture(cache);
            activeLoad.thenRun(() -> {
                try {
                    refreshedNames.set(cache.refreshNames());
                } catch (Throwable t) {
                    callbackFailure.set(t);
                } finally {
                    completionCallbackFinished.countDown();
                }
            });

            releaseFirstLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-1"), firstLoad.get(3, TimeUnit.SECONDS));
            Assert.assertTrue(completionCallbackFinished.await(3, TimeUnit.SECONDS));
            Assert.assertNull(callbackFailure.get());
            Assert.assertEquals(Lists.newArrayList("local-2"), refreshedNames.get());
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseFirstLoad.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshNamesDoesNotRejoinFailedActiveLoad() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        CountDownLatch completionCallbackFinished = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<List<String>> refreshedNames = new AtomicReference<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        firstLoadStarted.countDown();
                        Assert.assertTrue(releaseFirstLoad.await(3, TimeUnit.SECONDS));
                        throw new RuntimeException("first load failed");
                    }
                    return Lists.newArrayList(Pair.of("remote-2", "local-2"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> firstLoad = caller.submit(cache::listNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));

            CompletableFuture<?> activeLoad = getActiveNamesLoadFuture(cache);
            activeLoad.whenComplete((ignored, failure) -> {
                try {
                    refreshedNames.set(cache.refreshNames());
                } catch (Throwable t) {
                    callbackFailure.set(t);
                } finally {
                    completionCallbackFinished.countDown();
                }
            });

            releaseFirstLoad.countDown();
            try {
                firstLoad.get(3, TimeUnit.SECONDS);
                Assert.fail("first names load should fail");
            } catch (java.util.concurrent.ExecutionException e) {
                Assert.assertEquals("first load failed", e.getCause().getMessage());
            }
            Assert.assertTrue(completionCallbackFinished.await(3, TimeUnit.SECONDS));
            Assert.assertNull(callbackFailure.get());
            Assert.assertEquals(Lists.newArrayList("local-2"), refreshedNames.get());
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseFirstLoad.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private CompletableFuture<?> getActiveNamesLoadFuture(MetaCache<String> cache) throws Exception {
        Field activeNamesLoadField = MetaCache.class.getDeclaredField("activeNamesLoad");
        activeNamesLoadField.setAccessible(true);
        Object activeNamesLoad = activeNamesLoadField.get(cache);
        Assert.assertNotNull(activeNamesLoad);
        Field resultField = activeNamesLoad.getClass().getDeclaredField("result");
        resultField.setAccessible(true);
        return (CompletableFuture<?>) resultField.get(activeNamesLoad);
    }

    @Test
    public void testGetRemoteName() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);

        String remoteName = metaCache.getRemoteName("local1");
        Assert.assertEquals("remote1", remoteName);

        Assert.assertNull(metaCache.getRemoteName("nonexistent"));
    }

    @Test
    public void testGetRemoteNameKeepsFirstDuplicateLocalName() {
        ExecutorService executor = Executors.newCachedThreadPool();
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                100,
                key -> Lists.newArrayList(
                        Pair.of("first_remote", "duplicate_local"),
                        Pair.of("second_remote", "duplicate_local")),
                key -> Optional.empty(),
                (key, value, cause) -> {
                }
        );

        try {
            Assert.assertEquals(Lists.newArrayList("duplicate_local"), testCache.listNames());
            Assert.assertEquals("first_remote", testCache.getRemoteName("duplicate_local"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testGetMetaObj() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);

        Optional<String> metaObj = metaCache.getMetaObj("local1", 1L);
        Assert.assertTrue(metaObj.isPresent());
        Assert.assertEquals("meta1", metaObj.get());

        Assert.assertFalse(metaCache.getMetaObj("xxx", 2L).isPresent());

    }

    @Test
    public void testGetMetaObjById() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);
        metaCache.updateCache("remote3", "local3", "meta3", 1L);

        Optional<String> metaObj = metaCache.getMetaObjById(1L);
        Assert.assertTrue(metaObj.isPresent());
        Assert.assertEquals("meta3", metaObj.get());

        Assert.assertFalse(metaCache.getMetaObjById(99L).isPresent());
    }

    @Test
    public void testUpdateCache() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);

        List<String> names = metaCache.listNames();
        Assert.assertEquals(2, names.size());
        Assert.assertTrue(names.contains("local1"));
        Assert.assertTrue(names.contains("local2"));

        Optional<String> metaObj1 = metaCache.getMetaObj("local1", 1L);
        Assert.assertTrue(metaObj1.isPresent());
        Assert.assertEquals("meta1", metaObj1.get());

        Optional<String> metaObj2 = metaCache.getMetaObj("local2", 2L);
        Assert.assertTrue(metaObj2.isPresent());
        Assert.assertEquals("meta2", metaObj2.get());
    }

    @Test
    public void testInvalidate() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);

        // Invalidate local1 cache
        metaCache.invalidate("local1", 1L);

        List<String> names = metaCache.listNames();
        Assert.assertEquals(1, names.size());
        Assert.assertTrue(names.contains("local2"));

        Optional<String> metaObj1 = metaCache.getMetaObj("local1", 1L);
        Assert.assertFalse(metaObj1.isPresent());

        Optional<String> metaObj2 = metaCache.getMetaObj("local2", 2L);
        Assert.assertTrue(metaObj2.isPresent());
        Assert.assertEquals("meta2", metaObj2.get());
    }

    @Test
    public void testInvalidateAll() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        metaCache.updateCache("remote2", "local2", "meta2", 2L);

        metaCache.invalidateAll();

        List<String> names = metaCache.listNames();
        Assert.assertTrue(names.isEmpty());

        Assert.assertFalse(metaCache.getMetaObj("local1", 1L).isPresent());
        Assert.assertFalse(metaCache.getMetaObj("local2", 2L).isPresent());
    }

    @Test
    public void testInvalidateNamesRejectsInFlightLoad() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            AtomicInteger loadCount = new AtomicInteger();
            AtomicReference<List<Pair<String, String>>> publishedNames = new AtomicReference<>();
            MetaCache<String> cache = new MetaCache<>(
                    "databaseCache",
                    refreshExecutor,
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    10,
                    key -> {
                        int currentLoad = loadCount.incrementAndGet();
                        if (currentLoad == 1) {
                            firstLoadStarted.countDown();
                            Assert.assertTrue(releaseFirstLoad.await(3, TimeUnit.SECONDS));
                        }
                        return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                    },
                    publishedNames::set,
                    key -> Optional.of(key),
                    (key, value, cause) -> { });

            Future<List<String>> firstLoad = callers.submit(cache::listNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));
            Future<?> invalidation = callers.submit(cache::invalidateAll);
            invalidation.get(1, TimeUnit.SECONDS);
            releaseFirstLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-2"), firstLoad.get(3, TimeUnit.SECONDS));
            Assert.assertEquals("remote-2", cache.getRemoteName("local-2"));
            Assert.assertEquals(Lists.newArrayList(Pair.of("remote-2", "local-2")), publishedNames.get());
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseFirstLoad.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testInvalidateNamesRejectsInFlightRefresh() throws Exception {
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        CacheLoader<String, List<Pair<String, String>>> namesCacheLoader = key -> {
            int currentLoad = loadCount.incrementAndGet();
            if (currentLoad == 2) {
                refreshStarted.countDown();
                Assert.assertTrue(releaseRefresh.await(3, TimeUnit.SECONDS));
            }
            return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
        };
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.of(1),
                10,
                namesCacheLoader,
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            cache.refreshNamesForTest();
            Assert.assertTrue(refreshStarted.await(3, TimeUnit.SECONDS));
            cache.invalidateAll();
            releaseRefresh.countDown();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(Lists.newArrayList("local-3"), cache.listNames());
            Assert.assertEquals(3, loadCount.get());
        } finally {
            releaseRefresh.countDown();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testUpdateNamesDuringInFlightLoad() throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadStarted.countDown();
                    Assert.assertTrue(releaseLoad.await(3, TimeUnit.SECONDS));
                    return Lists.newArrayList(Pair.of("remote-1", "local-1"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> names = callers.submit(cache::listNames);
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            Future<?> update = callers.submit(() -> cache.updateCache("remote-2", "local-2", "meta-2", 2));
            update.get(1, TimeUnit.SECONDS);
            releaseLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-1", "local-2"), names.get(3, TimeUnit.SECONDS));
        } finally {
            releaseLoad.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testInvalidateNameDuringInFlightLoad() throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    if (loadCount.incrementAndGet() == 1) {
                        loadStarted.countDown();
                        Assert.assertTrue(releaseLoad.await(3, TimeUnit.SECONDS));
                        return Lists.newArrayList(
                                Pair.of("remote-1", "local-1"), Pair.of("remote-2", "local-2"));
                    }
                    return Lists.newArrayList(Pair.of("remote-2", "local-2"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> names = callers.submit(cache::listNames);
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            Future<?> invalidation = callers.submit(() -> cache.invalidate("local-1", 1));
            invalidation.get(1, TimeUnit.SECONDS);
            releaseLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-2"), names.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseLoad.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNamesLoaderRestoresInterrupt() throws InterruptedException {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    throw new InterruptedException("interrupted");
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            cache.listNames();
            Assert.fail("Expected names loading to fail");
        } catch (RuntimeException e) {
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNameMutationPublishesWhenNamesCacheIsEmpty() throws InterruptedException {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        Map<String, String> publishedNames = Maps.newConcurrentMap();
        publishedNames.put("existing", "remote-existing");
        AtomicInteger fullPublicationCount = new AtomicInteger();
        AtomicInteger loadCount = new AtomicInteger();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadCount.incrementAndGet();
                    throw new RuntimeException("listing should not be required for a known event entry");
                },
                names -> {
                    fullPublicationCount.incrementAndGet();
                    publishedNames.clear();
                    publishedNames.putAll(names.stream().collect(Collectors.toMap(Pair::value, Pair::key)));
                },
                (remoteName, localName) -> publishedNames.put(localName, remoteName),
                publishedNames::remove,
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            cache.updateCache("remote-1", "local-1", "meta-1", 1);
            Assert.assertEquals("remote-1", publishedNames.get("local-1"));
            cache.updateCache("remote-2", "local-2", "meta-2", 2);
            Assert.assertEquals("remote-existing", publishedNames.get("existing"));
            Assert.assertEquals("remote-2", cache.getRemoteName("local-2"));
            Assert.assertEquals(0, loadCount.get());
            Assert.assertEquals(0, fullPublicationCount.get());
            cache.invalidate("local-1", 1);
            Assert.assertFalse(publishedNames.containsKey("local-1"));
            Assert.assertEquals("remote-existing", publishedNames.get("existing"));
            cache.resetNames();
            Assert.assertEquals("remote-existing", publishedNames.get("existing"));
            Assert.assertEquals("remote-2", publishedNames.get("local-2"));
            Assert.assertEquals(0, fullPublicationCount.get());
        } finally {
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSameNameUpdateAndInvalidationAreSerialized() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        Map<String, String> publishedNames = Maps.newConcurrentMap();
        CountDownLatch objectUpdateReached = new CountDownLatch(1);
        CountDownLatch releaseObjectUpdate = new CountDownLatch(1);
        CountDownLatch invalidationStarted = new CountDownLatch(1);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                names -> { },
                (remoteName, localName) -> publishedNames.put(localName, remoteName),
                publishedNames::remove,
                key -> Optional.of(key),
                (key, value, cause) -> { });
        installBlockingObjectCache(cache, "local-1", objectUpdateReached, releaseObjectUpdate);

        try {
            Future<?> update = callers.submit(() -> cache.updateCache("remote-1", "local-1", "meta-1", 1));
            Assert.assertTrue(objectUpdateReached.await(3, TimeUnit.SECONDS));
            Future<?> invalidation = callers.submit(() -> {
                invalidationStarted.countDown();
                cache.invalidate("local-1", 1);
            });
            Assert.assertTrue(invalidationStarted.await(3, TimeUnit.SECONDS));

            try {
                invalidation.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException expected) {
                // The invalidation must wait for the same-name update to finish.
            }
            releaseObjectUpdate.countDown();
            update.get(3, TimeUnit.SECONDS);
            invalidation.get(3, TimeUnit.SECONDS);

            Assert.assertFalse(publishedNames.containsKey("local-1"));
            Assert.assertFalse(cache.tryGetMetaObj("local-1").isPresent());
            Assert.assertFalse(cache.getMetaObjById(1).isPresent());
        } finally {
            releaseObjectUpdate.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testBulkInvalidationDoesNotSplitEventObjectAndIdRoute() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        CountDownLatch objectUpdateReached = new CountDownLatch(1);
        CountDownLatch releaseObjectUpdate = new CountDownLatch(1);
        CountDownLatch invalidationStarted = new CountDownLatch(1);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> Optional.of(key),
                (key, value, cause) -> { });
        installBlockingObjectCache(cache, "local-1", objectUpdateReached, releaseObjectUpdate);

        try {
            Future<?> update = callers.submit(() -> cache.updateCache("remote-1", "local-1", "meta-1", 1));
            Assert.assertTrue(objectUpdateReached.await(3, TimeUnit.SECONDS));
            Future<?> invalidation = callers.submit(() -> {
                invalidationStarted.countDown();
                cache.invalidateObjects();
            });
            Assert.assertTrue(invalidationStarted.await(3, TimeUnit.SECONDS));

            try {
                invalidation.get(1, TimeUnit.SECONDS);
                Assert.fail("Bulk invalidation must not split an in-flight event publication");
            } catch (TimeoutException expected) {
                // The generation swap waits for the short object/ID publication section.
            }
            releaseObjectUpdate.countDown();
            update.get(3, TimeUnit.SECONDS);
            invalidation.get(3, TimeUnit.SECONDS);

            Assert.assertFalse(cache.tryGetMetaObj("local-1").isPresent());
            Assert.assertFalse(cache.getMetaObjById(1).isPresent());
        } finally {
            releaseObjectUpdate.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testExecuteIfCurrentRejectsActionRacingBulkInvalidation() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newSingleThreadExecutor();
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> Optional.of(key),
                (key, value, cause) -> { });
        cache.updateCache("remote-1", "local-1", "meta-1", 1);

        try {
            Future<Boolean> action = callers.submit(() -> cache.executeIfMetaObjCurrent(
                    "local-1", "meta-1", () -> {
                        actionStarted.countDown();
                        try {
                            return releaseAction.await(3, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(e);
                        }
                    }));
            Assert.assertTrue(actionStarted.await(3, TimeUnit.SECONDS));
            cache.invalidateObjects();
            releaseAction.countDown();

            Assert.assertFalse(action.get(3, TimeUnit.SECONDS));
            Assert.assertFalse(cache.tryGetMetaObj("local-1").isPresent());
            Assert.assertFalse(cache.getMetaObjById(1).isPresent());
        } finally {
            releaseAction.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unchecked")
    private void installBlockingObjectCache(MetaCache<String> cache, String blockedName,
            CountDownLatch objectUpdateReached, CountDownLatch releaseObjectUpdate) throws Exception {
        LoadingCache<String, Optional<String>> delegate = cache.getMetaObjCache();
        ConcurrentMap<String, Optional<String>> delegateMap = delegate.asMap();
        ConcurrentMap<String, Optional<String>> blockingMap =
                (ConcurrentMap<String, Optional<String>>) Proxy.newProxyInstance(
                        ConcurrentMap.class.getClassLoader(), new Class<?>[] {ConcurrentMap.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("compute") && blockedName.equals(args[0])) {
                                BiFunction<String, Optional<String>, Optional<String>> computation =
                                        (BiFunction<String, Optional<String>, Optional<String>>) args[1];
                                Object[] wrappedArgs = args.clone();
                                wrappedArgs[1] = (BiFunction<String, Optional<String>, Optional<String>>)
                                        (key, value) -> {
                                            Optional<String> result = computation.apply(key, value);
                                            if (result != null && result.isPresent()) {
                                                awaitObjectUpdate(objectUpdateReached, releaseObjectUpdate);
                                            }
                                            return result;
                                        };
                                return invoke(method, delegateMap, wrappedArgs);
                            }
                            return invoke(method, delegateMap, args);
                        });
        LoadingCache<String, Optional<String>> blockingCache =
                (LoadingCache<String, Optional<String>>) Proxy.newProxyInstance(
                        LoadingCache.class.getClassLoader(), new Class<?>[] {LoadingCache.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("asMap")) {
                                return blockingMap;
                            }
                            if (method.getName().equals("put") && blockedName.equals(args[0])) {
                                awaitObjectUpdate(objectUpdateReached, releaseObjectUpdate);
                            }
                            return invoke(method, delegate, args);
                        });
        Field metaObjCacheField = MetaCache.class.getDeclaredField("metaObjCache");
        metaObjCacheField.setAccessible(true);
        metaObjCacheField.set(cache, blockingCache);
    }

    private void awaitObjectUpdate(CountDownLatch objectUpdateReached, CountDownLatch releaseObjectUpdate) {
        objectUpdateReached.countDown();
        try {
            Assert.assertTrue(releaseObjectUpdate.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        }
    }

    private Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testConcurrentColdReadersJoinSameLoad() throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadCount.incrementAndGet();
                    loadStarted.countDown();
                    Assert.assertTrue(releaseLoad.await(3, TimeUnit.SECONDS));
                    return Lists.newArrayList(Pair.of("remote-1", "local-1"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> first = callers.submit(cache::listNames);
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            Future<List<String>> second = callers.submit(cache::listNames);
            releaseLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-1"), first.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(Lists.newArrayList("local-1"), second.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(1, loadCount.get());
        } finally {
            releaseLoad.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testGenerationAdvanceReleasesJoinedReaders() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<Thread> joinedThread = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    if (loadCount.incrementAndGet() == 1) {
                        firstLoadStarted.countDown();
                        Assert.assertTrue(releaseFirstLoad.await(3, TimeUnit.SECONDS));
                        return Lists.newArrayList(Pair.of("remote-old", "local-old"));
                    }
                    return Lists.newArrayList(Pair.of("remote-new", "local-new"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> first = callers.submit(cache::listNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));
            Future<List<String>> joined = callers.submit(() -> {
                joinedThread.set(Thread.currentThread());
                return cache.listNames();
            });
            waitForWaiting(joinedThread);

            cache.updateCache("remote-event", "local-event", "meta-event", 1);
            Assert.assertEquals(
                    Lists.newArrayList("local-new", "local-event"), joined.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(2, loadCount.get());
            Assert.assertFalse(first.isDone());

            releaseFirstLoad.countDown();
            Assert.assertEquals(
                    Lists.newArrayList("local-new", "local-event"), first.get(3, TimeUnit.SECONDS));
        } finally {
            releaseFirstLoad.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testObsoleteForegroundLoadsArePhysicallyBounded() throws Exception {
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CountDownLatch secondLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoads = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(3);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        firstLoadStarted.countDown();
                        Assert.assertTrue(releaseLoads.await(3, TimeUnit.SECONDS));
                    } else if (currentLoad == 2) {
                        secondLoadStarted.countDown();
                        Assert.assertTrue(releaseLoads.await(3, TimeUnit.SECONDS));
                    }
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> first = callers.submit(cache::listNames);
            Assert.assertTrue(firstLoadStarted.await(3, TimeUnit.SECONDS));
            cache.updateCache("event-remote-1", "event-local-1", "meta-1", 1);

            Future<List<String>> second = callers.submit(cache::listNames);
            Assert.assertTrue(secondLoadStarted.await(3, TimeUnit.SECONDS));
            cache.updateCache("event-remote-2", "event-local-2", "meta-2", 2);

            Future<List<String>> newest = callers.submit(cache::listNames);
            try {
                newest.get(3, TimeUnit.SECONDS);
                Assert.fail("Expected the bounded load admission to fail fast");
            } catch (java.util.concurrent.ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            }
            Assert.assertEquals(2, loadCount.get());

            releaseLoads.countDown();
            List<String> firstResult = first.get(3, TimeUnit.SECONDS);
            List<String> secondResult = second.get(3, TimeUnit.SECONDS);
            Assert.assertTrue(loadCount.get() <= 3);
            Assert.assertTrue(firstResult.contains("event-local-2"));
            Assert.assertTrue(secondResult.contains("event-local-2"));
        } finally {
            releaseLoads.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testOldRefreshDoesNotBlockNewGenerationRefresh() throws Exception {
        CountDownLatch oldRefreshStarted = new CountDownLatch(1);
        CountDownLatch releaseOldRefresh = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 2) {
                        oldRefreshStarted.countDown();
                        Assert.assertTrue(releaseOldRefresh.await(3, TimeUnit.SECONDS));
                    }
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            cache.refreshNamesForTest();
            Assert.assertTrue(oldRefreshStarted.await(3, TimeUnit.SECONDS));

            cache.updateCache("remote-event", "local-event", "meta-event", 1);
            cache.refreshNamesForTest();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(3, loadCount.get());
            Assert.assertEquals(Lists.newArrayList("local-3"), cache.listNames());
        } finally {
            releaseOldRefresh.countDown();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testHungRefreshFlightsAreBoundedAndCatchUpLatestGeneration() throws Exception {
        CountDownLatch firstRefreshStarted = new CountDownLatch(1);
        CountDownLatch secondRefreshStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstRefresh = new CountDownLatch(1);
        CountDownLatch releaseSecondRefresh = new CountDownLatch(1);
        CountDownLatch latestRefreshCompleted = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newFixedThreadPool(3);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 2) {
                        firstRefreshStarted.countDown();
                        Assert.assertTrue(releaseFirstRefresh.await(3, TimeUnit.SECONDS));
                    } else if (currentLoad == 3) {
                        secondRefreshStarted.countDown();
                        Assert.assertTrue(releaseSecondRefresh.await(3, TimeUnit.SECONDS));
                    } else if (currentLoad == 4) {
                        latestRefreshCompleted.countDown();
                    }
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            cache.refreshNamesForTest();
            Assert.assertTrue(firstRefreshStarted.await(3, TimeUnit.SECONDS));

            cache.updateCache("remote-event-1", "local-event-1", "meta-event-1", 1);
            cache.refreshNamesForTest();
            Assert.assertTrue(secondRefreshStarted.await(3, TimeUnit.SECONDS));

            cache.updateCache("remote-event-2", "local-event-2", "meta-event-2", 2);
            cache.refreshNamesForTest();
            cache.updateCache("remote-event-3", "local-event-3", "meta-event-3", 3);
            cache.refreshNamesForTest();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(3, loadCount.get());

            releaseFirstRefresh.countDown();
            Assert.assertTrue(latestRefreshCompleted.await(3, TimeUnit.SECONDS));
            releaseSecondRefresh.countDown();
            refreshExecutor.shutdown();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertEquals(4, loadCount.get());
            Assert.assertEquals(Lists.newArrayList("local-4"), cache.listNames());
        } finally {
            releaseFirstRefresh.countDown();
            releaseSecondRefresh.countDown();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshDoesNotWaitForForegroundLoad() throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadCount.incrementAndGet();
                    loadStarted.countDown();
                    Assert.assertTrue(releaseLoad.await(3, TimeUnit.SECONDS));
                    return Lists.newArrayList(Pair.of("remote-1", "local-1"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> foreground = caller.submit(cache::listNames);
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            cache.refreshNamesForTest();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(1, loadCount.get());
            Assert.assertFalse(foreground.isDone());
            releaseLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-1"), foreground.get(3, TimeUnit.SECONDS));
        } finally {
            releaseLoad.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testLoadErrorCompletesJoinedReaders() throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch throwError = new CountDownLatch(1);
        AtomicReference<Thread> joinedThread = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    loadStarted.countDown();
                    Assert.assertTrue(throwError.await(3, TimeUnit.SECONDS));
                    throw new AssertionError("test load error");
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> owner = callers.submit(cache::listNames);
            Assert.assertTrue(loadStarted.await(3, TimeUnit.SECONDS));
            Future<List<String>> joined = callers.submit(() -> {
                joinedThread.set(Thread.currentThread());
                return cache.listNames();
            });
            waitForWaiting(joinedThread);
            throwError.countDown();
            assertLoadError(owner);
            assertLoadError(joined);
        } finally {
            throwError.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRetiredNamesLoadFailureRetriesCurrentGeneration() throws Exception {
        CountDownLatch obsoleteLoadStarted = new CountDownLatch(1);
        CountDownLatch failObsoleteLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 1) {
                        obsoleteLoadStarted.countDown();
                        Assert.assertTrue(failObsoleteLoad.await(3, TimeUnit.SECONDS));
                        throw new RuntimeException("obsolete client was closed");
                    }
                    return Lists.newArrayList(Pair.of("remote-2", "local-2"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> obsoleteOwner = caller.submit(cache::listNames);
            Assert.assertTrue(obsoleteLoadStarted.await(3, TimeUnit.SECONDS));
            cache.invalidateNames();
            Assert.assertEquals(Lists.newArrayList("local-2"), cache.listNames());
            failObsoleteLoad.countDown();
            Assert.assertEquals(Lists.newArrayList("local-2"), obsoleteOwner.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(2, loadCount.get());
        } finally {
            failObsoleteLoad.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRetiredNamesLoadErrorAlwaysPropagates() throws Exception {
        CountDownLatch obsoleteLoadStarted = new CountDownLatch(1);
        CountDownLatch failObsoleteLoad = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    if (loadCount.incrementAndGet() == 1) {
                        obsoleteLoadStarted.countDown();
                        Assert.assertTrue(failObsoleteLoad.await(3, TimeUnit.SECONDS));
                        throw new AssertionError("obsolete load error");
                    }
                    return Lists.newArrayList(Pair.of("remote-2", "local-2"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Future<List<String>> obsoleteOwner = caller.submit(cache::listNames);
            Assert.assertTrue(obsoleteLoadStarted.await(3, TimeUnit.SECONDS));
            cache.invalidateNames();
            Assert.assertEquals(Lists.newArrayList("local-2"), cache.listNames());
            failObsoleteLoad.countDown();
            assertLoadError(obsoleteOwner);
            Assert.assertEquals(2, loadCount.get());
        } finally {
            failObsoleteLoad.countDown();
            caller.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testInvalidateUsesObjectCacheMutationLock() throws Exception {
        CountDownLatch updateEntered = new CountDownLatch(1);
        CountDownLatch releaseUpdate = new CountDownLatch(1);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        AtomicReference<Thread> invalidationThread = new AtomicReference<>();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                ignored -> { },
                (remoteName, localName) -> { },
                ignored -> { },
                key -> Optional.of(key),
                (key, value, cause) -> { },
                () -> 0L,
                epoch -> {
                    updateEntered.countDown();
                    try {
                        Assert.assertTrue(releaseUpdate.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                    return true;
                });

        try {
            Future<Boolean> update = callers.submit(
                    () -> cache.updateCache("remote-1", "local-1", "meta-1", 1, 0L));
            Assert.assertTrue(updateEntered.await(3, TimeUnit.SECONDS));
            Future<?> invalidation = callers.submit(() -> {
                invalidationThread.set(Thread.currentThread());
                cache.invalidate("local-1", 1);
            });
            waitForBlocked(invalidationThread);
            Assert.assertFalse(invalidation.isDone());
            releaseUpdate.countDown();
            Assert.assertTrue(update.get(3, TimeUnit.SECONDS));
            invalidation.get(3, TimeUnit.SECONDS);
            Assert.assertFalse(cache.listNames().contains("local-1"));
            Assert.assertFalse(cache.tryGetMetaObj("local-1").isPresent());
            Assert.assertFalse(cache.getMetaObjById(1).isPresent());
        } finally {
            releaseUpdate.countDown();
            callers.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testUpdateCacheRejectsEventPausedAcrossReset() throws Exception {
        AtomicLong lifecycleEpoch = new AtomicLong();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch eventBuilt = new CountDownLatch(1);
        CountDownLatch publishEvent = new CountDownLatch(1);
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                ignored -> { },
                (remoteName, localName) -> { },
                ignored -> { },
                key -> Optional.empty(),
                (key, value, cause) -> { },
                lifecycleEpoch::get,
                epoch -> lifecycleEpoch.get() == epoch);

        try {
            Future<Boolean> eventUpdate = eventExecutor.submit(() -> {
                long eventEpoch = lifecycleEpoch.get();
                eventBuilt.countDown();
                Assert.assertTrue(publishEvent.await(3, TimeUnit.SECONDS));
                return cache.updateCache("old-remote", "old-local", "old-meta", 1, eventEpoch);
            });
            Assert.assertTrue(eventBuilt.await(3, TimeUnit.SECONDS));
            lifecycleEpoch.incrementAndGet();
            cache.invalidateAll();
            publishEvent.countDown();
            Assert.assertFalse(eventUpdate.get(3, TimeUnit.SECONDS));
            Assert.assertTrue(cache.listNames().isEmpty());
            Assert.assertFalse(cache.tryGetMetaObj("old-local").isPresent());
            Assert.assertFalse(cache.getMetaObjById(1).isPresent());
        } finally {
            publishEvent.countDown();
            eventExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
            Assert.assertTrue(eventExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private void waitForWaiting(AtomicReference<Thread> threadReference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            Thread thread = threadReference.get();
            if (thread != null && thread.getState() == Thread.State.WAITING) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.fail("Thread did not start waiting");
    }

    private void waitForBlocked(AtomicReference<Thread> threadReference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            Thread thread = threadReference.get();
            if (thread != null && thread.getState() == Thread.State.BLOCKED) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.fail("Thread did not become blocked");
    }

    private void assertLoadError(Future<List<String>> future) throws Exception {
        try {
            future.get(3, TimeUnit.SECONDS);
            Assert.fail("Expected load error");
        } catch (java.util.concurrent.ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof AssertionError);
        }
    }

    @Test
    public void testNamesLoadFailsAfterBoundedConcurrentMutations() throws InterruptedException {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<MetaCache<String>> cacheReference = new AtomicReference<>();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    cacheReference.get().updateCache(
                            "event-remote-" + currentLoad, "event-local-" + currentLoad,
                            "meta-" + currentLoad, currentLoad);
                    return Lists.newArrayList(Pair.of("remote-1", "local-1"));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });
        cacheReference.set(cache);

        try {
            cache.listNames();
            Assert.fail("Expected bounded names loading to fail");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("metadata kept changing"));
            Assert.assertEquals(4, loadCount.get());
        } finally {
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFailedNamesRefreshIsContainedAndRetried() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<Thread> failedRefreshThread = new AtomicReference<>();
        AtomicReference<Thread> successfulRefreshThread = new AtomicReference<>();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    if (currentLoad == 2) {
                        failedRefreshThread.set(Thread.currentThread());
                        throw new RuntimeException("test refresh failure");
                    }
                    if (currentLoad == 3) {
                        successfulRefreshThread.set(Thread.currentThread());
                    }
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            cache.refreshNamesForTest();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            cache.refreshNamesForTest();
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(3, loadCount.get());
            Assert.assertSame(failedRefreshThread.get(), successfulRefreshThread.get());
            Assert.assertEquals(Lists.newArrayList("local-3"), cache.listNames());
        } finally {
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRejectedNamesRefreshKeepsCurrentValue() throws Exception {
        AtomicBoolean rejectNextTask = new AtomicBoolean(true);
        ThreadPoolExecutor refreshExecutor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                if (rejectNextTask.compareAndSet(true, false)) {
                    throw new RejectedExecutionException("test rejection");
                }
                super.execute(command);
            }
        };
        AtomicInteger loadCount = new AtomicInteger();
        MetaCache<String> cache = new MetaCache<>(
                "databaseCache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.of(0),
                10,
                key -> {
                    int currentLoad = loadCount.incrementAndGet();
                    return Lists.newArrayList(Pair.of("remote-" + currentLoad, "local-" + currentLoad));
                },
                key -> Optional.of(key),
                (key, value, cause) -> { });

        try {
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            Assert.assertEquals(1, loadCount.get());
            Assert.assertEquals(Lists.newArrayList("local-1"), cache.listNames());
            refreshExecutor.submit(() -> { }).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(2, loadCount.get());
            Assert.assertEquals(Lists.newArrayList("local-2"), cache.listNames());
        } finally {
            refreshExecutor.shutdownNow();
            Assert.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCacheExpiration() throws InterruptedException {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);
        Thread.sleep(2000);
        Optional<String> metaObj = metaCache.getMetaObj("local1", 1L);
        Assert.assertFalse(metaObj.isPresent());
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            final int id = i;
            executorService.submit(() -> {
                metaCache.updateCache("remote" + id, "local" + id, "meta" + id, id);
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        for (int i = 0; i < 10; i++) {
            Optional<String> metaObj = metaCache.getMetaObj("local" + i, i);
            Assert.assertTrue(metaObj.isPresent());
            Assert.assertEquals("meta" + i, metaObj.get());
        }
    }

    @Test
    public void testMetaObjCacheLoader() throws InterruptedException {

        CacheLoader<String, List<Pair<String, String>>> namesCacheLoader = key -> Lists.newArrayList();
        CountDownLatch latch = new CountDownLatch(2);
        CacheLoader<String, Optional<String>> metaObjCacheLoader = key -> {
            latch.countDown();
            return Optional.of("meta" + key);
        };

        RemovalListener<String, Optional<String>> removalListener = (key, value, cause) -> {};

        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                Executors.newCachedThreadPool(),
                OptionalLong.of(1),
                OptionalLong.of(1),
                100,
                namesCacheLoader,
                metaObjCacheLoader,
                removalListener
        );
        testCache.getMetaObj("local2", 1L);

        Optional<String> metaObj = testCache.getMetaObj("local1", 1L);
        Assert.assertTrue(metaObj.isPresent());
        Assert.assertEquals("metalocal1", metaObj.get());
        latch.await();

    }

    @Test
    public void testGetMetaObjCacheLoading() throws InterruptedException {
        // Create a CountDownLatch to track cache loading invocations
        CountDownLatch loadLatch = new CountDownLatch(2);

        // Create a custom cache loader that counts invocations
        CacheLoader<String, Optional<String>> metaObjCacheLoader = key -> {
            loadLatch.countDown();
            return Optional.of("loaded_" + key);
        };

        // Create a new MetaCache instance with our custom loader
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                Executors.newCachedThreadPool(),
                OptionalLong.of(1),
                OptionalLong.of(1),
                100,
                key -> Lists.newArrayList(),
                metaObjCacheLoader,
                (key, value, cause) -> {
                }
        );

        // Case 1: Test when key does not exist in cache (val == null)
        Optional<String> result1 = testCache.getMetaObj("non_existent_key", 1L);
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals("loaded_non_existent_key", result1.get());

        // Case 2: Test when key exists but value is empty Optional
        // First, manually put an empty Optional into cache
        testCache.getMetaObjCache().put("empty_key", Optional.empty());
        Optional<String> result2 = testCache.getMetaObj("empty_key", 2L);
        Assert.assertTrue(result2.isPresent());
        Assert.assertEquals("loaded_empty_key", result2.get());

        // Verify that cache loader was invoked exactly twice
        Assert.assertTrue(loadLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testGetMetaObjByIdRecoversAfterEmptyEntryEviction() {
        AtomicBoolean objectExists = new AtomicBoolean(false);
        ExecutorService executor = Executors.newCachedThreadPool();
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                executor,
                OptionalLong.of(1),
                OptionalLong.of(1),
                100,
                key -> Lists.newArrayList(),
                key -> objectExists.get() ? Optional.of("loaded_" + key) : Optional.empty(),
                (key, value, cause) -> {
                }
        );

        try {
            Assert.assertFalse(testCache.getMetaObj("recoverable_key", 1L).isPresent());
            testCache.getMetaObjCache().invalidate("recoverable_key");
            objectExists.set(true);

            Optional<String> recovered = testCache.getMetaObjById(1L);
            Assert.assertTrue(recovered.isPresent());
            Assert.assertEquals("loaded_recoverable_key", recovered.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateObjectsPreservesConcurrentReloadIdRoute() throws Exception {
        CountDownLatch removalStarted = new CountDownLatch(1);
        CountDownLatch releaseRemoval = new CountDownLatch(1);
        ExecutorService executor = Executors.newCachedThreadPool();
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                100,
                key -> Lists.newArrayList(),
                key -> Optional.of("reloaded_" + key),
                (key, value, cause) -> {
                    removalStarted.countDown();
                    try {
                        releaseRemoval.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                }
        );

        testCache.updateCache("remote_key", "local_key", "initial_value", 1L);
        Future<?> invalidation = executor.submit(testCache::invalidateObjects);
        try {
            Assert.assertTrue(removalStarted.await(3, TimeUnit.SECONDS));
            Future<Optional<String>> reload = executor.submit(() -> testCache.getMetaObj("local_key", 1L));
            Assert.assertEquals(Optional.of("reloaded_local_key"), reload.get(3, TimeUnit.SECONDS));

            releaseRemoval.countDown();
            invalidation.get(3, TimeUnit.SECONDS);
            Assert.assertEquals(Optional.of("reloaded_local_key"), testCache.getMetaObjById(1L));
        } finally {
            releaseRemoval.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReplacementGenerationBypassesRetiredObjectLoader() throws Exception {
        CountDownLatch retiredLoaderStarted = new CountDownLatch(1);
        CountDownLatch releaseRetiredLoader = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService executor = Executors.newCachedThreadPool();
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                100,
                key -> Lists.newArrayList(),
                key -> {
                    int load = loadCount.incrementAndGet();
                    if (load == 1) {
                        retiredLoaderStarted.countDown();
                        Assert.assertTrue(releaseRetiredLoader.await(5, TimeUnit.SECONDS));
                    }
                    return Optional.of("loaded-" + load);
                },
                (key, value, cause) -> { });

        Future<Optional<String>> retiredLoad = executor.submit(() -> testCache.getMetaObj("key", 1L));
        try {
            Assert.assertTrue(retiredLoaderStarted.await(3, TimeUnit.SECONDS));
            testCache.invalidateObjects();

            Future<Optional<String>> replacementLoad =
                    executor.submit(() -> testCache.getMetaObj("key", 1L));
            Assert.assertEquals(Optional.of("loaded-2"), replacementLoad.get(3, TimeUnit.SECONDS));
            Assert.assertEquals(2, loadCount.get());

            releaseRetiredLoader.countDown();
            Assert.assertEquals(Optional.of("loaded-2"), retiredLoad.get(3, TimeUnit.SECONDS));
        } finally {
            releaseRetiredLoader.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testPhysicalObjectLoadsRemainBoundedAcrossGenerations() throws Exception {
        CountDownLatch firstLoaderStarted = new CountDownLatch(1);
        CountDownLatch secondLoaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService executor = Executors.newCachedThreadPool();
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                100,
                key -> Lists.newArrayList(),
                key -> {
                    int load = loadCount.incrementAndGet();
                    if (load == 1) {
                        firstLoaderStarted.countDown();
                    } else if (load == 2) {
                        secondLoaderStarted.countDown();
                    }
                    releaseLoaders.await();
                    return Optional.of("loaded-" + load);
                },
                (key, value, cause) -> { });

        Future<Optional<String>> firstLoad = executor.submit(() -> testCache.getMetaObj("key", 1L));
        Future<Optional<String>> secondLoad = null;
        try {
            Assert.assertTrue(firstLoaderStarted.await(3, TimeUnit.SECONDS));
            testCache.invalidateObjects();
            secondLoad = executor.submit(() -> testCache.getMetaObj("key", 1L));
            Assert.assertTrue(secondLoaderStarted.await(3, TimeUnit.SECONDS));
            testCache.invalidateObjects();

            Future<Optional<String>> rejectedLoad = executor.submit(() -> testCache.getMetaObj("key", 1L));
            try {
                rejectedLoad.get(3, TimeUnit.SECONDS);
                Assert.fail("A third physical load for the same name must be rejected");
            } catch (java.util.concurrent.ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            }
            Assert.assertEquals(2, loadCount.get());
        } finally {
            firstLoad.cancel(true);
            if (secondLoad != null) {
                secondLoad.cancel(true);
            }
            releaseLoaders.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testGetMetaObjConcurrent() throws InterruptedException {
        // Create a CountDownLatch to track cache loading invocations
        CountDownLatch loadLatch = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger(0);

        // Create a custom cache loader that counts invocations and simulates slow loading
        CacheLoader<String, Optional<String>> metaObjCacheLoader = key -> {
            loadCount.incrementAndGet();
            Thread.sleep(100); // Simulate slow loading
            loadLatch.countDown();
            return Optional.of("loaded_" + key);
        };

        // Create a new MetaCache instance with our custom loader
        MetaCache<String> testCache = new MetaCache<>(
                "testCache",
                Executors.newCachedThreadPool(),
                OptionalLong.of(1),
                OptionalLong.of(1),
                100,
                key -> Lists.newArrayList(),
                metaObjCacheLoader,
                (key, value, cause) -> {
                }
        );

        // Test concurrent access to non-existent key
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Optional<String> result = testCache.getMetaObj("concurrent_key", 1L);
                    Assert.assertTrue(result.isPresent());
                    Assert.assertEquals("loaded_concurrent_key", result.get());
                } catch (Exception e) {
                    Assert.fail("Exception occurred: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Start all threads
        startLatch.countDown();
        // Wait for all threads to complete
        finishLatch.await(5, TimeUnit.SECONDS);
        // Wait for cache loading to complete
        loadLatch.await(5, TimeUnit.SECONDS);

        // Verify that cache loader was invoked exactly once
        Assert.assertEquals(1, loadCount.get());

        // Test concurrent access to existing but empty key
        loadCount.set(0);
        CountDownLatch loadLatch2 = new CountDownLatch(1);
        CacheLoader<String, Optional<String>> metaObjCacheLoader2 = key -> {
            loadCount.incrementAndGet();
            Thread.sleep(100); // Simulate slow loading
            loadLatch2.countDown();
            return Optional.of("loaded_" + key);
        };

        // Create another MetaCache instance
        MetaCache<String> testCache2 = new MetaCache<>(
                "testCache2",
                Executors.newCachedThreadPool(),
                OptionalLong.of(1),
                OptionalLong.of(1),
                100,
                key -> Lists.newArrayList(),
                metaObjCacheLoader2,
                (key, value, cause) -> {
                }
        );

        // Manually put an empty Optional into cache
        testCache2.getMetaObjCache().put("empty_concurrent_key", Optional.empty());

        // Reset latches for second test
        final CountDownLatch startLatch2 = new CountDownLatch(1);
        final CountDownLatch finishLatch2 = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                try {
                    startLatch2.await();
                    Optional<String> result = testCache2.getMetaObj("empty_concurrent_key", 2L);
                    Assert.assertTrue(result.isPresent());
                    Assert.assertEquals("loaded_empty_concurrent_key", result.get());
                } catch (Exception e) {
                    Assert.fail("Exception occurred: " + e.getMessage());
                } finally {
                    finishLatch2.countDown();
                }
            });
        }

        // Start all threads
        startLatch2.countDown();
        // Wait for all threads to complete
        finishLatch2.await(5, TimeUnit.SECONDS);
        // Wait for cache loading to complete
        loadLatch2.await(5, TimeUnit.SECONDS);

        // Verify that cache loader was invoked exactly once
        Assert.assertEquals(1, loadCount.get());

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }
}
