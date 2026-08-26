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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.Config;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MetaCacheEntryTest {

    @Test
    public void testCompactStringPayloadEstimate() {
        // Latin-1 characters weigh one byte, other characters two; the estimate scales with
        // string length so skewed payloads dominate their entries.
        long latin1 = MetaCacheWeightUtils.estimatedStringPayloadBytes("abc");
        long utf16 = MetaCacheWeightUtils.estimatedStringPayloadBytes("中文");
        Assert.assertEquals(3L, latin1);
        Assert.assertEquals(4L, utf16);
        Assert.assertTrue(MetaCacheWeightUtils.estimatedStringPayloadBytes("abcdefghijklmnopq")
                > latin1);
    }

    @Test
    public void testRefreshUsesConfiguredLoader() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            Map<String, String> properties = Maps.newHashMap();
            CacheSpec cacheSpec = CacheSpec.fromProperties(
                    properties,
                    "k.enable", true,
                    "k.ttl", CacheSpec.CACHE_NO_TTL,
                    "k.capacity", 10L);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor);

            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));

            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            loadingCache.refresh("k");

            long deadlineMs = System.currentTimeMillis() + 3000L;
            while (loadCounter.get() < 2 && System.currentTimeMillis() < deadlineMs) {
                Thread.sleep(20L);
            }
            Assert.assertTrue("refresh should trigger loader invocation", loadCounter.get() >= 2);
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testGetWithMissLoaderAndDisableAutoRefresh() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            Map<String, String> properties = Maps.newHashMap();
            CacheSpec cacheSpec = CacheSpec.fromProperties(
                    properties,
                    "k.enable", true,
                    "k.ttl", CacheSpec.CACHE_NO_TTL,
                    "k.capacity", 10L);
            AtomicInteger defaultLoaderCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> defaultLoaderCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor,
                    false);

            AtomicInteger missLoaderCounter = new AtomicInteger();
            Assert.assertEquals(Integer.valueOf(100), entry.get("k", key -> 100 + missLoaderCounter.getAndIncrement()));
            Assert.assertEquals(Integer.valueOf(100), entry.get("k"));
            Assert.assertEquals(1, missLoaderCounter.get());
            Assert.assertEquals(0, defaultLoaderCounter.get());

            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            Assert.assertFalse(loadingCache.policy().refreshAfterWrite().isPresent());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testStatsSnapshotTracksLoadAndLastError() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            Map<String, String> properties = Maps.newHashMap();
            CacheSpec cacheSpec = CacheSpec.fromProperties(
                    properties,
                    "k.enable", true,
                    "k.ttl", CacheSpec.CACHE_NO_TTL,
                    "k.capacity", 10L);

            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        if ("fail".equals(key)) {
                            throw new IllegalStateException("mock failure");
                        }
                        return 1;
                    },
                    cacheSpec,
                    refreshExecutor,
                    false);

            Assert.assertEquals(Integer.valueOf(1), entry.get("ok"));
            Assert.assertEquals(Integer.valueOf(1), entry.get("ok"));
            Assert.assertThrows(IllegalStateException.class, () -> entry.get("fail"));

            MetaCacheEntryStats failedStats = entry.stats();
            Assert.assertEquals(3L, failedStats.getRequestCount());
            Assert.assertEquals(1L, failedStats.getHitCount());
            Assert.assertEquals(2L, failedStats.getMissCount());
            Assert.assertEquals(1L, failedStats.getLoadSuccessCount());
            Assert.assertEquals(1L, failedStats.getLoadFailureCount());
            Assert.assertTrue(failedStats.getLastLoadSuccessTimeMs() > 0);
            Assert.assertTrue(failedStats.getLastLoadFailureTimeMs() > 0);
            Assert.assertTrue(failedStats.getLastError().contains("mock failure"));

            Assert.assertEquals(Integer.valueOf(101), entry.get("miss-loader", key -> 101));
            MetaCacheEntryStats recoveredStats = entry.stats();
            Assert.assertTrue(recoveredStats.getLastError().contains("mock failure"));
            Assert.assertTrue(recoveredStats.getLoadSuccessCount() >= 2L);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testStatsSnapshotContainsEffectiveEnabled() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            Map<String, String> properties = Maps.newHashMap();
            CacheSpec cacheSpec = CacheSpec.fromProperties(
                    properties,
                    "k.enable", true,
                    "k.ttl", 0L,
                    "k.capacity", 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> 1,
                    cacheSpec,
                    refreshExecutor,
                    false);

            MetaCacheEntryStats stats = entry.stats();
            Assert.assertTrue(stats.isConfigEnabled());
            Assert.assertFalse(stats.isEffectiveEnabled());
            Assert.assertEquals(10L, stats.getCapacity());
            Assert.assertEquals(0L, stats.getEstimatedSize());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testContextualOnlyEntryRejectsDefaultGet() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "contextual",
                    null,
                    cacheSpec,
                    refreshExecutor,
                    false,
                    true);

            UnsupportedOperationException exception = Assert.assertThrows(
                    UnsupportedOperationException.class, () -> entry.get("k"));
            Assert.assertTrue(exception.getMessage().contains("contextual miss loader"));
            Assert.assertEquals(Integer.valueOf(7), entry.get("k", key -> 7));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDeduplicatesSameKey() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return loadCounter.incrementAndGet();
                    },
                    cacheSpec,
                    refreshExecutor,
                    false);

            Future<Integer> first = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<Integer> second = queryExecutor.submit(() -> entry.get("k"));
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(1), second.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(1, loadCounter.get());
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDoesNotPutAfterInvalidate() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return loadCounter.incrementAndGet();
                    },
                    cacheSpec,
                    refreshExecutor,
                    false);

            Future<Integer> first = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertEquals(2, loadCounter.get());
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadRemovesValueWhenInvalidateHappensBeforePut() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch beforePutStarted = new CountDownLatch(1);
            CountDownLatch releaseBeforePut = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor,
                    false) {
                @Override
                void beforeManualCachePutForTest(String key, Integer loaded) {
                    beforePutStarted.countDown();
                    awaitLatch(releaseBeforePut);
                }
            };

            Future<Integer> first = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(beforePutStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseBeforePut.countDown();

            Assert.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertEquals(2, loadCounter.get());
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testExplicitPutWinsAgainstInFlightManualLoad() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch beforePutStarted = new CountDownLatch(1);
        CountDownLatch releaseBeforePut = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test", key -> 1, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor, false) {
                @Override
                void beforeManualCachePutForTest(String key, Integer loaded) {
                    beforePutStarted.countDown();
                    awaitLatch(releaseBeforePut);
                }
            };

            Future<Integer> load = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(beforePutStarted.await(3L, TimeUnit.SECONDS));
            entry.put("k", 2);
            releaseBeforePut.countDown();

            Assert.assertEquals(Integer.valueOf(1), load.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(2), entry.peekIfPresent("k"));
        } finally {
            releaseBeforePut.countDown();
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testWeightedReplacementDoesNotQueueOldValuesOnRefreshExecutor() throws Exception {
        CountDownLatch workerBlocked = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor refreshExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        ExternalMetaCacheBudgetManager budgetManager =
                new ExternalMetaCacheBudgetManager(OptionalLong.of(1_000L));
        ExternalMetaCacheBudgetManager.EntryBudget entryBudget = budgetManager.createEntryBudget(
                1L, "test", "value", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "value", key -> new byte[1], CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), entryBudget);
        try {
            refreshExecutor.execute(() -> {
                workerBlocked.countDown();
                awaitLatch(releaseWorker);
            });
            Assert.assertTrue(workerBlocked.await(3L, TimeUnit.SECONDS));

            entry.put("k", new byte[100]);
            for (int i = 0; i < 100; i++) {
                entry.put("k", new byte[100]);
            }

            Assert.assertTrue("removal callbacks must not retain replaced values in the executor queue",
                    refreshExecutor.getQueue().isEmpty());
            Assert.assertEquals(accountedWeight(100L), entry.stats().getEstimatedWeight());
        } finally {
            releaseWorker.countDown();
            entry.close();
            refreshExecutor.shutdown();
            Assert.assertTrue(refreshExecutor.awaitTermination(3L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testWeightedFirstPublicationInvokesReplacementListener() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager =
                new ExternalMetaCacheBudgetManager(OptionalLong.of(1_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "publication", OptionalLong.empty(), OptionalLong.empty());
        AtomicInteger publications = new AtomicInteger();
        AtomicReference<byte[]> previous = new AtomicReference<>();
        byte[] value = new byte[10];
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "publication", key -> value,
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_000L),
                refreshExecutor, false, false,
                (key, loaded) -> MetaCacheSizeEstimate.complete(loaded.length), budget,
                (key, oldValue, currentValue) -> {
                    publications.incrementAndGet();
                    previous.set(oldValue);
                    Assert.assertSame(value, currentValue);
                });
        try {
            entry.put("k", value);

            Assert.assertEquals(1, publications.get());
            Assert.assertNull(previous.get());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCountEntryLoadAndRefreshInvokeReplacementListener() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger publications = new AtomicInteger();
        AtomicReference<Integer> refreshPrevious = new AtomicReference<>();
        AtomicReference<Integer> refreshCurrent = new AtomicReference<>();
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "publication", key -> loads.incrementAndGet(),
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, true, false, null, null,
                (key, previousValue, currentValue) -> {
                    publications.incrementAndGet();
                    if (previousValue != null) {
                        refreshPrevious.set(previousValue);
                        refreshCurrent.set(currentValue);
                    }
                });
        try {
            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            Assert.assertEquals(1, publications.get());

            entry.triggerRefreshForTest("k");
            refreshExecutor.submit(() -> { }).get(3L, TimeUnit.SECONDS);

            Assert.assertEquals(Integer.valueOf(2), entry.peekIfPresent("k"));
            Assert.assertEquals(2, loads.get());
            Assert.assertEquals(2, publications.get());
            Assert.assertEquals(Integer.valueOf(1), refreshPrevious.get());
            Assert.assertEquals(Integer.valueOf(2), refreshCurrent.get());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testQueuedWeightedRefreshDoesNotCaptureCurrentValue() throws Exception {
        CountDownLatch workerBlocked = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor refreshExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger loaderCalls = new AtomicInteger();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "refresh-capture", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "refresh-capture", key -> {
                    loaderCalls.incrementAndGet();
                    return new byte[2];
                }, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            refreshExecutor.execute(() -> {
                workerBlocked.countDown();
                awaitLatch(releaseWorker);
            });
            Assert.assertTrue(workerBlocked.await(3L, TimeUnit.SECONDS));

            byte[] currentValue = new byte[1];
            entry.put("k", currentValue);
            entry.triggerRefreshForTest("k");

            Runnable queuedRefresh = refreshExecutor.getQueue().peek();
            Assert.assertNotNull(queuedRefresh);
            for (Field field : queuedRefresh.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                Assert.assertNotSame("queued refresh must not directly retain the cached value",
                        currentValue, field.get(queuedRefresh));
            }

            entry.invalidateAll();
            releaseWorker.countDown();
            refreshExecutor.shutdown();
            Assert.assertTrue(refreshExecutor.awaitTermination(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, loaderCalls.get());
        } finally {
            releaseWorker.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testIdentityConditionalFenceSuppressesOlderWeightedRefresh() throws Exception {
        CountDownLatch workerBlocked = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor refreshExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger loaderCalls = new AtomicInteger();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "refresh-fence", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "refresh-fence", key -> {
                    loaderCalls.incrementAndGet();
                    return new byte[2];
                }, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            refreshExecutor.execute(() -> {
                workerBlocked.countDown();
                awaitLatch(releaseWorker);
            });
            Assert.assertTrue(workerBlocked.await(3L, TimeUnit.SECONDS));

            byte[] currentValue = new byte[1];
            entry.put("k", currentValue);
            entry.triggerRefreshForTest("k");
            Assert.assertFalse(refreshExecutor.getQueue().isEmpty());

            Assert.assertTrue(entry.fenceInFlightLoadIfSame("k", currentValue));
            Assert.assertSame(currentValue, entry.peekIfPresent("k"));
            releaseWorker.countDown();
            refreshExecutor.shutdown();
            Assert.assertTrue(refreshExecutor.awaitTermination(3L, TimeUnit.SECONDS));

            Assert.assertEquals("the older refresh must be rejected before it calls the loader",
                    0, loaderCalls.get());
            Assert.assertSame(currentValue, entry.peekIfPresent("k"));
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());
        } finally {
            releaseWorker.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testIdentityConditionalFenceSuppressesOlderCountRefresh() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        CountDownLatch loaderFinished = new CountDownLatch(1);
        MetaCacheEntry<String, String> entry = new MetaCacheEntry<>(
                "count-refresh-fence", key -> {
                    loaderEntered.countDown();
                    awaitLatch(releaseLoader);
                    loaderFinished.countDown();
                    return "stale";
                },
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L), refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length()), null);
        try {
            String currentValue = new String("current");
            entry.put("k", currentValue);
            entry.triggerRefreshForTest("k");
            Assert.assertTrue(loaderEntered.await(3L, TimeUnit.SECONDS));

            Assert.assertTrue(entry.fenceInFlightLoadIfSame("k", currentValue));
            Assert.assertSame(currentValue, entry.peekIfPresent("k"));
            releaseLoader.countDown();
            Assert.assertTrue(loaderFinished.await(3L, TimeUnit.SECONDS));
            refreshExecutor.submit(() -> { }).get(3L, TimeUnit.SECONDS);
            Assert.assertSame("the event fence must suppress refresh write-back",
                    currentValue, entry.peekIfPresent("k"));
        } finally {
            releaseLoader.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentWeightedRefreshesForDifferentKeysDoNotFenceEachOther() throws Exception {
        ExecutorService refreshExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch loadersEntered = new CountDownLatch(2);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(4_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "multi-key-weighted-refresh", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "multi-key-weighted-refresh", key -> {
                    loadersEntered.countDown();
                    awaitLatch(releaseLoaders);
                    return new byte["a".equals(key) ? 2 : 3];
                }, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 4_000L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            entry.put("a", new byte[1]);
            entry.put("b", new byte[1]);
            entry.triggerRefreshForTest("a");
            entry.triggerRefreshForTest("b");
            Assert.assertTrue(loadersEntered.await(3L, TimeUnit.SECONDS));

            releaseLoaders.countDown();
            awaitValueLength(entry, "a", 2);
            awaitValueLength(entry, "b", 3);
            Assert.assertEquals(accountedWeight(2L) + accountedWeight(3L), manager.getGlobalUsedWeight());
        } finally {
            releaseLoaders.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentCountRefreshesForDifferentKeysDoNotFenceEachOther() throws Exception {
        ExecutorService refreshExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch loadersEntered = new CountDownLatch(2);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        MetaCacheEntry<String, String> entry = new MetaCacheEntry<>(
                "multi-key-count-refresh", key -> {
                    loadersEntered.countDown();
                    awaitLatch(releaseLoaders);
                    return key + "-refreshed";
                }, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length()), null);
        try {
            entry.put("a", "a-current");
            entry.put("b", "b-current");
            entry.triggerRefreshForTest("a");
            entry.triggerRefreshForTest("b");
            Assert.assertTrue(loadersEntered.await(3L, TimeUnit.SECONDS));

            releaseLoaders.countDown();
            awaitValue(entry, "a", "a-refreshed");
            awaitValue(entry, "b", "b-refreshed");
        } finally {
            releaseLoaders.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidatingOneKeyDoesNotSuppressAnotherKeysConcurrentMissAdmission() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch loadersEntered = new CountDownLatch(2);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(4_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "multi-key-miss", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "multi-key-miss", key -> {
                    loadersEntered.countDown();
                    awaitLatch(releaseLoaders);
                    return new byte[1];
                }, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 4_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            Future<byte[]> first = queryExecutor.submit(() -> entry.get("a"));
            Future<byte[]> second = queryExecutor.submit(() -> entry.get("b"));
            Assert.assertTrue(loadersEntered.await(3L, TimeUnit.SECONDS));

            entry.invalidateKey("a");
            releaseLoaders.countDown();
            first.get(3L, TimeUnit.SECONDS);
            second.get(3L, TimeUnit.SECONDS);

            Assert.assertNull(entry.peekIfPresent("a"));
            Assert.assertNotNull(entry.peekIfPresent("b"));
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());
        } finally {
            releaseLoaders.countDown();
            entry.close();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRemovalCleanupDoesNotDeadlockWithInvalidateAll() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "deadlock", OptionalLong.empty(), OptionalLong.empty());
        CountDownLatch removalListenerEntered = new CountDownLatch(1);
        CountDownLatch invalidateHasAdmissionLock = new CountDownLatch(1);
        CountDownLatch releaseRemovalListener = new CountDownLatch(1);
        AtomicBoolean armRemovalHook = new AtomicBoolean(false);
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "deadlock", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget) {
            @Override
            void beforeRemovalReleaseForTest(String key) {
                if (armRemovalHook.compareAndSet(true, false)) {
                    removalListenerEntered.countDown();
                    awaitLatch(releaseRemovalListener);
                }
            }

            @Override
            void beforeWeightedInvalidateAllForTest() {
                invalidateHasAdmissionLock.countDown();
            }
        };
        try {
            entry.put("k", new byte[1]);
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            armRemovalHook.set(true);

            Future<?> eviction = queryExecutor.submit(
                    () -> loadingCache.policy().eviction().get().setMaximum(1L));
            Assert.assertTrue(removalListenerEntered.await(3L, TimeUnit.SECONDS));
            Future<?> invalidate = queryExecutor.submit(entry::invalidateAll);
            Assert.assertTrue(invalidateHasAdmissionLock.await(3L, TimeUnit.SECONDS));

            releaseRemovalListener.countDown();
            eviction.get(3L, TimeUnit.SECONDS);
            invalidate.get(3L, TimeUnit.SECONDS);
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            releaseRemovalListener.countDown();
            entry.close();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDelayedRemovalDoesNotReleaseSameIdentityReinsert() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "same-identity-aba", OptionalLong.empty(), OptionalLong.empty());
        CountDownLatch oldRemovalBeforeOwnerSnapshot = new CountDownLatch(1);
        CountDownLatch replacementOwnerPublished = new CountDownLatch(1);
        CountDownLatch releaseOldRemoval = new CountDownLatch(1);
        CountDownLatch releaseReplacementPut = new CountDownLatch(1);
        CountDownLatch cleanupBeforeAdmissionLock = new CountDownLatch(1);
        CountDownLatch oldRemovalCleanupFinished = new CountDownLatch(1);
        AtomicBoolean armRemovalHook = new AtomicBoolean(false);
        AtomicBoolean armPutHook = new AtomicBoolean(false);
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "same-identity-aba", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget) {
            @Override
            void beforeRemovalOwnerSnapshotForTest(String key) {
                if (armRemovalHook.compareAndSet(true, false)) {
                    oldRemovalBeforeOwnerSnapshot.countDown();
                    awaitLatch(releaseOldRemoval);
                }
            }

            @Override
            void beforeWeightedCachePutForTest(String key, byte[] value) {
                if (armPutHook.compareAndSet(true, false)) {
                    replacementOwnerPublished.countDown();
                    awaitLatch(releaseReplacementPut);
                }
            }

            @Override
            void beforeRemovalCleanupLockForTest(String key) {
                cleanupBeforeAdmissionLock.countDown();
            }

            @Override
            void afterRemovalCleanupForTest(String key) {
                oldRemovalCleanupFinished.countDown();
            }
        };
        try {
            byte[] sameValue = new byte[1];
            entry.put("k", sameValue);
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            armRemovalHook.set(true);

            Future<?> oldRemoval = queryExecutor.submit(() -> loadingCache.invalidate("k"));
            Assert.assertTrue(oldRemovalBeforeOwnerSnapshot.await(3L, TimeUnit.SECONDS));
            armPutHook.set(true);
            Future<?> reinsert = queryExecutor.submit(() -> entry.put("k", sameValue));
            Assert.assertTrue(replacementOwnerPublished.await(3L, TimeUnit.SECONDS));

            releaseOldRemoval.countDown();
            oldRemoval.get(3L, TimeUnit.SECONDS);
            Assert.assertTrue(cleanupBeforeAdmissionLock.await(3L, TimeUnit.SECONDS));
            Assert.assertFalse("cleanup must wait for the publishing admission critical section",
                    reinsert.isDone());
            releaseReplacementPut.countDown();
            reinsert.get(3L, TimeUnit.SECONDS);
            Assert.assertTrue(oldRemovalCleanupFinished.await(3L, TimeUnit.SECONDS));
            Assert.assertSame(sameValue, entry.peekIfPresent("k"));
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());
        } finally {
            releaseOldRemoval.countDown();
            releaseReplacementPut.countDown();
            entry.close();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDelayedCountRemovalDoesNotDropSameIdentityRefreshOwner() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch oldRemovalBeforeOwnerSnapshot = new CountDownLatch(1);
        CountDownLatch replacementOwnerPublished = new CountDownLatch(1);
        CountDownLatch releaseOldRemoval = new CountDownLatch(1);
        CountDownLatch releaseReplacementPut = new CountDownLatch(1);
        CountDownLatch cleanupBeforeAdmissionLock = new CountDownLatch(1);
        CountDownLatch oldRemovalCleanupFinished = new CountDownLatch(1);
        AtomicBoolean armRemovalHook = new AtomicBoolean(false);
        AtomicBoolean armPutHook = new AtomicBoolean(false);
        AtomicInteger loaderCalls = new AtomicInteger();
        byte[] sameValue = new byte[1];
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "count-same-identity-aba", key -> {
                    loaderCalls.incrementAndGet();
                    return sameValue;
                }, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), null) {
            @Override
            void beforeRemovalOwnerSnapshotForTest(String key) {
                if (armRemovalHook.compareAndSet(true, false)) {
                    oldRemovalBeforeOwnerSnapshot.countDown();
                    awaitLatch(releaseOldRemoval);
                }
            }

            @Override
            void beforeNonWeightedCachePutForTest(String key, byte[] value) {
                if (armPutHook.compareAndSet(true, false)) {
                    replacementOwnerPublished.countDown();
                    awaitLatch(releaseReplacementPut);
                }
            }

            @Override
            void beforeRemovalCleanupLockForTest(String key) {
                cleanupBeforeAdmissionLock.countDown();
            }

            @Override
            void afterRemovalCleanupForTest(String key) {
                oldRemovalCleanupFinished.countDown();
            }
        };
        try {
            entry.put("k", sameValue);
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            armRemovalHook.set(true);

            Future<?> oldRemoval = queryExecutor.submit(() -> loadingCache.invalidate("k"));
            Assert.assertTrue(oldRemovalBeforeOwnerSnapshot.await(3L, TimeUnit.SECONDS));
            armPutHook.set(true);
            Future<?> reinsert = queryExecutor.submit(() -> entry.put("k", sameValue));
            Assert.assertTrue(replacementOwnerPublished.await(3L, TimeUnit.SECONDS));

            releaseOldRemoval.countDown();
            oldRemoval.get(3L, TimeUnit.SECONDS);
            Assert.assertTrue(cleanupBeforeAdmissionLock.await(3L, TimeUnit.SECONDS));
            Assert.assertFalse("cleanup must wait for the publishing admission critical section",
                    reinsert.isDone());
            releaseReplacementPut.countDown();
            reinsert.get(3L, TimeUnit.SECONDS);
            Assert.assertTrue(oldRemovalCleanupFinished.await(3L, TimeUnit.SECONDS));
            Assert.assertSame(sameValue, entry.peekIfPresent("k"));

            entry.triggerRefreshForTest("k");
            refreshExecutor.submit(() -> { }).get(3L, TimeUnit.SECONDS);
            Assert.assertEquals("the replacement refresh owner must remain usable", 1, loaderCalls.get());
            Assert.assertSame(sameValue, entry.peekIfPresent("k"));
        } finally {
            releaseOldRemoval.countDown();
            releaseReplacementPut.countDown();
            entry.close();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testExpiredSameIdentityCallbackKeepsCurrentCountRefreshOwner() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger loaderCalls = new AtomicInteger();
        byte[] sameValue = new byte[1];
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "count-expired-same-identity", key -> {
                    loaderCalls.incrementAndGet();
                    return sameValue;
                }, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), null);
        try {
            entry.put("k", sameValue);
            entry.put("k", sameValue);
            entry.notifyRemovalUnderAdmissionLockForTest("k", sameValue, RemovalCause.EXPIRED);

            entry.triggerRefreshForTest("k");
            refreshExecutor.submit(() -> { }).get(3L, TimeUnit.SECONDS);
            Assert.assertEquals("EXPIRED callback for the old mapping must retain the new refresh owner",
                    1, loaderCalls.get());
            Assert.assertSame(sameValue, entry.peekIfPresent("k"));
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testExpiredSameIdentityCallbackKeepsCurrentWeightedReservation() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted-expired-same-identity", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "weighted-expired-same-identity", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            byte[] sameValue = new byte[1];
            entry.put("k", sameValue);
            entry.put("k", sameValue);
            entry.notifyRemovalUnderAdmissionLockForTest("k", sameValue, RemovalCause.EXPIRED);

            Assert.assertSame(sameValue, entry.peekIfPresent("k"));
            Assert.assertEquals("EXPIRED callback for the old mapping must retain the new reservation",
                    accountedWeight(1L), manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testWeightedCacheUsesSoftValuesAndReleasesCollectedReservation() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "soft-value", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "soft-value", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            byte[] value = new byte[1];
            entry.put("k", value);
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            Reference<?> valueReference = extractValueReference(loadingCache);

            Assert.assertTrue("weighted values must be held through Caffeine SoftReference",
                    valueReference instanceof SoftReference);
            Map<?, ?> owners = (Map<?, ?>) readField(entry, "reservations");
            Object owner = owners.get("k");
            Assert.assertNotNull(owner);
            for (Field field : owner.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                Assert.assertNotSame("reservation ownership must not strongly retain V", value,
                        field.get(owner));
            }
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());

            valueReference.clear();
            Assert.assertTrue(valueReference.enqueue());
            loadingCache.cleanUp();

            awaitGlobalWeight(manager, 0L);
            Assert.assertNull(entry.peekIfPresent("k"));
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testAutomaticEvictionTelemetryKeepsExactWeightAboveWeigherLimit() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        long hugeEstimate = 3L << 30; // above Caffeine's int weigher limit
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(8L << 30));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "huge-eviction", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "huge-eviction", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 8L << 30),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(hugeEstimate), budget);
        try {
            entry.put("k", new byte[1]);
            Assert.assertEquals(accountedWeight(hugeEstimate), manager.getGlobalUsedWeight());
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            Reference<?> valueReference = extractValueReference(loadingCache);

            // A soft-value collection is an automatic eviction reported through Caffeine, whose
            // weigher saw at most Integer.MAX_VALUE; the statistics must report the reservation.
            valueReference.clear();
            Assert.assertTrue(valueReference.enqueue());
            loadingCache.cleanUp();
            awaitGlobalWeight(manager, 0L);

            Assert.assertEquals(accountedWeight(hugeEstimate), entry.stats().getEvictionWeight());
            Assert.assertTrue(entry.stats().getEvictionWeight() > Integer.MAX_VALUE);
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testStrongQueryReferenceSurvivesSoftValueCollectionChecks() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "query-reference", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "query-reference", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            byte[] queryReference = entry.get("k");
            WeakReference<byte[]> observed = new WeakReference<>(queryReference);

            for (int i = 0; i < 3; i++) {
                System.gc();
                extractLoadingCache(entry).cleanUp();
            }

            Assert.assertSame(queryReference, observed.get());
            Assert.assertSame(queryReference, entry.peekIfPresent("k"));
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDelayedCollectedCallbackCannotReleaseReplacementGeneration() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "collected-aba", OptionalLong.empty(), OptionalLong.empty());
        CountDownLatch collectedBeforeOwnerSnapshot = new CountDownLatch(1);
        CountDownLatch replacementOwnerPublished = new CountDownLatch(1);
        CountDownLatch releaseCollectedCallback = new CountDownLatch(1);
        CountDownLatch releaseReplacementPut = new CountDownLatch(1);
        CountDownLatch collectedCleanupFinished = new CountDownLatch(1);
        AtomicBoolean armRemovalHook = new AtomicBoolean(false);
        AtomicBoolean armPutHook = new AtomicBoolean(false);
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "collected-aba", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget) {
            @Override
            void beforeRemovalOwnerSnapshotForTest(String key) {
                if (armRemovalHook.compareAndSet(true, false)) {
                    collectedBeforeOwnerSnapshot.countDown();
                    awaitLatch(releaseCollectedCallback);
                }
            }

            @Override
            void beforeWeightedCachePutForTest(String key, byte[] value) {
                if (armPutHook.compareAndSet(true, false)) {
                    replacementOwnerPublished.countDown();
                    awaitLatch(releaseReplacementPut);
                }
            }

            @Override
            void afterRemovalCleanupForTest(String key) {
                collectedCleanupFinished.countDown();
            }
        };
        try {
            entry.put("k", new byte[1]);
            LoadingCache<String, byte[]> loadingCache = extractLoadingCache(entry);
            Reference<?> oldValueReference = extractValueReference(loadingCache);
            armRemovalHook.set(true);

            Future<?> collection = queryExecutor.submit(() -> {
                oldValueReference.clear();
                oldValueReference.enqueue();
                loadingCache.cleanUp();
            });
            Assert.assertTrue(collectedBeforeOwnerSnapshot.await(3L, TimeUnit.SECONDS));

            byte[] replacement = new byte[1];
            armPutHook.set(true);
            Future<?> replacementPut = queryExecutor.submit(() -> entry.put("k", replacement));
            Assert.assertTrue(replacementOwnerPublished.await(3L, TimeUnit.SECONDS));

            releaseCollectedCallback.countDown();
            collection.get(3L, TimeUnit.SECONDS);
            releaseReplacementPut.countDown();
            replacementPut.get(3L, TimeUnit.SECONDS);
            Assert.assertTrue(collectedCleanupFinished.await(3L, TimeUnit.SECONDS));

            Assert.assertSame(replacement, entry.peekIfPresent("k"));
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());
        } finally {
            releaseCollectedCallback.countDown();
            releaseReplacementPut.countDown();
            entry.close();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testOwnershipRecordsHaveNoGenericValueReference() {
        for (Class<?> nested : MetaCacheEntry.class.getDeclaredClasses()) {
            if (nested.getSimpleName().equals("ReservationRecord")
                    || nested.getSimpleName().equals("RefreshRecord")) {
                Assert.assertFalse(nested.getSimpleName() + " must not retain V",
                        Arrays.stream(nested.getDeclaredFields())
                                .anyMatch(field -> field.getType() == Object.class));
            }
        }
    }

    @Test
    public void testRemovalCleanupRetriesAfterTransientFailure() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "removal-retry", OptionalLong.empty(), OptionalLong.empty());
        CountDownLatch firstAttempt = new CountDownLatch(1);
        CountDownLatch cleanupFinished = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "removal-retry", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2_000L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget) {
            @Override
            void beforeRemovalCleanupLockForTest(String key) {
                if (attempts.incrementAndGet() == 1) {
                    firstAttempt.countDown();
                    throw new IllegalStateException("transient cleanup failure");
                }
            }

            @Override
            void afterRemovalCleanupForTest(String key) {
                cleanupFinished.countDown();
            }
        };
        try {
            entry.put("k", new byte[1]);
            extractLoadingCache(entry).invalidate("k");

            Assert.assertTrue(firstAttempt.await(3L, TimeUnit.SECONDS));
            Assert.assertTrue(cleanupFinished.await(3L, TimeUnit.SECONDS));
            Assert.assertTrue("cleanup should be retried", attempts.get() >= 2);
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testBulkInvalidateDoesNotEnqueueOneCleanupPerEntry() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        long maxWeight = 1_000L * accountedWeight(1L);
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(maxWeight));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "bulk-invalidate", OptionalLong.empty(), OptionalLong.empty());
        AtomicInteger queuedCleanupCount = new AtomicInteger();
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<String, byte[]>(
                "bulk-invalidate", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 2_000L, maxWeight),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget) {
            @Override
            void beforeRemovalReleaseForTest(String key) {
                queuedCleanupCount.incrementAndGet();
            }
        };
        try {
            for (int i = 0; i < 1_000; i++) {
                entry.put("k-" + i, new byte[1]);
            }

            entry.invalidateAll();

            Assert.assertEquals(0, queuedCleanupCount.get());
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testNonWeightedInvalidateLinearizesWithFinalManualPut() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch insideFinalPut = new CountDownLatch(1);
        CountDownLatch releaseFinalPut = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test", key -> 1, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor, false) {
                @Override
                void beforeNonWeightedManualCachePutForTest(String key, Integer loaded) {
                    insideFinalPut.countDown();
                    awaitLatch(releaseFinalPut);
                }
            };

            Future<Integer> load = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(insideFinalPut.await(3L, TimeUnit.SECONDS));
            Future<?> invalidate = queryExecutor.submit(() -> entry.invalidateKey("k"));
            Assert.assertFalse("invalidation must wait for the final put linearization point",
                    invalidate.isDone());
            releaseFinalPut.countDown();

            Assert.assertEquals(Integer.valueOf(1), load.get(3L, TimeUnit.SECONDS));
            invalidate.get(3L, TimeUnit.SECONDS);
            Assert.assertNull(entry.peekIfPresent("k"));
        } finally {
            releaseFinalPut.countDown();
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testNonWeightedInvalidateAllLinearizesWithFinalManualPut() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch insideFinalPut = new CountDownLatch(1);
        CountDownLatch releaseFinalPut = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test", key -> 1, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor, false) {
                @Override
                void beforeNonWeightedManualCachePutForTest(String key, Integer loaded) {
                    insideFinalPut.countDown();
                    awaitLatch(releaseFinalPut);
                }
            };

            Future<Integer> load = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(insideFinalPut.await(3L, TimeUnit.SECONDS));
            Future<?> invalidate = queryExecutor.submit(entry::invalidateAll);
            Assert.assertFalse("invalidation must wait for the final put linearization point",
                    invalidate.isDone());
            releaseFinalPut.countDown();

            Assert.assertEquals(Integer.valueOf(1), load.get(3L, TimeUnit.SECONDS));
            invalidate.get(3L, TimeUnit.SECONDS);
            Assert.assertNull(entry.peekIfPresent("k"));
        } finally {
            releaseFinalPut.countDown();
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadAllowsNullWithoutCaching() {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false);
            AtomicInteger missLoaderCounter = new AtomicInteger();

            // Verify manual miss load returns null directly and retries because null values are not cached.
            Assert.assertNull(entry.get("missing", key -> {
                missLoaderCounter.incrementAndGet();
                return null;
            }));
            Assert.assertNull(entry.getIfPresent("missing"));
            Assert.assertNull(entry.get("missing", key -> {
                missLoaderCounter.incrementAndGet();
                return null;
            }));
            Assert.assertEquals(2, missLoaderCounter.get());
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDoesNotCacheWhenEntryDisabled() {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor,
                    false);

            // Verify disabled entries bypass cache entirely even when manual miss load is enabled by config.
            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(2, loadCounter.get());

            entry.put("k", 100);
            Assert.assertNull(entry.getIfPresent("k"));
        } finally {
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testClosedEntryCanNotBeRepopulated() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));

            entry.close();

            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDoesNotWriteBackAcrossClose() throws Exception {
        boolean originalManualMissLoad = Config.enable_external_meta_cache_manual_miss_load;
        Config.enable_external_meta_cache_manual_miss_load = true;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch beforePut = new CountDownLatch(1);
        CountDownLatch releasePut = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test", key -> 1, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor, false) {
                @Override
                void beforeManualCachePutForTest(String key, Integer loaded) {
                    beforePut.countDown();
                    awaitLatch(releasePut);
                }
            };
            Future<Integer> load = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(beforePut.await(3L, TimeUnit.SECONDS));
            entry.close();
            releasePut.countDown();

            Assert.assertEquals(Integer.valueOf(1), load.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.peekIfPresent("k"));
        } finally {
            releasePut.countDown();
            Config.enable_external_meta_cache_manual_miss_load = originalManualMissLoad;
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCompareAndReplaceUsesIdentityAndPeekDoesNotPolluteStats() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, String> entry = new MetaCacheEntry<>(
                    "test", key -> "loaded", CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor, false);
            String current = new String("same");
            entry.put("k", current);

            Assert.assertSame(current, entry.peekIfPresent("k"));
            Assert.assertEquals(0L, entry.stats().getRequestCount());
            Assert.assertEquals(MetaCacheEntry.ReplaceResult.NOT_CURRENT,
                    entry.tryReplace("k", new String("same"), "wrong"));
            Assert.assertEquals(MetaCacheEntry.ReplaceResult.REPLACED,
                    entry.tryReplace("k", current, "new"));
            Assert.assertEquals("new", entry.peekIfPresent("k"));
            Assert.assertEquals(0L, entry.stats().getRequestCount());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testWeightedAdmissionAndReplacementAccounting() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(1_000L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted",
                key -> 30,
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_000L),
                refreshExecutor,
                false,
                false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.longValue()),
                budget);
        try {
            Assert.assertEquals(Integer.valueOf(30), entry.get("k"));
            Assert.assertEquals(accountedWeight(30L), manager.getGlobalUsedWeight());

            entry.put("k", 40);
            Assert.assertEquals(Integer.valueOf(40), entry.getIfPresent("k"));
            Assert.assertEquals(accountedWeight(40L), manager.getGlobalUsedWeight());

            entry.put("k", 10);
            Assert.assertEquals(Integer.valueOf(10), entry.getIfPresent("k"));
            Assert.assertEquals(accountedWeight(10L), manager.getGlobalUsedWeight());

            entry.invalidateKey("k");
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCaffeineWeigherOnlyReadsPreparedReservationWeight() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(1_000L));
        AtomicInteger estimateCalls = new AtomicInteger();
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted", key -> 1, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_000L),
                refreshExecutor, false, false,
                (key, value) -> {
                    estimateCalls.incrementAndGet();
                    return MetaCacheSizeEstimate.complete(value.longValue());
                }, budget);
        try {
            Integer first = Integer.valueOf(20);
            entry.put("k", first);
            Assert.assertEquals(1, estimateCalls.get());
            Assert.assertEquals(MetaCacheEntry.ReplaceResult.REPLACED,
                    entry.tryReplace("k", first, Integer.valueOf(30)));
            Assert.assertEquals(2, estimateCalls.get());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testIncompleteEstimateReturnsValueWithoutCaching() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(1_080L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted",
                key -> 30,
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_080L),
                refreshExecutor,
                false,
                false,
                (key, value) -> MetaCacheSizeEstimate.incomplete("unclassified_field"),
                budget);
        try {
            Assert.assertEquals(Integer.valueOf(30), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
            Assert.assertEquals(1L, entry.stats().getWeightAdmissionRejectedCount());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testNegativeEstimateFailsImmediately() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(100L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(50L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted",
                key -> 30,
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 50L),
                refreshExecutor,
                false,
                false,
                (key, value) -> MetaCacheSizeEstimate.complete(-1L),
                budget);
        try {
            Assert.assertThrows(IllegalArgumentException.class, () -> entry.put("k", 30));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testZeroEstimateIsRejectedWithoutCaching() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(100L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(50L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted", key -> 1, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 50L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(0L), budget);
        try {
            entry.put("k", 1);

            Assert.assertNull(entry.peekIfPresent("k"));
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
            Assert.assertEquals(1L, entry.stats().getWeightAdmissionRejectedCount());
            Assert.assertEquals("invalid_estimate", entry.stats().getLastWeightRejectReason());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testWeightedEntryEvictsItsOwnColdestValueBeforeAdmission() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(1_080L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted",
                String::length,
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_080L),
                refreshExecutor,
                false,
                false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.longValue()),
                budget);
        try {
            entry.put("first", 30);
            entry.put("second", 30);
            Assert.assertNull(entry.getIfPresent("first"));
            Assert.assertEquals(Integer.valueOf(30), entry.getIfPresent("second"));
            Assert.assertEquals(accountedWeight(30L), manager.getGlobalUsedWeight());
            Assert.assertEquals(1L, entry.stats().getEvictionCount());
            Assert.assertEquals(accountedWeight(30L), entry.stats().getEvictionWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testWeightedAdmissionCanReclaimMoreThanOneThousandSmallValues() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        long maxWeight = 1_500L * accountedWeight(1L);
        ExternalMetaCacheBudgetManager manager =
                new ExternalMetaCacheBudgetManager(OptionalLong.of(maxWeight));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "many-small-values", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "many-small-values", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10_000L, maxWeight),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            for (int i = 0; i < 1_500; i++) {
                entry.put("small-" + i, new byte[1]);
            }

            byte[] large = new byte[600_000];
            entry.put("large", large);

            Assert.assertSame(large, entry.peekIfPresent("large"));
            Assert.assertTrue(entry.stats().getEvictionCount() > 1_024L);
            Assert.assertTrue(entry.stats().getEstimatedWeight() <= maxWeight);
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testOversizedValueIsRejectedWithoutEvictingUsefulValues() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(2_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(1_100L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted", key -> 1, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1_100L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.longValue()), budget);
        try {
            entry.put("first", 20);
            entry.put("second", 20);
            entry.put("oversized", 600);

            Assert.assertEquals(Integer.valueOf(20), entry.peekIfPresent("first"));
            Assert.assertEquals(Integer.valueOf(20), entry.peekIfPresent("second"));
            Assert.assertNull(entry.peekIfPresent("oversized"));
            Assert.assertEquals(2L * accountedWeight(20L), manager.getGlobalUsedWeight());
            Assert.assertEquals(0L, entry.stats().getEvictionCount());
            Assert.assertEquals(1L, entry.stats().getWeightAdmissionRejectedCount());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRejectedAtomicReplacementKeepsExpectedValueUntilConditionalInvalidation() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(1_000L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(600L));
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted", key -> 1, CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 600L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.longValue()), budget);
        try {
            Integer current = Integer.valueOf(30);
            entry.put("k", current);
            Assert.assertEquals(MetaCacheEntry.ReplaceResult.REJECTED,
                    entry.tryReplace("k", current, Integer.valueOf(100)));
            Assert.assertSame(current, entry.peekIfPresent("k"));
            Assert.assertTrue(entry.invalidateKeyIfSame("k", current));
            Assert.assertNull(entry.peekIfPresent("k"));
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRejectedWeightedRefreshRetainsPreviousValue() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(600L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "refresh-reject", OptionalLong.empty(), OptionalLong.empty());
        byte[] current = new byte[1];
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "refresh-reject", key -> new byte[100],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 600L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            entry.put("k", current);
            entry.triggerRefreshForTest("k");
            refreshExecutor.submit(() -> { }).get(3L, TimeUnit.SECONDS);

            Assert.assertSame(current, entry.peekIfPresent("k"));
            Assert.assertEquals(accountedWeight(1L), manager.getGlobalUsedWeight());
            Assert.assertEquals(1L, entry.stats().getWeightAdmissionRejectedCount());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshFailureRetainsPreviousValueAndExecutorThread() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        String current = new String("current");
        MetaCacheEntry<String, String> entry = new MetaCacheEntry<>(
                "refresh-failure", key -> {
                    throw new IllegalStateException("temporary metastore failure");
                }, CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, true, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length()), null);
        try {
            entry.put("k", current);
            entry.triggerRefreshForTest("k");
            AtomicBoolean executorStillAlive = new AtomicBoolean();
            refreshExecutor.submit(() -> executorStillAlive.set(true)).get(3L, TimeUnit.SECONDS);

            Assert.assertSame(current, entry.peekIfPresent("k"));
            Assert.assertTrue(executorStillAlive.get());
            Assert.assertEquals(1L, entry.stats().getLoadFailureCount());
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testPeerReclamationPreventsGlobalBudgetStarvation() throws Exception {
        long valueWeight = accountedWeight(1L);
        ExternalMetaCacheBudgetManager manager =
                new ExternalMetaCacheBudgetManager(OptionalLong.of(2L * valueWeight));
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager.EntryBudget firstBudget = manager.createEntryBudget(
                1L, "test", "first", OptionalLong.empty(), OptionalLong.empty());
        ExternalMetaCacheBudgetManager.EntryBudget secondBudget = manager.createEntryBudget(
                2L, "test", "second", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> first = new MetaCacheEntry<>(
                "first", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2L * valueWeight),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), firstBudget);
        MetaCacheEntry<String, byte[]> second = new MetaCacheEntry<>(
                "second", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 2L * valueWeight),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), secondBudget);
        try {
            first.put("a", new byte[1]);
            first.put("b", new byte[1]);
            second.put("c", new byte[1]);
            Assert.assertNull(second.peekIfPresent("c"));

            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (manager.getGlobalUsedWeight() > valueWeight && System.nanoTime() < deadlineNanos) {
                Thread.sleep(10L);
            }
            second.put("c", new byte[1]);

            Assert.assertNotNull(second.peekIfPresent("c"));
            Assert.assertTrue(manager.getGlobalUsedWeight() <= 2L * valueWeight);
        } finally {
            first.close();
            second.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDisabledWeightedEntrySkipsEstimator() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(100L));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "weighted", OptionalLong.empty(), OptionalLong.of(50L));
        AtomicInteger estimateCalls = new AtomicInteger();
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "weighted", key -> 30, CacheSpec.ofWeight(false, CacheSpec.CACHE_NO_TTL, 10L, 50L),
                refreshExecutor, false, false,
                (key, value) -> {
                    estimateCalls.incrementAndGet();
                    return MetaCacheSizeEstimate.complete(value.longValue());
                }, budget);
        try {
            Assert.assertEquals(Integer.valueOf(30), entry.get("k"));
            Assert.assertEquals(0, estimateCalls.get());
            Assert.assertNull(entry.peekIfPresent("k"));
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    // Keep the loader blocking helper in one place so concurrent tests stay readable.
    private void awaitLatch(CountDownLatch latch) {
        try {
            Assert.assertTrue(latch.await(3L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void awaitValueLength(MetaCacheEntry<String, byte[]> entry, String key, int expectedLength)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (System.nanoTime() < deadlineNanos) {
            byte[] value = entry.peekIfPresent(key);
            if (value != null && value.length == expectedLength) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.assertEquals(expectedLength, entry.peekIfPresent(key).length);
    }

    private void awaitValue(MetaCacheEntry<String, String> entry, String key, String expected)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (System.nanoTime() < deadlineNanos) {
            if (expected.equals(entry.peekIfPresent(key))) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.assertEquals(expected, entry.peekIfPresent(key));
    }

    @Test
    public void testDeadReservationsReleaseBeforeDependencyRetirement() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(1L << 20));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "release-order", OptionalLong.empty(), OptionalLong.empty());
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "release-order", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1L << 20),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget, null,
                value -> "token",
                (key, token) -> {
                    listenerEntered.countDown();
                    awaitLatch(releaseListener);
                });
        try {
            entry.put("k", new byte[1500]);
            Assert.assertTrue(manager.getGlobalUsedWeight() >= 1500L);

            // Automatic eviction queues both the dead reservation and the dependency
            // notification for the shared cleanup worker.
            extractLoadingCache(entry).policy().eviction().get().setMaximum(1L);
            Assert.assertTrue(listenerEntered.await(3L, TimeUnit.SECONDS));

            // Dependency retirement can be arbitrarily slow (it is still blocked here), but the
            // dead reservation must already have released its global quota.
            Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        } finally {
            releaseListener.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRemovalListenerReceivesRemovedValuesButNotReplacements() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        java.util.List<String> removed = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                "removal-listener", key -> 1,
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, false, false, null, null, null,
                value -> value, (key, token) -> removed.add(key + "=" + token));
        try {
            entry.put("k", 1);
            entry.put("k", 2);
            entry.put("gone", 7);
            entry.invalidateKey("gone");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (removed.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(java.util.Collections.singletonList("gone=7"), removed);
            Assert.assertEquals(Integer.valueOf(2), entry.peekIfPresent("k"));

            entry.invalidateIf((key, value) -> Integer.valueOf(2).equals(value));
            Assert.assertNull(entry.peekIfPresent("k"));
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (removed.size() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(java.util.Arrays.asList("gone=7", "k=2"), removed);
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCloseDoesNotDropRemovalTokenClaimedByCleanupWorker() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch tokenClaimed = new CountDownLatch(1);
        CountDownLatch releaseClaimedToken = new CountDownLatch(1);
        CountDownLatch retired = new CountDownLatch(1);
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                "close-after-token-poll", key -> 1,
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, false, false, null, null, null,
                value -> value, (key, token) -> retired.countDown()) {
            @Override
            void afterRemovalNotificationPollForTest(String key) {
                tokenClaimed.countDown();
                awaitLatch(releaseClaimedToken);
            }
        };
        try {
            entry.put("k", 1);
            entry.invalidateKey("k");
            Assert.assertTrue(tokenClaimed.await(3L, TimeUnit.SECONDS));

            entry.close();
            Assert.assertEquals("the worker still owns the polled token", 1L, retired.getCount());
            releaseClaimedToken.countDown();

            Assert.assertTrue(retired.await(3L, TimeUnit.SECONDS));
        } finally {
            releaseClaimedToken.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRemovalPublicationAfterCloseDrainInvokesListenerInline() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService removalExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch callbackPassedOpenCheck = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch retired = new CountDownLatch(1);
        AtomicBoolean pauseCallback = new AtomicBoolean();
        MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                "publish-after-close-drain", key -> 1,
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                refreshExecutor, false, false, null, null, null,
                value -> value, (key, token) -> retired.countDown()) {
            @Override
            void beforeRemovalOwnerSnapshotForTest(String key) {
                if (pauseCallback.compareAndSet(true, false)) {
                    callbackPassedOpenCheck.countDown();
                    awaitLatch(releaseCallback);
                }
            }
        };
        try {
            entry.put("k", 1);
            pauseCallback.set(true);
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            Future<?> removal = removalExecutor.submit(() -> loadingCache.invalidate("k"));
            Assert.assertTrue(callbackPassedOpenCheck.await(3L, TimeUnit.SECONDS));

            entry.close();
            Assert.assertEquals("close drained before the callback published", 1L, retired.getCount());
            releaseCallback.countDown();
            removal.get(3L, TimeUnit.SECONDS);

            Assert.assertTrue(retired.await(3L, TimeUnit.SECONDS));
        } finally {
            releaseCallback.countDown();
            entry.close();
            removalExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testLocalEvictionStopsOnceTheDeficitIsReclaimed() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(1L << 20));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "skewed-eviction", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "skewed-eviction", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, 16_384L),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget);
        try {
            // One large cold value plus fifteen small warm ones; admitting a value that only
            // needs the large value's headroom must not flush the whole cold batch.
            entry.put("big", new byte[8192]);
            for (int i = 0; i < 15; i++) {
                entry.put("small_" + i, new byte[64]);
            }
            for (int i = 0; i < 15; i++) {
                Assert.assertNotNull(entry.getIfPresent("small_" + i));
            }

            entry.put("incoming", new byte[4096]);

            Assert.assertNotNull(entry.peekIfPresent("incoming"));
            Assert.assertNull("the cold large value pays for the admission", entry.peekIfPresent("big"));
            for (int i = 0; i < 15; i++) {
                Assert.assertNotNull("small_" + i + " must survive a satisfied admission",
                        entry.peekIfPresent("small_" + i));
            }
        } finally {
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testQueuedRemovalNotificationsDoNotRetainRemovedValues() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        java.util.List<Object> tokens = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(1L << 20));
        ExternalMetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                1L, "test", "removal-token", OptionalLong.empty(), OptionalLong.empty());
        MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                "removal-token", key -> new byte[1],
                CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10L, 1L << 20),
                refreshExecutor, false, false,
                (key, value) -> MetaCacheSizeEstimate.complete(value.length), budget, null,
                value -> (long) value.length, (key, token) -> {
                    tokens.add(token);
                    listenerEntered.countDown();
                    try {
                        releaseListener.await(5L, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        try {
            // Block the cleanup thread inside the first callback, then churn remove/refill cycles.
            entry.put("blocker", new byte[8]);
            entry.invalidateKey("blocker");
            Assert.assertTrue(listenerEntered.await(3L, TimeUnit.SECONDS));

            java.util.List<WeakReference<byte[]>> retired = new java.util.ArrayList<>();
            for (int i = 0; i < 8; i++) {
                byte[] value = new byte[64 * 1024];
                retired.add(new WeakReference<>(value));
                entry.put("k", value);
                entry.invalidateKey("k");
                value = null;
            }
            Assert.assertEquals("reservations are released with the removal", 0L, manager.getGlobalUsedWeight());
            for (int attempt = 0; attempt < 5 && retired.stream().anyMatch(ref -> ref.get() != null); attempt++) {
                System.gc();
                Thread.sleep(50L);
            }
            // The queued notifications carry only the extracted tokens; the removed values are
            // collectable while their reservations are already released.
            Assert.assertTrue("queued removal notifications must not retain removed values",
                    retired.stream().allMatch(ref -> ref.get() == null));

            releaseListener.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (tokens.size() < 9 && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(9, tokens.size());
            Assert.assertEquals(8L, tokens.get(0));
            Assert.assertEquals((long) (64 * 1024), tokens.get(8));
        } finally {
            releaseListener.countDown();
            entry.close();
            refreshExecutor.shutdownNow();
        }
    }

    private void awaitGlobalWeight(ExternalMetaCacheBudgetManager manager, long expected)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (manager.getGlobalUsedWeight() != expected && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10L);
        }
        Assert.assertEquals(expected, manager.getGlobalUsedWeight());
    }

    private Reference<?> extractValueReference(LoadingCache<?, ?> loadingCache) throws Exception {
        Object boundedLocalCache = readField(loadingCache, "cache");
        Map<?, ?> nodes = (Map<?, ?>) readField(boundedLocalCache, "data");
        Assert.assertEquals(1, nodes.size());
        Object node = nodes.values().iterator().next();
        Method valueReferenceMethod = findMethod(node.getClass(), "getValueReference");
        Object valueReference = valueReferenceMethod.invoke(node);
        Assert.assertTrue(valueReference instanceof Reference);
        return (Reference<?>) valueReference;
    }

    private Object readField(Object target, String name) throws Exception {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                // Continue through Caffeine's generated cache hierarchy.
            }
        }
        throw new NoSuchFieldException(name);
    }

    private Method findMethod(Class<?> type, String name) throws Exception {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                Method method = current.getDeclaredMethod(name);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                // Continue through Caffeine's generated node hierarchy.
            }
        }
        throw new NoSuchMethodException(name);
    }

    @SuppressWarnings("unchecked")
    private <V> LoadingCache<String, V> extractLoadingCache(MetaCacheEntry<String, V> entry) throws Exception {
        Field dataField = MetaCacheEntry.class.getDeclaredField("loadingData");
        dataField.setAccessible(true);
        Object raw = dataField.get(entry);
        Assert.assertTrue(raw instanceof LoadingCache);
        return (LoadingCache<String, V>) raw;
    }

    private static long accountedWeight(long estimatedPayloadBytes) {
        return estimatedPayloadBytes + MetaCacheEntry.FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES;
    }
}
