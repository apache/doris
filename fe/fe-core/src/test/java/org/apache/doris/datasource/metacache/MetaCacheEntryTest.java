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

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaCacheEntryTest {

    @Test
    public void testRefreshUsesConfiguredLoader() throws Exception {
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
    public void testStatsSnapshotContainsEvictionRate() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false);

            Assert.assertEquals(0D, entry.stats().getEvictionRate(), 0D);
            Assert.assertEquals(Integer.valueOf(1), entry.get("a"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("bb"));
            extractLoadingCache(entry).cleanUp();
            Assert.assertEquals(1L, entry.stats().getEvictionCount());
            Assert.assertEquals(0.5D, entry.stats().getEvictionRate(), 0D);
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
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDoesNotPutAfterInvalidate() throws Exception {
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
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadRemovesValueWhenInvalidateHappensBeforePut() throws Exception {
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
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadAllowsNullWithoutCaching() {
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
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testManualMissLoadDoesNotCacheWhenEntryDisabled() {
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
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testSyncRemovalListenerDisablesRefreshAndRunsSynchronously() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            AtomicInteger removalCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = MetaCacheEntry.withSyncRemovalListener(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    (key, value, cause) -> removalCounter.incrementAndGet());

            entry.get("abc");
            Assert.assertFalse(extractLoadingCache(entry).policy().refreshAfterWrite().isPresent());
            entry.invalidateAll();
            Assert.assertEquals(1, removalCounter.get());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshResultIsDroppedAfterInvalidate() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        int current = loadCounter.incrementAndGet();
                        if (current > 1) {
                            loaderStarted.countDown();
                            awaitLatch(releaseLoader);
                        }
                        return current;
                    },
                    cacheSpec,
                    refreshExecutor);

            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            extractLoadingCache(entry).refresh("k");
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseLoader.countDown();

            long deadlineMs = System.currentTimeMillis() + 3000L;
            while (loadCounter.get() < 2 && System.currentTimeMillis() < deadlineMs) {
                Thread.sleep(20L);
            }
            Assert.assertNull(entry.getIfPresent("k"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testComputeBumpsGenerationBeforeMutation() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    cacheSpec,
                    refreshExecutor,
                    false);

            Future<Integer> loaded = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(100), entry.compute("k", (key, value) -> 100));
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), loaded.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(100), entry.getIfPresent("k"));
        } finally {
            queryExecutor.shutdownNow();
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

    @SuppressWarnings("unchecked")
    private LoadingCache<String, Integer> extractLoadingCache(MetaCacheEntry<String, Integer> entry) throws Exception {
        Field dataField = MetaCacheEntry.class.getDeclaredField("loadingData");
        dataField.setAccessible(true);
        Object raw = dataField.get(entry);
        Assert.assertTrue(raw instanceof LoadingCache);
        return (LoadingCache<String, Integer>) raw;
    }
}
