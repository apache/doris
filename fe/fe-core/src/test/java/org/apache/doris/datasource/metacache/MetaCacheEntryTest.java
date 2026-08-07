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
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class MetaCacheEntryTest {

    @Test
    public void testDefaultConstructorsUseConfiguredObjectStripeCount() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        int originalStripeCount = Config.external_meta_cache_object_entry_lock_stripes;
        try {
            Config.external_meta_cache_object_entry_lock_stripes = 8;
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);

            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> 1,
                    cacheSpec,
                    refreshExecutor,
                    false);
            MetaCacheEntry<String, Integer> syncRemovalEntry = MetaCacheEntry.withSyncRemovalListener(
                    "test.sync",
                    key -> 1,
                    cacheSpec,
                    refreshExecutor,
                    (key, value, cause) -> {
                    });

            Assert.assertEquals(8, entry.stripeCountForTest());
            Assert.assertEquals(8, syncRemovalEntry.stripeCountForTest());
            Assert.assertEquals(0, entry.initializedStripeCountForTest());
            Assert.assertEquals(0, syncRemovalEntry.initializedStripeCountForTest());
        } finally {
            Config.external_meta_cache_object_entry_lock_stripes = originalStripeCount;
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testExplicitSingleKeyStripeCountIsSupported() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "single-key",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());

            Assert.assertEquals(1, entry.stripeCountForTest());
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            Assert.assertEquals("single-key", entry.name());
            Assert.assertEquals(Integer.valueOf(1), entry.get("a"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidStripeCountIsRejected() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                    () -> new MetaCacheEntry<>("invalid", String::length, cacheSpec, refreshExecutor, false, 0));
            Assert.assertTrue(exception.getMessage().contains("stripeCount"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMultiKeyStripeStatesInitializeLazily() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "lazy",
                    String::length,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    8);

            Assert.assertEquals(0, entry.initializedStripeCountForTest());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.initializedStripeCountForTest());

            Assert.assertEquals(Integer.valueOf(1), entry.get("a"));
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            // "a" and "i" have hash codes in the same stripe when stripeCount is 8.
            Assert.assertEquals(Integer.valueOf(1), entry.get("i"));
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            Assert.assertEquals(Integer.valueOf(1), entry.get("b"));
            Assert.assertEquals(2, entry.initializedStripeCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDisabledEntryInitializesStateOnlyForCoordinatedMutation() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger actionCount = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "disabled",
                    String::length,
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    8);

            Assert.assertEquals(0, entry.initializedStripeCountForTest());
            Assert.assertEquals(Integer.valueOf(1), entry.get("a"));
            entry.invalidateAll();
            Assert.assertEquals(0, entry.initializedStripeCountForTest());

            entry.invalidateKeyAndRun("a", actionCount::incrementAndGet);
            Assert.assertEquals(1, actionCount.get());
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            entry.invalidateAll();
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

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
    public void testMaximumWeightUsesEstimatorAndExposesWeightStats() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "weighted",
                    String::length,
                    CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, 5L),
                    refreshExecutor,
                    false,
                    false,
                    (key, value) -> value);

            entry.put("first", 4);
            waitUntil(() -> entry.stats().getEstimatedWeight() == 4L);
            entry.put("second", 4);
            waitUntil(() -> entry.stats().getEvictionCount() == 1L);

            MetaCacheEntryStats stats = entry.stats();
            Assert.assertTrue(stats.isWeightBounded());
            Assert.assertEquals(5L, stats.getMaxWeight());
            Assert.assertTrue(stats.getEstimatedWeight() <= 5L);
            Assert.assertEquals(1L, stats.getEvictionCount());
            Assert.assertEquals(4L, stats.getEvictionWeight());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMaximumWeightRequiresEstimator() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            IllegalArgumentException exception = Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> new MetaCacheEntry<>(
                            "weighted",
                            String::length,
                            CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, 5L),
                            refreshExecutor,
                            false));
            Assert.assertTrue(exception.getMessage().contains("size estimator"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMaximumWeightRejectsNegativeEstimatorResult() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "weighted",
                    String::length,
                    CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, 5L),
                    refreshExecutor,
                    false,
                    false,
                    (key, value) -> -1L);

            IllegalStateException exception = Assert.assertThrows(
                    IllegalStateException.class, () -> entry.put("key", 1));
            Assert.assertTrue(exception.getMessage().contains("negative weight"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMaximumWeightSaturatesEstimatorResultToCaffeineLimit() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "weighted",
                    String::length,
                    CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, Integer.MAX_VALUE),
                    refreshExecutor,
                    false,
                    false,
                    (key, value) -> Long.MAX_VALUE);

            entry.put("key", 1);
            waitUntil(() -> entry.stats().getEstimatedWeight() == Integer.MAX_VALUE);
            MetaCacheEntryStats stats = entry.stats();
            Assert.assertEquals(1L, stats.getEstimatedSize());
            Assert.assertEquals(Integer.MAX_VALUE, stats.getEstimatedWeight());
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
    public void testContextualOnlyEntryRejectsNonNullDefaultLoader() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);

            // Contextual-only entries must not keep a default loader because callers provide the load context.
            IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                    () -> new MetaCacheEntry<>("contextual", key -> 1, cacheSpec, refreshExecutor, false, true));
            Assert.assertTrue(exception.getMessage().contains("contextual-only entry loader must be null"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testContextualOnlyEntryRejectsAutoRefresh() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);

            // Contextual-only entries cannot auto refresh because refresh does not carry the original context.
            IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                    () -> new MetaCacheEntry<>("contextual", null, cacheSpec, refreshExecutor, true, true));
            Assert.assertTrue(exception.getMessage().contains("contextual-only entry can not enable auto refresh"));
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
    public void testInvalidateAllWaitsForManualPublicationAndRemovesValue() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
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
            // Start invalidateAll() while manual publication holds the stripe monitor. Invalidation must wait for
            // publication to finish and then remove the published value.
            Future<?> invalidateFuture = mutationExecutor.submit(() -> {
                entry.invalidateAll();
                return null;
            });
            releaseBeforePut.countDown();
            invalidateFuture.get(3L, TimeUnit.SECONDS);

            Assert.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertEquals(2, loadCounter.get());
        } finally {
            mutationExecutor.shutdownNow();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testHotValueActionDoesNotRunAfterInvalidate() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch valueObserved = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    String::length,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false) {
                @Override
                protected void beforeCurrentValueActionForTest(String key, Integer value) {
                    valueObserved.countDown();
                    awaitLatch(releaseAction);
                }
            };
            AtomicInteger actionCounter = new AtomicInteger();
            entry.put("k", 1);

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("k", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(valueObserved.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseAction.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("k"));
        } finally {
            releaseAction.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testHotValueSkipsPublicationProtocolWhenActionIsNotRequired() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger protocolCounter = new AtomicInteger();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    String::length,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false) {
                @Override
                protected void beforeCurrentValueActionForTest(String key, Integer value) {
                    protocolCounter.incrementAndGet();
                }
            };
            AtomicInteger actionCounter = new AtomicInteger();
            entry.put("k", 1);

            Assert.assertEquals(Integer.valueOf(1), entry.getAndRunIfCurrent(
                    "k",
                    (key, value) -> false,
                    (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertEquals(0, protocolCounter.get());
            Assert.assertEquals(0, actionCounter.get());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRejectedMissLoadDoesNotRunCurrentValueAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("k", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testUnrelatedSameStripeInvalidateKeepsEnabledCurrentValueAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("b");
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(1, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentDisabledSameStripeActionLoadsFenceOnlyMatchingKey() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch bothLoadersStarted = new CountDownLatch(2);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        bothLoadersStarted.countDown();
                        awaitLatch(releaseLoaders);
                        return key.length();
                    },
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            AtomicInteger actionA = new AtomicInteger();
            AtomicInteger actionB = new AtomicInteger();

            Future<Integer> lookupA = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionA.incrementAndGet()));
            Future<Integer> lookupB = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("b", (key, value) -> actionB.incrementAndGet()));
            Assert.assertTrue(bothLoadersStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertEquals(2, entry.activeActionReferenceCountForTest());

            entry.invalidateKey("a");
            releaseLoaders.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookupA.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(1), lookupB.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionA.get());
            Assert.assertEquals(1, actionB.get());
            // Disabled entries allow distinct same-stripe loads to overlap. The exact-key action fence must
            // suppress only key "a", and neither result may enter the object cache.
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertNull(entry.getIfPresent("b"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoaders.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateIfFencesOnlyMatchingCurrentValueAction() throws Exception {
        assertInvalidateIfCurrentValueAction("b", 1);
        assertInvalidateIfCurrentValueAction("a", 0);
    }

    @Test
    public void testInvalidateAllFencesCurrentValueAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateAll();
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testAllSameKeyMutationsFenceCurrentValueAction() throws Exception {
        assertSameKeyMutationFencesCurrentValueAction(entry -> entry.put("a", 100));
        assertSameKeyMutationFencesCurrentValueAction(
                entry -> entry.compute("a", (key, current) -> 100));
        assertSameKeyMutationFencesCurrentValueAction(
                entry -> entry.computeAndRun("a", (key, current) -> 100, () -> {
                }));
        assertSameKeyMutationFencesCurrentValueAction(
                entry -> entry.invalidateKeyAndRun("a", () -> {
                }));
    }

    @Test
    public void testReentrantInvalidateBeforeManualPutFencesCurrentValueAction() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger hookCount = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    key -> 1,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false) {
                @Override
                void beforeManualCachePutForTest(String key, Integer loaded) {
                    if (hookCount.getAndIncrement() == 0) {
                        invalidateKey(key);
                    }
                }
            };
            AtomicInteger actionCounter = new AtomicInteger();

            Assert.assertEquals(Integer.valueOf(1),
                    entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));

            Assert.assertEquals(0, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDisabledEntryRunsCurrentValueActionWithoutCaching() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    String::length,
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            AtomicInteger actionValue = new AtomicInteger();

            Assert.assertEquals(Integer.valueOf(3),
                    entry.getAndRunIfCurrent("key", (key, value) -> actionValue.set(value)));
            Assert.assertEquals(3, actionValue.get());
            Assert.assertNull(entry.getIfPresent("key"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testUnrelatedSameStripeInvalidateKeepsDisabledCurrentValueAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("b");
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(1, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDisabledEntryDoesNotRunActionAfterConcurrentInvalidate() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("k", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testEnabledActionStateIsReleasedOnEveryExit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        if ("null".equals(key)) {
                            return null;
                        }
                        if ("load_failure".equals(key)) {
                            throw new IllegalStateException("load failure");
                        }
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);

            Assert.assertNull(entry.getAndRunIfCurrent("null", (key, value) -> {
            }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent("load_failure", (key, value) -> {
                    }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent(
                            "predicate_failure",
                            (key, value) -> {
                                throw new IllegalStateException("predicate failure");
                            },
                            (key, value) -> {
                            }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent("action_failure", (key, value) -> {
                        throw new IllegalStateException("action failure");
                    }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDisabledActionStateIsReleasedOnEveryExit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        if ("null".equals(key)) {
                            return null;
                        }
                        if ("load_failure".equals(key)) {
                            throw new IllegalStateException("load failure");
                        }
                        return 1;
                    },
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);

            Assert.assertNull(entry.getAndRunIfCurrent("null", (key, value) -> {
            }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent("load_failure", (key, value) -> {
                    }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent(
                            "predicate_failure",
                            (key, value) -> {
                                throw new IllegalStateException("predicate failure");
                            },
                            (key, value) -> {
                            }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.getAndRunIfCurrent("action_failure", (key, value) -> {
                        throw new IllegalStateException("action failure");
                    }));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testComputeAndInvalidateActionsRunForColdAndDisabledEntries() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger enabledActions = new AtomicInteger();
            MetaCacheEntry<String, Integer> enabledEntry = new MetaCacheEntry<>(
                    "enabled",
                    String::length,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            enabledEntry.computeAndRun(
                    "cold",
                    (key, current) -> current == null ? null : 1,
                    enabledActions::incrementAndGet);
            enabledEntry.invalidateKeyAndRun("cold", enabledActions::incrementAndGet);
            Assert.assertEquals(2, enabledActions.get());
            Assert.assertNull(enabledEntry.getIfPresent("cold"));

            AtomicInteger disabledActions = new AtomicInteger();
            MetaCacheEntry<String, Integer> disabledEntry = new MetaCacheEntry<>(
                    "disabled",
                    String::length,
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            disabledEntry.computeAndRun("k", (key, current) -> 1, disabledActions::incrementAndGet);
            disabledEntry.invalidateKeyAndRun("k", disabledActions::incrementAndGet);
            Assert.assertEquals(2, disabledActions.get());
            Assert.assertNull(disabledEntry.getIfPresent("k"));
        } finally {
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

            // Verify disabled entries bypass cache entirely under the always-on manual miss load path.
            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(2, loadCounter.get());

            AtomicInteger predicateCounter = new AtomicInteger();
            AtomicInteger remappingCounter = new AtomicInteger();
            // Disabled entries should bypass cache-only helpers without invoking predicate or remapping callbacks.
            Assert.assertNull(entry.findIfPresent(key -> {
                predicateCounter.incrementAndGet();
                return true;
            }));
            Assert.assertNull(entry.compute("k", (key, value) -> {
                remappingCounter.incrementAndGet();
                return 100;
            }));
            Assert.assertEquals(0, predicateCounter.get());
            Assert.assertEquals(0, remappingCounter.get());

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
    public void testMaximumWeightEvictionRunsSyncRemovalListener() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 100L, 5L);
            AtomicInteger removalCounter = new AtomicInteger();
            AtomicReference<RemovalCause> removalCause = new AtomicReference<>();
            MetaCacheEntry<String, Integer> entry = MetaCacheEntry.withSyncRemovalListener(
                    "weighted-sync-listener",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    (key, value) -> value,
                    (key, value, cause) -> {
                        removalCounter.incrementAndGet();
                        removalCause.set(cause);
                    });

            entry.put("first", 4);
            entry.put("second", 4);

            Assert.assertEquals(1, removalCounter.get());
            Assert.assertEquals(RemovalCause.SIZE, removalCause.get());
            Assert.assertEquals(1L, entry.stats().getEstimatedSize());
            Assert.assertEquals(4L, entry.stats().getEstimatedWeight());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateAllDoesNotPutAfterInFlightManualMissLoad() throws Exception {
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
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            entry.invalidateAll();
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
            Assert.assertEquals(2, loadCounter.get());
        } finally {
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateIfDoesNotPutAfterInFlightManualMissLoad() throws Exception {
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
            entry.invalidateIf("k"::equals);
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
    public void testInvalidateIfTracksActualRemovedKeys() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false);

            entry.put("a", 1);
            entry.put("b", 2);
            entry.invalidateIf("a"::equals);
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(Integer.valueOf(2), entry.getIfPresent("b"));
            Assert.assertEquals(1L, entry.stats().getInvalidateCount());

            entry.invalidateAll();
            Assert.assertNull(entry.getIfPresent("b"));
            Assert.assertEquals(2L, entry.stats().getInvalidateCount());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshResultIsCancelledAfterInvalidate() throws Exception {
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
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadFailureCount = loadingCache.stats().loadFailureCount();
            loadingCache.refresh("k");
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("k");
            releaseLoader.countDown();
            waitUntil(() -> loadingCache.stats().loadFailureCount() >= initialLoadFailureCount + 1);
            assertStableValue(() -> entry.getIfPresent("k"), null);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshResultIsCancelledAfterInvalidateAll() throws Exception {
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
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadFailureCount = loadingCache.stats().loadFailureCount();
            loadingCache.refresh("k");
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateAll();
            releaseLoader.countDown();
            waitUntil(() -> loadingCache.stats().loadFailureCount() >= initialLoadFailureCount + 1);
            assertStableValue(() -> entry.getIfPresent("k"), null);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshResultIsCancelledAfterInvalidateIf() throws Exception {
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
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadFailureCount = loadingCache.stats().loadFailureCount();
            loadingCache.refresh("k");
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateIf("k"::equals);
            releaseLoader.countDown();
            waitUntil(() -> loadingCache.stats().loadFailureCount() >= initialLoadFailureCount + 1);
            assertStableValue(() -> entry.getIfPresent("k"), null);
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

    @Test
    public void testNullPreservingComputeFencesInFlightManualMissLoad() throws Exception {
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
                        int current = loadCounter.incrementAndGet();
                        if (current == 1) {
                            loaderStarted.countDown();
                            awaitLatch(releaseLoader);
                        }
                        return current;
                    },
                    cacheSpec,
                    refreshExecutor,
                    false);

            Future<Integer> staleLoad = queryExecutor.submit(() -> entry.get("k"));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.compute("k", (key, value) -> value));
            Assert.assertNull(entry.getIfPresent("k"));
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), staleLoad.get(3L, TimeUnit.SECONDS));
            Assert.assertNull(entry.getIfPresent("k"));
            Assert.assertEquals(Integer.valueOf(2), entry.get("k"));
        } finally {
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testPutPublishesBeforeConcurrentMissCanLoad() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch beforePutStarted = new CountDownLatch(1);
            CountDownLatch releaseBeforePut = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            // Hold the put inside the publish window and verify a concurrent miss observes the published value instead.
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor,
                    false) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    beforePutStarted.countDown();
                    awaitLatch(releaseBeforePut);
                }
            };

            Future<?> putFuture = queryExecutor.submit(() -> {
                entry.put("k", 100);
                return null;
            });
            Assert.assertTrue(beforePutStarted.await(3L, TimeUnit.SECONDS));
            Future<Integer> loaded = queryExecutor.submit(() -> entry.get("k"));
            releaseBeforePut.countDown();

            putFuture.get(3L, TimeUnit.SECONDS);
            Assert.assertEquals(Integer.valueOf(100), loaded.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(100), entry.getIfPresent("k"));
            Assert.assertEquals(0, loadCounter.get());
        } finally {
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testComputePublishesBeforeConcurrentMissCanLoad() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch beforeComputeStarted = new CountDownLatch(1);
            CountDownLatch releaseBeforeCompute = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            // Hold the compute inside the publish window and verify a concurrent miss does not trigger a stale load.
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor,
                    false) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    beforeComputeStarted.countDown();
                    awaitLatch(releaseBeforeCompute);
                }
            };

            Future<Integer> computeFuture = queryExecutor.submit(() -> entry.compute("k", (key, value) -> 200));
            Assert.assertTrue(beforeComputeStarted.await(3L, TimeUnit.SECONDS));
            Future<Integer> loaded = queryExecutor.submit(() -> entry.get("k"));
            releaseBeforeCompute.countDown();

            Assert.assertEquals(Integer.valueOf(200), computeFuture.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(200), loaded.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(200), entry.getIfPresent("k"));
            Assert.assertEquals(0, loadCounter.get());
        } finally {
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshDoesNotOverwriteConcurrentPut() throws Exception {
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
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadFailureCount = loadingCache.stats().loadFailureCount();
            loadingCache.refresh("k");
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.put("k", 100);
            releaseLoader.countDown();
            waitUntil(() -> loadingCache.stats().loadFailureCount() >= initialLoadFailureCount + 1);
            assertStableValue(() -> entry.getIfPresent("k"), Integer.valueOf(100));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateAllRemovesConcurrentPutResult() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService invalidateExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch beforePutStarted = new CountDownLatch(1);
            CountDownLatch releaseBeforePut = new CountDownLatch(1);
            CountDownLatch invalidateStarted = new CountDownLatch(1);
            AtomicInteger publicMutationCount = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    if (publicMutationCount.incrementAndGet() > 1) {
                        beforePutStarted.countDown();
                        awaitLatch(releaseBeforePut);
                    }
                }
            };

            entry.put("k", 1);
            Future<?> putFuture = mutationExecutor.submit(() -> {
                entry.put("k", 100);
                return null;
            });
            Assert.assertTrue(beforePutStarted.await(3L, TimeUnit.SECONDS));
            Future<?> invalidateFuture = invalidateExecutor.submit(() -> {
                invalidateStarted.countDown();
                entry.invalidateAll();
                return null;
            });
            Assert.assertTrue(invalidateStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertFalse(invalidateFuture.isDone());
            releaseBeforePut.countDown();
            putFuture.get(3L, TimeUnit.SECONDS);
            invalidateFuture.get(3L, TimeUnit.SECONDS);

            assertStableValue(() -> entry.getIfPresent("k"), null);
        } finally {
            invalidateExecutor.shutdownNow();
            mutationExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateIfRemovesConcurrentComputeResult() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService invalidateExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch beforeComputeStarted = new CountDownLatch(1);
            CountDownLatch releaseBeforeCompute = new CountDownLatch(1);
            CountDownLatch invalidateStarted = new CountDownLatch(1);
            AtomicInteger publicMutationCount = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor,
                    false) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    if (publicMutationCount.incrementAndGet() > 1) {
                        beforeComputeStarted.countDown();
                        awaitLatch(releaseBeforeCompute);
                    }
                }
            };

            entry.put("k", 1);
            Future<Integer> computeFuture = mutationExecutor.submit(() -> entry.compute("k", (key, value) -> 200));
            Assert.assertTrue(beforeComputeStarted.await(3L, TimeUnit.SECONDS));
            Future<?> invalidateFuture = invalidateExecutor.submit(() -> {
                invalidateStarted.countDown();
                entry.invalidateIf("k"::equals);
                return null;
            });
            Assert.assertTrue(invalidateStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertFalse(invalidateFuture.isDone());
            releaseBeforeCompute.countDown();
            Assert.assertEquals(Integer.valueOf(200), computeFuture.get(3L, TimeUnit.SECONDS));
            invalidateFuture.get(3L, TimeUnit.SECONDS);

            assertStableValue(() -> entry.getIfPresent("k"), null);
        } finally {
            invalidateExecutor.shutdownNow();
            mutationExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshFailurePreservesOldValueAndUpdatesFailureStats() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        int current = loadCounter.incrementAndGet();
                        if (current > 1) {
                            throw new IllegalStateException("mock refresh failure");
                        }
                        return current;
                    },
                    cacheSpec,
                    refreshExecutor);

            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadFailureCount = loadingCache.stats().loadFailureCount();
            loadingCache.refresh("k");
            waitUntil(() -> loadingCache.stats().loadFailureCount() >= initialLoadFailureCount + 1);

            Assert.assertEquals(Integer.valueOf(1), entry.getIfPresent("k"));
            Assert.assertTrue(entry.stats().getLastError().contains("mock refresh failure"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRefreshWorksWithDirectExecutor() throws Exception {
        ExecutorService refreshExecutor = MoreExecutors.newDirectExecutorService();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor);

            Assert.assertEquals(Integer.valueOf(1), entry.get("k"));
            LoadingCache<String, Integer> loadingCache = extractLoadingCache(entry);
            long initialLoadSuccessCount = loadingCache.stats().loadSuccessCount();
            loadingCache.refresh("k");
            waitUntil(() -> loadingCache.stats().loadSuccessCount() >= initialLoadSuccessCount + 1);

            assertStableValue(() -> entry.getIfPresent("k"), Integer.valueOf(2));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    private void assertInvalidateIfCurrentValueAction(String invalidatedKey, int expectedActionCount)
            throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            entry.invalidateIf(invalidatedKey::equals);
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(expectedActionCount, actionCounter.get());
            Assert.assertNull(entry.getIfPresent("a"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    private void assertSameKeyMutationFencesCurrentValueAction(
            Consumer<MetaCacheEntry<String, Integer>> mutation) throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return 1;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            AtomicInteger actionCounter = new AtomicInteger();

            Future<Integer> lookup = queryExecutor.submit(
                    () -> entry.getAndRunIfCurrent("a", (key, value) -> actionCounter.incrementAndGet()));
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            mutation.accept(entry);
            releaseLoader.countDown();

            Assert.assertEquals(Integer.valueOf(1), lookup.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actionCounter.get());
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            releaseLoader.countDown();
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

    // Sample the cache repeatedly so late write-back races cannot slip past a one-shot assertion.
    private <T> void assertStableValue(Supplier<T> actualSupplier, T expectedValue) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 500L;
        while (System.currentTimeMillis() < deadlineMs) {
            Assert.assertEquals(expectedValue, actualSupplier.get());
            Thread.sleep(20L);
        }
    }

    private void waitUntil(BooleanSupplier condition) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 3000L;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadlineMs) {
            Thread.sleep(20L);
        }
        Assert.assertTrue(condition.getAsBoolean());
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
