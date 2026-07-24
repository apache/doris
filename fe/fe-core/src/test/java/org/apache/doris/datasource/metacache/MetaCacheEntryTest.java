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
import java.util.function.BooleanSupplier;
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
    public void testManualMissLoadRemovesValueWhenInvalidateHappensBeforePut() throws Exception {
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
            // Run invalidateAll() while the hook is holding publishLock so bumpAllGenerations() lands
            // before the post-put generation re-check and triggers self-removal in the manual path.
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
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
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
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
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
            entry.invalidateAll();
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
            CountDownLatch invalidateKeyStarted = new CountDownLatch(1);
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

                @Override
                public void invalidateKey(String key) {
                    invalidateKeyStarted.countDown();
                    super.invalidateKey(key);
                }
            };

            entry.put("k", 1);
            Future<?> putFuture = mutationExecutor.submit(() -> {
                entry.put("k", 100);
                return null;
            });
            Assert.assertTrue(beforePutStarted.await(3L, TimeUnit.SECONDS));
            Future<?> invalidateFuture = invalidateExecutor.submit(() -> {
                entry.invalidateAll();
                return null;
            });
            Assert.assertTrue(invalidateKeyStarted.await(3L, TimeUnit.SECONDS));
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
            CountDownLatch invalidateKeyStarted = new CountDownLatch(1);
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

                @Override
                public void invalidateKey(String key) {
                    invalidateKeyStarted.countDown();
                    super.invalidateKey(key);
                }
            };

            entry.put("k", 1);
            Future<Integer> computeFuture = mutationExecutor.submit(() -> entry.compute("k", (key, value) -> 200));
            Assert.assertTrue(beforeComputeStarted.await(3L, TimeUnit.SECONDS));
            Future<?> invalidateFuture = invalidateExecutor.submit(() -> {
                entry.invalidateIf("k"::equals);
                return null;
            });
            Assert.assertTrue(invalidateKeyStarted.await(3L, TimeUnit.SECONDS));
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
