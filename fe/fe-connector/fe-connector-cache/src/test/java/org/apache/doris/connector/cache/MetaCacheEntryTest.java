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

package org.apache.doris.connector.cache;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Behaviour parity tests for the connector-side {@link MetaCacheEntry} copy. Where fe-core's
 * {@code MetaCacheEntryTest} toggles {@code Config.enable_external_meta_cache_manual_miss_load}, this copy
 * passes the flag through the constructor instead (the connector copy has no fe-core Config).
 */
public class MetaCacheEntryTest {

    @Test
    public void loaderGetTracksHitMissAndLastError() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        if ("fail".equals(key)) {
                            throw new IllegalStateException("mock failure");
                        }
                        return 1;
                    },
                    cacheSpec,
                    refreshExecutor);

            Assertions.assertEquals(Integer.valueOf(1), entry.get("ok"));
            Assertions.assertEquals(Integer.valueOf(1), entry.get("ok"));
            Assertions.assertThrows(IllegalStateException.class, () -> entry.get("fail"));

            MetaCacheEntryStats stats = entry.stats();
            Assertions.assertEquals(3L, stats.getRequestCount());
            Assertions.assertEquals(1L, stats.getHitCount());
            Assertions.assertEquals(2L, stats.getMissCount());
            Assertions.assertEquals(1L, stats.getLoadSuccessCount());
            Assertions.assertEquals(1L, stats.getLoadFailureCount());
            Assertions.assertTrue(stats.getLastLoadSuccessTimeMs() > 0);
            Assertions.assertTrue(stats.getLastLoadFailureTimeMs() > 0);
            Assertions.assertTrue(stats.getLastError().contains("mock failure"));

            // A per-call miss loader overrides the default loader.
            Assertions.assertEquals(Integer.valueOf(101), entry.get("miss-loader", key -> 101));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void disabledSpecMakesEntryIneffective() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            // ttl == 0 disables the cache (CacheSpec.isCacheEnabled).
            CacheSpec cacheSpec = CacheSpec.of(true, 0L, 10L);
            AtomicInteger loadCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> loadCounter.incrementAndGet(),
                    cacheSpec,
                    refreshExecutor);

            // getIfPresent short-circuits to null for a disabled entry, even right after a get() populated it.
            Assertions.assertNull(entry.getIfPresent("k"));
            Assertions.assertEquals(Integer.valueOf(1), entry.get("k"));
            Assertions.assertNull(entry.getIfPresent("k"));

            MetaCacheEntryStats stats = entry.stats();
            Assertions.assertTrue(stats.isConfigEnabled());
            Assertions.assertFalse(stats.isEffectiveEnabled());

            // The manual miss-load path bypasses the cache entirely when disabled, so every get reloads.
            AtomicInteger manualCounter = new AtomicInteger();
            MetaCacheEntry<String, Integer> manualEntry = new MetaCacheEntry<>(
                    "test-manual", key -> manualCounter.incrementAndGet(), cacheSpec, refreshExecutor,
                    false, false, 0L, true);
            manualEntry.get("k");
            manualEntry.get("k");
            Assertions.assertEquals(2, manualCounter.get());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void capacityEvictsAndStatsCountEviction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    String::length,
                    cacheSpec,
                    refreshExecutor);

            Assertions.assertEquals(0D, entry.stats().getEvictionRate(), 0D);
            Assertions.assertEquals(Integer.valueOf(1), entry.get("a"));
            Assertions.assertEquals(Integer.valueOf(2), entry.get("bb"));
            // Force Caffeine to run pending maintenance so the eviction is observable.
            extractLoadingCache(entry).cleanUp();
            Assertions.assertEquals(1L, entry.stats().getEvictionCount());
            Assertions.assertEquals(0.5D, entry.stats().getEvictionRate(), 0D);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void contextualOnlyEntryRejectsDefaultGetButServesMissLoader() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "contextual", null, cacheSpec, refreshExecutor,
                    false, true, 0L, false);

            UnsupportedOperationException ex = Assertions.assertThrows(
                    UnsupportedOperationException.class, () -> entry.get("k"));
            Assertions.assertTrue(ex.getMessage().contains("contextual miss loader"));
            Assertions.assertEquals(Integer.valueOf(7), entry.get("k", key -> 7));
            // Second call is a cache hit; the miss loader is not consulted again.
            Assertions.assertEquals(Integer.valueOf(7), entry.get("k", key -> 999));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void invalidationDropsEntriesAndCounts() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test", String::length, cacheSpec, refreshExecutor);

            entry.get("a");
            entry.get("bb");
            entry.get("ccc");
            entry.invalidateKey("a");
            Assertions.assertNull(entry.getIfPresent("a"));
            Assertions.assertEquals(Integer.valueOf(2), entry.getIfPresent("bb"));

            entry.invalidateIf(key -> key.length() == 2);
            Assertions.assertNull(entry.getIfPresent("bb"));
            Assertions.assertEquals(Integer.valueOf(3), entry.getIfPresent("ccc"));

            entry.invalidateAll();
            Assertions.assertNull(entry.getIfPresent("ccc"));
            Assertions.assertTrue(entry.stats().getInvalidateCount() >= 3L);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void manualMissLoadDeduplicatesConcurrentSameKey() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newFixedThreadPool(2);
        try {
            CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            AtomicInteger loadCounter = new AtomicInteger();
            // manualMissLoadEnabled = true (last ctor arg) exercises the striped-lock manual load path.
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "test",
                    key -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return loadCounter.incrementAndGet();
                    },
                    cacheSpec, refreshExecutor,
                    false, false, 0L, true);

            Future<Integer> first = queryExecutor.submit(() -> entry.get("k"));
            Assertions.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<Integer> second = queryExecutor.submit(() -> entry.get("k"));
            releaseLoader.countDown();

            Assertions.assertEquals(Integer.valueOf(1), first.get(3L, TimeUnit.SECONDS));
            Assertions.assertEquals(Integer.valueOf(1), second.get(3L, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loadCounter.get());
        } finally {
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static LoadingCache<Object, Object> extractLoadingCache(MetaCacheEntry<?, ?> entry) throws Exception {
        Field field = MetaCacheEntry.class.getDeclaredField("loadingData");
        field.setAccessible(true);
        return (LoadingCache<Object, Object>) field.get(entry);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(3L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
