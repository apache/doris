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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    @SuppressWarnings("unchecked")
    private LoadingCache<String, Integer> extractLoadingCache(MetaCacheEntry<String, Integer> entry) throws Exception {
        Field dataField = MetaCacheEntry.class.getDeclaredField("data");
        dataField.setAccessible(true);
        Object raw = dataField.get(entry);
        Assert.assertTrue(raw instanceof LoadingCache);
        return (LoadingCache<String, Integer>) raw;
    }
}
