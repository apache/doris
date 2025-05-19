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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testGetRemoteName() {
        metaCache.updateCache("remote1", "local1", "meta1", 1L);

        String remoteName = metaCache.getRemoteName("local1");
        Assert.assertEquals("remote1", remoteName);

        Assert.assertNull(metaCache.getRemoteName("nonexistent"));
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
