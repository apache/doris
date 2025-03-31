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
}
