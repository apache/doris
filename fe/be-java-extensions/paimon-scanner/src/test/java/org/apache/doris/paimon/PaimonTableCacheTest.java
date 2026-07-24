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

package org.apache.doris.paimon;

import org.apache.paimon.table.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PaimonTableCacheTest {
    @After
    public void tearDown() {
        PaimonTableCache.clearForTest();
    }

    @Test
    public void testAcquireIncrementsAndReleaseRemovesAtZero() {
        String cacheKey = "reference-count";
        PaimonTableCache.TableCacheEntry publishedEntry = newEntry();
        Assert.assertTrue(PaimonTableCache.publish(cacheKey, publishedEntry));

        PaimonTableCache.TableCacheEntry acquiredEntry = PaimonTableCache.acquire(cacheKey);
        Assert.assertSame(publishedEntry, acquiredEntry);

        PaimonTableCache.release(cacheKey, acquiredEntry);
        Assert.assertEquals(1, PaimonTableCache.size());

        PaimonTableCache.release(cacheKey, publishedEntry);
        Assert.assertEquals(0, PaimonTableCache.size());
        Assert.assertNull(PaimonTableCache.acquire(cacheKey));
    }

    @Test
    public void testConcurrentAcquireAndReleaseKeepsEntryWhilePublisherUsesIt() throws Exception {
        String cacheKey = "concurrent-reference-count";
        PaimonTableCache.TableCacheEntry publishedEntry = newEntry();
        Assert.assertTrue(PaimonTableCache.publish(cacheKey, publishedEntry));

        int threadCount = 16;
        int iterations = 1000;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = IntStream.range(0, threadCount)
                .mapToObj(ignored -> executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        PaimonTableCache.TableCacheEntry entry = PaimonTableCache.acquire(cacheKey);
                        Assert.assertSame(publishedEntry, entry);
                        PaimonTableCache.release(cacheKey, entry);
                    }
                    return null;
                }))
                .collect(Collectors.toList());

        try {
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        Assert.assertEquals(1, PaimonTableCache.size());
        PaimonTableCache.release(cacheKey, publishedEntry);
        Assert.assertEquals(0, PaimonTableCache.size());
    }

    @Test
    public void testOnlyFirstEntryIsPublished() {
        String cacheKey = "publish-race";
        PaimonTableCache.TableCacheEntry first = newEntry();
        PaimonTableCache.TableCacheEntry second = newEntry();

        Assert.assertTrue(PaimonTableCache.publish(cacheKey, first));
        Assert.assertFalse(PaimonTableCache.publish(cacheKey, second));
        PaimonTableCache.TableCacheEntry acquiredEntry = PaimonTableCache.acquire(cacheKey);
        Assert.assertSame(first, acquiredEntry);

        PaimonTableCache.release(cacheKey, acquiredEntry);
        PaimonTableCache.release(cacheKey, first);
        Assert.assertEquals(0, PaimonTableCache.size());
    }

    private PaimonTableCache.TableCacheEntry newEntry() {
        Table table = (Table) Proxy.newProxyInstance(
                Table.class.getClassLoader(), new Class[] {Table.class}, (proxy, method, args) -> {
                    if ("toString".equals(method.getName())) {
                        return "TestPaimonTable";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new PaimonTableCache.TableCacheEntry(table, Collections.singletonList("field"));
    }
}
