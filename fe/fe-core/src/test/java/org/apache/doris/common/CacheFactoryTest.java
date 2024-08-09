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

package org.apache.doris.common;

import org.apache.doris.statistics.BasicAsyncCacheLoader;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.testing.FakeTicker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CacheFactoryTest {

    private ExecutorService executor;

    public static class CacheValue {
        private final String value;

        public CacheValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static CacheValue createValue(String val, AtomicLong counter) {
            try {
                System.out.println("before create value: " + val + ", Thread: " + Thread.currentThread().getName());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("create value: " + val);
            counter.incrementAndGet();
            return new CacheValue(val);
        }
    }

    public static class CacheValueLoader extends BasicAsyncCacheLoader<Integer, Optional<CacheValue>> {

        private AtomicLong counter;

        public CacheValueLoader(AtomicLong counter) {
            this.counter = counter;
        }

        @Override
        protected Optional<CacheValue> doLoad(Integer key) {
            return Optional.of(CacheValue.createValue("value" + key, counter));
        }
    }

    public static class LoaderRunner implements Runnable {
        private final AsyncLoadingCache<Integer, Optional<CacheValue>> cache;
        private final int key;
        private final AtomicLong counter;

        public LoaderRunner(AsyncLoadingCache<Integer, Optional<CacheValue>> cache, int key, AtomicLong counter) {
            this.cache = cache;
            this.key = key;
            this.counter = counter;
        }

        @Override
        public void run() {
            try {
                CompletableFuture<Optional<CacheValue>> cacheValue = cache.get(key);
                System.out.println("key: " + key + ", value: " + cacheValue.get().get().getValue());
            } catch (RejectedExecutionException e) {
                counter.incrementAndGet();
                throw e;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @BeforeEach
    public void setUp() {
        executor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 1,
                "testCacheFactory", 0, false);
    }

    @Test
    public void testLoadingCacheWithoutExpireAfterWrite() throws InterruptedException {
        FakeTicker ticker = new FakeTicker();
        AtomicLong counter = new AtomicLong(0);
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.empty(),
                OptionalLong.of(10),
                1000,
                false,
                ticker::read);
        LoadingCache<Integer, CacheValue> loadingCache = cacheFactory.buildCache(
                key -> CacheValue.createValue("value" + key, counter), null, executor);
        CacheValue value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        // advance 11 seconds to pass the refreshAfterWrite
        ticker.advance(11, TimeUnit.SECONDS);
        // trigger refresh
        value = loadingCache.get(1);
        // refresh in background, so still get value1
        Assertions.assertEquals("value1", value.getValue());
        // sleep longer to wait for refresh
        Thread.sleep(2500);
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(2, counter.get());
    }

    @Test
    public void testLoadingCacheWithExpireAfterWrite() throws InterruptedException {
        FakeTicker ticker = new FakeTicker();
        AtomicLong counter = new AtomicLong(0);
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(60L),
                OptionalLong.of(10),
                1000,
                false,
                ticker::read);
        LoadingCache<Integer, CacheValue> loadingCache = cacheFactory.buildCache(
                key -> CacheValue.createValue("value" + key, counter), null, executor);
        CacheValue value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        // advance 11 seconds to pass the refreshAfterWrite
        ticker.advance(11, TimeUnit.SECONDS);
        // trigger refresh
        value = loadingCache.get(1);
        // refresh in background, so still get value1
        Assertions.assertEquals("value1", value.getValue());
        // sleep longer to wait for refresh
        Thread.sleep(2500);
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        // refreshed, so counter +1
        Assertions.assertEquals(2, counter.get());
        // advance 61 seconds to pass the expireAfterWrite
        ticker.advance(61, TimeUnit.SECONDS);
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        // expired, so counter +1
        Assertions.assertEquals(3, counter.get());
    }

    @Test
    public void testLoadingCacheWithoutRefreshAfterWrite() throws InterruptedException {
        FakeTicker ticker = new FakeTicker();
        AtomicLong counter = new AtomicLong(0);
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(60L),
                OptionalLong.empty(),
                1000,
                false,
                ticker::read);
        LoadingCache<Integer, CacheValue> loadingCache = cacheFactory.buildCache(
                key -> CacheValue.createValue("value" + key, counter), null, executor);
        CacheValue value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        // advance 30 seconds, key still not expired
        ticker.advance(30, TimeUnit.SECONDS);
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        Assertions.assertEquals(1, counter.get());
        // advance 31 seconds to pass the expireAfterWrite
        ticker.advance(31, TimeUnit.SECONDS);
        value = loadingCache.get(1);
        Assertions.assertEquals("value1", value.getValue());
        // expired, so counter +1
        Assertions.assertEquals(2, counter.get());
    }

    @Test
    public void testAsyncLoadingCacheWithExpireAfterWrite() throws InterruptedException, ExecutionException {
        FakeTicker ticker = new FakeTicker();
        AtomicLong counter = new AtomicLong(0);
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(60L),
                OptionalLong.of(10),
                1000,
                false,
                ticker::read);
        CacheValueLoader loader = new CacheValueLoader(counter);
        AsyncLoadingCache<Integer, Optional<CacheValue>> loadingCache = cacheFactory.buildAsyncCache(loader, executor);
        CompletableFuture<Optional<CacheValue>> futureValue = loadingCache.get(1);
        Assertions.assertFalse(futureValue.isDone());
        Assertions.assertEquals("value1", futureValue.get().get().getValue());
        Assertions.assertEquals(1, counter.get());
        futureValue = loadingCache.get(1);
        Assertions.assertTrue(futureValue.isDone());
        Assertions.assertEquals("value1", futureValue.get().get().getValue());
        Assertions.assertEquals(1, counter.get());
        // advance 11 seconds to pass the refreshAfterWrite
        ticker.advance(11, TimeUnit.SECONDS);
        // trigger refresh
        futureValue = loadingCache.get(1);
        // refresh in background, so still get value1
        Assertions.assertTrue(futureValue.isDone());
        Assertions.assertEquals("value1", futureValue.get().get().getValue());
        // sleep longer to wait for refresh
        Thread.sleep(2500);
        futureValue = loadingCache.get(1);
        Assertions.assertEquals("value1", futureValue.get().get().getValue());
        // refreshed, so counter +1
        Assertions.assertEquals(2, counter.get());
        // advance 61 seconds to pass the expireAfterWrite
        ticker.advance(61, TimeUnit.SECONDS);
        futureValue = loadingCache.get(1);
        Assertions.assertFalse(futureValue.isDone());
        Assertions.assertEquals("value1", futureValue.get().get().getValue());
        // expired, so counter +1
        Assertions.assertEquals(3, counter.get());
    }

    @Test
    public void testTooManyGetRequests() {
        ExecutorService executor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 1,
                "testCacheFactory", 0, false);
        FakeTicker ticker = new FakeTicker();
        AtomicLong counter = new AtomicLong(0);
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(60L),
                OptionalLong.of(10),
                1000,
                false,
                ticker::read);
        CacheValueLoader loader = new CacheValueLoader(counter);
        AsyncLoadingCache<Integer, Optional<CacheValue>> loadingCache = cacheFactory.buildAsyncCache(loader, executor);

        AtomicLong rejectCounter = new AtomicLong(0);
        List<Thread> threads = Lists.newArrayList();
        for (int i = 0; i < 12; i++) {
            Thread thread = new Thread(new LoaderRunner(loadingCache, i, rejectCounter));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // The thread pool size is 10, add 1 queue size.
        // So there will be 11 threads executed, and 1 thread will be rejected.
        Assertions.assertTrue(counter.get() < 12);
        Assertions.assertTrue(rejectCounter.get() >= 1);
    }
}
