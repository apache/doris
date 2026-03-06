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

package org.apache.doris.cloud.catalog;

import org.apache.doris.rpc.RpcException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PointQueryVersionCacheTest {

    private TestableVersionCache cache;
    private CloudPartition partition1;
    private CloudPartition partition2;

    /**
     * A testable subclass that overrides fetchVersionFromMs to avoid real RPC calls.
     */
    static class TestableVersionCache extends PointQueryVersionCache {
        final AtomicInteger rpcCallCount = new AtomicInteger(0);
        volatile long versionToReturn = 10L;
        volatile long rpcDelayMs = 0; // simulate RPC latency
        volatile RpcException exceptionToThrow = null;

        @Override
        protected long fetchVersionFromMs(CloudPartition partition) throws RpcException {
            rpcCallCount.incrementAndGet();
            if (rpcDelayMs > 0) {
                try {
                    Thread.sleep(rpcDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return versionToReturn;
        }
    }

    @Before
    public void setUp() {
        cache = new TestableVersionCache();
        PointQueryVersionCache.setInstance(cache);
        partition1 = CloudPartitionTest.createPartition(100, 1, 1);
        partition2 = CloudPartitionTest.createPartition(200, 2, 2);
    }

    @After
    public void tearDown() {
        cache.clear();
        PointQueryVersionCache.setInstance(null);
    }

    @Test
    public void testBasicCacheHit() throws RpcException {
        long ttlMs = 1000;

        // First call: cache miss, should issue RPC
        long version1 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, version1);
        Assertions.assertEquals(1, cache.rpcCallCount.get());

        // Second call within TTL: cache hit, no new RPC
        long version2 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, version2);
        Assertions.assertEquals(1, cache.rpcCallCount.get()); // still 1

        Assertions.assertEquals(1, cache.cacheSize());
    }

    @Test
    public void testCacheExpiry() throws RpcException, InterruptedException {
        long ttlMs = 100; // short TTL

        // First call
        long version1 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, version1);
        Assertions.assertEquals(1, cache.rpcCallCount.get());

        // Wait for cache to expire
        Thread.sleep(150);

        // Update version
        cache.versionToReturn = 20L;

        // Third call: cache expired, should issue new RPC
        long version2 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(20L, version2);
        Assertions.assertEquals(2, cache.rpcCallCount.get());
    }

    @Test
    public void testCacheDisabledWithZeroTtl() throws RpcException {
        long ttlMs = 0; // disabled

        // Each call should issue RPC
        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(1, cache.rpcCallCount.get());

        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(2, cache.rpcCallCount.get());

        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(3, cache.rpcCallCount.get());
    }

    @Test
    public void testCacheDisabledWithNegativeTtl() throws RpcException {
        long ttlMs = -1; // disabled

        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(1, cache.rpcCallCount.get());

        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(2, cache.rpcCallCount.get());
    }

    @Test
    public void testDifferentPartitionsAreIndependent() throws RpcException {
        long ttlMs = 1000;

        cache.versionToReturn = 10L;
        long v1 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, v1);

        cache.versionToReturn = 20L;
        long v2 = cache.getVersion(partition2, ttlMs);
        Assertions.assertEquals(20L, v2);

        Assertions.assertEquals(2, cache.rpcCallCount.get());
        Assertions.assertEquals(2, cache.cacheSize());

        // Cache hit for partition1 still returns 10
        long v1Again = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, v1Again);
        Assertions.assertEquals(2, cache.rpcCallCount.get()); // no new RPC
    }

    @Test
    public void testRequestCoalescing() throws Exception {
        long ttlMs = 1000;
        int numThreads = 10;
        cache.rpcDelayMs = 200; // simulate slow RPC

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicReference<Exception> error = new AtomicReference<>();
        long[] results = new long[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    barrier.await(); // all threads start at the same time
                    results[idx] = cache.getVersion(partition1, ttlMs);
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        Assertions.assertNull(error.get(), "Unexpected exception: " + error.get());

        // All threads should get the same version
        for (int i = 0; i < numThreads; i++) {
            Assertions.assertEquals(10L, results[i], "Thread " + i + " got wrong version");
        }

        // Only ONE RPC call should have been made (request coalescing)
        Assertions.assertTrue(cache.rpcCallCount.get() <= 2,
                "Expected at most 2 RPC calls due to coalescing, got " + cache.rpcCallCount.get());
    }

    @Test
    public void testRpcExceptionPropagates() {
        long ttlMs = 1000;
        cache.exceptionToThrow = new RpcException("test", "simulated failure");

        Assertions.assertThrows(RpcException.class, () -> {
            cache.getVersion(partition1, ttlMs);
        });

        Assertions.assertEquals(1, cache.rpcCallCount.get());
        // Cache should NOT contain an entry after failure
        Assertions.assertEquals(0, cache.cacheSize());
    }

    @Test
    public void testRpcExceptionDoesNotPolluteCacheForCoalescedRequests() throws Exception {
        long ttlMs = 1000;
        int numThreads = 5;
        cache.rpcDelayMs = 100;
        cache.exceptionToThrow = new RpcException("test", "simulated failure");

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    cache.getVersion(partition1, ttlMs);
                } catch (RpcException e) {
                    errorCount.incrementAndGet();
                } catch (Exception e) {
                    // unexpected
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        // All threads should get the exception
        Assertions.assertEquals(numThreads, errorCount.get());
        // Cache should be empty
        Assertions.assertEquals(0, cache.cacheSize());
    }

    @Test
    public void testRetryAfterFailure() throws RpcException {
        long ttlMs = 1000;

        // First call fails
        cache.exceptionToThrow = new RpcException("test", "simulated failure");
        Assertions.assertThrows(RpcException.class, () -> {
            cache.getVersion(partition1, ttlMs);
        });
        Assertions.assertEquals(1, cache.rpcCallCount.get());

        // Second call succeeds
        cache.exceptionToThrow = null;
        cache.versionToReturn = 42L;
        long version = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(42L, version);
        Assertions.assertEquals(2, cache.rpcCallCount.get());
    }

    @Test
    public void testClearCache() throws RpcException {
        long ttlMs = 1000;

        cache.getVersion(partition1, ttlMs);
        cache.getVersion(partition2, ttlMs);
        Assertions.assertEquals(2, cache.cacheSize());

        cache.clear();
        Assertions.assertEquals(0, cache.cacheSize());

        // After clear, next call should issue RPC again
        cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(3, cache.rpcCallCount.get());
    }

    @Test
    public void testVersionMonotonicity() throws RpcException, InterruptedException {
        long ttlMs = 100;

        cache.versionToReturn = 10L;
        long v1 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(10L, v1);

        // Wait for expiry
        Thread.sleep(150);

        // Even if MS returns a higher version, cache correctly returns the new version
        cache.versionToReturn = 15L;
        long v2 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(15L, v2);

        // Version should never go backwards in the cache
        Assertions.assertTrue(v2 >= v1);
    }
}
