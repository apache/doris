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

        TestableVersionCache() {
            super();
        }

        TestableVersionCache(int maxCacheSize) {
            super(maxCacheSize);
        }

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

    @Test
    public void testVersionMonotonicityRejectLowerVersion() throws RpcException, InterruptedException {
        long ttlMs = 100;

        // Cache version 15
        cache.versionToReturn = 15L;
        long v1 = cache.getVersion(partition1, ttlMs);
        Assertions.assertEquals(15L, v1);

        // Wait for expiry
        Thread.sleep(150);

        // MetaService returns an older version (e.g. stale node)
        cache.versionToReturn = 10L;
        long v2 = cache.getVersion(partition1, ttlMs);
        // Cache should NOT regress — must still return 15
        Assertions.assertEquals(15L, v2, "Cache should not regress to a lower version");
    }

    @Test
    public void testCacheSizeBound() throws RpcException {
        int maxSize = 5;
        cache = new TestableVersionCache(maxSize);
        PointQueryVersionCache.setInstance(cache);
        long ttlMs = 1000;

        // Insert more entries than the max cache size
        for (int i = 0; i < maxSize + 3; i++) {
            CloudPartition p = CloudPartitionTest.createPartition(1000 + i, i + 1, i + 1);
            cache.versionToReturn = 100L + i;
            cache.getVersion(p, ttlMs);
        }

        // Cache should not exceed maxSize
        Assertions.assertTrue(cache.cacheSize() <= maxSize,
                "Cache size " + cache.cacheSize() + " exceeds max " + maxSize);
    }

    @Test
    public void testCoalescingTimeout() throws Exception {
        long ttlMs = 1000;
        // Make the leader request hang for longer than the coalescing timeout
        cache.rpcDelayMs = 15_000; // 15 seconds, well beyond the 10s timeout

        // Start a leader request in a background thread
        CountDownLatch leaderStarted = new CountDownLatch(1);
        CountDownLatch followerDone = new CountDownLatch(1);
        AtomicReference<Exception> followerError = new AtomicReference<>();

        // Leader thread
        new Thread(() -> {
            try {
                leaderStarted.countDown();
                cache.getVersion(partition1, ttlMs);
            } catch (Exception e) {
                // expected — leader will eventually finish or be interrupted
            }
        }).start();

        // Wait for leader to start
        leaderStarted.await();
        Thread.sleep(50); // brief pause so leader registers the inflight future

        // Follower thread — should time out
        new Thread(() -> {
            try {
                cache.getVersion(partition1, ttlMs);
            } catch (RpcException e) {
                if (e.getMessage().contains("timed out")) {
                    followerError.set(e);
                }
            } catch (Exception e) {
                // unexpected
            } finally {
                followerDone.countDown();
            }
        }).start();

        // Wait for follower to complete (should timeout after ~10s)
        boolean completed = followerDone.await(20, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertTrue(completed, "Follower thread should have completed");
        Assertions.assertNotNull(followerError.get(),
                "Follower should have received a timeout RpcException");
    }
}
