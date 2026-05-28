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

package org.apache.doris.cloud.rpc;

import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRateLimiterTest {
    private static final Logger LOG = LogManager.getLogger(RpcRateLimiterTest.class);

    private long originalWaitTimeout;
    private int originalMaxWaiting;

    @Before
    public void setUp() {
        originalWaitTimeout = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
        originalMaxWaiting = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeout;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = originalMaxWaiting;
    }

    // ==================== QpsLimiter Tests ====================

    @Test
    public void testQpsLimiterConstructor() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(qps, limiter.qps);
        Assert.assertNotNull(limiter.getRateLimiter());
        Assert.assertEquals(qps, limiter.getRateLimiter().getRate(), 0.001);
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // Invalid Arguments
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 10, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 10, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 0, 10));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, -1, 10));
    }

    @Test
    public void testQpsLimiterAcquireSuccess() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 1000;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assertions.assertDoesNotThrow(() -> limiter.acquire());
    }

    @Test
    public void testQpsLimiterAcquireWaitingQueueFull() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 2;
        int qps = 1;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 500;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicInteger tooManyWaitingFailCount = new AtomicInteger(0);
        AtomicInteger rateLimiterFailCount = new AtomicInteger(0);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    limiter.acquire();
                    successCount.incrementAndGet();
                } catch (RpcRateLimitException e) {
                    if (e.getMessage().contains("too many waiting requests")) {
                        tooManyWaitingFailCount.incrementAndGet();
                    } else {
                        rateLimiterFailCount.incrementAndGet();
                    }
                    failCount.incrementAndGet();
                } catch (InterruptedException e) {
                    failCount.incrementAndGet();
                    Thread.currentThread().interrupt();
                }
            });
        }
        startLatch.countDown();

        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            Assert.assertTrue("Executor did not terminate in the expected time", terminated);
        } catch (Exception e) {
            LOG.error("Test interrupted", e);
            Assert.fail("Test was interrupted: " + e.getMessage());
        }
        int successes = successCount.get();
        int failures = failCount.get();
        Assert.assertEquals("Total results should match thread count", threadCount, successes + failures);
        Assert.assertEquals(1, successes);
        Assert.assertEquals(threadCount - successes, failures);
        // Assert.assertEquals(8, tooManyWaitingFailCount.get());
        // Assert.assertEquals(1, rateLimiterFailCount.get());
    }

    @Test
    public void testQpsLimiterUpdateQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int initialQps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // Update with same values - do nothing
        limiter.update(maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // increase QPS
        int newQps = 200;
        limiter.update(maxWaitRequestNum, newQps);
        Assert.assertEquals(newQps, limiter.qps);
        Assert.assertEquals(newQps, limiter.getRateLimiter().getRate(), 0.001);

        // decrease QPS back to initial
        limiter.update(maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(initialQps, limiter.getRateLimiter().getRate(), 0.001);

        // zero qps
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.update(maxWaitRequestNum, 0));
        // negative qps
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.update(maxWaitRequestNum, -1));
    }

    @Test
    public void testQpsLimiterUpdateMaxWait() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // same value - should do nothing
        limiter.update(maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // increase maxWaitRequestNum
        int newMaxWait = 20;
        limiter.update(newMaxWait, qps);
        Assert.assertEquals(newMaxWait, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(newMaxWait, limiter.getAllowWaiting());

        // decrease maxWaitRequestNum back to initial
        limiter.update(maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // zero maxWaitRequestNum
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.update(0, qps));
        // negative maxWaitRequestNum
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.update(-1, qps));
    }

    // ==================== OverloadQpsLimiter Tests ====================

    @Test
    public void testOverloadQpsLimiterConstructor() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        // factor is 1.0
        double factor = 1.0;
        RpcRateLimiter.OverloadQpsLimiter limiter =
                new RpcRateLimiter.OverloadQpsLimiter(methodName, maxWaitRequestNum, qps, factor);
        Assert.assertEquals(methodName, limiter.methodName);
        // Effective QPS should be qps * factor = 100 * 1.0 = 100
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(100, limiter.qps);
        Assert.assertEquals(100, limiter.getRateLimiter().getRate(), 0.001);

        // factor is 0.9
        factor = 0.9;
        limiter = new RpcRateLimiter.OverloadQpsLimiter(methodName, maxWaitRequestNum, qps, factor);
        // Effective QPS should be qps * factor = 100 * 0.9 = 90
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(90, limiter.qps);
        Assert.assertEquals(90, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testOverloadQpsLimiterApplyFactorOne() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        // Start with factor 0.9 - effective QPS should be 90
        RpcRateLimiter.OverloadQpsLimiter limiter =
                new RpcRateLimiter.OverloadQpsLimiter(methodName, maxWaitRequestNum, qps, 0.9);
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(90, limiter.qps);
        Assert.assertEquals(90, limiter.getRateLimiter().getRate(), 0.001);

        // Apply factor 0.8 - effective QPS should be 80
        limiter.applyFactor(0.8);
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(80, limiter.qps);
        Assert.assertEquals(80, limiter.getRateLimiter().getRate(), 0.001);

        // Apply factor 0.85 - effective QPS should be 85
        limiter.applyFactor(0.85);
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(85, limiter.qps);
        Assert.assertEquals(85, limiter.getRateLimiter().getRate(), 0.001);

        // Even with very small factor, should be at least 1
        limiter.applyFactor(0.0001);
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(1, limiter.qps);
        Assert.assertEquals(1, limiter.getRateLimiter().getRate(), 0.001);

        // Apply factor 0.0 - effective QPS should be at least 1
        limiter.applyFactor(0);
        Assert.assertEquals(100, limiter.getBaseQps());
        Assert.assertEquals(1, limiter.qps);
        Assert.assertEquals(1, limiter.getRateLimiter().getRate(), 0.001);

        // Apply factor 2.0
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.applyFactor(2.0));
    }

    // ==================== CostLimiter Tests ====================

    @Test
    public void testCostLimiterConstructor() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);
        Assert.assertEquals(limit, limiter.getLimit());
        Assert.assertEquals(0, limiter.getCurrentCost());

        // zero limit
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.CostLimiter(methodName, 0));
        // negative limit
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.CostLimiter(methodName, -1));
    }

    @Test
    public void testCostLimiterSetLimit() {
        String methodName = "testMethod";
        int initialLimit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, initialLimit);
        Assert.assertEquals(initialLimit, limiter.getLimit());
        Assert.assertEquals(0, limiter.getCurrentCost());

        int newLimit = 200;
        limiter.setLimit(newLimit);
        Assert.assertEquals(newLimit, limiter.getLimit());

        // zero limit
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.setLimit(0));
        // negative limit
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.setLimit(-1));
    }

    @Test
    public void testCostLimiterAcquire() {
        String methodName = "testMethod";
        int limit = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1;
        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // acquire with cost exceeds limit - should fail
        Assert.assertThrows("exceeds the limit", RpcRateLimitException.class, () -> limiter.acquire(200));
        Assert.assertEquals(0, limiter.getCurrentCost());

        // acquire with cost less than limit: 50
        Assertions.assertDoesNotThrow(() -> limiter.acquire(50));
        Assert.assertEquals(50, limiter.getCurrentCost());

        // acquire 0 cost - should succeed and not change current cost
        Assertions.assertDoesNotThrow(() -> limiter.acquire(0));
        Assert.assertEquals(50, limiter.getCurrentCost());

        // acquire 20 cost - should succeed and current cost should be 70
        Assertions.assertDoesNotThrow(() -> limiter.acquire(20));
        Assert.assertEquals(70, limiter.getCurrentCost());

        // acquire 30 cost - should fail because 70 + 30 = 100 > limit
        Assertions.assertDoesNotThrow(() -> limiter.acquire(30));
        Assert.assertEquals(100, limiter.getCurrentCost());

        // acquire 1 cost - should fail because 100 + 1 > limit
        Assert.assertThrows("timeout", RpcRateLimitException.class, () -> limiter.acquire(1));
        Assert.assertEquals(100, limiter.getCurrentCost());

        // release 30 cost - current cost should be 70
        limiter.release(30);
        Assert.assertEquals(70, limiter.getCurrentCost());

        // acquire 40 cost - should fail because 70 + 40 = 110 > limit
        Assert.assertThrows("timeout", RpcRateLimitException.class, () -> limiter.acquire(40));
        Assert.assertEquals(70, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterRelease() {
        String methodName = "testMethod";
        int limit = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1;
        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        int cost = 50;
        Assertions.assertDoesNotThrow(() -> limiter.acquire(cost));
        Assert.assertEquals(cost, limiter.getCurrentCost());
        limiter.release(cost);
        Assert.assertEquals(0, limiter.getCurrentCost());

        // Acquire 30, then acquire 40, then release 40, then release 30
        Assertions.assertDoesNotThrow(() -> limiter.acquire(30));
        Assert.assertEquals(30, limiter.getCurrentCost());
        Assertions.assertDoesNotThrow(() -> limiter.acquire(40));
        Assert.assertEquals(70, limiter.getCurrentCost());
        limiter.release(40);
        Assert.assertEquals(30, limiter.getCurrentCost());
        limiter.release(30);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterReleaseMoreThanCurrent() {
        String methodName = "testMethod";
        int limit = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10;

        int cost = 30;
        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);
        Assertions.assertDoesNotThrow(() -> limiter.acquire(cost));
        Assert.assertEquals(cost, limiter.getCurrentCost());

        // Release more than current cost - should clamp to 0
        limiter.release(50);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterWaitForCapacity() {
        String methodName = "testMethod";
        int limit = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 100; // Short timeout

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        Assertions.assertDoesNotThrow(() -> limiter.acquire(50));
        Assert.assertEquals(50, limiter.getCurrentCost());

        // Try to acquire more - should wait and fail due to timeout
        Assert.assertThrows(RpcRateLimitException.class, () -> limiter.acquire(10));
        Assert.assertEquals(50, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterAcquireAwait() {
        String methodName = "testMethod";
        int limit = 50;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 20;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        Assertions.assertDoesNotThrow(() -> limiter.acquire(50));
        Assert.assertEquals(50, limiter.getCurrentCost());

        // Start a thread that will try to acquire
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean acquired1 = new AtomicBoolean(false);
        AtomicBoolean acquired2 = new AtomicBoolean(false);

        Thread waiter1 = new Thread(() -> {
            try {
                limiter.acquire(10);
                acquired1.set(true);
            } catch (RpcRateLimitException e) {
                acquired1.set(false);
            } finally {
                latch.countDown();
            }
        });
        waiter1.start();

        Thread waiter2 = new Thread(() -> {
            try {
                Thread.sleep(100);
                limiter.acquire(20);
                acquired2.set(true);
            } catch (RpcRateLimitException | InterruptedException e) {
                acquired2.set(false);
            } finally {
                latch.countDown();
            }
        });
        waiter2.start();

        // waiter1 is timeout, waiter2 is sleep for 100ms and then try to acquire
        Assertions.assertDoesNotThrow(() -> Thread.sleep(40));
        // Release some capacity
        limiter.release(20);
        Assert.assertEquals(30, limiter.getCurrentCost());

        // Wait for the waiter to complete
        try {
            latch.await(2, TimeUnit.SECONDS);
            waiter1.join(1000);
            waiter2.join(1000);
        } catch (InterruptedException e) {
            LOG.error("Test interrupted", e);
        }

        // The waiter should have succeeded
        Assert.assertFalse(acquired1.get());
        Assert.assertTrue(acquired2.get());
    }

    @Test
    public void testCostLimiterAcquireInterrupted() {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 50;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10000; // Long timeout
        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        Assertions.assertDoesNotThrow(() -> limiter.acquire(100));
        Assert.assertEquals(100, limiter.getCurrentCost());

        // Create a thread that will try to acquire and get interrupted
        AtomicBoolean acquired = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            try {
                limiter.acquire(cost);
                acquired.set(true);
            } catch (RpcRateLimitException e) {
                acquired.set(false);
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("interrupted"));
            }
        });
        waiter.start();

        // Give the waiter time to block
        try {
            Thread.sleep(100);
            // Interrupt the waiter
            waiter.interrupt();
            waiter.join(1000);
        } catch (InterruptedException e) {
            LOG.error("Test interrupted", e);
        }
        // The thread should have been interrupted
        Assert.assertFalse(waiter.isAlive());
        Assert.assertFalse(acquired.get());
    }

    @Test
    public void testConcurrentAccessToCostLimiter() {
        String methodName = "testMethod";
        int limit = 100;
        int threads = 10;
        int iterations = 20;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 2000;
        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterations; j++) {
                        try {
                            limiter.acquire(10);
                            Thread.sleep(new Random().nextInt(50));
                            limiter.release(10);
                            successCount.incrementAndGet();
                        } catch (RpcRateLimitException e) {
                            failCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        try {
            startLatch.countDown();
            endLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
        } catch (InterruptedException e) {
            LOG.error("Test interrupted", e);
        }

        LOG.info("Concurrent test - Success: {}, Failed: {}", successCount.get(), failCount.get());
        Assert.assertTrue(successCount.get() > 0);
        Assert.assertEquals(iterations * threads, successCount.get());
        Assert.assertEquals(0, failCount.get());
    }
}
