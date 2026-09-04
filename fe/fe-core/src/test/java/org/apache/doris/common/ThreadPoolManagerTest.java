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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolManagerTest {

    @Test
    public void testBlockedPolicySkipsCancelledTask() {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(1);
        Runnable queued = () -> { };
        queue.add(queued);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, queue);
        FutureTask<Void> cancelled = new FutureTask<>(() -> null);
        cancelled.cancel(false);

        new ThreadPoolManager.BlockedPolicy("test", 10).rejectedExecution(cancelled, executor);

        Assert.assertEquals(1, queue.size());
        Assert.assertSame(queued, queue.peek());
        executor.shutdownNow();
    }

    @Test
    public void testBlockedPolicyRestoresInterrupt() {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(1);
        queue.add(() -> { });
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, queue);
        FutureTask<Void> task = new FutureTask<>(() -> null);
        ThreadPoolManager.BlockedPolicy blockedPolicy = new ThreadPoolManager.BlockedPolicy("test", 10);

        try {
            Thread.currentThread().interrupt();
            Assert.assertThrows(RejectedExecutionException.class,
                    () -> blockedPolicy.rejectedExecution(task, executor));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            executor.shutdownNow();
        }
    }

    @Test
    public void testNormal() throws InterruptedException {
        ThreadPoolExecutor testCachedPool = ThreadPoolManager.newDaemonCacheThreadPool(2, "test_cache_pool", true);
        ThreadPoolExecutor testFixedThreaddPool = ThreadPoolManager.newDaemonFixedThreadPool(2, 2,
                "test_fixed_thread_pool", true);

        ThreadPoolManager.registerThreadPoolMetric("test_cache_pool", testCachedPool);
        ThreadPoolManager.registerThreadPoolMetric("test_fixed_thread_pool", testFixedThreaddPool);

        Assert.assertEquals(ThreadPoolManager.LogDiscardPolicy.class,
                testCachedPool.getRejectedExecutionHandler().getClass());
        Assert.assertEquals(ThreadPoolManager.BlockedPolicy.class,
                testFixedThreaddPool.getRejectedExecutionHandler().getClass());

        Runnable task = () -> {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        for (int i = 0; i < 4; i++) {
            testCachedPool.submit(task);
        }

        Assert.assertEquals(2, testCachedPool.getPoolSize());
        Assert.assertEquals(2, testCachedPool.getActiveCount());
        Assert.assertEquals(0, testCachedPool.getQueue().size());
        Assert.assertEquals(0, testCachedPool.getCompletedTaskCount());

        Thread.sleep(700);

        Assert.assertEquals(2, testCachedPool.getPoolSize());
        Assert.assertEquals(0, testCachedPool.getActiveCount());
        Assert.assertEquals(0, testCachedPool.getQueue().size());
        Assert.assertEquals(2, testCachedPool.getCompletedTaskCount());

        for (int i = 0; i < 4; i++) {
            testFixedThreaddPool.submit(task);
        }

        Assert.assertTrue(testFixedThreaddPool.getActiveCount() <= 2);
        Assert.assertTrue(testFixedThreaddPool.getQueue().size() > 0);
        Assert.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assert.assertEquals(0, testFixedThreaddPool.getCompletedTaskCount());

        Thread.sleep(2000);

        Assert.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assert.assertEquals(0, testFixedThreaddPool.getActiveCount());
        Assert.assertEquals(0, testFixedThreaddPool.getQueue().size());
        Assert.assertEquals(4, testFixedThreaddPool.getCompletedTaskCount());
    }
}
