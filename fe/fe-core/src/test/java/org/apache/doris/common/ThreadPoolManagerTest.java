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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolManagerTest {

    @Test
    public void callerHelpDoesNotBlockOnFullPoolAndExecutesExactlyOnce() throws Exception {
        ThreadPoolExecutor pool = ThreadPoolManager.newDaemonFixedThreadPool(1, 1, "help_full", false);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        try {
            pool.submit(() -> {
                pool.execute(() -> { }); // fill the one queue slot while this parent owns the worker
                java.util.concurrent.RunnableFuture<Integer> task =
                        ThreadPoolManager.submitWithCallerHelp(pool, calls::incrementAndGet);
                task.run();
                task.run();
                try {
                    Assertions.assertEquals(1, task.get().intValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).get(5, java.util.concurrent.TimeUnit.SECONDS);
            Assertions.assertEquals(1, calls.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void helpedAndCancelledTasksDoNotAccumulateInTheSharedQueue() throws Exception {
        ThreadPoolExecutor pool = ThreadPoolManager.newDaemonFixedThreadPool(1, 1, "help_remove", false);
        try {
            pool.submit(() -> {
                java.util.concurrent.RunnableFuture<Integer> task =
                        ThreadPoolManager.submitWithCallerHelp(pool, () -> 42);
                Assertions.assertEquals(1, pool.getQueue().size());
                task.run();
                Assertions.assertTrue(pool.getQueue().isEmpty());
                java.util.concurrent.RunnableFuture<Integer> unused =
                        ThreadPoolManager.submitWithCallerHelp(pool, () -> 43);
                unused.cancel(true);
                Assertions.assertTrue(pool.getQueue().isEmpty());
            }).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testNormal() throws InterruptedException {
        ThreadPoolExecutor testCachedPool = ThreadPoolManager.newDaemonCacheThreadPool(2, "test_cache_pool", true);
        ThreadPoolExecutor testFixedThreaddPool = ThreadPoolManager.newDaemonFixedThreadPool(2, 2,
                "test_fixed_thread_pool", true);

        ThreadPoolManager.registerThreadPoolMetric("test_cache_pool", testCachedPool);
        ThreadPoolManager.registerThreadPoolMetric("test_fixed_thread_pool", testFixedThreaddPool);

        Assertions.assertEquals(ThreadPoolManager.LogDiscardPolicy.class,
                testCachedPool.getRejectedExecutionHandler().getClass());
        Assertions.assertEquals(ThreadPoolManager.BlockedPolicy.class,
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

        Assertions.assertEquals(2, testCachedPool.getPoolSize());
        Assertions.assertEquals(2, testCachedPool.getActiveCount());
        Assertions.assertEquals(0, testCachedPool.getQueue().size());
        Assertions.assertEquals(0, testCachedPool.getCompletedTaskCount());

        Thread.sleep(700);

        Assertions.assertEquals(2, testCachedPool.getPoolSize());
        Assertions.assertEquals(0, testCachedPool.getActiveCount());
        Assertions.assertEquals(0, testCachedPool.getQueue().size());
        Assertions.assertEquals(2, testCachedPool.getCompletedTaskCount());

        for (int i = 0; i < 4; i++) {
            testFixedThreaddPool.submit(task);
        }

        Assertions.assertTrue(testFixedThreaddPool.getActiveCount() <= 2);
        Assertions.assertTrue(testFixedThreaddPool.getQueue().size() > 0);
        Assertions.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assertions.assertEquals(0, testFixedThreaddPool.getCompletedTaskCount());

        Thread.sleep(2000);

        Assertions.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assertions.assertEquals(0, testFixedThreaddPool.getActiveCount());
        Assertions.assertEquals(0, testFixedThreaddPool.getQueue().size());
        Assertions.assertEquals(4, testFixedThreaddPool.getCompletedTaskCount());
    }
}
