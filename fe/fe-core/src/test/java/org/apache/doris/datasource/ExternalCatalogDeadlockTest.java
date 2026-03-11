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

import org.apache.doris.datasource.InitCatalogLog.Type;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalCatalogDeadlockTest {

    @Test
    public void testResetToUninitializedShouldNotDeadlockWithCacheLoader() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToTouchCatalog = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();

        // The loader holds Caffeine's per-key lock before it calls back into the catalog.
        LoadingCache<String, String> cache = Caffeine.newBuilder().build(key -> {
            loaderEntered.countDown();
            awaitLatch(allowLoaderToTouchCatalog);
            catalog.makeSureInitialized();
            return key;
        });

        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> cache.get("deadlock-key")),
                "deadlock-cache-loader");
        queryThread.setDaemon(true);
        queryThread.start();
        Assertions.assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));

        Thread refreshThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    // resetToUninitialized() grabs the catalog monitor before invalidating the cache.
                    catalog.setInvalidator(() -> {
                        allowLoaderToTouchCatalog.countDown();
                        cache.invalidate("deadlock-key");
                    });
                    catalog.resetToUninitialized(true);
                }),
                "deadlock-catalog-refresh");
        refreshThread.setDaemon(true);
        refreshThread.start();

        assertNoDeadlock(queryThread, refreshThread, backgroundFailure);
    }

    private static void assertNoDeadlock(Thread queryThread, Thread refreshThread,
            AtomicReference<Throwable> backgroundFailure) throws Exception {
        long[] deadlockedThreads = waitForDeadlock(queryThread, refreshThread);
        queryThread.join(TimeUnit.SECONDS.toMillis(5));
        refreshThread.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertNull(backgroundFailure.get(), "unexpected background failure: " + backgroundFailure.get());
        Assertions.assertNull(deadlockedThreads,
                String.format("detected deadlock between threads %s and %s",
                        queryThread.getName(), refreshThread.getName()));
        Assertions.assertFalse(queryThread.isAlive(), queryThread.getName() + " is still running");
        Assertions.assertFalse(refreshThread.isAlive(), refreshThread.getName() + " is still running");
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private static void runQuietly(AtomicReference<Throwable> failure, ThrowingRunnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static long[] waitForDeadlock(Thread queryThread, Thread refreshThread) throws InterruptedException {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        for (int i = 0; i < 100; i++) {
            long[] deadlockedThreads = threadMxBean.findDeadlockedThreads();
            if (deadlockedThreads != null
                    && contains(deadlockedThreads, queryThread.getId())
                    && contains(deadlockedThreads, refreshThread.getId())) {
                return deadlockedThreads;
            }
            Thread.sleep(50);
        }
        return null;
    }

    private static boolean contains(long[] ids, long targetId) {
        return Arrays.stream(ids).anyMatch(id -> id == targetId);
    }

    private static class DeadlockCatalog extends ExternalCatalog {
        private Runnable invalidator = () -> {
        };

        DeadlockCatalog() {
            super(1L, "deadlock-catalog", Type.TEST, "");
            initialized = true;
        }

        void setInvalidator(Runnable invalidator) {
            this.invalidator = invalidator;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public void onClose() {
        }

        @Override
        public void onRefreshCache(boolean invalidCache) {
            // Keep the harness catalog usable after refresh so the test only checks lock ordering.
            initialized = true;
            if (invalidCache) {
                invalidator.run();
            }
        }

        @Override
        protected java.util.List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return java.util.Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
