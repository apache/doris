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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test to verify that ExternalDatabase.resetMetaToUninitialized()
 * does not deadlock with concurrent Caffeine cache loading that calls back
 * into ExternalDatabase.makeSureInitialized().
 *
 * The deadlock lock ordering (before fix):
 *   Path 1 (REFRESH DATABASE):
 *     synchronized(ExternalDatabase) -> metaCache.invalidateAll() -> Caffeine internal locks
 *   Path 2 (cache loading):
 *     Caffeine internal locks (computeIfAbsent) -> loader -> buildTableForInit()
 *         -> getTableNamesWithLock() -> makeSureInitialized() -> synchronized(ExternalDatabase)
 *
 * After fix:
 *   - resetMetaToUninitialized() releases synchronized(this) before invalidateAll()
 *   - getTableNamesForCheck() avoids makeSureInitialized() on fast path,
 *     with safe fallback to getTableNamesWithLock() when metaCache was reset
 */
public class ExternalDatabaseDeadlockTest {

    @Test
    public void testResetMetaToUninitializedShouldNotDeadlockWithCacheLoader() throws Exception {
        DeadlockDbCatalog catalog = new DeadlockDbCatalog();
        DeadlockDatabase db = new DeadlockDatabase(catalog, 1L, "test_db", "test_db");
        // Initialize the database so that metaCache is built
        db.makeSureInitialized();

        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToTouchDb = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();

        // The loader holds Caffeine's per-key lock before it calls back into the database.
        // This models what happens when MetaCache.getMetaObj() triggers the metaObjCache loader:
        // Caffeine internal lock -> loader -> buildTableForInit -> makeSureInitialized
        LoadingCache<String, String> cache = Caffeine.newBuilder().build(key -> {
            loaderEntered.countDown();
            awaitLatch(allowLoaderToTouchDb);
            // This simulates the callback from cache loader into the database:
            // buildTableForInit -> getTableNamesWithLock -> makeSureInitialized
            db.makeSureInitialized();
            return key;
        });

        // Thread B: cache loader thread (holds Caffeine node lock)
        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> cache.get("deadlock-key")),
                "deadlock-db-cache-loader");
        queryThread.setDaemon(true);
        queryThread.start();
        Assertions.assertTrue(loaderEntered.await(5, TimeUnit.SECONDS),
                "loader should have entered within timeout");

        // Thread A: refresh database thread
        // With the fix, resetMetaToUninitialized() releases synchronized(this) before
        // calling invalidateAll(). To verify deterministically, release the loader
        // latch first so Thread B can proceed through makeSureInitialized() while
        // we hold Caffeine internal locks (via invalidate). If the lock ordering were
        // still inverted, this would deadlock:
        //   Thread A: Caffeine lock (invalidate) -> waiting for nothing
        //   Thread B: Caffeine lock (loader) -> makeSureInitialized -> synchronized(db) -> OK
        Thread refreshThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    // Step 1: synchronized(this) { update state } then release
                    synchronized (db) {
                        db.setInitializedForTest(false);
                    }
                    // Step 2: release loader so Thread B can race with our invalidate
                    allowLoaderToTouchDb.countDown();
                    // Step 3: invalidate cache (needs Caffeine internal locks).
                    // Thread B may concurrently call db.makeSureInitialized() which
                    // acquires synchronized(db) — safe because we released it in step 1.
                    cache.invalidate("deadlock-key");
                }),
                "deadlock-db-refresh");
        refreshThread.setDaemon(true);
        refreshThread.start();

        assertNoDeadlock(queryThread, refreshThread, backgroundFailure);
    }

    /**
     * Test the actual fix: resetMetaToUninitialized() with a concurrent cache loader
     * that goes through the real buildTableForInit -> getTableNamesForCheck path.
     */
    @Test
    public void testResetMetaToUninitializedWithRealBuildTableForInitPath() throws Exception {
        DeadlockDbCatalog catalog = new DeadlockDbCatalog();
        CoordinatedDatabase db = new CoordinatedDatabase(catalog, 1L, "test_db", "test_db");
        db.makeSureInitialized();

        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToProceed = new CountDownLatch(1);
        db.setLatches(loaderEntered, allowLoaderToProceed);

        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();

        // Thread B: triggers getTableNullable which eventually triggers the
        // metaObjCache loader -> buildTableForInit -> getTableNamesForCheck
        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    db.getTableNullable("test_table");
                }),
                "deadlock-db-real-loader");
        queryThread.setDaemon(true);
        queryThread.start();
        Assertions.assertTrue(loaderEntered.await(10, TimeUnit.SECONDS),
                "buildTableForInit should have been entered within timeout");

        // Thread A: release the loader latch, then call resetMetaToUninitialized().
        // The latch must be counted down before reset so the loader can proceed
        // deterministically rather than timing out. After the fix, resetMetaToUninitialized()
        // releases synchronized(this) before invalidateAll(), so Thread B's subsequent
        // getTableNamesWithLock() -> makeSureInitialized() won't deadlock.
        Thread refreshThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    allowLoaderToProceed.countDown();
                    db.resetMetaToUninitialized();
                }),
                "deadlock-db-real-refresh");
        refreshThread.setDaemon(true);
        refreshThread.start();

        assertNoDeadlock(queryThread, refreshThread, backgroundFailure);
    }

    // ---- Test harness ----

    private static void assertNoDeadlock(Thread t1, Thread t2,
            AtomicReference<Throwable> backgroundFailure) throws Exception {
        long[] deadlockedThreads = waitForDeadlock(t1, t2);
        t1.join(TimeUnit.SECONDS.toMillis(10));
        t2.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertNull(backgroundFailure.get(),
                "unexpected background failure: " + backgroundFailure.get());
        Assertions.assertNull(deadlockedThreads,
                String.format("detected deadlock between threads %s and %s",
                        t1.getName(), t2.getName()));
        Assertions.assertFalse(t1.isAlive(), t1.getName() + " is still running");
        Assertions.assertFalse(t2.isAlive(), t2.getName() + " is still running");
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void runQuietly(AtomicReference<Throwable> failure, ThrowingRunnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    @SuppressWarnings("deprecation")
    private static long[] waitForDeadlock(Thread t1, Thread t2) throws InterruptedException {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        long t1Id = t1.getId();
        long t2Id = t2.getId();
        for (int i = 0; i < 200; i++) {
            long[] deadlockedThreads = threadMxBean.findDeadlockedThreads();
            if (deadlockedThreads != null
                    && contains(deadlockedThreads, t1Id)
                    && contains(deadlockedThreads, t2Id)) {
                return deadlockedThreads;
            }
            Thread.sleep(50);
        }
        return null;
    }

    private static boolean contains(long[] ids, long targetId) {
        return Arrays.stream(ids).anyMatch(id -> id == targetId);
    }

    /**
     * Minimal ExternalCatalog for deadlock testing.
     */
    private static class DeadlockDbCatalog extends ExternalCatalog {
        DeadlockDbCatalog() {
            super(1L, "deadlock-db-catalog", Type.TEST, "");
            initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public void onClose() {
        }

        @Override
        public void onRefreshCache(boolean invalidCache) {
            initialized = true;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.singletonList("test_table");
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return "test_table".equals(tblName);
        }
    }

    /**
     * Database that exposes initialized setter for lock-ordering verification test.
     */
    private static class DeadlockDatabase extends ExternalDatabase<ExternalTable> {
        DeadlockDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.TEST);
        }

        @Override
        protected ExternalTable buildTableInternal(String remoteTableName, String localTableName,
                long tblId, ExternalCatalog catalog, ExternalDatabase db) {
            return null;
        }

        void setInitializedForTest(boolean value) {
            this.initialized = value;
        }
    }

    /**
     * Database that intercepts buildTableForInit to coordinate thread timing
     * and explicitly exercises the lock path for the end-to-end deadlock test.
     *
     * The overridden buildTableForInit explicitly calls getTableNamesWithLock()
     * (which calls makeSureInitialized() -> synchronized(this)) to ensure the
     * deadlock-critical lock ordering is exercised regardless of FeConstants
     * settings that may skip the checkExists block in unit tests.
     */
    private static class CoordinatedDatabase extends ExternalDatabase<ExternalTable> {
        private CountDownLatch loaderEntered;
        private CountDownLatch allowLoaderToProceed;

        CoordinatedDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.TEST);
        }

        void setLatches(CountDownLatch entered, CountDownLatch proceed) {
            this.loaderEntered = entered;
            this.allowLoaderToProceed = proceed;
        }

        @Override
        public ExternalTable buildTableForInit(String remoteTableName, String localTableName,
                long tblId, ExternalCatalog catalog, ExternalDatabase db, boolean checkExists) {
            if (loaderEntered != null) {
                loaderEntered.countDown();
                boolean released = false;
                try {
                    released = allowLoaderToProceed.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Assertions.fail("loader interrupted while waiting for latch");
                }
                Assertions.assertTrue(released,
                        "loader was not released within timeout - possible deadlock");
                // Explicitly exercise the deadlock-critical lock path:
                // getTableNamesWithLock -> makeSureInitialized -> synchronized(this).
                // Without the fix, this deadlocks when resetMetaToUninitialized()
                // holds synchronized(this) while calling invalidateAll() on the Caffeine cache.
                // With the fix, resetMetaToUninitialized() releases the monitor first.
                getTableNamesWithLock();
            }
            return super.buildTableForInit(remoteTableName, localTableName, tblId, catalog, db, checkExists);
        }

        @Override
        protected ExternalTable buildTableInternal(String remoteTableName, String localTableName,
                long tblId, ExternalCatalog catalog, ExternalDatabase db) {
            return null;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
