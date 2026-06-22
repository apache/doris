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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for cross-lock deadlock (issue 1675416).
 *
 * <p>Deadlock cycle:
 * <pre>
 *   T1: holds synchronized(ExternalDatabase.this), waits for Caffeine's internal CHM bin lock
 *   T2: holds the CHM bin lock (inside loader), waits for synchronized(ExternalDatabase.this)
 * </pre>
 *
 * <p>Fix: {@code metaCache.invalidateAll()} in {@code ExternalDatabase#resetMetaToUninitialized()}
 * must be moved out of the {@code synchronized(this)} block.
 *
 * <p>This UT orchestrates the AB-BA scenario via the real {@code getTableNullable} and
 * {@code resetMetaToUninitialized}. If anyone moves {@code invalidateAll()} back inside
 * {@code synchronized(this)}, this UT will time out.
 */
public class MetaCacheCrossLockDeadlockTest {

    @Test(timeout = 30_000)
    public void testNoDeadlockBetweenResetAndGetTableNullable() throws Exception {
        // 1) mock TestExternalCatalog to bypass Env / catalog provider
        TestExternalCatalog catalog = Mockito.mock(TestExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1001L);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(0);
        Mockito.when(catalog.getLowerCaseMetaNames()).thenReturn("false");
        Mockito.when(catalog.getMetaNamesMapping()).thenReturn("");
        Mockito.doNothing().when(catalog).makeSureInitialized();

        // 2) real TestExternalDatabase
        TestExternalDatabase db = new TestExternalDatabase(catalog, 2002L, "test_db", "test_db");

        // 3) custom loader: blocks on a latch to orchestrate AB-BA
        final CountDownLatch loaderEntered = new CountDownLatch(1);
        final CountDownLatch resetInFlight = new CountDownLatch(1);

        CacheLoader<String, List<Pair<String, String>>> namesLoader =
                key -> Lists.newArrayList(Pair.of("t1", "t1"));
        CacheLoader<String, Optional<TestExternalTable>> objLoader = key -> {
            // CHM bin lock is already held here
            loaderEntered.countDown();
            try {
                resetInFlight.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // simulate production path: buildTableForInit -> getTableNamesWithLock -> makeSureInitialized
            // acquire db monitor while still holding the bin lock to form the second leg of AB-BA
            synchronized (db) {
                // no-op
            }
            return Optional.of(new TestExternalTable(
                    System.identityHashCode(key), key, key, catalog, db));
        };

        ExecutorService cacheExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "metacache-deadlock-ut-cache-exec");
            t.setDaemon(true);
            return t;
        });
        MetaCache<TestExternalTable> metaCache = new MetaCache<>(
                "deadlock-ut",
                cacheExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                64,
                namesLoader,
                objLoader,
                (k, v, c) -> { /* no-op */ });

        // inject metaCache and initialized=true via reflection, skipping buildMetaCache() (which depends on Env)
        Field metaCacheField = org.apache.doris.datasource.ExternalDatabase.class
                .getDeclaredField("metaCache");
        metaCacheField.setAccessible(true);
        metaCacheField.set(db, metaCache);

        Field initializedField = org.apache.doris.datasource.ExternalDatabase.class
                .getDeclaredField("initialized");
        initializedField.setAccessible(true);
        initializedField.setBoolean(db, true);

        // 4) mock Env.getCurrentEnv() so the invalidateDb at the tail of resetMetaToUninitialized is a no-op
        ExternalMetaCacheMgr mgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.doNothing().when(mgr).invalidateDb(Mockito.anyLong(), Mockito.anyString());

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(mgr);

        ExecutorService runner = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        AtomicReference<Throwable> threadAError = new AtomicReference<>();
        AtomicReference<Throwable> threadBError = new AtomicReference<>();

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);

            // Thread A: getTableNullable -> MetaCache.getMetaObj -> custom loader blocks
            runner.submit(() -> {
                try {
                    Object t = db.getTableNullable("t1");
                    if (t == null) {
                        threadAError.set(new AssertionError("getTableNullable returned null"));
                    }
                } catch (Throwable e) {
                    threadAError.set(e);
                }
            });

            Assert.assertTrue("Loader did not enter in time",
                    loaderEntered.await(10, TimeUnit.SECONDS));

            // Thread B: resetMetaToUninitialized -> synchronized(this) -> metaCache.invalidateAll()
            runner.submit(() -> {
                try {
                    resetInFlight.countDown();
                    db.resetMetaToUninitialized();
                } catch (Throwable e) {
                    threadBError.set(e);
                }
            });

            runner.shutdown();
            boolean terminated = runner.awaitTermination(20, TimeUnit.SECONDS);
            Assert.assertTrue(
                    "Cross-lock deadlock detected: resetMetaToUninitialized must not call "
                            + "invalidateAll() while holding synchronized(this)",
                    terminated);
        } finally {
            if (!runner.isTerminated()) {
                runner.shutdownNow();
            }
            cacheExecutor.shutdownNow();
        }

        if (threadAError.get() != null) {
            throw new AssertionError("getTableNullable thread failed", threadAError.get());
        }
        if (threadBError.get() != null) {
            throw new AssertionError("resetMetaToUninitialized thread failed", threadBError.get());
        }
    }
}
