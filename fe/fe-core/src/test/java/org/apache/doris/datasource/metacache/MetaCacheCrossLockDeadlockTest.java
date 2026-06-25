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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Regression test for cross-lock deadlock (issue 1675416).
 *
 * <p>Two locks are involved in production:
 * <ul>
 *   <li><b>A</b> = {@code ExternalDatabase} instance monitor.</li>
 *   <li><b>B</b> = a CHM bin lock inside the db-level {@code MetaCache}
 *       (held by Caffeine while running the cache loader / removal listener).</li>
 * </ul>
 *
 * <p>The deadlock occurs when:
 * <pre>
 *   T1 (refresh): holds A inside resetMetaToUninitialized, then waits for B
 *                 because metaCache.invalidateAll() needs the bin lock.
 *   T2 (query):   holds B inside the cache loader, then waits for A
 *                 because the loader calls makeSureInitialized().
 * </pre>
 *
 * <p>Fix: move {@code metaCache.invalidateAll()} <em>out</em> of
 * {@code synchronized(this)} in {@code ExternalDatabase#resetMetaToUninitialized()}.
 *
 * <p>This UT does <b>not</b> rely on Caffeine's internal CHM bin lock (which
 * makes timing brittle). Instead it substitutes the db's {@code MetaCache}
 * with a subclass whose {@code invalidateAll()} acquires an external lock
 * <b>B</b>. Then we orchestrate exactly the AB-BA topology:
 * <ul>
 *   <li>T1 holds B and waits for A (mimics the loader path).</li>
 *   <li>T2 calls {@code resetMetaToUninitialized()} (which would acquire A
 *       and then call {@code invalidateAll()} that needs B).</li>
 * </ul>
 *
 * <p>With the fix, T2 releases A before calling {@code invalidateAll()}, so
 * T1 can finish; without the fix the test deterministically deadlocks.
 */
public class MetaCacheCrossLockDeadlockTest {

    @Test(timeout = 30_000)
    public void testNoDeadlockBetweenResetAndInvalidateAll() throws Exception {
        // ----- Mock catalog (avoid pulling in Env's catalog wiring) -----
        TestExternalCatalog catalog = Mockito.mock(TestExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1001L);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(0);
        Mockito.when(catalog.getLowerCaseMetaNames()).thenReturn("false");
        Mockito.when(catalog.getMetaNamesMapping()).thenReturn("");
        Mockito.doNothing().when(catalog).makeSureInitialized();

        // Real ExternalDatabase under test.
        TestExternalDatabase db = new TestExternalDatabase(catalog, 2002L, "test_db", "test_db");

        // ----- Build a MetaCache subclass whose invalidateAll() acquires lock B -----
        final ReentrantLock lockB = new ReentrantLock();
        final CountDownLatch t1HoldsB = new CountDownLatch(1);
        final CountDownLatch t2InvalidateAllStarted = new CountDownLatch(1);

        ExecutorService cacheExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "metacache-deadlock-ut-cache-exec");
            t.setDaemon(true);
            return t;
        });

        CacheLoader<String, List<Pair<String, String>>> namesLoader =
                key -> java.util.Collections.emptyList();
        CacheLoader<String, Optional<TestExternalTable>> objLoader =
                key -> Optional.empty();

        MetaCache<TestExternalTable> spyCache = new MetaCache<TestExternalTable>(
                "deadlock-ut",
                cacheExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                64,
                namesLoader,
                objLoader,
                (k, v, c) -> { /* no-op */ }) {
            @Override
            public void invalidateAll() {
                // Mark that T2 has reached invalidateAll so T1 can release B in time.
                t2InvalidateAllStarted.countDown();
                // Acquire lock B (this is the bin-lock in production). If T2 still
                // holds A here (i.e. fix has been reverted), and T1 holds B waiting
                // for A, this lock() will block forever.
                lockB.lock();
                try {
                    // no-op
                } finally {
                    lockB.unlock();
                }
            }
        };

        // Inject MetaCache and initialized=true via reflection.
        Field metaCacheField = org.apache.doris.datasource.ExternalDatabase.class
                .getDeclaredField("metaCache");
        metaCacheField.setAccessible(true);
        metaCacheField.set(db, spyCache);

        Field initializedField = org.apache.doris.datasource.ExternalDatabase.class
                .getDeclaredField("initialized");
        initializedField.setAccessible(true);
        initializedField.setBoolean(db, true);

        // ----- Mock Env so the tail-call invalidateDb is a no-op -----
        ExternalMetaCacheMgr mgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.doNothing().when(mgr).invalidateDb(Mockito.anyLong(), Mockito.anyString());
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(mgr);

        ExecutorService runner = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        AtomicReference<Throwable> t1Error = new AtomicReference<>();
        AtomicReference<Throwable> t2Error = new AtomicReference<>();

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);

            // ----- T1: hold B, then ask for A -----
            // Mirrors the production cache-loader path that holds the bin lock and
            // calls makeSureInitialized(), which acquires synchronized(db).
            runner.submit(() -> {
                try {
                    lockB.lock();
                    try {
                        t1HoldsB.countDown();
                        // Wait until T2 has entered resetMetaToUninitialized and reached
                        // the point of calling invalidateAll(). With the fix, T2 must
                        // already have released A here.
                        if (!t2InvalidateAllStarted.await(10, TimeUnit.SECONDS)) {
                            t1Error.set(new AssertionError(
                                    "T2 did not reach invalidateAll() in time"));
                            return;
                        }
                        // Now try to acquire A. If reset still holds A while waiting
                        // for B, this would deadlock.
                        synchronized (db) {
                            // no-op; just prove we can acquire A while holding B.
                        }
                    } finally {
                        lockB.unlock();
                    }
                } catch (Throwable e) {
                    t1Error.set(e);
                }
            });

            // Make sure T1 is holding B before T2 starts.
            Assert.assertTrue("T1 failed to acquire lock B in time",
                    t1HoldsB.await(5, TimeUnit.SECONDS));

            // ----- T2: resetMetaToUninitialized() -----
            // With the fix, the synchronized(this) section is short and is released
            // before invalidateAll() is invoked, so it must not block on T1.
            runner.submit(() -> {
                try {
                    db.resetMetaToUninitialized();
                } catch (Throwable e) {
                    t2Error.set(e);
                }
            });

            runner.shutdown();
            boolean terminated = runner.awaitTermination(20, TimeUnit.SECONDS);
            Assert.assertTrue(
                    "Cross-lock deadlock detected: resetMetaToUninitialized must not call "
                            + "invalidateAll() while still holding synchronized(this)",
                    terminated);
        } finally {
            if (!runner.isTerminated()) {
                runner.shutdownNow();
            }
            cacheExecutor.shutdownNow();
        }

        if (t1Error.get() != null) {
            throw new AssertionError("T1 failed", t1Error.get());
        }
        if (t2Error.get() != null) {
            throw new AssertionError("T2 (resetMetaToUninitialized) failed", t2Error.get());
        }
    }
}
