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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalRowCountCacheTest {
    @Test
    public void testCatalogCacheResetSuppressesPerDatabaseRowCountScan() {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };
        ExternalDatabase<ExternalTable> db = new ExternalDatabase<ExternalTable>(
                catalog, 2L, "db", "db", InitDatabaseLog.Type.TEST) {
            @Override
            protected ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
                    ExternalCatalog externalCatalog, ExternalDatabase externalDatabase) {
                return null;
            }
        };

        // A catalog-wide refresh suppresses the per-database engine flush, so no per-database
        // row-count fence may run; the catalog scope is fenced once by the caller instead.
        db.resetMetaToUninitialized(true, false);
        Mockito.verify(metaCacheMgr).invalidateDb(db);
        Mockito.verify(metaCacheMgr, Mockito.never()).invalidateRowCountCache(1L, 2L);

        // An ordinary removal fences both the engine cache and this database's row counts.
        db.resetMetaToUninitialized();
        Mockito.verify(metaCacheMgr).invalidateRowCountCache(1L, 2L);
    }

    @Test
    public void testDatabaseRemovalFencesRowCountWhenEngineInvalidationFails() {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };
        ExternalDatabase<ExternalTable> db = new ExternalDatabase<ExternalTable>(
                catalog, 2L, "db", "db", InitDatabaseLog.Type.TEST) {
            @Override
            protected ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
                    ExternalCatalog externalCatalog, ExternalDatabase externalDatabase) {
                return null;
            }
        };
        Mockito.doThrow(new IllegalStateException("engine invalidation failed"))
                .when(metaCacheMgr).invalidateDb(db);

        Assertions.assertThrows(IllegalStateException.class, () -> db.resetMetaToUninitialized(true, true));

        // The independent row-count fence must still run when the routed invalidation throws.
        Mockito.verify(metaCacheMgr).invalidateRowCountCache(1L, 2L);
    }

    @Test
    public void testRowCountKeyUsesTableIdAsCacheIdentity() {
        ExternalRowCountCache.RowCountKey key1 = new ExternalRowCountCache.RowCountKey(1, 2, 3);
        ExternalRowCountCache.RowCountKey key2 = new ExternalRowCountCache.RowCountKey(2, 3, 3);

        Assertions.assertEquals(key1, key2);
        Assertions.assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testLoadRowCountPassesFillMetaCacheToTable() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.fetchRowCountWithMetaCache(true)).thenReturn(100L);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenReturn(200L);

        new MockUp<StatisticsUtil>() {
            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return table;
            }
        };

        ExternalRowCountCache.RowCountKey key = new ExternalRowCountCache.RowCountKey(1, 2, 3);
        Assertions.assertEquals(100L, ExternalRowCountCache.loadRowCount(key, true).get());
        Assertions.assertEquals(200L, ExternalRowCountCache.loadRowCount(key, false).get());

        Mockito.verify(table).fetchRowCountWithMetaCache(true);
        Mockito.verify(table).fetchRowCountWithMetaCache(false);
    }

    @Test
    public void testGetCachedRowCountPassesFillMetaCacheToLoader() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.fetchRowCountWithMetaCache(true)).thenReturn(100L);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenReturn(200L);

        new MockUp<StatisticsUtil>() {
            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return table;
            }
        };

        ExternalRowCountCache cache = new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
        Assertions.assertEquals(100L, cache.getCachedRowCount(1, 2, 3, true));
        Assertions.assertEquals(200L, cache.getCachedRowCount(1, 2, 4, false));

        Mockito.verify(table).fetchRowCountWithMetaCache(true);
        Mockito.verify(table).fetchRowCountWithMetaCache(false);
    }

    @Test
    public void testInvalidationScopes() {
        ExternalRowCountCache.RowCountCacheLoader loader = new ExternalRowCountCache.RowCountCacheLoader() {
            @Override
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                return Optional.of(rowCountKey.getTableId());
            }
        };

        ExternalRowCountCache cache = new ExternalRowCountCache(
                MoreExecutors.newDirectExecutorService(), null, loader);
        Assertions.assertEquals(100L, cache.getCachedRowCount(1, 10, 100, false));
        Assertions.assertEquals(101L, cache.getCachedRowCount(1, 10, 101, false));
        Assertions.assertEquals(102L, cache.getCachedRowCount(1, 11, 102, false));
        Assertions.assertEquals(200L, cache.getCachedRowCount(2, 20, 200, false));

        cache.invalidateTable(1, 10, 100);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCountIfPresent(1, 10, 100));
        Assertions.assertEquals(101L, cache.getCachedRowCountIfPresent(1, 10, 101));

        cache.invalidateDb(1, 10);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCountIfPresent(1, 10, 101));
        Assertions.assertEquals(102L, cache.getCachedRowCountIfPresent(1, 11, 102));

        cache.invalidateCatalog(1);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCountIfPresent(1, 11, 102));
        Assertions.assertEquals(200L, cache.getCachedRowCountIfPresent(2, 20, 200));
    }

    @Test
    public void testInvalidateWhileRefreshIsRunningDoesNotRepublishStaleValue() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch allowRefreshToFinish = new CountDownLatch(1);
        ExternalRowCountCache.RowCountCacheLoader loader = new ExternalRowCountCache.RowCountCacheLoader() {
            @Override
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                int currentLoad = loadCount.incrementAndGet();
                if (currentLoad == 2) {
                    refreshStarted.countDown();
                    Assertions.assertTrue(Uninterruptibles.awaitUninterruptibly(
                            allowRefreshToFinish, 30, TimeUnit.SECONDS));
                }
                return Optional.of(currentLoad * 100L);
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ExternalRowCountCache cache = new ExternalRowCountCache(executor, null, loader);
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 10, 100, false));

            cache.refreshForTest(1, 10, 100);
            Assertions.assertTrue(refreshStarted.await(30, TimeUnit.SECONDS));
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 10, 100, false));

            cache.invalidateTable(1, 10, 100);
            allowRefreshToFinish.countDown();
            executor.submit(() -> { }).get(30, TimeUnit.SECONDS);

            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    cache.getCachedRowCountIfPresent(1, 10, 100));
            Assertions.assertEquals(300L, cache.getCachedRowCount(1, 10, 100, false));
        } finally {
            allowRefreshToFinish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCatalogInvalidationUsesFullInFlightLoadIdentity() throws Exception {
        CountDownLatch oldLoadStarted = new CountDownLatch(1);
        CountDownLatch newLoadStarted = new CountDownLatch(1);
        CountDownLatch allowLoadsToFinish = new CountDownLatch(1);
        ExternalRowCountCache.RowCountCacheLoader loader = new ExternalRowCountCache.RowCountCacheLoader() {
            @Override
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                if (rowCountKey.getCatalogId() == 1L) {
                    oldLoadStarted.countDown();
                } else {
                    newLoadStarted.countDown();
                }
                Assertions.assertTrue(Uninterruptibles.awaitUninterruptibly(
                        allowLoadsToFinish, 30, TimeUnit.SECONDS));
                return Optional.of(rowCountKey.getCatalogId() * 100L);
            }
        };

        ExecutorService loaderExecutor = Executors.newFixedThreadPool(2);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            ExternalRowCountCache cache = new ExternalRowCountCache(loaderExecutor, null, loader);
            Future<Long> oldLoad = callers.submit(() -> cache.getCachedRowCount(1, 10, 100, false));
            Assertions.assertTrue(oldLoadStarted.await(30, TimeUnit.SECONDS));
            cache.invalidateCatalog(1L);

            Future<Long> newLoad = callers.submit(() -> cache.getCachedRowCount(2, 20, 100, false));
            Assertions.assertTrue(newLoadStarted.await(30, TimeUnit.SECONDS));
            cache.invalidateCatalog(2L);
            allowLoadsToFinish.countDown();

            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, oldLoad.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, newLoad.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    cache.getCachedRowCountIfPresent(2, 20, 100));
        } finally {
            allowLoadsToFinish.countDown();
            callers.shutdownNow();
            loaderExecutor.shutdownNow();
        }
    }

    @Test
    public void testClosingFenceInvalidatesLoadAdmittedBeforeIt() throws Exception {
        CountDownLatch loadAdmitted = new CountDownLatch(1);
        CountDownLatch allowLoadToFinish = new CountDownLatch(1);
        ExternalRowCountCache.RowCountCacheLoader loader = new ExternalRowCountCache.RowCountCacheLoader() {
            @Override
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                loadAdmitted.countDown();
                Assertions.assertTrue(Uninterruptibles.awaitUninterruptibly(
                        allowLoadToFinish, 30, TimeUnit.SECONDS));
                return Optional.of(100L);
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newSingleThreadExecutor();
        try {
            ExternalRowCountCache cache = new ExternalRowCountCache(executor, null, loader);
            // Opening fence, then a load admitted inside the admission window.
            cache.invalidateTable(1, 10, 100);
            Future<Long> staleLoad = callers.submit(() -> cache.getCachedRowCount(1, 10, 100, false));
            Assertions.assertTrue(loadAdmitted.await(30, TimeUnit.SECONDS));

            // The selective refresh finishes and closes the window before the load can publish.
            cache.invalidateTable(1, 10, 100);
            allowLoadToFinish.countDown();

            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, staleLoad.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    cache.getCachedRowCountIfPresent(1, 10, 100));
        } finally {
            allowLoadToFinish.countDown();
            callers.shutdownNow();
            executor.shutdownNow();
        }
    }

    @Test
    public void testRejectedSubmissionRemovesInFlightFence() {
        ExecutorService rejectingExecutor = Mockito.mock(ExecutorService.class);
        Mockito.doThrow(new RejectedExecutionException("rejected"))
                .when(rejectingExecutor).execute(Mockito.any(Runnable.class));
        ExternalRowCountCache cache = new ExternalRowCountCache(rejectingExecutor);

        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                cache.getCachedRowCount(1, 10, 100, false));
        Assertions.assertEquals(0, cache.getInFlightLoadCountForTest());
    }

    @Test
    public void testLoadWithException() throws Exception {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, Integer.MAX_VALUE, "TEST", true);
        AtomicInteger counter = new AtomicInteger(0);

        ExternalRowCountCache.RowCountCacheLoader loader = new ExternalRowCountCache.RowCountCacheLoader() {
            @Override
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                int currentLoad = counter.incrementAndGet();
                if (currentLoad == 1) {
                    return null;
                }
                if (currentLoad == 3) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Optional.of(100L);
            }
        };
        ExternalRowCountCache cache = new ExternalRowCountCache(executor, null, loader);
        long cachedRowCount = cache.getCachedRowCount(1, 1, 1, false);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cachedRowCount);
        for (int i = 0; i < 60; i++) {
            if (counter.get() == 1) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertEquals(1, counter.get());

        cache.getCachedRowCount(1, 1, 1, false);
        for (int i = 0; i < 60; i++) {
            cachedRowCount = cache.getCachedRowCount(1, 1, 1, false);
            if (cachedRowCount != TableIf.UNKNOWN_ROW_COUNT) {
                Assertions.assertEquals(100, cachedRowCount);
                break;
            }
            Thread.sleep(1000);
        }
        cachedRowCount = cache.getCachedRowCount(1, 1, 1, false);
        Assertions.assertEquals(100, cachedRowCount);
        Assertions.assertEquals(2, counter.get());

        cachedRowCount = cache.getCachedRowCount(2, 2, 2, false);
        Assertions.assertEquals(100, cachedRowCount);
        Thread.sleep(1000);
        cachedRowCount = cache.getCachedRowCount(2, 2, 2, false);
        Assertions.assertEquals(100, cachedRowCount);
        for (int i = 0; i < 60; i++) {
            if (counter.get() == 3) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertEquals(3, counter.get());
    }
}
