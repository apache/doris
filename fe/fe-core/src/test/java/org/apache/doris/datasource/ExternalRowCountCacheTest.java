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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalRowCountCacheTest {
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

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 2, 3)).thenReturn(table);

            ExternalRowCountCache.RowCountKey key = new ExternalRowCountCache.RowCountKey(1, 2, 3);
            Assertions.assertEquals(100L, ExternalRowCountCache.loadRowCount(key, true).get());
            Assertions.assertEquals(200L, ExternalRowCountCache.loadRowCount(key, false).get());
        }

        Mockito.verify(table).fetchRowCountWithMetaCache(true);
        Mockito.verify(table).fetchRowCountWithMetaCache(false);
    }

    @Test
    public void testGetCachedRowCountPassesFillMetaCacheToLoader() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.fetchRowCountWithMetaCache(true)).thenReturn(100L);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenReturn(200L);

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 2, 3)).thenReturn(table);
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 2, 4)).thenReturn(table);

            ExternalRowCountCache cache = new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 2, 3, true));
            Assertions.assertEquals(200L, cache.getCachedRowCount(1, 2, 4, false));
        }

        Mockito.verify(table).fetchRowCountWithMetaCache(true);
        Mockito.verify(table).fetchRowCountWithMetaCache(false);
    }

    @Test
    public void testInvalidateTableReloadsRowCount() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        AtomicLong rowCount = new AtomicLong(100L);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenAnswer(inv -> rowCount.get());

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 2, 3)).thenReturn(table);

            ExternalRowCountCache cache = new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 2, 3, false));

            rowCount.set(200L);
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 2, 3, false));

            cache.invalidateTable(1, 2, 3);
            Assertions.assertEquals(200L, cache.getCachedRowCount(1, 2, 3, false));
        }

        Mockito.verify(table, Mockito.times(2)).fetchRowCountWithMetaCache(false);
    }

    @Test
    public void testCatalogAndDbInvalidationKeepUnrelatedRowCounts() {
        ExternalTable firstTable = Mockito.mock(ExternalTable.class);
        ExternalTable secondTable = Mockito.mock(ExternalTable.class);
        ExternalTable otherCatalogTable = Mockito.mock(ExternalTable.class);
        AtomicLong firstRowCount = new AtomicLong(100L);
        AtomicLong secondRowCount = new AtomicLong(200L);
        AtomicLong otherCatalogRowCount = new AtomicLong(300L);
        Mockito.when(firstTable.fetchRowCountWithMetaCache(false)).thenAnswer(inv -> firstRowCount.get());
        Mockito.when(secondTable.fetchRowCountWithMetaCache(false)).thenAnswer(inv -> secondRowCount.get());
        Mockito.when(otherCatalogTable.fetchRowCountWithMetaCache(false))
                .thenAnswer(inv -> otherCatalogRowCount.get());

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 11, 111)).thenReturn(firstTable);
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(1, 12, 112)).thenReturn(secondTable);
            mockedStatisticsUtil.when(() -> StatisticsUtil.findTable(2, 11, 211)).thenReturn(otherCatalogTable);

            ExternalRowCountCache cache = new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
            Assertions.assertEquals(100L, cache.getCachedRowCount(1, 11, 111, false));
            Assertions.assertEquals(200L, cache.getCachedRowCount(1, 12, 112, false));
            Assertions.assertEquals(300L, cache.getCachedRowCount(2, 11, 211, false));

            firstRowCount.set(101L);
            secondRowCount.set(202L);
            otherCatalogRowCount.set(303L);
            cache.invalidateDb(1, 11);
            Assertions.assertEquals(101L, cache.getCachedRowCount(1, 11, 111, false));
            Assertions.assertEquals(200L, cache.getCachedRowCount(1, 12, 112, false));
            Assertions.assertEquals(300L, cache.getCachedRowCount(2, 11, 211, false));

            firstRowCount.set(111L);
            cache.invalidateCatalog(1);
            Assertions.assertEquals(111L, cache.getCachedRowCount(1, 11, 111, false));
            Assertions.assertEquals(202L, cache.getCachedRowCount(1, 12, 112, false));
            Assertions.assertEquals(300L, cache.getCachedRowCount(2, 11, 211, false));
        }
    }

    @Test
    public void testTableInvalidationWaitsForFuturePublication() throws Exception {
        ExecutorService loadExecutor = Mockito.mock(ExecutorService.class);
        CountDownLatch loadSubmitted = new CountDownLatch(1);
        CountDownLatch allowPublication = new CountDownLatch(1);
        Mockito.doAnswer(inv -> {
            loadSubmitted.countDown();
            Assertions.assertTrue(allowPublication.await(3L, TimeUnit.SECONDS));
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(loadExecutor).execute(Mockito.any());
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (MockedConstruction<ExternalRowCountCache.RowCountCacheLoader> mocked =
                Mockito.mockConstruction(ExternalRowCountCache.RowCountCacheLoader.class,
                        Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                        (loader, context) -> Mockito.doReturn(Optional.of(100L))
                                .when(loader).doLoad(Mockito.any()))) {
            ExternalRowCountCache cache = new ExternalRowCountCache(loadExecutor);

            Future<Long> load = callers.submit(() -> cache.getCachedRowCount(1, 2, 3, false));
            Assertions.assertTrue(loadSubmitted.await(3L, TimeUnit.SECONDS));
            CountDownLatch invalidationStarted = new CountDownLatch(1);
            Future<?> invalidation = callers.submit(() -> {
                invalidationStarted.countDown();
                cache.invalidateTable(1, 2, 3);
            });
            Assertions.assertTrue(invalidationStarted.await(3L, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class,
                    () -> invalidation.get(200L, TimeUnit.MILLISECONDS));

            allowPublication.countDown();
            Assertions.assertEquals(100L, load.get(3L, TimeUnit.SECONDS));
            invalidation.get(3L, TimeUnit.SECONDS);
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCountIfPresent(1, 2, 3));
        } finally {
            allowPublication.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    public void testTableInvalidationDiscardsPendingLoadCompletion() throws Exception {
        CompletableFuture<Optional<Long>> loadedRowCount = new CompletableFuture<>();
        CountDownLatch loadStarted = new CountDownLatch(1);
        ExecutorService callers = Executors.newSingleThreadExecutor();
        try (MockedConstruction<ExternalRowCountCache.RowCountCacheLoader> mocked =
                Mockito.mockConstruction(ExternalRowCountCache.RowCountCacheLoader.class,
                        (loader, context) -> Mockito.when(loader.asyncLoad(Mockito.any(), Mockito.any()))
                                .thenAnswer(inv -> {
                                    loadStarted.countDown();
                                    return loadedRowCount;
                                }))) {
            ExternalRowCountCache cache =
                    new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());

            Future<Long> load = callers.submit(() -> cache.getCachedRowCount(1, 2, 3, false));
            Assertions.assertTrue(loadStarted.await(3L, TimeUnit.SECONDS));
            cache.invalidateTable(1, 2, 3);

            loadedRowCount.complete(Optional.of(100L));
            Assertions.assertEquals(100L, load.get(3L, TimeUnit.SECONDS));
            Assertions.assertEquals(
                    TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCountIfPresent(1, 2, 3));
        } finally {
            loadedRowCount.complete(Optional.empty());
            callers.shutdownNow();
        }
    }

    @Test
    public void testUnregisterMissingIdentityInvalidatesDeterministicScopes() {
        Map<String, String> properties = Collections.singletonMap(
                "catalog_provider.class", EmptyCatalogProvider.class.getName());
        TestExternalCatalog catalog = new TestExternalCatalog(1L, "catalog1", "", properties, "");
        catalog.setInitializedForTest(true);
        long dbId = Util.genIdByName("catalog1", "db1");
        TestExternalDatabase db = new TestExternalDatabase(catalog, dbId, "db1", "db1");
        db.setInitializedForTest(true);

        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            db.unregisterTable("tbl1");
            catalog.unregisterDatabase("db1");
        }

        Mockito.verify(metaCacheMgr).invalidateTable(
                1L, dbId, "db1", Util.genIdByName("catalog1", "db1", "tbl1"), "tbl1");
        Mockito.verify(metaCacheMgr).invalidateDb(
                1L, Util.genIdByName("catalog1", "db1"), "db1");
    }

    @Test
    public void testTableHelpersUseOwningDatabaseIdentityBeforeTableInitialization() {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        ExternalDatabase database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(database.getId()).thenReturn(11L);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDb()).thenReturn(database);
        Mockito.when(table.getDbId()).thenReturn(0L);
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getId()).thenReturn(22L);
        Mockito.when(table.getName()).thenReturn("tbl1");

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalRowCountCache rowCountCache = Mockito.mock(ExternalRowCountCache.class);
        Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            metaCacheMgr.invalidateTable(table);
            metaCacheMgr.invalidateTableRowCountCache(table);
        }

        Mockito.verify(rowCountCache, Mockito.times(2)).invalidateTable(1L, 11L, 22L);
        Mockito.verify(rowCountCache, Mockito.never()).invalidateTable(1L, 0L, 22L);
    }

    @Test
    public void testRenameInvalidationCompletesDestinationBarrierWhenSourceMetadataFails() {
        ExternalMetaCache engineCache = Mockito.mock(ExternalMetaCache.class);
        Mockito.when(engineCache.engine()).thenReturn("default");
        Mockito.when(engineCache.aliases()).thenReturn(Collections.emptySet());
        Mockito.when(engineCache.isCatalogInitialized(1L)).thenReturn(true);
        Mockito.doThrow(new IllegalStateException("metadata invalidation failed"))
                .when(engineCache).invalidateTable(1L, "db1", "tbl1");

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        metaCacheMgr.replaceEngineCachesForTest(Collections.singletonList(engineCache));
        ExternalRowCountCache rowCountCache = Mockito.mock(ExternalRowCountCache.class);
        Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("catalog1");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        NereidsSortedPartitionsCacheManager sortedPartitionsCacheManager =
                Mockito.mock(NereidsSortedPartitionsCacheManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(sortedPartitionsCacheManager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> metaCacheMgr.invalidateTableRename(
                            1L, 11L, "db1", 22L, "tbl1", 23L, "tbl2"));
        }

        InOrder inOrder = Mockito.inOrder(engineCache, sortedPartitionsCacheManager, rowCountCache);
        inOrder.verify(engineCache).invalidateTable(1L, "db1", "tbl1");
        inOrder.verify(sortedPartitionsCacheManager).invalidateTable("catalog1", "db1", "tbl1");
        inOrder.verify(rowCountCache).invalidateTable(1L, 11L, 22L);
        inOrder.verify(engineCache).invalidateTable(1L, "db1", "tbl2");
        inOrder.verify(sortedPartitionsCacheManager).invalidateTable("catalog1", "db1", "tbl2");
        inOrder.verify(rowCountCache).invalidateTable(1L, 11L, 23L);
    }

    @Test
    public void testCatalogRefreshScansRowCountCacheOnceForMultipleHotDatabases() {
        Map<String, String> properties = Collections.singletonMap(
                "catalog_provider.class", EmptyCatalogProvider.class.getName());
        TestExternalCatalog catalog = new TestExternalCatalog(1L, "catalog1", "", properties, "");
        catalog.setInitializedForTest(true);
        catalog.addDatabaseForTest(new TestExternalDatabase(catalog, 11L, "db1", "db1"));
        catalog.addDatabaseForTest(new TestExternalDatabase(catalog, 12L, "db2", "db2"));

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalRowCountCache rowCountCache = Mockito.spy(
                new ExternalRowCountCache(MoreExecutors.newDirectExecutorService()));
        Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getSortedPartitionsCacheManager())
                .thenReturn(Mockito.mock(NereidsSortedPartitionsCacheManager.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.onRefreshCache(true);
        }

        Mockito.verify(rowCountCache).invalidateCatalog(1L);
        Mockito.verify(rowCountCache, Mockito.never()).invalidateDb(Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    public void testInvalidateDbMetadataCacheDoesNotAccessDbForRowCount() {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalRowCountCache rowCountCache = Mockito.spy(
                new ExternalRowCountCache(MoreExecutors.newDirectExecutorService()));
        Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            metaCacheMgr.invalidateDbMetadataCache(1L, "db1");
        }

        Mockito.verify(catalog, Mockito.never()).getDbNullable("db1");
        Mockito.verify(rowCountCache, Mockito.never()).invalidateDb(Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    public void testLoadWithException() throws Exception {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, Integer.MAX_VALUE, "TEST", true);
        AtomicInteger counter = new AtomicInteger(0);
        AtomicReference<ExternalRowCountCache.RowCountCacheLoader> loaderRef = new AtomicReference<>();

        try (MockedConstruction<ExternalRowCountCache.RowCountCacheLoader> mocked =
                Mockito.mockConstruction(ExternalRowCountCache.RowCountCacheLoader.class,
                        Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                        (mock, context) -> {
                            loaderRef.set(mock);
                            Mockito.doAnswer(inv -> {
                                counter.incrementAndGet();
                                return null;
                            }).when(mock).doLoad(Mockito.any());
                        })) {

            ExternalRowCountCache cache = new ExternalRowCountCache(executor);
            long cachedRowCount = cache.getCachedRowCount(1, 1, 1, false);
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cachedRowCount);
            for (int i = 0; i < 60; i++) {
                if (counter.get() == 1) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assertions.assertEquals(1, counter.get());

            // Re-stub for second behavior
            Mockito.doAnswer(inv -> {
                counter.incrementAndGet();
                return Optional.of(100L);
            }).when(loaderRef.get()).doLoad(Mockito.any());

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

            // Re-stub for third behavior
            Mockito.doAnswer(inv -> {
                counter.incrementAndGet();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return Optional.of(100L);
            }).when(loaderRef.get()).doLoad(Mockito.any());

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

    public static class EmptyCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Collections.emptyMap();
        }
    }
}
