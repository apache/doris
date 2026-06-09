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
import org.apache.doris.common.util.Util;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
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
    public void testInvalidateTableInvalidatesRowCountCache() throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("catalog1");
        long dbId = Util.genIdByName("catalog1", "db1");
        long tableId = Util.genIdByName("catalog1", "db1", "tbl1");

        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDbId()).thenReturn(dbId);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("tbl1");
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db1");
        Mockito.when(table.getRemoteName()).thenReturn("remote_tbl1");

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalRowCountCache rowCountCache = Mockito.spy(
                new ExternalRowCountCache(MoreExecutors.newDirectExecutorService()));
        metaCacheMgr.setRowCountCache(rowCountCache);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            metaCacheMgr.invalidateTable(table);
        }

        Mockito.verify(rowCountCache).invalidateTable(1L, dbId, tableId);
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
}
