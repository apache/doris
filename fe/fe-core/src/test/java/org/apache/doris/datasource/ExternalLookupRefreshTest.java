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
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ExternalLookupRefreshTest {
    @Test
    void removedResourcesReleaseOutsideTheRepairGuardWithoutRemovingReplacement() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Object guard = new Object();
        AtomicReference<String> retired = new AtomicReference<>();
        AtomicBoolean retiredUnderGuard = new AtomicBoolean();
        MetaCacheEntry<String, String> entry = new MetaCacheEntry<>("deferred", ignored -> "loaded",
                CacheSpec.of(true, -1, 10), executor, false, false, null, null, null,
                value -> {
                    retiredUnderGuard.set(Thread.holdsLock(guard));
                    retired.set(value);
                    return value;
                }, (key, token) -> { }, null, false);
        try {
            entry.put("t", "old");
            MetaCacheEntry.runWithDeferredRemovalCallbacks(() -> {
                synchronized (guard) {
                    entry.invalidateKey("t");
                    Assertions.assertNull(entry.getIfPresent("t"));
                    Assertions.assertNull(retired.get());
                    entry.put("t", "replacement");
                }
            });
            Assertions.assertEquals("old", retired.get());
            Assertions.assertFalse(retiredUnderGuard.get());
            Assertions.assertEquals("replacement", entry.getIfPresent("t"));
            Assertions.assertThrows(IllegalStateException.class, () ->
                    MetaCacheEntry.runWithDeferredRemovalCallbacks(() -> {
                        entry.invalidateKey("t");
                        throw new IllegalStateException("guarded cleanup failed");
                    }));
            Assertions.assertEquals("replacement", retired.get());
            entry.put("t", "after-failure");
            entry.invalidateKey("t");
            Assertions.assertEquals("after-failure", retired.get());
        } finally {
            entry.close();
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void parentObjectGuardIsAcquiredBeforeCatalogMonitor() {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class, Mockito.CALLS_REAL_METHODS);
        MetaCache<ExternalTable> cache = Mockito.mock(MetaCache.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getRemoteName()).thenReturn("t");
        Deencapsulation.setField(database, "extCatalog", catalog);
        Deencapsulation.setField(database, "metaCache", cache);
        Deencapsulation.setField(database, "initialized", true);
        Mockito.doReturn(0L).when(database).acquireTableMetadataLoadEpoch();
        Mockito.doReturn(Optional.of((Runnable) () -> { })).when(cache).observeLookup(
                Mockito.eq("t"), Mockito.eq("t"), Mockito.same(table), Mockito.eq(false), Mockito.any());
        Mockito.doAnswer(call -> {
            // A parent loader may already hold this key while waiting for the catalog monitor.
            Assertions.assertFalse(Thread.holdsLock(catalog));
            return false;
        }).when(catalog).executeIfDatabaseCurrent(Mockito.same(database), Mockito.any());
        database.observeTableLookup(table).get().run();
        Mockito.verify(catalog).executeIfDatabaseCurrent(Mockito.same(database), Mockito.any());
    }

    @Test
    void definiteMissingRepairsButTimeoutDoesNot() {
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        AtomicInteger repairs = new AtomicInteger();
        Mockito.when(db.observeTableLookup(Mockito.any())).thenReturn(Optional.of(repairs::incrementAndGet));
        ExternalTable table = new ExternalTable();
        table.setDb(db);
        RuntimeException missing = new RuntimeException(new org.apache.hadoop.hive.metastore.api.NoSuchObjectException());
        Assertions.assertSame(missing, Assertions.assertThrows(RuntimeException.class,
                () -> table.withMissingTableRepair(() -> {
                    throw missing;
                })));
        Assertions.assertEquals(1, repairs.get());
        RuntimeException timeout = new RuntimeException("metastore timeout");
        Assertions.assertSame(timeout, Assertions.assertThrows(RuntimeException.class,
                () -> table.withMissingTableRepair(() -> {
                    throw timeout;
                })));
        Assertions.assertEquals(1, repairs.get());
        RuntimeException icebergMissing = new org.apache.iceberg.exceptions.NoSuchTableException("missing");
        Assertions.assertSame(icebergMissing, Assertions.assertThrows(RuntimeException.class,
                () -> table.withMissingTableRepair(() -> {
                    throw icebergMissing;
                })));
        Assertions.assertEquals(2, repairs.get());
        synchronized (table) {
            Assertions.assertSame(missing, Assertions.assertThrows(RuntimeException.class,
                    () -> table.withMissingTableRepair(() -> {
                        throw missing;
                    })));
        }
        Assertions.assertEquals(2, repairs.get());
    }

    @Test
    void explicitRefreshLogsLookupInvalidationAndOrdinaryWriteDoesNot() throws Exception {
        verifyRefresh(true);
        verifyRefresh(false);
    }

    private void verifyRefresh(boolean explicit) throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ExternalMetaCacheMgr caches = Mockito.mock(ExternalMetaCacheMgr.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(caches);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("catalog");
        Mockito.doReturn(db).when(catalog).getDbNullable("db");
        Mockito.doReturn(table).when(db).getTableNullable("Foo");
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(2);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.doCallRealMethod().when(db).invalidateTableLookupDependencies();
        Mockito.when(table.getName()).thenReturn("Foo");
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(db);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            if (explicit) {
                new RefreshManager().handleRefreshTable("catalog", "db", "Foo", false);
                Mockito.verify(db).invalidateTableLookup("Foo");
                Mockito.verify(caches).invalidateDb(db);
                Mockito.verify(caches, Mockito.times(2)).invalidateRowCountCache(1L, 0L);
            } else {
                new RefreshManager().refreshTableAfterCommit(table);
                Mockito.verify(db, Mockito.never()).invalidateTableLookup(Mockito.anyString());
                Mockito.verify(caches, Mockito.never()).invalidateDb(db);
            }
            ArgumentCaptor<ExternalObjectLog> log = ArgumentCaptor.forClass(ExternalObjectLog.class);
            Mockito.verify(editLog).logRefreshExternalTable(log.capture());
            Assertions.assertEquals(explicit, log.getValue().isInvalidateTableLookup());
        }
    }

    @Test
    void explicitReplayRepairsBothHotAndColdLookupWithoutRemoteLoad() {
        for (boolean hot : new boolean[] {true, false}) {
            ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
            ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
            ExternalTable table = Mockito.mock(ExternalTable.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            ExternalMetaCacheMgr caches = Mockito.mock(ExternalMetaCacheMgr.class);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(caches);
            Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
            Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db");
            Mockito.doReturn(hot ? Optional.of(table) : Optional.empty()).when(db).getTableForReplay("Foo");
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.getName()).thenReturn("catalog");
            Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(2);
            Mockito.when(db.getFullName()).thenReturn("db");
            Mockito.when(db.getCatalog()).thenReturn(catalog);
            Mockito.doCallRealMethod().when(db).invalidateTableLookupDependencies();
            Mockito.when(table.getName()).thenReturn("Foo");
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
                mocked.when(Env::getCurrentEnv).thenReturn(env);
                new RefreshManager().replayRefreshTable(
                        ExternalObjectLog.createForRefreshTable(1L, "db", "Foo", 10L, true));
                Mockito.verify(db).invalidateTableLookup("Foo");
                Mockito.verify(caches).invalidateDb(db);
                Mockito.verify(caches, Mockito.times(2)).invalidateRowCountCache(1L, 0L);
                Mockito.verify(db, Mockito.never()).getTableNullable(Mockito.anyString());
            }
        }
    }

    @Test
    void legacyRefreshRecordKeepsNarrowInvalidation() {
        ExternalObjectLog log = GsonUtils.GSON.fromJson(
                "{\"catalogId\":1,\"dbName\":\"db\",\"tableName\":\"t\"}", ExternalObjectLog.class);
        Assertions.assertFalse(log.isInvalidateTableLookup());
        Assertions.assertTrue(ExternalObjectLog.createForRefreshTable(1L, "db", "t", 3L, true)
                .isInvalidateTableLookup());
    }
}
