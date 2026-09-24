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

package org.apache.doris.catalog;

import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class RefreshManagerTest {

    @Test
    void testColdDatabaseReplayInvalidatesCatalogRowCount() {
        long catalogId = 51L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn(Optional.empty());
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().replayRefreshDb(ExternalObjectLog.createForRefreshDb(catalogId, "db1"));
        }

        Mockito.verify(cacheMgr).invalidateCatalog(catalogId);
    }

    @Test
    void testPartitionReplayInvalidatesRowCountBeforeCacheFailure() {
        long catalogId = 52L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn(Optional.of(db));
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("tbl1");

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        Mockito.doThrow(new IllegalStateException("partition cache failure"))
                .when(hiveCache).refreshAffectedPartitionsCache(
                        Mockito.eq(table), Mockito.anyList(), Mockito.anyList());
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshPartitions(
                catalogId, "db1", "tbl1",
                java.util.Collections.singletonList("p=1"), java.util.Collections.emptyList(), 1L);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // Replay cleanup failures are logged and swallowed so a committed refresh does not take
            // the FE down; the row-count fence must still have been published first.
            Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshTable(log));
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(table);
    }

    @Test
    void testAlterPartitionInvalidatesRowCountBeforeCacheFailure() throws Exception {
        long catalogId = 53L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(table).when(db).getTableNullable("tbl1");
        Mockito.when(table.getCatalog()).thenReturn(catalog);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("hms");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(cacheMgr.hive(catalogId)).thenThrow(new IllegalStateException("partition cache failure"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new RefreshManager().refreshPartitions(
                            "hms", "db1", "tbl1", java.util.Collections.singletonList("p=1"), 1L, true));
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, "db1", "tbl1");
    }

    @Test
    void testColdAlterPartitionEventFencesRowCountBeforeIgnoredTableMiss() throws Exception {
        long catalogId = 54L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(null).when(db).getTableNullable("tbl1");

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("hms");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().refreshPartitions(
                    "hms", "db1", "tbl1", java.util.Collections.singletonList("p=1"), 1L, true);
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr).invalidateTableByNameOrWider(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr, Mockito.never()).hive(catalogId);
    }

    @Test
    void testSuccessfulPartitionRefreshClosesRowCountAdmissionWindow() throws Exception {
        long catalogId = 69L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(table).when(db).getTableNullable("tbl1");
        Mockito.when(table.getCatalog()).thenReturn(catalog);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("hms");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().refreshPartitions(
                    "hms", "db1", "tbl1", java.util.Collections.singletonList("p=1"), 1L, true);
        }

        // One fence before the selective invalidation, one after it closes the admission window.
        Mockito.verify(cacheMgr, Mockito.times(2))
                .invalidateRowCountCache(catalogId, "db1", "tbl1");
    }

    @Test
    void testCommittedRefreshUsesHeldTableAndStillLogsAfterCacheFailure() {
        long catalogId = 55L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getName()).thenReturn("tbl1");
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("ctl.db1.tbl1");

        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.doThrow(new IllegalStateException("table cache failure"))
                .when(cacheMgr).invalidateTableCache(table);
        EditLog editLog = Mockito.mock(EditLog.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertDoesNotThrow(() -> new RefreshManager().refreshTableAfterCommit(table));
        }

        InOrder order = Mockito.inOrder(cacheMgr, editLog);
        order.verify(cacheMgr).invalidateRowCountCache(table);
        order.verify(cacheMgr).invalidateTableCache(table);
        order.verify(editLog).logRefreshExternalTable(Mockito.argThat(log ->
                log.getCatalogId() == catalogId
                        && "db1".equals(log.getDbName())
                        && "tbl1".equals(log.getTableName())));
    }

    @Test
    void testWholeTableEventFencesRowCountBeforeIgnoredTableMiss() throws Exception {
        long catalogId = 56L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(null).when(db).getTableNullable("tbl1");

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("hms");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().refreshExternalTableFromEvent("hms", "db1", "tbl1", 1L);
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr).invalidateTableByNameOrWider(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr, Mockito.never()).hive(catalogId);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testColdRefreshTableReplayWidensToCanonicalDatabaseScope() {
        long catalogId = 57L;
        long dbId = 58L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn((Optional) Optional.of(db));
        Mockito.when(db.getId()).thenReturn(dbId);
        Mockito.when(db.getFullName()).thenReturn("MixedDb");
        Mockito.doReturn(Optional.empty()).when(db).getTableForReplay("tbl1");

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalogId, "db1", "tbl1", 1L);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().replayRefreshTable(log);
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, dbId);
    }

    @Test
    void testColdRefreshDbReplayWidensToCatalogScope() {
        long catalogId = 59L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn(Optional.empty());

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalogId, "db1", "tbl1", 1L);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().replayRefreshTable(log);
        }

        Mockito.verify(cacheMgr).invalidateCatalog(catalogId);
    }
}
