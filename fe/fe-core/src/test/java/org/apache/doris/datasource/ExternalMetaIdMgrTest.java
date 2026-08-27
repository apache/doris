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
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.datasource.log.MetaIdMappingsLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ExternalMetaIdMgrTest {

    @Test
    public void testReplayMetaIdMappingsLog() {
        ExternalMetaIdMgr mgr = new ExternalMetaIdMgr();
        MetaIdMappingsLog log1 = new MetaIdMappingsLog();
        log1.setCatalogId(1L);
        log1.setFromHmsEvent(false);
        log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                    "db1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log1);
        Assertions.assertNotEquals(-1L, mgr.getDbId(1L, "db1"));

        MetaIdMappingsLog log2 = new MetaIdMappingsLog();
        log2.setCatalogId(1L);
        log2.setFromHmsEvent(false);
        log2.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                    "db1"));
        mgr.replayMetaIdMappingsLog(log2);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));

        MetaIdMappingsLog log3 = new MetaIdMappingsLog();
        log3.setCatalogId(1L);
        log3.setFromHmsEvent(false);
        log3.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    "db1", "tbl1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log3);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));
        Assertions.assertNotEquals(-1L, mgr.getTblId(1L, "db1", "tbl1"));

        MetaIdMappingsLog log4 = new MetaIdMappingsLog();
        log4.setCatalogId(1L);
        log4.setFromHmsEvent(false);
        log4.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    "db1", "tbl1"));
        log4.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_PARTITION,
                    "db1", "tbl1", "p1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log4);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));
        Assertions.assertEquals(-1L, mgr.getTblId(1L, "db1", "tbl1"));
        Assertions.assertNotEquals(-1L, mgr.getPartitionId(1L, "db1", "tbl1", "p1"));
    }

    /**
     * An HMS-event id-mapping log carries the master's synced-event-id cursor and is replayed on
     * every FE. A flipped HMS catalog is served by a generic {@link PluginDrivenExternalCatalog}, so
     * replay must persist the cursor there without casting it to {@code HMSExternalCatalog} — that cast
     * would throw {@link ClassCastException} and abort edit-log replay, wedging FE startup.
     */
    @Test
    public void testReplayHmsEventCursorDoesNotRequireHmsCatalogType() {
        final long catalogId = 7L;
        final long lastSyncedEventId = 42L;

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        // The live post-cutover catalog is a generic PluginDrivenExternalCatalog (never HMSExternalCatalog);
        // doReturn avoids stubbing the wildcard-generic return type of getCatalog(long).
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        MetastoreEventSyncDriver syncDriver = Mockito.mock(MetastoreEventSyncDriver.class);
        Env env = new TestingEnv(catalogMgr, constraintManager, syncDriver);

        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(catalogId);
        log.setFromHmsEvent(true);
        log.setLastSyncedEventId(lastSyncedEventId);
        log.setConstraintTransitionsPersisted(true);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

            // A (HMSExternalCatalog) cast here would throw ClassCastException on the generic catalog.
            Assertions.assertDoesNotThrow(() -> new ExternalMetaIdMgr().replayMetaIdMappingsLog(log));

            // The live catalog and its next image both retain the committed cursor.
            Mockito.verify(syncDriver).updateMasterLastSyncedEventId(
                    catalogId, lastSyncedEventId);
            Mockito.verify(catalog).updateLastSyncedMetastoreEventId(lastSyncedEventId);
            Mockito.verify(constraintManager).reconcileUntrustedCatalogConstraints("test_catalog");
        }
    }

    @Test
    public void testReplayLegacyHmsEventCursorQuarantinesConstraints() {
        long catalogId = 7L;
        String catalogName = "test_catalog";
        TableNameInfo ownedTable = new TableNameInfo(catalogName, "db1", "tbl1");
        TableNameInfo relatedTable = new TableNameInfo("other_catalog", "db2", "tbl2");
        List<TableNameInfo> affectedTables = List.of(ownedTable, relatedTable);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(catalogName);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(constraintManager.markCatalogConstraintsUntrusted(catalogName)).thenReturn(true, false);
        Mockito.when(constraintManager.getCatalogConstraintRelatedTables(catalogName)).thenReturn(affectedTables);
        MetastoreEventSyncDriver syncDriver = Mockito.mock(MetastoreEventSyncDriver.class);
        NereidsSqlCacheManager sqlCacheManager = Mockito.mock(NereidsSqlCacheManager.class);
        Env env = new TestingEnv(catalogMgr, constraintManager, syncDriver, sqlCacheManager);

        MetaIdMappingsLog legacyLog = new MetaIdMappingsLog();
        legacyLog.setCatalogId(catalogId);
        legacyLog.setFromHmsEvent(true);
        legacyLog.setLastSyncedEventId(41L);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

            ExternalMetaIdMgr externalMetaIdMgr = new ExternalMetaIdMgr();
            externalMetaIdMgr.replayMetaIdMappingsLog(legacyLog);
            externalMetaIdMgr.replayMetaIdMappingsLog(legacyLog);

            Mockito.verify(catalog).markMetastoreConstraintStateUnreconciled();
            InOrder replayOrder = Mockito.inOrder(constraintManager, sqlCacheManager, syncDriver);
            replayOrder.verify(constraintManager).markCatalogConstraintsUntrusted(catalogName);
            replayOrder.verify(constraintManager).getCatalogConstraintRelatedTables(catalogName);
            replayOrder.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(ownedTable);
            replayOrder.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(relatedTable);
            replayOrder.verify(syncDriver).updateMasterLastSyncedEventId(catalogId, 41L);
            Mockito.verify(constraintManager, Mockito.times(2)).markCatalogConstraintsUntrusted(catalogName);
            Mockito.verify(constraintManager).getCatalogConstraintRelatedTables(catalogName);
            Mockito.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(ownedTable);
            Mockito.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(relatedTable);
            Mockito.verify(syncDriver, Mockito.times(2)).updateMasterLastSyncedEventId(catalogId, 41L);
            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables,
                    "when replaying a legacy metastore event cursor for " + catalogName));
        }
    }

    @Test
    public void testMarkedHmsEventCursorDropsFollowerOnlyQuarantinedConstraints() {
        long catalogId = 7L;
        String catalogName = "test_catalog";
        TableNameInfo tableNameInfo = new TableNameInfo(catalogName, "db1", "tbl1");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(catalogName);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ConstraintManager constraintManager = new ConstraintManager();
        constraintManager.addConstraint(tableNameInfo, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("k1")), true);
        constraintManager.markCatalogConstraintsUntrusted(catalogName);
        Assertions.assertNull(constraintManager.getConstraint(tableNameInfo, "uk"));
        MetastoreEventSyncDriver syncDriver = Mockito.mock(MetastoreEventSyncDriver.class);
        NereidsSqlCacheManager sqlCacheManager = Mockito.mock(NereidsSqlCacheManager.class);
        Env env = new TestingEnv(catalogMgr, constraintManager, syncDriver, sqlCacheManager);

        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(catalogId);
        log.setFromHmsEvent(true);
        log.setLastSyncedEventId(42L);
        log.setConstraintTransitionsPersisted(true);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            new ExternalMetaIdMgr().replayMetaIdMappingsLog(log);

            Mockito.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(tableNameInfo);
            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    List.of(tableNameInfo),
                    "when reconciling legacy metastore constraints for " + catalogName));
        }

        Assertions.assertNull(constraintManager.getConstraint(tableNameInfo, "uk"));
        Assertions.assertTrue(constraintManager.isEmpty());
        Mockito.verify(catalog).updateLastSyncedMetastoreEventId(42L);
        Mockito.verify(syncDriver).updateMasterLastSyncedEventId(catalogId, 42L);
    }

    @Test
    public void testConstraintTransitionMarkerSerializationIsBackwardCompatible() {
        String legacyJson = "{\"ctlId\":7,\"fromEvent\":true,\"lastEventId\":41,"
                + "\"metaIdMappings\":[]}";
        MetaIdMappingsLog legacyLog = GsonUtils.GSON.fromJson(legacyJson, MetaIdMappingsLog.class);
        Assertions.assertFalse(legacyLog.isConstraintTransitionsPersisted());

        MetaIdMappingsLog markedLog = new MetaIdMappingsLog();
        markedLog.setCatalogId(7L);
        markedLog.setFromHmsEvent(true);
        markedLog.setLastSyncedEventId(42L);
        markedLog.setConstraintTransitionsPersisted(true);
        String markedJson = GsonUtils.GSON.toJson(markedLog);
        MetaIdMappingsLog restored = GsonUtils.GSON.fromJson(markedJson, MetaIdMappingsLog.class);
        Assertions.assertTrue(restored.isConstraintTransitionsPersisted());
        Assertions.assertEquals(markedLog, restored);
    }

    private static final class TestingEnv extends Env {
        private final CatalogMgr catalogMgr;
        private final ConstraintManager constraintManager;
        private final MetastoreEventSyncDriver syncDriver;
        private final NereidsSqlCacheManager sqlCacheManager;

        private TestingEnv(CatalogMgr catalogMgr, ConstraintManager constraintManager,
                MetastoreEventSyncDriver syncDriver) {
            this(catalogMgr, constraintManager, syncDriver,
                    Mockito.mock(NereidsSqlCacheManager.class));
        }

        private TestingEnv(CatalogMgr catalogMgr, ConstraintManager constraintManager,
                MetastoreEventSyncDriver syncDriver, NereidsSqlCacheManager sqlCacheManager) {
            super(true);
            this.catalogMgr = catalogMgr;
            this.constraintManager = constraintManager;
            this.syncDriver = syncDriver;
            this.sqlCacheManager = sqlCacheManager;
        }

        @Override
        public CatalogMgr getCatalogMgr() {
            return catalogMgr;
        }

        @Override
        public ConstraintManager getConstraintManager() {
            return constraintManager;
        }

        @Override
        public MetastoreEventSyncDriver getMetastoreEventSyncDriver() {
            return syncDriver;
        }

        @Override
        public NereidsSqlCacheManager getSqlCacheManager() {
            return sqlCacheManager;
        }
    }

}
