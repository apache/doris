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
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.event.ConnectorEventSource;
import org.apache.doris.connector.spi.event.EventPollResult;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.datasource.log.MetaIdMappingsLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class MetastoreEventSyncDriverRenameTest {

    @Test
    public void followerUsesLegacyCursorAsUpperBoundWithoutTreatingItAsDurable() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        Mockito.when(eventSource.pollOnce(Mockito.argThat(request ->
                request.getLastSyncedEventId() == -1L
                        && request.getMasterUpperBound() == 41L
                        && !request.isMaster())))
                .thenReturn(EventPollResult.ofNothing(-1L));

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        driver.updateMasterLastSyncedEventId(catalog.getId(), 41L);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);
        }

        Mockito.verify(eventSource).pollOnce(Mockito.any());
        Assertions.assertEquals(-1L, catalog.getLastSyncedMetastoreEventId());
    }

    @Test
    public void followerDoesNotReapplyJournaledConstraintTransition() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 2L, 3L);
        Mockito.when(env.isMaster()).thenReturn(false);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(eventSource.pollOnce(Mockito.any()))
                .thenReturn(EventPollResult.ofChanges(2L, List.of(descriptor)));

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        catalog.updateLastSyncedMetastoreEventId(2L);
        driver.updateMasterLastSyncedEventId(catalog.getId(), 2L);
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        localCursors.put(catalog.getId(), 1L);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);
        }

        Mockito.verify(catalogMgr).unregisterExternalTableFromEvent(
                "db1", "tbl1", "test_catalog");
        Mockito.verifyNoInteractions(constraintManager);
        Assertions.assertEquals(Long.valueOf(2L), localCursors.get(catalog.getId()));
    }

    @Test
    public void promotedFollowerDoesNotReapplyJournaledConstraintTransition() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        Env env = Mockito.mock(Env.class);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 2L, 3L);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(eventSource.pollOnce(Mockito.any()))
                .thenReturn(EventPollResult.ofChanges(2L, List.of(descriptor)));

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        catalog.updateLastSyncedMetastoreEventId(2L);
        driver.updateMasterLastSyncedEventId(catalog.getId(), 2L);
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        localCursors.put(catalog.getId(), 1L);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);
        }

        Mockito.verify(catalogMgr).unregisterExternalTableFromEvent(
                "db1", "tbl1", "test_catalog");
        Mockito.verifyNoInteractions(constraintManager);
        Mockito.verifyNoInteractions(editLog, externalMetaIdMgr);
        Assertions.assertEquals(Long.valueOf(2L), localCursors.get(catalog.getId()));
    }

    @Test
    public void followerSkipsConstraintTransitionThroughQuarantinedLegacyCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 2L, 3L);
        Mockito.when(env.isMaster()).thenReturn(false);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(eventSource.pollOnce(Mockito.any()))
                .thenReturn(EventPollResult.ofChanges(2L, List.of(descriptor)));

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        catalog.updateLastSyncedMetastoreEventId(1L);
        driver.updateMasterLastSyncedEventId(catalog.getId(), 1L);
        driver.updateMasterLastSyncedEventId(catalog.getId(), 2L);
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        localCursors.put(catalog.getId(), 1L);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);
        }

        Mockito.verifyNoInteractions(constraintManager);
        Assertions.assertEquals(Long.valueOf(2L), localCursors.get(catalog.getId()));
    }

    @Test
    public void structuralEventAdvancesConstraintMetadataSequence() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        long baseline = catalog.snapshotConstraintMetadata();
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forDatabase(
                Op.REGISTER_DATABASE, "db1", null, 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        Assertions.assertNotEquals(baseline, catalog.snapshotConstraintMetadata());
    }

    @Test
    public void partitionEventDoesNotAdvanceConstraintMetadataSequence() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        long baseline = catalog.snapshotConstraintMetadata();
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forPartitions(
                Op.ADD_PARTITIONS, "db1", "tbl1", List.of("p1"), 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        Assertions.assertEquals(baseline, catalog.snapshotConstraintMetadata());
    }

    @Test
    public void masterStructuralEventPersistsConstraintTransitionBeforeCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        Env env = Mockito.mock(Env.class);
        TableNameInfo droppedTable = new TableNameInfo("test_catalog", "db1", "tbl1");
        List<TableNameInfo> affectedTables = List.of(droppedTable);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 2L, 2L);
        catalog.updateLastSyncedMetastoreEventId(1L);
        Mockito.when(eventSource.pollOnce(Mockito.argThat(request ->
                request.isMaster() && request.getLastSyncedEventId() == 1L))).thenReturn(
                        EventPollResult.ofChanges(2L, List.of(descriptor)));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        localCursors.put(catalog.getId(), 1L);
        AtomicReference<MetaIdMappingsLog> cursorLogReference = new AtomicReference<>();
        List<String> operations = new ArrayList<>();
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    operations.add("constraint-mutation");
                    Assertions.assertEquals(Long.valueOf(1L), localCursors.get(catalog.getId()));
                    Assertions.assertThrows(DdlException.class, () -> {
                        try (ExternalCatalog.ConstraintMetadataReadGuard ignored =
                                catalog.lockConstraintMetadata(catalog.snapshotConstraintMetadata())) {
                            // The event batch remains fenced until its cursor is durable.
                        }
                    });
                    EditLog.EditLogOperation cursorOperation = invocation.getArgument(1);
                    cursorLogReference.set(Deencapsulation.getField(cursorOperation, "writable"));
                    operations.add("cursor-journal");
                    return affectedTables;
                });
        Mockito.doAnswer(invocation -> {
            operations.add("cursor-replay");
            return null;
        }).when(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.any());

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            mtmvUtil.when(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables, "after applying external table drop event for " + droppedTable))
                    .thenAnswer(invocation -> {
                        operations.add("mtmv");
                        return null;
                    });
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);

            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables, "after applying external table drop event for " + droppedTable));
        }

        MetaIdMappingsLog cursorLog = cursorLogReference.get();
        Assertions.assertNotNull(cursorLog);
        Assertions.assertEquals(2L, cursorLog.getLastSyncedEventId());
        Assertions.assertTrue(cursorLog.isConstraintTransitionsPersisted());
        Mockito.verify(externalMetaIdMgr).replayMetaIdMappingsLog(cursorLog);
        Assertions.assertEquals(List.of(
                "constraint-mutation", "cursor-journal", "cursor-replay", "mtmv"),
                operations);
        Mockito.verifyNoInteractions(editLog);
        try (ExternalCatalog.ConstraintMetadataReadGuard ignored =
                catalog.lockConstraintMetadata(catalog.snapshotConstraintMetadata())) {
            // The batch fence is released after the cursor is durable.
        }
        Assertions.assertEquals(Long.valueOf(2L), localCursors.get(catalog.getId()));
    }

    @Test
    public void masterFullRefreshPersistsCatalogConstraintCleanupBeforeCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        List<TableNameInfo> affectedTables = List.of(
                new TableNameInfo("test_catalog", "db1", "tbl1"));
        AtomicReference<MetaIdMappingsLog> cursorLogReference = new AtomicReference<>();
        List<String> operations = new ArrayList<>();

        Mockito.when(eventSource.pollOnce(Mockito.argThat(request ->
                request.isMaster() && request.getLastSyncedEventId() == -1L))).thenReturn(
                        EventPollResult.ofFullRefresh(5L));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(Mockito.any()))
                .thenAnswer(invocation -> {
                    operations.add("constraint-mutation");
                    return affectedTables;
                });
        Mockito.doAnswer(invocation -> {
            cursorLogReference.set(invocation.getArgument(0));
            operations.add("cursor-journal");
            return null;
        }).when(editLog).logMetaIdMappingsLog(Mockito.any());
        Mockito.doAnswer(invocation -> {
            operations.add("refresh");
            return null;
        }).when(refreshManager).replayRefreshCatalog(Mockito.any());
        Mockito.doAnswer(invocation -> {
            operations.add("cursor-replay");
            return null;
        }).when(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.any());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            mtmvUtil.when(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables,
                    "before recovering an external catalog from a metastore event gap for test_catalog"))
                    .thenAnswer(invocation -> {
                        operations.add("mtmv");
                        return null;
                    });

            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "syncCatalog", catalog, connector, eventSource);
        }

        MetaIdMappingsLog cursorLog = cursorLogReference.get();
        Assertions.assertNotNull(cursorLog);
        Assertions.assertTrue(cursorLog.isConstraintTransitionsPersisted());
        Assertions.assertEquals(List.of(
                "constraint-mutation", "mtmv", "refresh", "cursor-journal", "cursor-replay"), operations);
        Mockito.verify(constraintManager).applyMetastoreConstraintMutation(Mockito.any());
        Mockito.verify(editLog).logMetaIdMappingsLog(cursorLog);
    }

    @Test
    public void promotedFollowerRefreshesLocalCacheAtDurableCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        catalog.updateLastSyncedMetastoreEventId(5L);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(eventSource.pollOnce(Mockito.argThat(request ->
                request.isMaster()
                        && request.getLastSyncedEventId() == -1L
                        && request.getMasterUpperBound() == 5L)))
                .thenReturn(EventPollResult.ofFullRefresh(5L));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource);
        }

        Mockito.verify(refreshManager).replayRefreshCatalog(Mockito.any());
        Mockito.verifyNoInteractions(constraintManager, editLog, externalMetaIdMgr);
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        Assertions.assertEquals(Long.valueOf(5L), localCursors.get(catalog.getId()));
    }

    @Test
    public void committedEventCursorSurvivesCatalogImageRoundTrip() {
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(
                1L, "test_catalog", null, Collections.singletonMap("type", "iceberg"), "", null);
        catalog.updateLastSyncedMetastoreEventId(42L);

        String json = GsonUtils.GSON.toJson(catalog, CatalogIf.class);
        PluginDrivenExternalCatalog restored =
                (PluginDrivenExternalCatalog) GsonUtils.GSON.fromJson(json, CatalogIf.class);

        Assertions.assertEquals(42L, restored.getLastSyncedMetastoreEventId());
    }

    @Test
    public void legacyCatalogImageRequiresConstraintReconciliation() {
        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.providesEventSource()).thenReturn(true);
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(
                1L, "test_catalog", null, Collections.singletonMap("type", "hms"), "", null);
        String json = GsonUtils.GSON.toJson(catalog, CatalogIf.class);
        JsonObject legacyJson = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertNotNull(legacyJson.remove("msei"));
        PluginDrivenExternalCatalog restored = (PluginDrivenExternalCatalog) GsonUtils.GSON.fromJson(
                legacyJson, CatalogIf.class);

        try (MockedStatic<ConnectorFactory> connectorFactory = Mockito.mockStatic(ConnectorFactory.class)) {
            connectorFactory.when(() -> ConnectorFactory.findProvider(
                    Mockito.anyString(), Mockito.anyMap())).thenReturn(Optional.of(provider));

            Assertions.assertFalse(catalog.needsMetastoreConstraintReconciliation());
            Assertions.assertTrue(restored.needsMetastoreConstraintReconciliation());
            Assertions.assertEquals(-1L, restored.getLastSyncedMetastoreEventId());
        }
    }

    @Test
    public void prepareAfterImageLoadQuarantinesLegacyEventCatalog() {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.needsMetastoreConstraintReconciliation()).thenReturn(true);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(catalogMgr.getCatalogIds()).thenReturn(List.of(1L));
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            new MetastoreEventSyncDriver().prepareConstraintStateAfterImageLoad();
        }

        Mockito.verify(constraintManager).markCatalogConstraintsUntrusted("test_catalog");
    }

    @Test
    public void masterPromotionJournalsLegacyConstraintCleanupBeforeMarkedCursor() {
        PluginDrivenExternalCatalog catalog = Mockito.spy(new IdentityEventCatalog(Mockito.mock(Connector.class)));
        Mockito.doReturn(true).when(catalog).needsMetastoreConstraintReconciliation();
        long catalogId = catalog.getId();
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(catalogMgr.getCatalogIds()).thenReturn(List.of(catalogId));
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        AtomicReference<MetaIdMappingsLog> cursorLogReference = new AtomicReference<>();
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                    EditLog.EditLogOperation cursorOperation = invocation.getArgument(1);
                    cursorLogReference.set(Deencapsulation.getField(cursorOperation, "writable"));
                    return List.of();
                });
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            new MetastoreEventSyncDriver().reconcileConstraintStateBeforeMasterReady();
        }

        MetaIdMappingsLog cursorLog = cursorLogReference.get();
        Assertions.assertNotNull(cursorLog);
        Assertions.assertEquals(-1L, cursorLog.getLastSyncedEventId());
        Assertions.assertTrue(cursorLog.isConstraintTransitionsPersisted());
        Mockito.verify(constraintManager).applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any());
        Mockito.verifyNoInteractions(editLog);
        Mockito.verify(externalMetaIdMgr).replayMetaIdMappingsLog(cursorLog);
    }

    @Test
    public void laterDescriptorFailureKeepsEarlierTransitionCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        Env env = Mockito.mock(Env.class);
        MetastoreChangeDescriptor drop = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 1L, 2L);
        MetastoreChangeDescriptor failingCreate = MetastoreChangeDescriptor.forTable(
                Op.REGISTER_TABLE, "db1", "tbl2", null, 2L, 3L);
        Mockito.when(eventSource.pollOnce(Mockito.any())).thenReturn(
                EventPollResult.ofChanges(3L, List.of(drop, failingCreate)));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any()))
                .thenReturn(List.of());
        Mockito.doThrow(new IllegalStateException("injected descriptor failure"))
                .when(catalogMgr).registerExternalTableFromEvent(
                        "db1", "tbl2", "tbl2", "test_catalog", 3L);

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(RuntimeException.class,
                    () -> Deencapsulation.invoke(driver, "syncCatalog", catalog, connector, eventSource));
        }

        Mockito.verify(constraintManager).applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any());
        Mockito.verify(editLog, Mockito.never()).logMetaIdMappingsLog(Mockito.any());
        Mockito.verify(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.argThat(log ->
                log.getLastSyncedEventId() == 1L && log.isConstraintTransitionsPersisted()));
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        Assertions.assertFalse(localCursors.containsKey(catalog.getId()));
    }

    @Test
    public void failedMasterFullRefreshDoesNotAdvanceCursor() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(eventSource.pollOnce(Mockito.any())).thenReturn(EventPollResult.ofFullRefresh(5L));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(Mockito.any()))
                .thenReturn(List.of());
        Mockito.doThrow(new IllegalStateException("injected refresh failure"))
                .when(refreshManager).replayRefreshCatalog(Mockito.any());

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(RuntimeException.class,
                    () -> Deencapsulation.invoke(
                            driver, "syncCatalog", catalog, connector, eventSource));
        }

        Mockito.verify(constraintManager).applyMetastoreConstraintMutation(Mockito.any());
        Mockito.verify(editLog, Mockito.never()).logMetaIdMappingsLog(Mockito.any());
        Mockito.verifyNoInteractions(externalMetaIdMgr);
        Map<Long, Long> localCursors = Deencapsulation.getField(driver, "lastSyncedEventIdMap");
        Assertions.assertFalse(localCursors.containsKey(catalog.getId()));
    }

    @Test
    public void masterSchemaRefreshPersistsConstraintsAndInvalidatesMtmvsBeforeRefresh() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        TableNameInfo tableNameInfo = new TableNameInfo("test_catalog", "db1", "tbl1");
        List<String> removedColumns = List.of("obsolete");
        List<TableNameInfo> affectedTables = List.of(
                tableNameInfo, new TableNameInfo("test_catalog", "db1", "dependent"));
        List<String> operations = new ArrayList<>();
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
        Mockito.when(constraintManager.applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    operations.add("constraints");
                    operations.add("cursor-journal");
                    return affectedTables;
                });
        Mockito.doAnswer(invocation -> {
            operations.add("cursor-replay");
            return null;
        }).when(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.any());
        Mockito.doAnswer(invocation -> {
            operations.add("refresh");
            return null;
        }).when(refreshManager).refreshExternalTableFromEvent(
                "test_catalog", "db1", "tbl1", 2L);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTableRefresh(
                "db1", "tbl1", removedColumns, 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            mtmvUtil.when(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables,
                    "before applying external table schema refresh event for " + tableNameInfo))
                    .thenAnswer(invocation -> {
                        operations.add("mtmv");
                        return null;
                    });
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, true);

            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables,
                    "before applying external table schema refresh event for " + tableNameInfo));
        }

        Assertions.assertEquals(List.of(
                "constraints", "cursor-journal", "cursor-replay", "mtmv", "refresh"),
                operations);

        InOrder inOrder = Mockito.inOrder(
                constraintManager, refreshManager, externalMetaIdMgr);
        inOrder.verify(constraintManager).applyMetastoreConstraintMutation(
                Mockito.any(), Mockito.any());
        inOrder.verify(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.any());
        inOrder.verify(refreshManager).refreshExternalTableFromEvent(
                "test_catalog", "db1", "tbl1", 2L);
        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    public void dataRefreshDoesNotDropConstraints() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        long baseline = catalog.snapshotConstraintMetadata();
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTable(
                Op.REFRESH_TABLE, "db1", "tbl1", null, 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        Mockito.verifyNoInteractions(constraintManager);
        Mockito.verify(refreshManager).refreshExternalTableFromEvent(
                "test_catalog", "db1", "tbl1", 2L);
        Assertions.assertEquals(baseline, catalog.snapshotConstraintMetadata());
    }

    @Test
    public void databaseRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forDatabase(
                Op.RENAME_DATABASE, "OldDb", "NewDb", 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateDb("OldDb");
        inOrder.verify(connector).invalidateDb("NewDb");
        inOrder.verify(catalogMgr).unregisterExternalDatabaseFromEvent("OldDb", "test_catalog");
        inOrder.verify(catalogMgr).registerExternalDatabaseFromEvent("NewDb", "NewDb", "test_catalog");
        Mockito.verify(constraintManager)
                .renameDatabase("test_catalog", "OldDb", "NewDb");
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void tableRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTableRename(
                "OldDb", "OldTable", "NewDb", "NewTable", 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateTable("OldDb", "OldTable");
        inOrder.verify(connector).invalidateTable("NewDb", "NewTable");
        inOrder.verify(catalogMgr).unregisterExternalTableFromEvent(
                "OldDb", "OldTable", "test_catalog");
        inOrder.verify(catalogMgr).registerExternalTableFromEvent(
                "NewDb", "NewTable", "NewTable", "test_catalog", 2L);
        Mockito.verify(constraintManager).renameTable(
                new TableNameInfo("test_catalog", "OldDb", "OldTable"),
                new TableNameInfo("test_catalog", "NewDb", "NewTable"));
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameViewRecreateInvalidatesConnectorOnceBeforeReplacement() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTableRename(
                "db1", "view1", "db1", "view1", 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            applyOne(new MetastoreEventSyncDriver(), catalog, connector, descriptor, false);
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateTable("db1", "view1");
        inOrder.verify(catalogMgr).unregisterExternalTableFromEvent(
                "db1", "view1", "test_catalog");
        inOrder.verify(catalogMgr).registerExternalTableFromEvent(
                "db1", "view1", "view1", "test_catalog", 2L);
        Mockito.verify(constraintManager).renameTable(
                new TableNameInfo("test_catalog", "db1", "view1"),
                new TableNameInfo("test_catalog", "db1", "view1"));
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameTableDropCreateInvalidatesBothIncarnations() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        TableNameInfo droppedTable = new TableNameInfo("test_catalog", "db1", "tbl1");
        List<TableNameInfo> affectedTables = List.of(droppedTable);
        Mockito.when(constraintManager.dropTableConstraints(droppedTable)).thenReturn(affectedTables);
        MetastoreChangeDescriptor drop = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 1L, 2L);
        MetastoreChangeDescriptor create = MetastoreChangeDescriptor.forTable(
                Op.REGISTER_TABLE, "db1", "tbl1", null, 2L, 3L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
            applyOne(driver, catalog, connector, drop, false);
            applyOne(driver, catalog, connector, create, false);
            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables, "after applying external table drop event for " + droppedTable));
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateTable("db1", "tbl1");
        inOrder.verify(catalogMgr).unregisterExternalTableFromEvent(
                "db1", "tbl1", "test_catalog");
        inOrder.verify(connector).invalidateTable("db1", "tbl1");
        inOrder.verify(catalogMgr).registerExternalTableFromEvent(
                "db1", "tbl1", "tbl1", "test_catalog", 3L);
        Mockito.verify(constraintManager).dropTableConstraints(droppedTable);
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameDatabaseDropCreateInvalidatesBothIncarnations() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        List<TableNameInfo> affectedTables = List.of(
                new TableNameInfo("test_catalog", "db1", "tbl1"));
        Mockito.when(constraintManager.dropDatabaseConstraints("test_catalog", "db1"))
                .thenReturn(affectedTables);
        MetastoreChangeDescriptor drop = MetastoreChangeDescriptor.forDatabase(
                Op.UNREGISTER_DATABASE, "db1", null, 1L, 2L);
        MetastoreChangeDescriptor create = MetastoreChangeDescriptor.forDatabase(
                Op.REGISTER_DATABASE, "db1", null, 2L, 3L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
            applyOne(driver, catalog, connector, drop, false);
            applyOne(driver, catalog, connector, create, false);
            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables, "after applying external database drop event for test_catalog.db1"));
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateDb("db1");
        inOrder.verify(catalogMgr).unregisterExternalDatabaseFromEvent("db1", "test_catalog");
        inOrder.verify(connector).invalidateDb("db1");
        inOrder.verify(catalogMgr).registerExternalDatabaseFromEvent("db1", "db1", "test_catalog");
        Mockito.verify(constraintManager).dropDatabaseConstraints("test_catalog", "db1");
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    private static void applyOne(MetastoreEventSyncDriver driver,
            PluginDrivenExternalCatalog catalog, Connector connector,
            MetastoreChangeDescriptor descriptor, boolean persistConstraintChanges) {
        Deencapsulation.invoke(driver, "applyDescriptorsAndCommit",
                catalog, connector, List.of(descriptor),
                persistConstraintChanges, -1L, descriptor.getEventId());
    }

    private static class IdentityEventCatalog extends PluginDrivenExternalCatalog {
        IdentityEventCatalog(Connector connector) {
            super(1L, "test_catalog", null, Collections.singletonMap("type", "iceberg"), "", connector);
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            return remoteDatabaseName;
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            return remoteTableName;
        }
    }
}
