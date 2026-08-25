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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.mtmv.MTMVUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetastoreEventSyncDriverRenameTest {

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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
        }

        Assertions.assertEquals(baseline, catalog.snapshotConstraintMetadata());
    }

    @Test
    public void schemaRefreshDropsAffectedConstraintsAndInvalidatesMtmvsBeforeRefresh() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
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
        Mockito.when(constraintManager.dropConstraintsReferencingColumns(
                tableNameInfo, removedColumns)).thenAnswer(invocation -> {
                    operations.add("constraints");
                    return affectedTables;
                });
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);

            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables,
                    "before applying external table schema refresh event for " + tableNameInfo));
        }

        Assertions.assertEquals(List.of("constraints", "mtmv", "refresh"), operations);

        InOrder inOrder = Mockito.inOrder(constraintManager, refreshManager);
        inOrder.verify(constraintManager).dropConstraintsReferencingColumns(
                tableNameInfo, removedColumns);
        inOrder.verify(refreshManager).refreshExternalTableFromEvent(
                "test_catalog", "db1", "tbl1", 2L);
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
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
            Deencapsulation.invoke(
                    new MetastoreEventSyncDriver(), "applyOne", catalog, connector, descriptor);
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
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, drop);
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, create);
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
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, drop);
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, create);
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
