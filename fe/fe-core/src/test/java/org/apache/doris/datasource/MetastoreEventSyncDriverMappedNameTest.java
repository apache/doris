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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class MetastoreEventSyncDriverMappedNameTest {

    @Test
    public void databaseRenameKeepsRemoteConnectorKeysAndUsesLocalFeNames() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new MappingEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forDatabase(
                Op.RENAME_DATABASE, "OldDb", "NewDb", 1L, 2L);

        apply(catalog, connector, descriptor, env);

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateDb("OldDb");
        inOrder.verify(connector).invalidateDb("NewDb");
        inOrder.verify(catalogMgr).unregisterExternalDatabaseFromEvent("local_OldDb", "test_catalog");
        inOrder.verify(catalogMgr).registerExternalDatabaseFromEvent(
                "NewDb", "local_NewDb", "test_catalog");
        Mockito.verify(constraintManager)
                .renameDatabase("test_catalog", "local_OldDb", "local_NewDb");
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void tableRenameKeepsRemoteConnectorKeysAndUsesBothTableIdentities() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new MappingEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTableRename(
                "OldDb", "OldTable", "NewDb", "NewTable", 1L, 2L);

        apply(catalog, connector, descriptor, env);

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateTable("OldDb", "OldTable");
        inOrder.verify(connector).invalidateTable("NewDb", "NewTable");
        inOrder.verify(catalogMgr).unregisterExternalTableFromEvent(
                "local_OldDb", "local_OldDb_OldTable", "test_catalog");
        inOrder.verify(catalogMgr).registerExternalTableFromEvent(
                "local_NewDb", "NewTable", "local_NewDb_NewTable", "test_catalog", 2L);
        Mockito.verify(constraintManager).renameTable(
                new TableNameInfo("test_catalog", "local_OldDb", "local_OldDb_OldTable"),
                new TableNameInfo("test_catalog", "local_NewDb", "local_NewDb_NewTable"));
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void refreshAndPartitionEventsUseMappedLocalNames() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new MappingEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        MetastoreChangeDescriptor refreshTable = MetastoreChangeDescriptor.forTable(
                Op.REFRESH_TABLE, "RemoteDb", "RemoteTable", null, 1L, 2L);
        MetastoreChangeDescriptor addPartitions = MetastoreChangeDescriptor.forPartitions(
                Op.ADD_PARTITIONS, "RemoteDb", "RemoteTable",
                Collections.singletonList("p=1"), 2L, 3L);
        MetastoreChangeDescriptor dropPartitions = MetastoreChangeDescriptor.forPartitions(
                Op.DROP_PARTITIONS, "RemoteDb", "RemoteTable",
                Collections.singletonList("p=1"), 3L, 4L);
        MetastoreChangeDescriptor refreshPartitions = MetastoreChangeDescriptor.forPartitions(
                Op.REFRESH_PARTITIONS, "RemoteDb", "RemoteTable",
                Collections.singletonList("p=1"), 4L, 5L);

        apply(catalog, connector, refreshTable, env);
        apply(catalog, connector, addPartitions, env);
        apply(catalog, connector, dropPartitions, env);
        apply(catalog, connector, refreshPartitions, env);

        Mockito.verify(refreshManager).refreshExternalTableFromEvent(
                "test_catalog", "local_RemoteDb", "local_RemoteDb_RemoteTable", 2L);
        Mockito.verify(catalogMgr).addExternalPartitionsFromEvent(
                "test_catalog", "local_RemoteDb", "local_RemoteDb_RemoteTable",
                Collections.singletonList("p=1"), 3L);
        Mockito.verify(catalogMgr).dropExternalPartitionsFromEvent(
                "test_catalog", "local_RemoteDb", "local_RemoteDb_RemoteTable",
                Collections.singletonList("p=1"), 4L);
        Mockito.verify(refreshManager).refreshPartitionsFromEvent(
                "test_catalog", "local_RemoteDb", "local_RemoteDb_RemoteTable",
                Collections.singletonList("p=1"), 5L);
        Mockito.verifyNoInteractions(connector);
    }

    private static void apply(PluginDrivenExternalCatalog catalog, Connector connector,
            MetastoreChangeDescriptor descriptor, Env env) throws Exception {
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(new MetastoreEventSyncDriver(), "applyDescriptorsAndCommit",
                    catalog, connector, List.of(descriptor), false, -1L, descriptor.getEventId());
        }
    }

    private static class MappingEventCatalog extends PluginDrivenExternalCatalog {
        MappingEventCatalog(Connector connector) {
            super(1L, "test_catalog", null, Collections.singletonMap("type", "iceberg"), "", connector);
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            return "local_" + remoteDatabaseName;
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            return "local_" + remoteDatabaseName + "_" + remoteTableName;
        }
    }
}
