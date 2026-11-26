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

public class MetastoreEventSyncDriverRenameTest {

    @Test
    public void databaseRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
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
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void tableRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
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
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameViewRecreateInvalidatesConnectorOnceBeforeReplacement() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
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
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameTableDropCreateInvalidatesBothIncarnations() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        MetastoreChangeDescriptor drop = MetastoreChangeDescriptor.forTable(
                Op.UNREGISTER_TABLE, "db1", "tbl1", null, 1L, 2L);
        MetastoreChangeDescriptor create = MetastoreChangeDescriptor.forTable(
                Op.REGISTER_TABLE, "db1", "tbl1", null, 2L, 3L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, drop);
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, create);
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateTable("db1", "tbl1");
        inOrder.verify(catalogMgr).unregisterExternalTableFromEvent(
                "db1", "tbl1", "test_catalog");
        inOrder.verify(connector).invalidateTable("db1", "tbl1");
        inOrder.verify(catalogMgr).registerExternalTableFromEvent(
                "db1", "tbl1", "tbl1", "test_catalog", 3L);
        Mockito.verifyNoMoreInteractions(connector, catalogMgr);
    }

    @Test
    public void sameNameDatabaseDropCreateInvalidatesBothIncarnations() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new IdentityEventCatalog(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        MetastoreChangeDescriptor drop = MetastoreChangeDescriptor.forDatabase(
                Op.UNREGISTER_DATABASE, "db1", null, 1L, 2L);
        MetastoreChangeDescriptor create = MetastoreChangeDescriptor.forDatabase(
                Op.REGISTER_DATABASE, "db1", null, 2L, 3L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, drop);
            Deencapsulation.invoke(driver, "applyOne", catalog, connector, create);
        }

        InOrder inOrder = Mockito.inOrder(connector, catalogMgr);
        inOrder.verify(connector).invalidateDb("db1");
        inOrder.verify(catalogMgr).unregisterExternalDatabaseFromEvent("db1", "test_catalog");
        inOrder.verify(connector).invalidateDb("db1");
        inOrder.verify(catalogMgr).registerExternalDatabaseFromEvent("db1", "db1", "test_catalog");
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
