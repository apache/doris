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
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MetastoreEventSyncDriverRenameTest {

    @Test
    public void databaseRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forDatabase(
                Op.RENAME_DATABASE, "OldDb", "NewDb", 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(new MetastoreEventSyncDriver(), "applyOne", catalog, descriptor);
        }

        Mockito.verify(catalogMgr).unregisterExternalDatabase("OldDb", "test_catalog");
        Mockito.verify(catalogMgr).registerExternalDatabaseFromEvent("NewDb", "test_catalog");
        Mockito.verifyNoMoreInteractions(catalogMgr);
    }

    @Test
    public void tableRenameAlwaysCleansOldNameAndRegistersNewName() throws Exception {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forTableRename(
                "OldDb", "OldTable", "NewDb", "NewTable", 1L, 2L);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(new MetastoreEventSyncDriver(), "applyOne", catalog, descriptor);
        }

        Mockito.verify(catalogMgr).unregisterExternalTable(
                "OldDb", "OldTable", "test_catalog", true);
        Mockito.verify(catalogMgr).registerExternalTableFromEvent(
                "NewDb", "NewTable", "test_catalog", 2L, true);
        Mockito.verifyNoMoreInteractions(catalogMgr);
    }
}
