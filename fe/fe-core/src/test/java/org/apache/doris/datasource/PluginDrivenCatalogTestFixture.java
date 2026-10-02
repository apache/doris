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
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.persist.EditLog;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

/** Delta-shaped catalog fixture exercising the real, statement-scoped FE DDL routing. */
final class PluginDrivenCatalogTestFixture implements AutoCloseable {
    final Connector connector = Mockito.mock(Connector.class);
    final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
    final ConnectorSession session = ConnectorSessionBuilder.create().withCatalogId(1L).build();
    final ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
    @SuppressWarnings("unchecked")
    final ExternalDatabase<PluginDrivenExternalTable> database = Mockito.mock(ExternalDatabase.class);
    final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
    final EditLog editLog = Mockito.mock(EditLog.class);
    final RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
    final ExternalMetaCacheMgr cacheManager = Mockito.mock(ExternalMetaCacheMgr.class);
    final TestablePluginCatalog catalog;
    private final MockedStatic<Env> mockedEnv;

    PluginDrivenCatalogTestFixture() {
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(session, "remote_db", "events")).thenReturn(Optional.of(handle));
        Mockito.when(database.getFullName()).thenReturn("local_db");
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
        Mockito.when(database.isInitialized()).thenReturn(true);
        Mockito.when(database.canonicalLocalTableNameFromRemote("events")).thenReturn("events");
        Mockito.when(database.getTableNullable("events")).thenReturn(table);
        Mockito.when(database.getTableForReplay("events")).thenReturn(Optional.of(table));
        Mockito.when(table.getName()).thenReturn("events");
        Mockito.when(table.getRemoteName()).thenReturn("events");
        Mockito.when(table.getDbName()).thenReturn("local_db");
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        catalog = new TestablePluginCatalog(connector);

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheManager);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @Override
    public void close() {
        mockedEnv.close();
    }

    final class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        TestablePluginCatalog(Connector connector) {
            super(1L, "delta_catalog", null, Map.of("type", "delta"), "", connector);
            initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // The fixture injects the connector; skip plugin discovery and remote initialization.
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return session;
        }

        @Override
        public void validateCreateTableProperties(CreateTableInfo info) {
            // Provider-specific policy is covered by connector tests; this fixture tests FE routing.
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String name) {
            return database;
        }

        @Override
        public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String name) {
            return Optional.of(database);
        }
    }
}
