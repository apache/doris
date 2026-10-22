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

import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/** Tests connector and local-cache invalidation during external table rename replay. */
public class RefreshManagerRenameReplayTest {

    private MockedStatic<Env> mockedEnv;
    private final CatalogMetaCache root = CatalogMetaCache.unmanaged();
    private CatalogMgr catalogMgr;
    private ExternalMetaCacheMgr metaCacheMgr;
    private ConstraintManager constraintManager;
    private RefreshManager refreshManager;
    private PluginDrivenExternalCatalog catalog;
    private Connector connector;
    private ExternalDatabase<? extends ExternalTable> db;
    private long dbId;
    private long sourceTableId;
    private long destinationTableId;

    @BeforeEach
    public void setUp() {
        Env mockEnv = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        constraintManager = Mockito.mock(ConstraintManager.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(mockEnv.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        Mockito.when(mockEnv.getConstraintManager()).thenReturn(constraintManager);
        refreshManager = new RefreshManager();

        catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        connector = Mockito.mock(Connector.class);
        db = mockDb();
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(7L);
        Mockito.when(catalog.getName()).thenReturn("c");
        Mockito.when(catalog.getConnectorIfPresent()).thenReturn(connector);
        Mockito.when(catalog.getMetaCacheOwnerIfPresent()).thenReturn(root);
        Mockito.when(catalog.resolveDatabaseNameForInvalidation("db1")).thenReturn("db1");
        Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db1");
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        dbId = Util.genIdByName("c", "db1");
        sourceTableId = Util.genIdByName("c", "db1", "t1");
        destinationTableId = Util.genIdByName("c", "db1", "t2");
    }

    @AfterEach
    public void tearDown() {
        root.close();
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<? extends ExternalTable> mockDb() {
        return (ExternalDatabase<? extends ExternalTable>) Mockito.mock(ExternalDatabase.class);
    }

    @Test
    public void testRenameReplayInvalidatesConnectorDatabaseOnFollower() {
        replayRename();

        Mockito.verify(connector).cacheDependencies(ScopePath.database("DB1"));
        Mockito.verify(connector, Mockito.never()).invalidateDb(Mockito.anyString());
        Mockito.verify(constraintManager).renameTable(Mockito.any(), Mockito.any());
    }

    @Test
    public void testRenameReplayWithoutDatabaseObjectInvalidatesBothIdentitiesWithoutConnectorInit() {
        Mockito.doReturn(Optional.empty()).when(catalog).getDbForReplay("db1");
        MetaCache<String, String> old = root.create(MetaCacheDefinition.<String, String>builder("old",
                CacheSpec.of(true, -1, 10), key -> ScopePath.table("db1", "t1")
                        .in(ScopePath.Coordinates.LOCAL)).build());
        MetaCache<String, String> destination = root.create(MetaCacheDefinition.<String, String>builder("new",
                CacheSpec.of(true, -1, 10), key -> ScopePath.table("db1", "t2")
                        .in(ScopePath.Coordinates.LOCAL)).build());
        old.put("key", "old");
        destination.put("key", "previous incarnation");
        replayRename();
        Assertions.assertNull(old.getIfPresent("key"));
        Assertions.assertNull(destination.getIfPresent("key"));
        Mockito.verify(catalog, Mockito.never()).getConnector();
    }

    private void replayRename() {
        refreshManager.replayRefreshTable(ExternalObjectLog.createForRenameTable(
                7L, "db1", "t1", "t2"));
    }
}
