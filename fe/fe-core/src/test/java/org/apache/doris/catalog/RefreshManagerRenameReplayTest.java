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
import org.apache.doris.connector.api.Connector;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Tests {@link RefreshManager#replayRefreshTable}'s RENAME branch (R4). A connector-driven RENAME TABLE is
 * logged as a {@code createForRenameTable} external-object log and replayed here on followers/observers.
 *
 * <p>Before R4 the replay only fixed the FE name cache ({@code unregisterTable} + {@code resetMetaCacheNames}
 * + constraint rename) — it never dropped the connector's OWN caches, so a follower that had queried the
 * source table kept its latest-snapshot pin (and paimon's schema memo) to the 24h TTL after an atomic table
 * swap. This pins that the replay now propagates {@code connector.invalidateTable} for BOTH the source and
 * target names, keyed exactly like the coordinator {@code PluginDrivenExternalCatalog.renameTable} hook.
 */
public class RefreshManagerRenameReplayTest {

    private MockedStatic<Env> mockedEnv;
    private CatalogMgr catalogMgr;
    private RefreshManager refreshManager;

    @BeforeEach
    public void setUp() {
        Env mockEnv = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(mockEnv.getConstraintManager()).thenReturn(constraintManager);
        refreshManager = new RefreshManager();
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<? extends ExternalTable> mockDb() {
        return (ExternalDatabase<? extends ExternalTable>) Mockito.mock(ExternalDatabase.class);
    }

    @Test
    public void testRenameReplayInvalidatesConnectorSourceAndTargetOnFollower() {
        // local db1.t1 -> t2, mapping to remote DB1.TBL1 (source). The source table is still in the replay
        // cache when the rename replays (getDbForReplay/getTableForReplay resolve it), so the catalog is
        // already initialized on this FE and getConnector() does not force-init.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        ExternalDatabase<? extends ExternalTable> db = mockDb();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        // doReturn (not when().thenReturn) because getCatalog returns a wildcard CatalogIf<? extends ...>
        // whose capture rejects a concrete PluginDrivenExternalCatalog in the typed stubbing form.
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(7L);
        Mockito.when(catalog.getName()).thenReturn("c");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db1");
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("t1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");

        refreshManager.replayRefreshTable(ExternalObjectLog.createForRenameTable(7L, "db1", "t1", "t2"));

        // WHY (Rule 9 / R4): both the source (REMOTE DB1.TBL1) and the target (DB1.t2, new name NOT
        // remote-resolved — parity with the coordinator) connector caches must be dropped so an atomic swap
        // doesn't serve the pre-rename pin. MUTATION: removing the rename-branch invalidation, or passing the
        // LOCAL names, turns this red.
        Mockito.verify(connector).invalidateTable("DB1", "TBL1");
        Mockito.verify(connector).invalidateTable("DB1", "t2");
        // Base bookkeeping is preserved: the source name is unregistered and the db's name cache reset.
        Mockito.verify(db).unregisterTable("t1");
        Mockito.verify(db).resetMetaCacheNames();
    }

    @Test
    public void testRenameReplayUninitializedCatalogSkipsInvalidate() {
        // A follower that never initialized this catalog: getDbForReplay returns empty, so the replay resolves
        // no db/table and returns early — the connector is never consulted (no force-init, no cache to drop).
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(7L);
        Mockito.doReturn(Optional.empty()).when(catalog).getDbForReplay("db1");

        refreshManager.replayRefreshTable(ExternalObjectLog.createForRenameTable(7L, "db1", "t1", "t2"));

        // WHY (R4 no-force-init): an uninitialized catalog has no connector cache to drop; the replay must not
        // touch (or force-build) the connector. MUTATION: force-initializing / invalidating here -> red.
        Mockito.verify(catalog, Mockito.never()).getConnector();
        Mockito.verifyNoInteractions(connector);
    }
}
