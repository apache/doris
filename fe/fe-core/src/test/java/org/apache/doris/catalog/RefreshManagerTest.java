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

import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/** Refresh entry points use one resource change for FE, connector and derived cache state. */
public class RefreshManagerTest {
    private final CatalogMetaCache root = CatalogMetaCache.unmanaged();
    private MockedStatic<Env> env;
    private CatalogMgr catalogs;
    private PluginDrivenExternalCatalog catalog;
    private ExternalDatabase db;
    private ExternalTable table;
    private Connector connector;
    private EditLog editLog;

    @BeforeEach
    public void setup() {
        Env current = Mockito.mock(Env.class);
        env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentEnv).thenReturn(current);
        catalogs = Mockito.mock(CatalogMgr.class);
        editLog = Mockito.mock(EditLog.class);
        Mockito.when(current.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(current.getEditLog()).thenReturn(editLog);
        catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        db = Mockito.mock(ExternalDatabase.class);
        table = Mockito.mock(ExternalTable.class);
        connector = Mockito.mock(Connector.class);
        Mockito.doReturn(catalog).when(catalogs).getCatalog(10L);
        Mockito.when(catalog.getId()).thenReturn(10L);
        Mockito.when(catalog.getName()).thenReturn("c");
        Mockito.when(catalog.getMetaCacheOwnerIfPresent()).thenReturn(root);
        Mockito.when(catalog.getConnectorIfPresent()).thenReturn(connector);
        Mockito.when(catalog.resolveDatabaseNameForInvalidation("db")).thenReturn("db");
        Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("DB");
        Mockito.when(db.getId()).thenReturn(Util.genIdByName("c", "db"));
        Mockito.when(db.resolveTableNameForInvalidation(Mockito.anyString())).thenAnswer(inv -> inv.getArgument(0));
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("t");
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDb()).thenReturn(db);
        Mockito.when(table.getId()).thenReturn(Util.genIdByName("c", "db", "t"));
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getRemoteDbName()).thenReturn("DB");
        Mockito.when(table.getRemoteName()).thenReturn("T");
    }

    @AfterEach
    public void cleanup() {
        root.close();
        env.close();
    }

    private MetaCache<String, String> cache(String name, ScopePath scope) {
        MetaCache<String, String> cache = root.create(MetaCacheDefinition.<String, String>builder(name,
                CacheSpec.of(true, -1, 10), key -> scope).build());
        cache.put("key", "value");
        return cache;
    }

    @Test
    public void replayMissingCatalogDoesNothing() {
        Mockito.doReturn(null).when(catalogs).getCatalog(10L);
        new RefreshManager().replayRefreshDb(ExternalObjectLog.createForRefreshDb(10, "db"));
        Mockito.verifyNoInteractions(catalog, connector);
    }

    @Test
    public void writeRefreshPreservesLookupAndMembershipWhileUpdatingContent() {
        MetaCache<String, String> names = cache("names",
                ScopePath.tableCollection("db").in(ScopePath.Coordinates.LOCAL));
        MetaCache<String, String> object = cache("object",
                ScopePath.table("db", "t").in(ScopePath.Coordinates.LOCAL));
        MetaCache<String, String> source = cache("source",
                ScopePath.table("DB", "T").in(ScopePath.Coordinates.REMOTE));
        new RefreshManager().refreshTableInternal(db, table, 123L);
        Assertions.assertEquals("value", names.getIfPresent("key"));
        Assertions.assertEquals("value", object.getIfPresent("key"));
        Assertions.assertNull(source.getIfPresent("key"));
        Mockito.verify(table, Mockito.atLeastOnce()).unsetObjectCreated();
        Mockito.verify(table, Mockito.atLeastOnce()).setUpdateTime(123L);
        Mockito.verify(connector, Mockito.never()).invalidateTable(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void coldExplicitReplayUsesPersistedCoordinatesWithoutInitialization() {
        Mockito.doReturn(Optional.empty()).when(catalog).getDbForReplay("db");
        MetaCache<String, String> names = cache("names",
                ScopePath.tableCollection("db").in(ScopePath.Coordinates.LOCAL));
        MetaCache<String, String> remote = cache("remote",
                ScopePath.table("DB", "T").in(ScopePath.Coordinates.REMOTE));
        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(10, "db", "t", 0)
                .withRemoteNames("DB", "T"));
        Assertions.assertNull(names.getIfPresent("key"));
        Assertions.assertNull(remote.getIfPresent("key"));
        Mockito.verify(catalog, Mockito.never()).getConnector();
        Mockito.verify(catalog, Mockito.never()).getFeMetaCacheOwner();
    }

    @Test
    public void mutationJournalPrecedesFallibleInvalidationAndCarriesBothCoordinates() {
        RefreshManager manager = Mockito.spy(new RefreshManager());
        IllegalStateException failure = new IllegalStateException("invalidation");
        Mockito.doThrow(failure).when(manager).refreshTableInternal(Mockito.eq(db), Mockito.eq(table),
                Mockito.anyLong());
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> manager.refreshTableAfterExternalMutation(table)));
        InOrder order = Mockito.inOrder(editLog, manager);
        ArgumentCaptor<ExternalObjectLog> change = ArgumentCaptor.forClass(ExternalObjectLog.class);
        order.verify(editLog).logRefreshExternalTable(change.capture());
        order.verify(manager).refreshTableInternal(db, table, change.getValue().getLastUpdateTime());
        Assertions.assertEquals("db", change.getValue().getDbName());
        Assertions.assertEquals("DB", change.getValue().getRemoteDbName());
        Assertions.assertEquals("T", change.getValue().getRemoteTableName());
        Assertions.assertFalse(change.getValue().isInvalidateTableLookup());
    }

    @Test
    public void projectionFailureStillInvalidatesKnownLocalAndNumericDependencies() {
        MetaCache<String, String> schema = cache("schema",
                ScopePath.tableMetadata("db", "t").in(ScopePath.Coordinates.LOCAL));
        MetaCache<String, String> rows = cache("rows", ScopePath.table(
                Long.toString(db.getId()), Long.toString(table.getId())).in(ScopePath.Coordinates.ID));
        Mockito.when(connector.cacheDependencies(Mockito.any()))
                .thenThrow(new IllegalStateException("projection"));
        Assertions.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().refreshTableInternal(db, table, 0));
        Assertions.assertNull(schema.getIfPresent("key"));
        Assertions.assertNull(rows.getIfPresent("key"));
    }

    @Test
    public void databaseReplayInvalidatesBothCoordinatesAndParentMembership() {
        MetaCache<String, String> names = cache("names",
                ScopePath.databaseCollection().in(ScopePath.Coordinates.LOCAL));
        MetaCache<String, String> source = cache("source",
                ScopePath.table("DB", "T").in(ScopePath.Coordinates.REMOTE));
        new RefreshManager().replayRefreshDb(ExternalObjectLog.createForRefreshDb(10, "db")
                .withRemoteNames("DB", null));
        Assertions.assertNull(names.getIfPresent("key"));
        Assertions.assertNull(source.getIfPresent("key"));
        Mockito.verify(catalog, Mockito.never()).getConnector();
    }
}
