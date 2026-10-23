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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.FeMetaCacheEntry;
import org.apache.doris.datasource.metacache.IdNameIndex;
import org.apache.doris.datasource.metacache.NameCacheValue;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Tests missing-table repair through real FE lookup/schema caches and a controllable connector. */
public class PluginDrivenMissingTableTest extends TestWithFeService {
    private static final AtomicLong IDS = new AtomicLong(910000);
    private final Map<String, String> remoteTables = new ConcurrentHashMap<>();
    private final Set<String> remoteViews = ConcurrentHashMap.newKeySet();
    private Connector connector;
    private ConnectorMetadata metadata;
    private TestCatalog catalog;
    private TestDatabase database;
    private final Map<String, String> remoteCache = new ConcurrentHashMap<>();
    private ExecutorService worker;
    private CountDownLatch missingStarted;
    private CountDownLatch releaseMissing;
    private AtomicBoolean blockMissing;
    private boolean failLookup;
    private boolean failInvalidation;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @BeforeEach
    void setup() {
        failLookup = false;
        failInvalidation = false;
        remoteTables.put("a", "old_column");
        remoteTables.put("b", "sibling_column");
        missingStarted = new CountDownLatch(1);
        releaseMissing = new CountDownLatch(1);
        blockMissing = new AtomicBoolean();
        worker = Executors.newFixedThreadPool(2);
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.doAnswer(call -> {
            if (failInvalidation) {
                throw new IllegalStateException("connector cleanup failed");
            }
            remoteCache.remove(call.getArgument(1));
            return null;
        }).when(connector).invalidateTable(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(call -> {
            remoteCache.clear();
            return null;
        }).when(connector).invalidateDb(Mockito.anyString());
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> {
                    if (failLookup) {
                        throw new IllegalStateException("metastore timeout");
                    }
                    String name = call.getArgument(2);
                    if (remoteViews.contains(name)) {
                        return Optional.empty();
                    }
                    String column = remoteTables.get(name);
                    if (column == null) {
                        if (blockMissing.compareAndSet(true, false)) {
                            missingStarted.countDown();
                            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
                        }
                        return Optional.empty();
                    }
                    return Optional.of(new Handle(name, column));
                });
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> remoteViews.contains(call.getArgument(2)));
        Mockito.when(metadata.getTableSchema(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            Handle handle = call.getArgument(1);
            return new ConnectorTableSchema(handle.name, Collections.singletonList(new ConnectorColumn(
                    handle.column, ConnectorType.of("INT", -1, -1), "", true, null, true)),
                    null, Collections.emptyMap());
        });
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).thenAnswer(call -> call.getArgument(3));
        Mockito.when(metadata.fromRemoteTableName(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> call.getArgument(2));
        Mockito.when(metadata.fromRemoteDatabaseName(Mockito.any(), Mockito.anyString()))
                .thenAnswer(call -> call.getArgument(1));
        catalog = new TestCatalog(IDS.incrementAndGet());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().put(catalog.getId(), catalog);
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.put(catalog.getName(), catalog);
        installDatabase();
    }

    @AfterEach
    void cleanup() throws Exception {
        releaseMissing.countDown();
        worker.shutdownNow();
        Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().remove(catalog.getId());
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.remove(catalog.getName());
        remoteTables.clear();
        remoteViews.clear();
        remoteCache.clear();
    }

    private void installDatabase() {
        catalog.setInitializedForTest(true);
        database = new TestDatabase();
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
    }

    private org.apache.doris.connector.cache.MetaCache<SchemaCacheKey, SchemaCacheValue> schemas() {
        ExternalMetaCache cache = Deencapsulation.invoke(Env.getCurrentEnv().getExtMetaCacheMgr(), "engine", "default");
        return cache.entry(catalog.getId(), "schema", SchemaCacheKey.class, SchemaCacheValue.class);
    }

    private PluginDrivenExternalTable warmTarget() {
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        PluginDrivenExternalTable table = database.getTableNullable("a");
        table.getFullSchema();
        database.getTableNullable("b").getFullSchema();
        remoteCache.put("a", "old");
        remoteCache.put("b", "sibling");
        // Keep membership/object entries hot while forcing the next schema read to consult the source.
        schemas().invalidateKey(new SchemaCacheKey(table.getOrBuildNameMapping()));
        return table;
    }

    @Test
    void missingTableRepairsNamesObjectsIdsAndConnectorMetadata() {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenExternalTable sibling = database.getTableNullable("b");
        List<Column> siblingSchema = sibling.getFullSchema();
        remoteTables.remove("a");
        Assertions.assertThrows(CacheException.class, old::getFullSchema);
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
        Assertions.assertEquals("sibling", remoteCache.get("b"));
        Assertions.assertSame(sibling, database.getTableNullable("b"));
        Assertions.assertSame(siblingSchema, sibling.getFullSchema());
    }

    @Test
    void missingHandleRepairsNamesBeforeSchemaLoading() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(table.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(table.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void existingViewWithoutTableHandleKeepsLookupCaches() {
        remoteViews.add("v");
        PluginDrivenExternalTable view = database.getTableNullable("v");
        Assertions.assertNotNull(view);
        remoteCache.put("v", "view metadata");
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(view.resolveConnectorTableHandle(session, metadata).isPresent());
            Assertions.assertFalse(view.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
        Assertions.assertSame(view, database.getTableNullable("v"));
        Assertions.assertSame(view, database.getTableNullable(view.getId()));
        Assertions.assertTrue(database.getTableNamesWithLock().contains("v"));
        Assertions.assertEquals("view metadata", remoteCache.get("v"));

        remoteViews.remove("v");
        readMissingHandle(view);
        Assertions.assertNull(database.getTableNullable(view.getId()));
        Assertions.assertFalse(database.getTableNamesWithLock().contains("v"));
    }

    @Test
    void delayedMissingDoesNotInvalidateRecreatedTable() throws Exception {
        verifyDelayedMissing(false);
    }

    @Test
    void delayedMissingDoesNotInvalidateExactKeyReplacement() throws Exception {
        verifyDelayedMissing(true);
    }

    @Test
    void delayedMissingDoesNotInvalidateMetadataOnlyRefresh() throws Exception {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> missing = worker.submit(() -> readMissingHandle(table));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));

        remoteTables.put("a", "new_column");
        new RefreshManager().refreshTableInternal(database, table, 123L);
        List<Column> refreshedSchema = table.getFullSchema();
        remoteCache.put("a", "refreshed");
        int nameLoads = catalog.nameLoads.get();

        releaseMissing.countDown();
        missing.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(table, database.getTableNullable("a"));
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertSame(refreshedSchema, table.getFullSchema());
        Assertions.assertEquals("new_column", refreshedSchema.get(0).getName());
        Assertions.assertEquals("refreshed", remoteCache.get("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    private void verifyDelayedMissing(boolean replaceWithoutScopeInvalidation) throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<CacheException> result = worker.submit(() -> Assertions.assertThrows(CacheException.class,
                old::getFullSchema));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        remoteTables.put("a", "new_column");
        PluginDrivenExternalTable replacement;
        if (replaceWithoutScopeInvalidation) {
            replacement = database.buildTableInternal("a", "a", old.getId(), catalog, database);
            database.addTableForTest(replacement);
            schemas().put(new SchemaCacheKey(replacement.getOrBuildNameMapping()), replacement.initSchema().get());
        } else {
            new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a", false);
            replacement = database.getTableNullable("a");
        }
        List<Column> newSchema = replacement.getFullSchema();
        remoteCache.put("a", "new");
        database.getTableNamesWithLock();
        int nameLoads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertSame(newSchema, replacement.getFullSchema());
        Assertions.assertTrue(replacement.isObjectCreated());
        Assertions.assertEquals("new_column", newSchema.get(0).getName());
        Assertions.assertEquals("new", remoteCache.get("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    @Test
    void delayedMissingDoesNotInvalidateReplacementDatabase() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> result = worker.submit(() -> readMissingHandle(old));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        remoteTables.put("a", "replacement_column");
        database = new TestDatabase();
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "replacement");
        database.getTableNamesWithLock();
        int loads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void delayedMissingDoesNotInvalidateReplacementCatalog() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> result = worker.submit(() -> readMissingHandle(old));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        long catalogId = catalog.getId();
        catalog = new TestCatalog(catalogId);
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().put(catalogId, catalog);
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.put(catalog.getName(), catalog);
        remoteTables.put("a", "replacement_column");
        installDatabase();
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "replacement");
        database.getTableNamesWithLock();
        int loads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void failedConnectorCleanupStillRemovesObservedLookup() {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        failInvalidation = true;
        Assertions.assertThrows(IllegalStateException.class, () -> readMissingHandle(old));
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(old.getId()));
    }

    private void readMissingHandle(PluginDrivenExternalTable table) {
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(table.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
    }

    @Test
    void changedCanonicalSpellingFencesBothExistingAndNewOldObjectReads() throws Exception {
        catalog.lowerCaseMode = 2;
        try {
            remoteTables.remove("a");
            remoteTables.put("Foo", "old_column");
            PluginDrivenExternalTable old = database.getTableNullable("foo");
            old.getFullSchema();
            schemas().invalidateKey(new SchemaCacheKey(old.getOrBuildNameMapping()));
            remoteTables.remove("Foo");
            blockMissing.set(true);
            Future<CacheException> missing = worker.submit(() -> Assertions.assertThrows(CacheException.class,
                    old::getFullSchema));
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            remoteTables.put("FOO", "new_column");
            FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
            names.invalidateKey("");
            PluginDrivenExternalTable replacement = database.getTableNullable("foo");
            Assertions.assertEquals("FOO", replacement.getName());
            List<Column> schema = replacement.getFullSchema();
            remoteCache.put("FOO", "new");
            int loads = catalog.nameLoads.get();
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(replacement, database.getTableNullable("foo"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals("new", remoteCache.get("FOO"));
            Assertions.assertEquals(loads, catalog.nameLoads.get());
            // A fresh request holding the old object must not combine it with the new names snapshot.
            ConnectorSession session = catalog.buildCrossStatementSession();
            try {
                Assertions.assertFalse(old.resolveConnectorTableHandle(session, metadata).isPresent());
            } finally {
                session.getStatementScope().closeAll();
            }
            Assertions.assertSame(replacement, database.getTableNullable("foo"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals("new", remoteCache.get("FOO"));
        } finally {
            catalog.lowerCaseMode = 0;
        }
    }

    @Test
    void lookupFailureDoesNotRepairNamesOrEvictObjects() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        failLookup = true;
        Assertions.assertThrows(IllegalStateException.class, table::getFullSchema);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertEquals("old", remoteCache.get("a"));
        Assertions.assertEquals(1, catalog.nameLoads.get());
    }

    @Test
    void numericLookupCapturedBeforeRefreshRepublishesItsIdentity() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        IdNameIndex original = Deencapsulation.getField(database, "tableIdNameIndex");
        IdNameIndex index = Mockito.spy(original);
        Deencapsulation.setField(database, "tableIdNameIndex", index);
        CountDownLatch captured = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        Mockito.doAnswer(call -> {
            String name = original.getName(old.getId());
            if (pause.compareAndSet(true, false)) {
                captured.countDown();
                Assertions.assertTrue(proceed.await(10, TimeUnit.SECONDS));
            }
            return name;
        }).when(index).getName(old.getId());
        Future<PluginDrivenExternalTable> lookup = worker.submit(() -> database.getTableNullable(old.getId()));
        try {
            Assertions.assertTrue(captured.await(10, TimeUnit.SECONDS));
            remoteTables.put("a", "new_column");
            new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a", false);
            Assertions.assertNull(original.getName(old.getId()));
            proceed.countDown();
            PluginDrivenExternalTable replacement = lookup.get(10, TimeUnit.SECONDS);
            Assertions.assertNotSame(old, replacement);
            Assertions.assertEquals("a", index.getName(old.getId()));
            Assertions.assertSame(replacement, database.getTableNullable(old.getId()));
        } finally {
            proceed.countDown();
        }
    }

    @Test
    void foldedNamesRefreshClearsHistoricalRemoteSpellings() throws Exception {
        catalog.lowerCaseMode = 1;
        remoteTables.remove("a");
        remoteTables.put("Foo", "first");
        Assertions.assertNotNull(database.getTableNullable("foo"));
        remoteCache.put("Foo", "first");
        remoteTables.remove("Foo");
        remoteTables.put("FOO", "second");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        database.evictTableObjectForTest("foo");
        PluginDrivenExternalTable second = database.getTableNullable("foo");
        Assertions.assertEquals("FOO", second.getRemoteName());
        remoteCache.put("FOO", "second");
        remoteTables.remove("FOO");
        remoteTables.put("Foo", "third");
        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "foo", false);
        Assertions.assertTrue(remoteCache.isEmpty());
        PluginDrivenExternalTable replacement = database.getTableNullable("foo");
        Assertions.assertEquals("Foo", replacement.getRemoteName());
        Assertions.assertEquals("third", replacement.getFullSchema().get(0).getName());
    }

    @Test
    void ordinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(0);
    }

    @Test
    void foldedNameOrdinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(1);
    }

    @Test
    void caseInsensitiveOrdinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(2);
    }

    private void verifyOrdinaryWrite(int mode) {
        catalog.lowerCaseMode = mode;
        PluginDrivenExternalTable table = warmTarget();
        int loads = catalog.nameLoads.get();
        new RefreshManager().refreshTableInternal(database, table, 123L);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void sessionBypassDoesNotInvalidateSharedNames() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        catalog.bypassSchema = true;
        readMissingHandle(table);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertEquals("old", remoteCache.get("a"));
    }

    private static class Handle implements ConnectorTableHandle {
        private final String name;
        private final String column;

        Handle(String name, String column) {
            this.name = name;
            this.column = column;
        }
    }

    private class TestCatalog extends PluginDrivenExternalCatalog {
        private final AtomicInteger nameLoads = new AtomicInteger();
        private boolean bypassSchema;
        private int lowerCaseMode;

        TestCatalog(long id) {
            super(id, "missing_test_" + id, "", new HashMap<>(), "", connector);
        }

        @Override
        protected void initLocalObjectsImpl() {
            // The fixture injects the connector directly instead of loading a production plugin.
        }

        @Override
        public int getLowerCaseTableNames() {
            return lowerCaseMode;
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.singletonList("db");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String dbName) {
            nameLoads.incrementAndGet();
            List<String> names = new ArrayList<>(remoteTables.keySet());
            names.addAll(remoteViews);
            return names;
        }

        @Override
        public boolean shouldBypassSchemaCache(SessionContext context) {
            return bypassSchema;
        }
    }

    private class TestDatabase extends ExternalDatabase<PluginDrivenExternalTable> {
        TestDatabase() {
            super(catalog, Util.genIdByName(catalog.getName(), "db"), "db", "db");
        }

        @Override
        protected PluginDrivenExternalTable buildTableInternal(String remoteName, String localName, long id,
                ExternalCatalog owner, ExternalDatabase db) {
            return new PluginDrivenExternalTable(id, localName, remoteName, owner, db) {
                @Override
                protected synchronized void makeSureInitialized() {
                    // The fixture registers this database explicitly; connector schema loading remains real.
                    objectCreated = true;
                }
            };
        }
    }
}
