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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
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
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.metacache.CatalogMetadataChanges;
import org.apache.doris.datasource.metacache.FeMetaCacheEntry;
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

/** Tests read-observed absence through real FE object/schema caches and a controllable connector. */
public class PluginDrivenMissingTableTest extends TestWithFeService {
    private static final AtomicLong IDS = new AtomicLong(910000);
    private final Map<String, String> remoteTables = new ConcurrentHashMap<>();
    private Connector connector;
    private ConnectorMetadata metadata;
    private TestCatalog catalog;
    private TestDatabase database;
    private MetaCache<String, String> remoteCache;
    private ExecutorService worker;
    private CountDownLatch missingStarted;
    private CountDownLatch releaseMissing;
    private AtomicBoolean blockMissing;
    private boolean failLookup;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @BeforeEach
    void setup() {
        failLookup = false;
        remoteTables.put("a", "old_column");
        remoteTables.put("b", "sibling_column");
        missingStarted = new CountDownLatch(1);
        releaseMissing = new CountDownLatch(1);
        blockMissing = new AtomicBoolean();
        worker = Executors.newFixedThreadPool(2);
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> {
                    if (failLookup) {
                        throw new IllegalStateException("metastore timeout");
                    }
                    String name = call.getArgument(2);
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
        catalog.closeFeMetaCacheOwner();
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().remove(catalog.getId());
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.remove(catalog.getName());
        remoteTables.clear();
    }

    private void installDatabase() {
        catalog.setInitializedForTest(true);
        database = new TestDatabase();
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        remoteCache = catalog.getFeMetaCacheOwner().openDomain("missing-test", ScopePath.Coordinates.REMOTE,
                getClass().getClassLoader()).create(MetaCacheDefinition.<String, String>builder(
                        "remote", CacheSpec.of(true, -1, 100), key -> ScopePath.table("db", key)).build());
    }

    @SuppressWarnings("unchecked")
    private MetaCache<SchemaCacheKey, SchemaCacheValue> schemas() {
        return (MetaCache<SchemaCacheKey, SchemaCacheValue>) catalog.getFeMetaCacheOwner().entries().get("schema");
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
        Assertions.assertNull(remoteCache.getIfPresent("a"));
        Assertions.assertEquals("sibling", remoteCache.getIfPresent("b"));
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
        Assertions.assertNull(remoteCache.getIfPresent("a"));
    }

    @Test
    void delayedMissingDoesNotInvalidateRecreatedTable() throws Exception {
        verifyDelayedMissing(false);
    }

    @Test
    void delayedMissingDoesNotInvalidateExactKeyReplacement() throws Exception {
        verifyDelayedMissing(true);
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
            CatalogMetadataChanges.apply(catalog, ExternalObjectLog.createForExplicitRefreshTable(
                    catalog.getId(), "db", "a", 0).withRemoteNames("db", "a"));
            replacement = database.getTableNullable("a");
        }
        List<Column> newSchema = replacement.getFullSchema();
        remoteCache.put("a", "new");
        database.getTableNamesWithLock();
        int nameLoads = catalog.nameLoads.get();
        releaseMissing.countDown();
        Assertions.assertTrue(result.get(10, TimeUnit.SECONDS).getMessage().contains("does not exist"));
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertSame(newSchema, replacement.getFullSchema());
        Assertions.assertTrue(replacement.isObjectCreated());
        Assertions.assertEquals("new_column", newSchema.get(0).getName());
        Assertions.assertEquals("new", remoteCache.getIfPresent("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    @Test
    void delayedMissingDoesNotInvalidateReplacementCatalogRoot() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<CacheException> result = worker.submit(() -> Assertions.assertThrows(CacheException.class,
                old::getFullSchema));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        CatalogMetaCache retired = catalog.getFeMetaCacheOwner();
        catalog.closeFeMetaCacheOwner();
        remoteTables.put("a", "new_root_column");
        installDatabase();
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "new_root");
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        int nameLoads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertTrue(retired.isClosed());
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertTrue(replacement.isObjectCreated());
        Assertions.assertEquals("new_root", remoteCache.getIfPresent("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    @Test
    void retiredDatabaseWithAnActiveLoadCannotClearReplacement() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> missing = worker.submit(() -> {
            ConnectorSession session = catalog.buildCrossStatementSession();
            try {
                Assertions.assertFalse(old.resolveConnectorTableHandle(session, metadata).isPresent());
            } finally {
                session.getStatementScope().closeAll();
            }
        });
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        FeMetaCacheEntry<String, PluginDrivenExternalTable> oldTables = Deencapsulation.getField(database, "tables");
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        Future<?> activeLoad = worker.submit(() -> oldTables.get("loading", key -> {
            loadStarted.countDown();
            try {
                Assertions.assertTrue(releaseLoad.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return null;
        }));
        try {
            Assertions.assertTrue(loadStarted.await(10, TimeUnit.SECONDS));
            database.releaseCachedEntries();
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
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(replacement, database.getTableNullable("a"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals("replacement", remoteCache.getIfPresent("a"));
            Assertions.assertEquals(loads, catalog.nameLoads.get());
        } finally {
            releaseLoad.countDown();
            activeLoad.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void changedCanonicalSpellingFencesBothExistingAndNewOldObjectReads() throws Exception {
        catalog.lowerCaseMode = 2;
        try {
            catalog.closeFeMetaCacheOwner();
            installDatabase();
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
            Assertions.assertEquals("new", remoteCache.getIfPresent("FOO"));
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
            Assertions.assertEquals("new", remoteCache.getIfPresent("FOO"));
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
        Assertions.assertEquals("old", remoteCache.getIfPresent("a"));
        Assertions.assertEquals(1, catalog.nameLoads.get());
    }

    @Test
    void sessionBypassDoesNotInvalidateSharedNames() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        catalog.bypassSchema = true;
        Assertions.assertThrows(CacheException.class, table::getFullSchema);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertEquals("old", remoteCache.getIfPresent("a"));
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
            return new ArrayList<>(remoteTables.keySet());
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
