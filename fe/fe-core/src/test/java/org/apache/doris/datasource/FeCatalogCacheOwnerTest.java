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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCacheGovernance;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.metacache.FeMetaCacheEntry;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FeCatalogCacheOwnerTest extends TestWithFeService {
    private static final AtomicLong IDS = new AtomicLong(72000);
    private CountingCatalog catalog;
    private CountingDatabase database;
    private Gate namesGate;
    private Gate objectsGate;
    private Gate schemaGate;
    private ExecutorService workers;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @BeforeEach
    public void prepareCatalog() {
        namesGate = new Gate();
        objectsGate = new Gate();
        schemaGate = new Gate();
        Provider.TABLES.clear();
        putTable("a", "old_column");
        putTable("b", "sibling_column");
        catalog = new CountingCatalog(IDS.incrementAndGet());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().put(catalog.getId(), catalog);
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.put(catalog.getName(), catalog);
        catalog.setInitializedForTest(true);
        database = new CountingDatabase(catalog);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        workers = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    public void releaseCatalog() throws Exception {
        namesGate.release.countDown();
        objectsGate.release.countDown();
        schemaGate.release.countDown();
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        catalog.closeFeMetaCacheOwner();
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().remove(catalog.getId());
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.remove(catalog.getName());
    }

    @Test
    public void fullRefreshSharesOwnerAndPreservesSibling() throws Exception {
        TestExternalTable a = database.getTableNullable("a");
        TestExternalTable b = database.getTableNullable("b");
        a.getFullSchema();
        List<Column> siblingSchema = b.getFullSchema();
        Assertions.assertEquals(1, MetaCacheGovernance.catalogCaches(catalog.getId()).size());
        Assertions.assertEquals(1, catalog.nameLoads.get());
        Provider.TABLES.remove("a");

        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a", false);

        Assertions.assertEquals(Collections.singleton("b"), database.getTableNamesWithLock());
        Assertions.assertEquals(2, catalog.nameLoads.get());
        Assertions.assertNull(database.getTableNullable("a"));
        Assertions.assertNull(database.getCachedTableNameByIdForTest(a.getId()));
        Assertions.assertSame(b, database.getTableNullable("b"));
        Assertions.assertSame(siblingSchema, b.getFullSchema());
    }

    @Test
    public void committedWritesAndTheirReplayDoNotReloadNames() {
        TestExternalTable a = database.getTableNullable("a");
        database.getTableNamesWithLock();
        RefreshManager manager = new RefreshManager();
        for (int i = 0; i < 3; i++) {
            manager.refreshTableAfterExternalMutation(a);
            manager.replayRefreshTable(ExternalObjectLog.createForRefreshTable(
                    catalog.getId(), "db", "a", i + 1L));
            Assertions.assertSame(a, database.getTableNullable("a"));
            Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        }
        Assertions.assertEquals(1, catalog.nameLoads.get());
    }

    @Test
    public void explicitReplayInvalidatesNamesWhenTableObjectIsCold() {
        database.getTableNamesWithLock();
        Provider.TABLES.remove("a");
        refreshExplicitly();
        Assertions.assertEquals(Collections.singleton("b"), database.getTableNamesWithLock());
        Assertions.assertEquals(2, catalog.nameLoads.get());
    }

    @Test
    public void oldNameLoadCannotRefillAfterExplicitRefresh() throws Exception {
        database.getTableNullable("a");
        database.resetMetaCacheNames();
        namesGate.arm();
        Future<Set<String>> old = workers.submit(database::getTableNamesWithLock);
        namesGate.awaitStarted();
        Provider.TABLES.remove("a");
        refreshExplicitly();
        namesGate.release.countDown();
        old.get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(Collections.singleton("b"), database.getTableNamesWithLock());
    }

    @Test
    public void oldObjectLoadCannotReplaceSameNameRecreation() throws Exception {
        database.getTableNamesWithLock();
        objectsGate.arm();
        Future<TestExternalTable> old = workers.submit(() -> database.getTableNullable("a"));
        objectsGate.awaitStarted();
        Provider.TABLES.remove("a");
        refreshExplicitly();
        putTable("a", "new_column");
        TestExternalTable recreated = database.getTableNullable("a");
        Assertions.assertEquals("new_column", recreated.getFullSchema().get(0).getName());
        objectsGate.release.countDown();
        Assertions.assertNotSame(recreated, old.get(10, TimeUnit.SECONDS));
        Assertions.assertSame(recreated, database.getTableNullable("a"));
        Assertions.assertEquals("a", database.getCachedTableNameByIdForTest(recreated.getId()));
    }

    @Test
    public void oldSchemaLoadCannotReplaceSameNameRecreation() throws Exception {
        TestExternalTable oldTable = database.getTableNullable("a");
        schemaGate.arm();
        Future<List<Column>> old = workers.submit(() -> {
            return oldTable.getFullSchema();
        });
        schemaGate.awaitStarted();
        Provider.TABLES.remove("a");
        refreshExplicitly();
        putTable("a", "new_column");
        TestExternalTable recreated = database.getTableNullable("a");
        List<Column> newSchema = recreated.getFullSchema();
        Assertions.assertEquals("new_column", newSchema.get(0).getName());
        schemaGate.release.countDown();
        Assertions.assertEquals("old_column", old.get(10, TimeUnit.SECONDS).get(0).getName());
        Assertions.assertSame(newSchema, recreated.getFullSchema());
    }

    @Test
    public void oldDatabaseCleanupDoesNotInvalidateReplacementOrAccumulateEntries() {
        database.getTableNullable("a").getFullSchema();
        for (int i = 0; i < 5; i++) {
            CountingDatabase previous = database;
            database = new CountingDatabase(catalog);
            database.setInitializedForTest(true);
            catalog.addDatabaseForTest(database);
            TestExternalTable replacement = database.getTableNullable("a");
            List<Column> schema = replacement.getFullSchema();
            previous.releaseCachedEntries();
            Assertions.assertSame(replacement, database.getTableNullable("a"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals(5, catalog.getFeMetaCacheOwner().entries().size());
            Assertions.assertNotNull(previous.getTableNullable("a"));
            Assertions.assertEquals(5, catalog.getFeMetaCacheOwner().entries().size());
        }
    }

    @Test
    public void catalogCloseReleasesNamingOwnerBeforeAnySchemaWasLoaded() {
        database.getTableNamesWithLock();
        CatalogMetaCache owner = catalog.getFeMetaCacheOwner();
        catalog.onClose();
        Assertions.assertTrue(owner.isClosed());
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalog.getId()).isEmpty());
        Assertions.assertThrows(IllegalStateException.class, catalog::getFeMetaCacheOwner);
    }

    @Test
    public void ownerRetirementDuringOldSchemaLoadCannotRefillReplacementRuntime() throws Exception {
        TestExternalTable oldTable = database.getTableNullable("a");
        schemaGate.arm();
        Future<List<Column>> old = workers.submit(() -> {
            return oldTable.getFullSchema();
        });
        schemaGate.awaitStarted();
        CatalogMetaCache oldOwner = catalog.getFeMetaCacheOwner();
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        putTable("a", "new_owner_column");
        ExternalDatabase<?> replacement = catalog.getDbNullable("db");
        List<Column> schema = replacement.getTableNullable("a").getFullSchema();
        Assertions.assertTrue(oldOwner.isClosed());
        Assertions.assertNotSame(oldOwner, catalog.getFeMetaCacheOwner());
        schemaGate.release.countDown();
        Assertions.assertEquals("old_column", old.get(10, TimeUnit.SECONDS).get(0).getName());
        Assertions.assertSame(schema, replacement.getTableNullable("a").getFullSchema());
        Assertions.assertEquals("new_owner_column", schema.get(0).getName());
        Assertions.assertEquals(1, MetaCacheGovernance.catalogCaches(catalog.getId()).size());
    }

    @Test
    public void perSessionDatabaseBypassDoesNotRegisterSharedChildEntries() {
        catalog.bypassDatabaseNames = true;
        int entries = catalog.getFeMetaCacheOwner().entries().size();
        for (int i = 0; i < 20; i++) {
            ExternalDatabase<?> transientDatabase = catalog.getDbNullable("db");
            Assertions.assertEquals(Set.of("a", "b"), transientDatabase.getTableNamesWithLock());
            Assertions.assertNotNull(transientDatabase.getTableNullable("a"));
        }
        Assertions.assertEquals(entries, catalog.getFeMetaCacheOwner().entries().size());
    }

    @Test
    public void weightedPayloadBudgetPreservesLookupCachingAndIdResolution() {
        catalog.modifyCatalogProps(Collections.singletonMap("meta.cache.max-weight", "1MB"));
        ExternalDatabase<?> current = catalog.getDbNullable("db");
        ExternalTable table = current.getTableNullable("a");
        for (int i = 0; i < 20; i++) {
            Assertions.assertSame(current, catalog.getDbNullable("db"));
            Assertions.assertEquals(Set.of("a", "b"), current.getTableNamesWithLock());
            Assertions.assertSame(table, current.getTableNullable("a"));
            Assertions.assertEquals("old_column", table.getFullSchema().get(0).getName());
        }
        Assertions.assertEquals(1, catalog.nameLoads.get());
        Assertions.assertSame(current, catalog.getDbNullable(current.getId()));
        Assertions.assertSame(table, current.getTableNullable(table.getId()));
        Assertions.assertEquals(5, catalog.getFeMetaCacheOwner().entries().size());
        Assertions.assertTrue(Env.getCurrentEnv().getExtMetaCacheMgr().getCatalogCacheStats(catalog.getId()).stream()
                .filter(stat -> "schema".equals(stat.getEntryName()))
                .anyMatch(stat -> stat.getEntryStats().getEstimatedWeight() > 0));
    }

    @Test
    public void coldDatabaseRefreshReplayAfterOwnerRetirementDoesNotReinitializeCatalog() {
        database.getTableNamesWithLock();
        catalog.modifyCatalogProps(Collections.singletonMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "600"));
        Assertions.assertFalse(catalog.isInitialized());
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalog.getId()).isEmpty());
        Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshDb(
                ExternalObjectLog.createForRefreshDb(catalog.getId(), "db")));
        Assertions.assertFalse(catalog.isInitialized());
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalog.getId()).isEmpty());
    }

    @Test
    public void schemaEntryReconfigurationDoesNotRetireTheBorrowedCatalogOwner() {
        database.getTableNullable("a").getFullSchema();
        CatalogMetaCache owner = catalog.getFeMetaCacheOwner();
        catalog.modifyCatalogProps(Collections.singletonMap("meta.cache.default.schema.ttl-second", "600"));
        Assertions.assertFalse(owner.isClosed());
        Assertions.assertFalse(catalog.isInitialized());
        Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshDb(
                ExternalObjectLog.createForRefreshDb(catalog.getId(), "db")));
        Assertions.assertFalse(catalog.isInitialized());
        Assertions.assertSame(owner, catalog.getFeMetaCacheOwner());
        Assertions.assertEquals("old_column", catalog.getDbNullable("db").getTableNullable("a")
                .getFullSchema().get(0).getName());
        Assertions.assertSame(owner, catalog.getFeMetaCacheOwner());
    }

    @Test
    public void namingOnlyOwnerIsVisibleInStatsWithoutSchemaInitialization() {
        database.getTableNamesWithLock();
        Assertions.assertEquals(4, Env.getCurrentEnv().getExtMetaCacheMgr()
                .getCatalogCacheStats(catalog.getId()).size());
        database.getTableNullable("a").getFullSchema();
        Assertions.assertEquals(5, Env.getCurrentEnv().getExtMetaCacheMgr()
                .getCatalogCacheStats(catalog.getId()).size());
    }

    @Test
    public void retiredDatabaseEntriesReadThroughWithoutPublishingIds() {
        database.getTableNamesWithLock();
        FeMetaCacheEntry<String, TestExternalTable> entries = Deencapsulation.getField(database, "tables");
        database.releaseCachedEntries();
        AtomicInteger actions = new AtomicInteger();
        TestExternalTable table = entries.getAndRunIfCurrent("a", (key, value) -> actions.incrementAndGet());
        Assertions.assertNotNull(table);
        Assertions.assertEquals(0, actions.get());
        Assertions.assertNull(entries.getIfPresent("a"));
        Assertions.assertDoesNotThrow(() -> database.invalidateTableLookup("a"));
        Assertions.assertNull(database.getCachedTableNameByIdForTest(table.getId()));
    }

    @Test
    public void databaseRetirementDoesNotCloseAnAcceptedObjectLoad() throws Exception {
        database.getTableNamesWithLock();
        CountingDatabase previous = database;
        objectsGate.arm();
        Future<TestExternalTable> old = workers.submit(() -> previous.getTableNullable("a"));
        objectsGate.awaitStarted();
        // Catalog eviction/replacement must not wait for the in-flight remote table lookup.
        previous.releaseCachedEntries();
        database = new CountingDatabase(catalog);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        TestExternalTable replacement = database.getTableNullable("a");
        objectsGate.release.countDown();
        Assertions.assertNotSame(replacement, old.get(10, TimeUnit.SECONDS));
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertNull(previous.getCachedTableNameByIdForTest(replacement.getId()));
        Assertions.assertEquals(4, catalog.getFeMetaCacheOwner().entries().size());
    }

    @Test
    public void refreshLogRetainsScopeAndOldRecordsDefaultToMetadataOnly() {
        ExternalObjectLog old = GsonUtils.GSON.fromJson(
                "{\"catalogId\":1,\"dbName\":\"db\",\"tableName\":\"a\"}", ExternalObjectLog.class);
        Assertions.assertFalse(old.isInvalidateTableLookup());
        ExternalObjectLog full = ExternalObjectLog.createForExplicitRefreshTable(1L, "db", "a", 2L);
        Assertions.assertTrue(GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(full), ExternalObjectLog.class).isInvalidateTableLookup());
    }

    private void refreshExplicitly() {
        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                catalog.getId(), "db", "a", System.currentTimeMillis()));
    }

    private static void putTable(String name, String column) {
        Provider.TABLES.put(name, Collections.singletonList(new Column(column, PrimitiveType.INT)));
    }

    public static class Provider implements TestExternalCatalog.TestCatalogProvider {
        static final Map<String, List<Column>> TABLES = new ConcurrentHashMap<>();

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Collections.singletonMap("db", TABLES);
        }
    }

    private class CountingCatalog extends TestExternalCatalog {
        private final AtomicInteger nameLoads = new AtomicInteger();
        private boolean bypassDatabaseNames;

        CountingCatalog(long id) {
            super(id, "owner_test_" + id, "",
                    new HashMap<>(Collections.singletonMap("catalog_provider.class", Provider.class.getName())), "");
        }

        @Override
        protected boolean shouldBypassDbNameCache(SessionContext context) {
            return bypassDatabaseNames;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String dbName) {
            nameLoads.incrementAndGet();
            List<String> snapshot = new ArrayList<>(super.listTableNamesFromRemote(context, dbName));
            namesGate.pause();
            return snapshot;
        }
    }

    private class CountingDatabase extends TestExternalDatabase {
        CountingDatabase(CountingCatalog owner) {
            super(owner, Util.genIdByName(owner.getName(), "db"), "db", "db");
        }

        @Override
        public TestExternalTable buildTableInternal(String remoteName, String localName, long id,
                ExternalCatalog owner, ExternalDatabase db) {
            TestExternalTable table = new TestExternalTable(
                    id, localName, remoteName, (TestExternalCatalog) owner, this) {
                @Override
                public Optional<SchemaCacheValue> initSchema() {
                    List<Column> snapshot =
                            new ArrayList<>(((TestExternalCatalog) catalog).mockedSchema("db", getName()));
                    schemaGate.pause();
                    return Optional.of(new SchemaCacheValue(snapshot));
                }
            };
            objectsGate.pause();
            return table;
        }
    }

    private static class Gate {
        private final AtomicInteger armed = new AtomicInteger();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        void arm() {
            armed.set(1);
        }

        void awaitStarted() throws InterruptedException {
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        }

        void pause() {
            if (armed.compareAndSet(1, 0)) {
                started.countDown();
                try {
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
