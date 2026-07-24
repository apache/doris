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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.NameCacheValue;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalDatabaseTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @Test
    public void testResetMetaCacheNamesKeepsObjectAndIdCache() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 200L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(201L, "tbl_keep", "tbl_keep", catalog, db);

        // Warm names first so resetMetaCacheNames() only clears the names entry and preserves object/id state.
        Assertions.assertTrue(db.getTableNamesWithLock().contains("tbl_base"));
        db.addTableForTest(table);
        db.resetMetaCacheNames();

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertSame(table, db.getCachedTableForTest("tbl_keep"));
        Assertions.assertEquals("tbl_keep", db.getCachedTableNameByIdForTest(201L));
        Assertions.assertEquals(table, db.getTableForReplay("tbl_keep").orElse(null));
    }

    @Test
    public void testGetTableForReplayFallsBackToCaseInsensitiveHotObjectKeyWhenNamesAreCold() {
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db_ci", "Foo");
            CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 210L, "db_ci", "db_ci");
            db.setInitializedForTest(true);
            TestExternalTable table = new TestExternalTable(211L, "Foo", "Foo", catalog, db);

            // Warm names first so resetMetaCacheNames() clears only the names snapshot.
            Assertions.assertTrue(db.getTableNamesWithLock().contains("Foo"));
            db.addTableForTest(table);
            db.resetMetaCacheNames();

            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertSame(table, db.getTableForReplay("foo").orElse(null));
            Assertions.assertNull(db.getCachedTableNamesForTest());
        } finally {
            NameMissTableCatalogProvider.reset();
        }
    }

    @Test
    public void testCaseInsensitiveTableUnregisterClearsCanonicalHotEntries() {
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db_ci", "Foo");
            CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 220L, "db_ci", "db_ci");
            db.setInitializedForTest(true);

            TestExternalTable table = db.getTableNullable("foo");
            Assertions.assertNotNull(table);
            Assertions.assertTrue(db.getCachedTableNamesForTest().containsLocalName("Foo"));
            Assertions.assertSame(table, db.getCachedTableForTest("Foo"));
            Assertions.assertEquals("Foo", db.getCachedTableNameByIdForTest(table.getId()));

            db.unregisterTable("foo");

            Assertions.assertFalse(db.getCachedTableNamesForTest().containsLocalName("Foo"));
            Assertions.assertNull(db.getCachedTableForTest("Foo"));
            Assertions.assertNull(db.getCachedTableNameByIdForTest(table.getId()));
        } finally {
            NameMissTableCatalogProvider.reset();
        }
    }

    @Test
    public void testCaseInsensitiveTableUnregisterFallsBackToHotObjectWhenNamesAreCold() {
        CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 230L, "db_ci", "db_ci");
        db.setInitializedForTest(true);

        // Seed only the object/id cache so unregister must resolve the canonical key from the hot object entry.
        TestExternalTable table = new TestExternalTable(232L, "Foo", "Foo", catalog, db);
        db.addTableForTest(table);

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertSame(table, db.getCachedTableForTest("Foo"));
        Assertions.assertEquals("Foo", db.getCachedTableNameByIdForTest(232L));

        db.unregisterTable("foo");

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("Foo"));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(232L));
    }

    @Test
    public void testCaseInsensitiveTableUnregisterClearsCanonicalColdIdMap() {
        CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 231L, "db_ci", "db_ci");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(233L, "Foo", "Foo", catalog, db);
        db.registerTable(table);

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("Foo"));
        Assertions.assertEquals("Foo", db.getCachedTableNameByIdForTest(233L));

        db.unregisterTable("foo");

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("Foo"));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(233L));
    }

    @Test
    public void testHotNamedLookupDoesNotRestoreIdAfterUnregister() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch valueObserved = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        try {
            InspectableCatalog catalog = new InspectableCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 240L, "db1", "db1");
            db.setInitializedForTest(true);
            TestExternalTable table = db.getTableNullable("tbl_base");
            Assertions.assertNotNull(table);

            MetaCacheEntry<String, TestExternalTable> objectEntry =
                    new MetaCacheEntry<String, TestExternalTable>(
                            "table_hot_lookup_race",
                            ignored -> table,
                            CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                            refreshExecutor,
                            false) {
                        @Override
                        protected void beforeCurrentValueActionForTest(String key, TestExternalTable value) {
                            valueObserved.countDown();
                            awaitLatch(releaseAction);
                        }
                    };
            objectEntry.put(table.getName(), table);
            setTablesEntryForTest(db, objectEntry);
            extractTableIdToName(db).clear();

            Future<TestExternalTable> lookup = queryExecutor.submit(() -> db.getTableNullable(table.getName()));
            Assertions.assertTrue(valueObserved.await(3L, TimeUnit.SECONDS));
            db.unregisterTable(table.getName());
            releaseAction.countDown();

            Assertions.assertSame(table, lookup.get(3L, TimeUnit.SECONDS));
            Assertions.assertNull(objectEntry.getIfPresent(table.getName()));
            Assertions.assertNull(db.getCachedTableNameByIdForTest(table.getId()));
        } finally {
            releaseAction.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testHotNamedLookupSkipsLockWhenIdAlreadyMapped() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger actionInvocations = new AtomicInteger();
        try {
            InspectableCatalog catalog = new InspectableCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 242L, "db1", "db1");
            db.setInitializedForTest(true);
            TestExternalTable table = db.getTableNullable("tbl_base");
            Assertions.assertNotNull(table);

            MetaCacheEntry<String, TestExternalTable> objectEntry =
                    new MetaCacheEntry<String, TestExternalTable>(
                            "table_skip_lock_when_mapped",
                            ignored -> table,
                            CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                            refreshExecutor,
                            false) {
                        @Override
                        protected void beforeCurrentValueActionForTest(String key, TestExternalTable value) {
                            actionInvocations.incrementAndGet();
                        }
                    };
            objectEntry.put(table.getName(), table);
            setTablesEntryForTest(db, objectEntry);
            extractTableIdToName(db).clear();

            // First lookup: id->name is empty, so the action runs (takes the publication lock) and writes the mapping.
            TestExternalTable first = db.getTableNullable(table.getName());
            Assertions.assertSame(table, first);
            Assertions.assertEquals(1, actionInvocations.get());
            Assertions.assertEquals(table.getName(), db.getCachedTableNameByIdForTest(table.getId()));

            // Second lookup: id->name is already consistent, so the action is skipped (no publication lock).
            TestExternalTable second = db.getTableNullable(table.getName());
            Assertions.assertSame(table, second);
            Assertions.assertEquals(1, actionInvocations.get());
            Assertions.assertEquals(table.getName(), db.getCachedTableNameByIdForTest(table.getId()));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRejectedNamedMissLoadDoesNotRestoreIdAfterUnregister() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            InspectableCatalog catalog = new InspectableCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 241L, "db1", "db1");
            db.setInitializedForTest(true);
            TestExternalTable table = db.getTableNullable("tbl_base");
            Assertions.assertNotNull(table);

            MetaCacheEntry<String, TestExternalTable> objectEntry = new MetaCacheEntry<>(
                    "table_miss_load_race",
                    ignored -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return table;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            setTablesEntryForTest(db, objectEntry);
            extractTableIdToName(db).clear();

            Future<TestExternalTable> lookup = queryExecutor.submit(() -> db.getTableNullable(table.getName()));
            Assertions.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            db.unregisterTable(table.getName());
            releaseLoader.countDown();

            Assertions.assertSame(table, lookup.get(3L, TimeUnit.SECONDS));
            Assertions.assertNull(objectEntry.getIfPresent(table.getName()));
            Assertions.assertNull(db.getCachedTableNameByIdForTest(table.getId()));
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testGetTableNullableUpdatesIdMapWithActualTableId() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 300L, "db1", "db1");
        db.setInitializedForTest(true);

        // Loading through the normal object path should backfill the ID map with the actual table id.
        TestExternalTable table = db.getTableNullable("tbl_base");

        Assertions.assertNotNull(table);
        Assertions.assertEquals("tbl_base", db.getCachedTableNameByIdForTest(table.getId()));
    }

    @Test
    public void testDisabledObjectCacheKeepsNamedLookupIdNavigation() {
        long originalTtl = Config.external_cache_expire_time_seconds_after_access;
        try {
            Config.external_cache_expire_time_seconds_after_access = 0L;
            InspectableCatalog catalog = new InspectableCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 310L, "db1", "db1");
            db.setInitializedForTest(true);

            TestExternalTable table = db.getTableNullable("tbl_base");

            Assertions.assertNotNull(table);
            Assertions.assertNull(db.getCachedTableForTest("tbl_base"));
            Assertions.assertEquals("tbl_base", db.getCachedTableNameByIdForTest(table.getId()));
            Assertions.assertNotNull(db.getTableNullable(table.getId()));
            Assertions.assertNull(db.getCachedTableForTest("tbl_base"));
        } finally {
            Config.external_cache_expire_time_seconds_after_access = originalTtl;
        }
    }

    @Test
    public void testDisabledObjectCacheLookupDoesNotRestoreIdAfterUnregister() throws Exception {
        long originalTtl = Config.external_cache_expire_time_seconds_after_access;
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            Config.external_cache_expire_time_seconds_after_access = 0L;
            InspectableCatalog catalog = new InspectableCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 311L, "db1", "db1");
            db.setInitializedForTest(true);
            TestExternalTable table = db.getTableNullable("tbl_base");
            Assertions.assertNotNull(table);

            MetaCacheEntry<String, TestExternalTable> disabledEntry = new MetaCacheEntry<>(
                    "table_disabled_lookup_race",
                    ignored -> {
                        loaderStarted.countDown();
                        awaitLatch(releaseLoader);
                        return table;
                    },
                    CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 10L),
                    refreshExecutor,
                    false);
            setTablesEntryForTest(db, disabledEntry);
            extractTableIdToName(db).clear();

            Future<TestExternalTable> lookup = queryExecutor.submit(() -> db.getTableNullable(table.getName()));
            Assertions.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            db.unregisterTable(table.getName());
            releaseLoader.countDown();

            Assertions.assertSame(table, lookup.get(3L, TimeUnit.SECONDS));
            Assertions.assertNull(disabledEntry.getIfPresent(table.getName()));
            Assertions.assertNull(db.getCachedTableNameByIdForTest(table.getId()));
            Assertions.assertNull(db.getCachedTableForTest(table.getName()));
        } finally {
            releaseLoader.countDown();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
            Config.external_cache_expire_time_seconds_after_access = originalTtl;
        }
    }

    @Test
    public void testGetTableNullableByIdLoadsColdObjectEntry() throws Exception {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 301L, "db1", "db1");
        db.setInitializedForTest(true);
        long tableId = Util.genIdByName(catalog.getName(), db.getFullName(), "tbl_base");
        extractTableIdToName(db).put(tableId, "tbl_base");

        Assertions.assertNull(db.getCachedTableForTest("tbl_base"));

        TestExternalTable table = db.getTableNullable(tableId);

        Assertions.assertNotNull(table);
        Assertions.assertEquals(tableId, table.getId());
        Assertions.assertEquals("tbl_base", table.getName());
        Assertions.assertSame(table, db.getCachedTableForTest("tbl_base"));
        Assertions.assertEquals(1, db.getBuildTableCallCount());
    }

    @Test
    public void testGetTableForReplayByIdReturnsEmptyWhenDatabaseIsUninitialized() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 302L, "db1", "db1");

        // Replay-by-ID must stay cache-only even before the database finishes initialization.
        Assertions.assertTrue(db.getTableForReplay(9999L).isEmpty());
        Assertions.assertTrue(db.getTableNameForReplay(9999L).isEmpty());
        Assertions.assertEquals(0, db.getBuildTableCallCount());
    }

    @Test
    public void testGetTableForReplayByIdIsCacheOnlyAcrossIdMapStates() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 303L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(304L, "tbl_replay", "tbl_replay", catalog, db);

        // Verify replay-by-ID handles ID misses, cold object entries, and hot object hits without remote loading.
        Assertions.assertTrue(db.getTableForReplay(304L).isEmpty());
        Assertions.assertEquals(0, db.getBuildTableCallCount());

        db.registerTable(table);
        Assertions.assertEquals("tbl_replay", db.getCachedTableNameByIdForTest(304L));
        Assertions.assertNull(db.getCachedTableForTest("tbl_replay"));
        Assertions.assertTrue(db.getTableForReplay(304L).isEmpty());
        Assertions.assertEquals(0, db.getBuildTableCallCount());

        db.addTableForTest(table);
        Assertions.assertSame(table, db.getTableForReplay(304L).orElse(null));
        Assertions.assertEquals(0, db.getBuildTableCallCount());
    }

    @Test
    public void testReplayRetainedNameSurvivesObjectCacheEviction() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 305L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(306L, "tbl_evicted", "tbl_evicted", catalog, db);
        db.addTableForTest(table);

        db.evictTableObjectForTest("tbl_evicted");

        Assertions.assertNull(db.getCachedTableForTest("tbl_evicted"));
        Assertions.assertEquals("tbl_evicted", db.getTableNameForReplay(306L).orElse(null));
        Assertions.assertTrue(db.getTableForReplay(306L).isEmpty());
        Assertions.assertTrue(db.getTableForReplay("tbl_evicted").isEmpty());
        Assertions.assertEquals(0, db.getBuildTableCallCount());
    }

    @Test
    public void testForceUpdateTableCachePublishesAllStates() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 400L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(401L, "tbl_new", "tbl_new", catalog, db);

        // Force-update should publish names/object/id atomically for migration helpers.
        db.forceUpdateTableCache(table);

        Assertions.assertTrue(db.getCachedTableNamesForTest().containsLocalName("tbl_new"));
        Assertions.assertSame(table, db.getCachedTableForTest("tbl_new"));
        Assertions.assertEquals("tbl_new", db.getCachedTableNameByIdForTest(401L));
    }

    @Test
    public void testForceUpdateTableCacheConflictLeavesStateUnchanged() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 500L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable original = new TestExternalTable(501L, "tbl_original", "remote_a", catalog, db);
        TestExternalTable conflicted = new TestExternalTable(502L, "tbl_conflict", "remote_a", catalog, db);

        db.forceUpdateTableCache(original);
        NameCacheValue namesBefore = db.getCachedTableNamesForTest();

        // Preserve the existing names/object/id view when the names compute detects an exact remote-name conflict.
        Assertions.assertThrows(IllegalArgumentException.class, () -> db.forceUpdateTableCache(conflicted));
        Assertions.assertSame(namesBefore, db.getCachedTableNamesForTest());
        Assertions.assertTrue(db.getCachedTableNamesForTest().containsLocalName("tbl_original"));
        Assertions.assertFalse(db.getCachedTableNamesForTest().containsLocalName("tbl_conflict"));
        Assertions.assertSame(original, db.getCachedTableForTest("tbl_original"));
        Assertions.assertNull(db.getCachedTableForTest("tbl_conflict"));
        Assertions.assertEquals("tbl_original", db.getCachedTableNameByIdForTest(501L));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(502L));
    }

    @Test
    public void testRegisterTableUsesCanonicalObjectNameForMappedNames() {
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "RemoteTable");
            MappingCatalog catalog = new MappingCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 550L, "db1", "db1");
            db.setInitializedForTest(true);

            String localTableName = ((ExternalDatabase<?>) db)
                    .canonicalLocalTableNameFromRemote("RemoteTable");
            Assertions.assertEquals("local_RemoteTable", localTableName);
            Assertions.assertTrue(db.getTableNamesWithLock().contains(localTableName));

            long tableId = Util.genIdByName(catalog.getName(), db.getFullName(), localTableName);
            TestExternalTable cachedTable = new TestExternalTable(
                    tableId, localTableName, "RemoteTable", catalog, db);
            db.addTableForTest(cachedTable);
            TestExternalTable eventTable = new TestExternalTable(
                    tableId, localTableName, "RemoteTable", catalog, db);

            db.registerTable(eventTable);

            Assertions.assertEquals("RemoteTable",
                    db.getCachedTableNamesForTest().remoteNameOfLocalName(localTableName));
            Assertions.assertSame(eventTable, db.getCachedTableForTest(localTableName));
            Assertions.assertNull(db.getCachedTableForTest("local_" + localTableName));
            Assertions.assertEquals(localTableName, db.getCachedTableNameByIdForTest(tableId));
        } finally {
            NameMissTableCatalogProvider.reset();
        }
    }

    @Test
    public void testColdTableNameEventsFenceInFlightLoads() throws Exception {
        assertColdTableNameEventFencesInFlightLoad(true);
        assertColdTableNameEventFencesInFlightLoad(false);
    }

    @Test
    public void testSystemDatabasesUseBuiltInTableNames() {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase infoSchemaDb = new InspectableDatabase(
                catalog, 600L, InfoSchemaDb.DATABASE_NAME, InfoSchemaDb.DATABASE_NAME);
        InspectableDatabase mysqlDb = new InspectableDatabase(catalog, 601L, MysqlDb.DATABASE_NAME, MysqlDb.DATABASE_NAME);
        infoSchemaDb.setInitializedForTest(true);
        mysqlDb.setInitializedForTest(true);

        // System databases should enumerate their built-in table sets without consulting the remote provider.
        Assertions.assertEquals(new HashSet<>(ExternalInfoSchemaDatabase.listTableNames()), infoSchemaDb.getTableNamesWithLock());
        Assertions.assertEquals(new HashSet<>(ExternalMysqlDatabase.listTableNames()), mysqlDb.getTableNamesWithLock());
    }

    @Test
    public void testTableNameMissRefreshDisabledSkipsHotSnapshotReload() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = false;
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "TblBase");
            NameMissTableCatalog catalog = new NameMissTableCatalog();
            NameMissDatabase db = new NameMissDatabase(catalog, 700L, "db1", "db1");
            db.setInitializedForTest(true);

            Assertions.assertNotNull(db.getTableNullable("tblbase"));
            Assertions.assertEquals(1, catalog.getListTableNamesCount());

            NameMissTableCatalogProvider.putTable("db1", "TblHot");
            Assertions.assertFalse(db.isTableExist("tblhot"));
            Assertions.assertEquals(1, catalog.getListTableNamesCount());
        } finally {
            NameMissTableCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testTableNameMissRefreshEnabledReloadsHotSnapshot() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "TblBase");
            NameMissTableCatalog catalog = new NameMissTableCatalog();
            NameMissDatabase db = new NameMissDatabase(catalog, 701L, "db1", "db1");
            db.setInitializedForTest(true);

            Assertions.assertNotNull(db.getTableNullable("tblbase"));
            Assertions.assertEquals(1, catalog.getListTableNamesCount());

            NameMissTableCatalogProvider.putTable("db1", "TblHot");
            Assertions.assertTrue(db.isTableExist("tblhot"));
            Assertions.assertEquals(2, catalog.getListTableNamesCount());
        } finally {
            NameMissTableCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testTableReplayMissSkipsNameRefresh() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "TblBase");
            NameMissTableCatalog catalog = new NameMissTableCatalog();
            NameMissDatabase db = new NameMissDatabase(catalog, 702L, "db1", "db1");
            db.setInitializedForTest(true);

            Assertions.assertTrue(db.getTableForReplay("missing").isEmpty());
            Assertions.assertEquals(0, catalog.getListTableNamesCount());
        } finally {
            NameMissTableCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testColdTableMissLoadsNamesOnlyOnceWhenRefreshEnabled() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "TblBase");
            NameMissTableCatalog catalog = new NameMissTableCatalog();
            NameMissDatabase db = new NameMissDatabase(catalog, 703L, "db1", "db1");
            db.setInitializedForTest(true);

            Assertions.assertNull(db.getTableNullable("missing"));
            Assertions.assertEquals(1, catalog.getListTableNamesCount());
        } finally {
            NameMissTableCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testBuildTableForInitColdMissLoadsNamesOnlyOnceForModeZeroAndOne() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        try {
            // Cover mode-0 and mode-1 object-loader existence checks on cold snapshot misses.
            for (int mode : new int[] {0, 1}) {
                NameMissTableCatalogProvider.reset();
                try {
                    NameMissTableCatalogProvider.putTable("db1", remoteBaseTableNameForMode(mode));
                    NameMissTableCatalog catalog = new NameMissTableCatalog(mode);
                    NameMissDatabase db = new NameMissDatabase(catalog, 704L + mode, "db1", "db1");
                    db.setInitializedForTest(true);

                    Assertions.assertNull(db.getTableNullable(missingTableLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListTableNamesCount(), "mode=" + mode);
                } finally {
                    NameMissTableCatalogProvider.reset();
                }
            }
        } finally {
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testBuildTableForInitHotMissHonorsMutableRefreshConfigForModeZeroAndOne() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        try {
            // Use the same database instance to prove the mutable config takes effect after the snapshot is already hot.
            for (int mode : new int[] {0, 1}) {
                NameMissTableCatalogProvider.reset();
                try {
                    Config.enable_external_meta_cache_name_miss_refresh = false;
                    NameMissTableCatalogProvider.putTable("db1", remoteBaseTableNameForMode(mode));
                    NameMissTableCatalog catalog = new NameMissTableCatalog(mode);
                    NameMissDatabase db = new NameMissDatabase(catalog, 710L + mode, "db1", "db1");
                    db.setInitializedForTest(true);

                    Assertions.assertNotNull(db.getTableNullable(baseTableLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListTableNamesCount(), "mode=" + mode);

                    NameMissTableCatalogProvider.putTable("db1", remoteHotTableNameForMode(mode));
                    Assertions.assertNull(db.getTableNullable(hotTableLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListTableNamesCount(), "mode=" + mode);

                    Config.enable_external_meta_cache_name_miss_refresh = true;
                    Assertions.assertNotNull(db.getTableNullable(hotTableLookupNameForMode(mode)));
                    Assertions.assertEquals(2, catalog.getListTableNamesCount(), "mode=" + mode);
                } finally {
                    NameMissTableCatalogProvider.reset();
                }
            }
        } finally {
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testTableReplayHotSnapshotMissSkipsReloadAndKeepsSnapshot() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissTableCatalogProvider.reset();
        try {
            NameMissTableCatalogProvider.putTable("db1", "TblBase");
            NameMissTableCatalog catalog = new NameMissTableCatalog();
            NameMissDatabase db = new NameMissDatabase(catalog, 720L, "db1", "db1");
            db.setInitializedForTest(true);

            // Warm the names snapshot first so replay hits the hot-snapshot negative-lookup branch.
            Assertions.assertNotNull(db.getTableNullable("tblbase"));
            NameCacheValue namesSnapshot = db.getCachedTableNamesForTest();

            Assertions.assertNotNull(namesSnapshot);
            Assertions.assertTrue(db.getTableForReplay("missing").isEmpty());
            Assertions.assertEquals(1, catalog.getListTableNamesCount());
            Assertions.assertSame(namesSnapshot, db.getCachedTableNamesForTest());
        } finally {
            NameMissTableCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    private void assertColdTableNameEventFencesInFlightLoad(boolean createEvent) throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            CountDownLatch loaderStarted = new CountDownLatch(1);
            AtomicInteger loadCount = new AtomicInteger();
            CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
            InspectableDatabase db = new InspectableDatabase(catalog, 250L, "db_ci", "db_ci");
            db.setInitializedForTest(true);
            TestExternalTable eventTable = new TestExternalTable(251L, "Foo", "Foo", catalog, db);
            if (!createEvent) {
                db.registerTable(eventTable);
            }

            NameCacheValue staleSnapshot = createEvent ? namesSnapshot("Bar") : namesSnapshot("Foo");
            NameCacheValue currentSnapshot = createEvent
                    ? namesSnapshot("Bar", "Foo") : NameCacheValue.empty();
            MetaCacheEntry<String, NameCacheValue> namesEntry = new MetaCacheEntry<>(
                    "table_names_event_test",
                    ignored -> {
                        if (loadCount.incrementAndGet() == 1) {
                            loaderStarted.countDown();
                            awaitLatch(releaseLoader);
                            return staleSnapshot;
                        }
                        return currentSnapshot;
                    },
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            setTableNamesEntryForTest(db, namesEntry);

            Future<Set<String>> staleLoad = queryExecutor.submit(db::getTableNamesWithLock);
            Assertions.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<?> event = eventExecutor.submit(() -> {
                if (createEvent) {
                    db.registerTable(eventTable);
                } else {
                    db.unregisterTable("foo");
                }
            });

            // Incremental events must not wait for the slow names loader and must not materialize a cold snapshot.
            event.get(3L, TimeUnit.SECONDS);
            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertEquals(createEvent ? "Foo" : null,
                    db.getCachedTableNameByIdForTest(eventTable.getId()));
            releaseLoader.countDown();

            Assertions.assertEquals(new HashSet<>(staleSnapshot.localNames()), staleLoad.get(3L, TimeUnit.SECONDS));
            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertEquals(new HashSet<>(currentSnapshot.localNames()), db.getTableNamesWithLock());
            Assertions.assertEquals(2, loadCount.get());
        } finally {
            releaseLoader.countDown();
            eventExecutor.shutdownNow();
            queryExecutor.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    private static NameCacheValue namesSnapshot(String... names) {
        List<Pair<String, String>> pairs = Lists.newArrayList();
        for (String name : names) {
            pairs.add(Pair.of(name, name));
        }
        return NameCacheValue.of(pairs);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private MetaCacheEntry<String, TestExternalTable> extractTablesEntry(InspectableDatabase db) throws Exception {
        Field tablesField = ExternalDatabase.class.getDeclaredField("tables");
        tablesField.setAccessible(true);
        return (MetaCacheEntry<String, TestExternalTable>) tablesField.get(db);
    }

    private void setTablesEntryForTest(InspectableDatabase db,
            MetaCacheEntry<String, TestExternalTable> tablesEntry) throws Exception {
        Field tablesField = ExternalDatabase.class.getDeclaredField("tables");
        tablesField.setAccessible(true);
        tablesField.set(db, tablesEntry);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, String> extractTableIdToName(InspectableDatabase db) throws Exception {
        Field idMapField = ExternalDatabase.class.getDeclaredField("tableIdToName");
        idMapField.setAccessible(true);
        return (Map<Long, String>) idMapField.get(db);
    }

    private void setTableNamesEntryForTest(InspectableDatabase db,
            MetaCacheEntry<String, NameCacheValue> namesEntry) throws Exception {
        Field namesField = ExternalDatabase.class.getDeclaredField("tableNames");
        namesField.setAccessible(true);
        namesField.set(db, namesEntry);
    }

    public static class DatabaseCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        static final Map<String, Map<String, List<Column>>> MOCKED_META = new HashMap<>();

        static {
            Map<String, List<Column>> db1Tables = Maps.newHashMap();
            db1Tables.put("tbl_base", Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
            db1Tables.put("tbl_other", Lists.newArrayList(new Column("k2", PrimitiveType.BIGINT)));
            MOCKED_META.put("db1", db1Tables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    public static class NameMissTableCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private static final Map<String, Map<String, List<Column>>> MOCKED_META = new HashMap<>();

        static void reset() {
            MOCKED_META.clear();
        }

        static void putTable(String remoteDbName, String remoteTableName) {
            MOCKED_META.computeIfAbsent(remoteDbName, ignored -> Maps.newHashMap())
                    .put(remoteTableName, Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    private static class InspectableCatalog extends TestExternalCatalog {
        InspectableCatalog() {
            super(1000L, "db_test_catalog", "", buildProps(), "");
        }

        private static Map<String, String> buildProps() {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", DatabaseCatalogProvider.class.getName());
            return props;
        }
    }

    private static class CaseInsensitiveCatalog extends TestExternalCatalog {
        CaseInsensitiveCatalog() {
            super(1001L, "db_case_insensitive_catalog", "", buildCaseInsensitiveProps(), "");
        }

        private static Map<String, String> buildCaseInsensitiveProps() {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", NameMissTableCatalogProvider.class.getName());
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, "2");
            return props;
        }
    }

    private static class MappingCatalog extends TestExternalCatalog {
        MappingCatalog() {
            super(1002L, "table_mapping_catalog", "", buildMappingProps(), "");
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            return "local_" + remoteTableName;
        }

        private static Map<String, String> buildMappingProps() {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", NameMissTableCatalogProvider.class.getName());
            props.put(ExternalCatalog.META_NAMES_MAPPING, "test_mapping");
            return props;
        }
    }

    private static class NameMissTableCatalog extends TestExternalCatalog {
        private final AtomicInteger listTableNamesCount = new AtomicInteger();

        NameMissTableCatalog() {
            this(2);
        }

        NameMissTableCatalog(int mode) {
            super(1100L + mode, "table_name_miss_catalog_" + mode, "", buildProps(mode), "");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            listTableNamesCount.incrementAndGet();
            return Lists.newArrayList(NameMissTableCatalogProvider.MOCKED_META.getOrDefault(dbName, Maps.newHashMap())
                    .keySet());
        }

        int getListTableNamesCount() {
            return listTableNamesCount.get();
        }

        private static Map<String, String> buildProps(int mode) {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", NameMissTableCatalogProvider.class.getName());
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, String.valueOf(mode));
            return props;
        }
    }

    private static class InspectableDatabase extends TestExternalDatabase {
        private final AtomicInteger buildTableCallCount = new AtomicInteger();

        InspectableDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }

        @Override
        public TestExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
                ExternalCatalog catalog, ExternalDatabase db) {
            buildTableCallCount.incrementAndGet();
            return super.buildTableInternal(remoteTableName, localTableName, tblId, catalog, db);
        }

        // Expose the protected migration helper so the test can assert the names/object/id publication contract.
        void forceUpdateTableCache(TestExternalTable table) {
            updateTableCache(table, table.getRemoteName(), table.getName(), true);
        }

        int getBuildTableCallCount() {
            return buildTableCallCount.get();
        }
    }

    private static class NameMissDatabase extends TestExternalDatabase {
        NameMissDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }
    }

    // Keep lookup strings explicit so mode-0 and mode-1 tests exercise the intended object-loader branch.
    private String remoteBaseTableNameForMode(int mode) {
        return mode == 0 ? "tbl_base" : "TblBase";
    }

    private String remoteHotTableNameForMode(int mode) {
        return mode == 0 ? "tbl_hot" : "TblHot";
    }

    private String baseTableLookupNameForMode(int mode) {
        return mode == 0 ? "tbl_base" : "tblbase";
    }

    private String hotTableLookupNameForMode(int mode) {
        return mode == 0 ? "tbl_hot" : "tblhot";
    }

    private String missingTableLookupNameForMode(int mode) {
        return mode == 0 ? "tbl_missing" : "tblmissing";
    }
}
