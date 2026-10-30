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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalRowCountCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.FeMetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RefreshManagerTest {
    private static final long CATALOG_ID = 10L;
    private static final String CATALOG_NAME = "test_catalog";
    private static final String DATABASE_NAME = "db1";
    private static final String TABLE_NAME = "tbl1";
    private static final String NEW_TABLE_NAME = "tbl2";
    private static final long DATABASE_ID = Util.genIdByName(CATALOG_NAME, DATABASE_NAME);
    private static final long TABLE_ID = Util.genIdByName(CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

    private MockedStatic<Env> mockedEnv;
    private TestExternalCatalog catalog;
    private CountingDatabase database;
    private RecordingExternalMetaCache engineCache;
    private ExternalMetaCacheMgr metaCacheMgr;
    private RecordingConstraintManager constraintManager;
    private EditLog editLog;
    private AtomicInteger databaseObjectLoadCalls;
    private TestingCatalogMgr testingCatalogMgr;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = Collections.singletonMap(
                "catalog_provider.class", EmptyCatalogProvider.class.getName());
        catalog = new TestExternalCatalog(CATALOG_ID, CATALOG_NAME, "", properties, "");
        catalog.setInitializedForTest(true);

        database = new CountingDatabase(catalog, DATABASE_ID, DATABASE_NAME, DATABASE_NAME);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);

        engineCache = new RecordingExternalMetaCache();
        databaseObjectLoadCalls = new AtomicInteger();
        metaCacheMgr = Mockito.spy(new ExternalMetaCacheMgr(true));
        Map<String, ExternalMetaCache> cacheTypes = Deencapsulation.getField(metaCacheMgr, "cacheTypes");
        cacheTypes.clear();
        cacheTypes.put(engineCache.engine(), engineCache);
        constraintManager = new RecordingConstraintManager();
        testingCatalogMgr = new TestingCatalogMgr(catalog);
        TestingEnv testingEnv = new TestingEnv(testingCatalogMgr, metaCacheMgr, constraintManager);
        editLog = Mockito.mock(EditLog.class);
        testingEnv.setEditLog(editLog);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(testingEnv);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
            mockedEnv = null;
        }
        MutableCatalogProvider.reset();
    }

    @Test
    public void testReplayRefreshDbReturnsWhenCatalogIsMissing() {
        testingCatalogMgr.setCatalog(null);
        Mockito.clearInvocations(metaCacheMgr);

        new RefreshManager().replayRefreshDb(
                ExternalObjectLog.createForRefreshDb(CATALOG_ID, DATABASE_NAME));

        Mockito.verifyNoInteractions(metaCacheMgr);
    }

    @Test
    public void testReplayHotRefreshDbInvalidatesMetadataThenRowCountOnce() {
        ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(CATALOG_ID, DATABASE_NAME);

        new RefreshManager().replayRefreshDb(log);

        InOrder order = Mockito.inOrder(metaCacheMgr);
        order.verify(metaCacheMgr).invalidateDbMetadataCache(CATALOG_ID, DATABASE_NAME);
        order.verify(metaCacheMgr).invalidateDbRowCountCache(CATALOG_ID, DATABASE_ID);
        Mockito.verify(metaCacheMgr, Mockito.never()).invalidateDb(
                Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    public void testReplayHotRefreshDbKeepsRowCountBarrierWhenMetadataInvalidationFails() {
        engineCache.failDbInvalidation = true;
        ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(CATALOG_ID, DATABASE_NAME);

        Assertions.assertThrows(IllegalStateException.class, () -> new RefreshManager().replayRefreshDb(log));

        InOrder order = Mockito.inOrder(metaCacheMgr);
        order.verify(metaCacheMgr).invalidateDbMetadataCache(CATALOG_ID, DATABASE_NAME);
        order.verify(metaCacheMgr).invalidateDbRowCountCache(CATALOG_ID, DATABASE_ID);
    }

    @Test
    public void testReplayNameBasedRefreshInvalidatesColdTableByLogName() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L);

        new RefreshManager().replayRefreshTable(log);

        assertColdTableInvalidatedByName();
        Mockito.verify(metaCacheMgr).invalidateTable(
                CATALOG_ID, DATABASE_ID, DATABASE_NAME, TABLE_ID, TABLE_NAME);
    }

    @Test
    public void testExplicitRefreshReloadsTableNamesAndObject() throws Exception {
        useMutableCatalog();
        TestExternalTable oldTable = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(oldTable);

        MutableCatalogProvider.putTable("new_column");
        new RefreshManager().handleRefreshTable(CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

        Assertions.assertNull(database.getCachedTableNamesForTest());
        Assertions.assertNull(database.getCachedTableForTest(TABLE_NAME));
        TestExternalTable refreshed = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotSame(oldTable, refreshed);
        ArgumentCaptor<ExternalObjectLog> log = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog).logRefreshExternalTable(log.capture());
        Assertions.assertTrue(log.getValue().isInvalidateTableLookup());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testExplicitRefreshOnColdMasterInvalidatesDroppedTableOnFollower(int mode) throws Exception {
        useMutableCatalog(mode);
        TestExternalCatalog followerCatalog = catalog;
        CountingDatabase followerDatabase = database;
        TestExternalTable staleTable = followerDatabase.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(staleTable);
        Assertions.assertNotNull(followerDatabase.getCachedTableNamesForTest());

        // Each FE owns its lookup cache; warming the follower does not warm the elected master.
        useMutableCatalog(mode);
        Assertions.assertNull(database.getCachedTableNamesForTest());
        Assertions.assertNull(database.getCachedTableForTest(TABLE_NAME));
        MutableCatalogProvider.removeTable();

        Assertions.assertDoesNotThrow(() ->
                new RefreshManager().handleRefreshTable(CATALOG_NAME, DATABASE_NAME, TABLE_NAME));

        Assertions.assertEquals(0, database.buildTableCalls.get());
        Assertions.assertNull(database.getCachedTableNamesForTest());
        ArgumentCaptor<ExternalObjectLog> logCaptor = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog).logRefreshExternalTable(logCaptor.capture());
        ExternalObjectLog log = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(logCaptor.getValue()), ExternalObjectLog.class);
        Assertions.assertTrue(log.isInvalidateTableLookup());
        Assertions.assertEquals(CATALOG_ID, log.getCatalogId());
        Assertions.assertEquals(DATABASE_NAME, log.getDbName());
        Assertions.assertEquals(TABLE_NAME, log.getTableName());

        testingCatalogMgr.setCatalog(followerCatalog);
        new RefreshManager().replayRefreshTable(log);

        Assertions.assertNull(followerDatabase.getCachedTableNamesForTest());
        Assertions.assertNull(followerDatabase.getCachedTableForTest(TABLE_NAME));
        Assertions.assertNull(followerDatabase.getCachedTableNameByIdForTest(TABLE_ID));
        Assertions.assertTrue(followerDatabase.getTableNamesWithLock().isEmpty());
        Assertions.assertNull(followerDatabase.getTableNullable(TABLE_NAME));

        MutableCatalogProvider.putTable("replacement_column");
        followerDatabase.invalidateTableLookup(TABLE_NAME);
        TestExternalTable replacement = followerDatabase.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(replacement);
        Assertions.assertNotSame(staleTable, replacement);
    }

    @Test
    public void testExplicitRefreshRejectsMissingDatabaseWithoutJournal() {
        useMutableCatalog();

        Assertions.assertThrows(DdlException.class, () ->
                new RefreshManager().handleRefreshTable(CATALOG_NAME, "missing_db", TABLE_NAME));

        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    public void testExplicitRefreshWithColdTableDoesNotLoadRemoteMetadata() throws Exception {
        PluginCatalogFixture fixture = usePluginCatalog();
        Mockito.when(fixture.connector.getMetadata(Mockito.any()))
                .thenThrow(new AssertionError("Refresh must not load remote table metadata"));

        new RefreshManager().handleRefreshTable(CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

        Assertions.assertEquals(0, fixture.database.buildTableCalls.get());
        Assertions.assertNull(fixture.database.getCachedTableNamesForTest());
        Mockito.verify(fixture.connector, Mockito.never()).getMetadata(Mockito.any());
        Mockito.verify(fixture.connector).invalidateDb(DATABASE_NAME);
        Mockito.verify(editLog).logRefreshExternalTable(Mockito.any(ExternalObjectLog.class));
    }

    @Test
    public void testExplicitRefreshAcceptsAbsentTableNameRepeatedly() throws Exception {
        useMutableCatalog();
        RefreshManager manager = new RefreshManager();

        manager.handleRefreshTable(CATALOG_NAME, DATABASE_NAME, "absent_table");
        manager.handleRefreshTable(CATALOG_NAME, DATABASE_NAME, "absent_table");

        Assertions.assertEquals(0, database.buildTableCalls.get());
        Assertions.assertNull(database.getCachedTableNamesForTest());
        ArgumentCaptor<ExternalObjectLog> logs = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog, Mockito.times(2)).logRefreshExternalTable(logs.capture());
        for (ExternalObjectLog log : logs.getAllValues()) {
            Assertions.assertEquals("absent_table", log.getTableName());
            Assertions.assertTrue(log.isInvalidateTableLookup());
        }
        Assertions.assertEquals(Collections.singleton(TABLE_NAME), database.getTableNamesWithLock());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testColdMasterRefreshClearsMixedCaseFollowerIdentity(int mode) throws Exception {
        useMutableCatalog(mode);
        MutableCatalogProvider.removeTable();
        String remoteName = "MiXeD_Table";
        MutableCatalogProvider.putTable(remoteName, "old_column");
        TestExternalCatalog followerCatalog = catalog;
        CountingDatabase followerDatabase = database;
        TestExternalTable staleTable = followerDatabase.getTableNullable("MIXED_TABLE");
        Assertions.assertNotNull(staleTable);
        String localName = staleTable.getName();

        // The master receives the SQL spelling, while the follower retains its canonical local identity.
        useMutableCatalog(mode);
        MutableCatalogProvider.removeTable();
        MutableCatalogProvider.METADATA.get(DATABASE_NAME).remove(remoteName);
        new RefreshManager().handleRefreshTable(CATALOG_NAME, DATABASE_NAME, "MIXED_TABLE");
        Assertions.assertEquals(0, database.buildTableCalls.get());
        ArgumentCaptor<ExternalObjectLog> logCaptor = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog).logRefreshExternalTable(logCaptor.capture());
        ExternalObjectLog log = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(logCaptor.getValue()), ExternalObjectLog.class);

        testingCatalogMgr.setCatalog(followerCatalog);
        new RefreshManager().replayRefreshTable(log);

        Assertions.assertNull(followerDatabase.getCachedTableNamesForTest());
        Assertions.assertNull(followerDatabase.getCachedTableForTest(localName));
        Assertions.assertNull(followerDatabase.getCachedTableNameByIdForTest(staleTable.getId()));
        Assertions.assertTrue(followerDatabase.getTableNamesWithLock().isEmpty());
        Mockito.verify(metaCacheMgr, Mockito.times(2)).invalidateDb(CATALOG_ID, DATABASE_ID, DATABASE_NAME);

        MutableCatalogProvider.putTable(remoteName, "replacement_column");
        followerDatabase.invalidateTableLookup("MIXED_TABLE");
        TestExternalTable replacement = followerDatabase.getTableNullable("MIXED_TABLE");
        Assertions.assertNotNull(replacement);
        Assertions.assertNotSame(staleTable, replacement);
        Assertions.assertEquals(localName, replacement.getName());
    }

    @Test
    public void testExplicitRefreshReplayRetainsReplacementUpdateTimeBeforeSchemaLoad() {
        useMutableCatalog();
        TestExternalTable oldTable = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(oldTable);
        long refreshTime = System.currentTimeMillis();

        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, refreshTime));

        TestExternalTable replacement = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotSame(oldTable, replacement);
        Assertions.assertTrue(replacement.getUpdateTime() >= refreshTime);
    }

    @Test
    public void testContentRefreshRetainsTableNamesAndObject() {
        useMutableCatalog();
        TestExternalTable table = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(table);

        new RefreshManager().refreshTableAfterExternalMutation(table);

        Assertions.assertNotNull(database.getCachedTableNamesForTest());
        Assertions.assertSame(table, database.getCachedTableForTest(TABLE_NAME));
        ArgumentCaptor<ExternalObjectLog> log = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog).logRefreshExternalTable(log.capture());
        Assertions.assertFalse(log.getValue().isInvalidateTableLookup());
    }

    @Test
    public void testExplicitRefreshReplayClearsNamesWhenTableObjectIsCold() {
        useMutableCatalog();
        TestExternalTable table = database.getTableNullable(TABLE_NAME);
        Assertions.assertNotNull(table);
        database.evictTableObjectForTest(TABLE_NAME);
        MutableCatalogProvider.removeTable();

        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L));

        Assertions.assertNull(database.getCachedTableNamesForTest());
        Assertions.assertTrue(database.getTableNamesWithLock().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testExplicitRefreshReplayInvalidatesDatabaseInCaseInsensitiveMode(int mode) {
        useMutableCatalog(mode);
        Assertions.assertNotNull(database.getTableNullable(TABLE_NAME));
        ExternalObjectLog log = ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L);

        new RefreshManager().replayRefreshTable(
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log), ExternalObjectLog.class));

        Assertions.assertNull(database.getCachedTableNamesForTest());
        Mockito.verify(metaCacheMgr).invalidateDb(CATALOG_ID, DATABASE_ID, DATABASE_NAME);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testExplicitRefreshReplayInvalidatesColdDatabaseInCaseInsensitiveMode(int mode) {
        useMutableCatalog(mode);
        catalog.setInitializedForTest(false);

        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L));

        Mockito.verify(metaCacheMgr).invalidateDb(CATALOG_ID, DATABASE_ID, DATABASE_NAME);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testExplicitRefreshReplayInvalidatesConnectorDatabaseInCaseInsensitiveMode(int mode) {
        PluginCatalogFixture fixture = usePluginCatalog(mode);
        ExternalTable oldTable = new ExternalTable(TABLE_ID, TABLE_NAME, TABLE_NAME,
                fixture.catalog, fixture.database, TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        String otherSpelling = TABLE_NAME.toUpperCase(java.util.Locale.ROOT);
        ExternalTable otherTable = new ExternalTable(
                Util.genIdByName(CATALOG_NAME, DATABASE_NAME, otherSpelling), otherSpelling, otherSpelling,
                fixture.catalog, fixture.database, TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        fixture.database.addTableForTest(oldTable);
        fixture.database.addTableForTest(otherTable);

        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L));

        Mockito.verify(fixture.connector).invalidateDb(DATABASE_NAME);
        Mockito.verify(fixture.connector, Mockito.never()).invalidateTable(DATABASE_NAME, TABLE_NAME);
        Mockito.verify(metaCacheMgr).invalidateDb(CATALOG_ID, DATABASE_ID, DATABASE_NAME);
        Assertions.assertFalse(fixture.database.getTableForReplay(otherSpelling).isPresent());
    }

    @Test
    public void testExplicitRefreshInLowerCaseModeClearsHistoricalRemoteSpellings() {
        PluginCatalogFixture fixture = usePluginCatalog(1);
        ExternalTable table = new ExternalTable(TABLE_ID, TABLE_NAME, "FOO",
                fixture.catalog, fixture.database, TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        fixture.database.addTableForTest(table);
        Map<String, String> connectorEntries = new HashMap<>();
        connectorEntries.put("Foo", "old incarnation");
        connectorEntries.put("FOO", "current incarnation");
        Mockito.doAnswer(inv -> {
            connectorEntries.clear();
            return null;
        }).when(fixture.connector).invalidateDb(DATABASE_NAME);
        Mockito.doAnswer(inv -> {
            connectorEntries.remove(inv.getArgument(1));
            return null;
        }).when(fixture.connector).invalidateTable(Mockito.eq(DATABASE_NAME), Mockito.anyString());

        new RefreshManager().replayRefreshTable(ExternalObjectLog.createForExplicitRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L));

        Assertions.assertTrue(connectorEntries.isEmpty());
        Mockito.verify(fixture.connector).invalidateDb(DATABASE_NAME);
        Mockito.verify(fixture.connector, Mockito.never()).invalidateTable(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(metaCacheMgr).invalidateDb(CATALOG_ID, DATABASE_ID, DATABASE_NAME);
    }

    @Test
    public void testRefreshTableRemovesRowCountLoadedDuringConnectorInvalidation() throws Exception {
        PluginCatalogFixture fixture = usePluginCatalog();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(fixture.catalog);
        Mockito.when(table.getDb()).thenReturn(fixture.database);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getDbName()).thenReturn(DATABASE_NAME);
        Mockito.when(table.getName()).thenReturn(TABLE_NAME);
        Mockito.when(table.getRemoteName()).thenReturn(TABLE_NAME);
        AtomicLong sourceRowCount = new AtomicLong(100L);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenAnswer(inv -> sourceRowCount.get());

        CountDownLatch connectorInvalidationStarted = new CountDownLatch(1);
        CountDownLatch finishConnectorInvalidation = new CountDownLatch(1);
        Mockito.doAnswer(inv -> {
            connectorInvalidationStarted.countDown();
            Assertions.assertTrue(finishConnectorInvalidation.await(3L, TimeUnit.SECONDS));
            sourceRowCount.set(200L);
            return null;
        }).when(fixture.connector).invalidateTable(DATABASE_NAME, TABLE_NAME);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedConstruction<ExternalRowCountCache.RowCountCacheLoader> mocked =
                Mockito.mockConstruction(ExternalRowCountCache.RowCountCacheLoader.class,
                        (loader, context) -> Mockito.when(loader.asyncLoad(Mockito.any(), Mockito.any()))
                                .thenAnswer(inv -> CompletableFuture.completedFuture(
                                        Optional.of(sourceRowCount.get()))))) {
            ExternalRowCountCache rowCountCache =
                    new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
            Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);

            Future<Long> loadDuringConnectorInvalidation = executor.submit(() -> {
                Assertions.assertTrue(connectorInvalidationStarted.await(3L, TimeUnit.SECONDS));
                try {
                    return rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, TABLE_ID, false);
                } finally {
                    finishConnectorInvalidation.countDown();
                }
            });
            new RefreshManager().refreshTableInternal(fixture.database, table, 123L);

            Assertions.assertEquals(100L, loadDuringConnectorInvalidation.get(3L, TimeUnit.SECONDS).longValue());
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    rowCountCache.getCachedRowCountIfPresent(CATALOG_ID, DATABASE_ID, TABLE_ID));
            Assertions.assertEquals(200L,
                    rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, TABLE_ID, false));
            Mockito.verify(metaCacheMgr).invalidateTable(
                    CATALOG_ID, DATABASE_ID, DATABASE_NAME, TABLE_ID, TABLE_NAME);
        } finally {
            finishConnectorInvalidation.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testRefreshTableKeepsLocalBarrierWhenConnectorInvalidationFails() {
        PluginCatalogFixture fixture = usePluginCatalog();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(fixture.catalog);
        Mockito.when(table.getDb()).thenReturn(fixture.database);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getDbName()).thenReturn(DATABASE_NAME);
        Mockito.when(table.getName()).thenReturn(TABLE_NAME);
        Mockito.when(table.getRemoteName()).thenReturn(TABLE_NAME);
        Mockito.doThrow(new IllegalStateException("connector invalidation failed"))
                .when(fixture.connector).invalidateTable(DATABASE_NAME, TABLE_NAME);

        Assertions.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().refreshTableInternal(fixture.database, table, 123L));

        Mockito.verify(metaCacheMgr).invalidateTable(table);
    }

    @Test
    public void testRefreshTableAfterExternalMutationLogsBeforeFallibleInvalidation() {
        ExternalCatalog externalCatalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase database = Mockito.mock(ExternalDatabase.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(externalCatalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(database.getFullName()).thenReturn(DATABASE_NAME);
        Mockito.when(table.getCatalog()).thenReturn(externalCatalog);
        Mockito.when(table.getDb()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn(TABLE_NAME);
        RefreshManager refreshManager = Mockito.spy(new RefreshManager());
        IllegalStateException refreshFailure = new IllegalStateException("cache invalidation failed");
        Mockito.doThrow(refreshFailure).when(refreshManager)
                .refreshTableInternal(Mockito.eq(database), Mockito.eq(table), Mockito.anyLong());

        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class,
                () -> refreshManager.refreshTableAfterExternalMutation(table));
        Assertions.assertSame(refreshFailure, thrown);

        InOrder order = Mockito.inOrder(editLog, refreshManager);
        ArgumentCaptor<ExternalObjectLog> logCaptor = ArgumentCaptor.forClass(ExternalObjectLog.class);
        order.verify(editLog).logRefreshExternalTable(logCaptor.capture());
        ExternalObjectLog log = logCaptor.getValue();
        Assertions.assertEquals(CATALOG_ID, log.getCatalogId());
        Assertions.assertEquals(DATABASE_NAME, log.getDbName());
        Assertions.assertEquals(TABLE_NAME, log.getTableName());
        order.verify(refreshManager).refreshTableInternal(database, table, log.getLastUpdateTime());
    }

    @Test
    public void testReplayRefreshDbKeepsLocalBarrierWhenConnectorInvalidationFails() {
        PluginCatalogFixture fixture = usePluginCatalog();
        Mockito.doThrow(new IllegalStateException("connector invalidation failed"))
                .when(fixture.connector).invalidateDb(DATABASE_NAME);

        Assertions.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().replayRefreshDb(
                        ExternalObjectLog.createForRefreshDb(CATALOG_ID, DATABASE_NAME)));

        Assertions.assertFalse(fixture.database.isInitialized());
        Mockito.verify(metaCacheMgr).invalidateDbMetadataCache(CATALOG_ID, DATABASE_NAME);
        Mockito.verify(metaCacheMgr).invalidateDbRowCountCache(CATALOG_ID, DATABASE_ID);
    }

    @Test
    public void testReplayRenameEvictsPreexistingSourceAndDestinationRowCounts() {
        long sourceTableId = Util.genIdByName(catalog.getName(), DATABASE_NAME, TABLE_NAME);
        long destinationTableId = Util.genIdByName(catalog.getName(), DATABASE_NAME, NEW_TABLE_NAME);
        try (MockedConstruction<ExternalRowCountCache.RowCountCacheLoader> mocked =
                Mockito.mockConstruction(ExternalRowCountCache.RowCountCacheLoader.class,
                        (loader, context) -> Mockito.when(loader.asyncLoad(Mockito.any(), Mockito.any()))
                                .thenAnswer(inv -> {
                                    ExternalRowCountCache.RowCountKey key = inv.getArgument(0);
                                    long rowCount = key.getTableId() == sourceTableId ? 100L : 200L;
                                    return CompletableFuture.completedFuture(Optional.of(rowCount));
                                }))) {
            ExternalRowCountCache rowCountCache =
                    new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());
            Deencapsulation.setField(metaCacheMgr, "rowCountCache", rowCountCache);
            Assertions.assertEquals(100L,
                    rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, sourceTableId, false));
            Assertions.assertEquals(200L,
                    rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, destinationTableId, false));

            database.addTableForTest(
                    new TestExternalTable(sourceTableId, TABLE_NAME, TABLE_NAME, catalog, database));
            database.addTableForTest(
                    new TestExternalTable(destinationTableId, NEW_TABLE_NAME, NEW_TABLE_NAME, catalog, database));
            Assertions.assertNotNull(database.getCachedTableForTest(NEW_TABLE_NAME));
            new RefreshManager().replayRefreshTable(ExternalObjectLog.createForRenameTable(
                    CATALOG_ID, DATABASE_NAME, TABLE_NAME, NEW_TABLE_NAME));

            Assertions.assertNull(database.getCachedTableForTest(TABLE_NAME));
            Assertions.assertNull(database.getCachedTableForTest(NEW_TABLE_NAME));
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    rowCountCache.getCachedRowCountIfPresent(CATALOG_ID, DATABASE_ID, sourceTableId));
            Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    rowCountCache.getCachedRowCountIfPresent(CATALOG_ID, DATABASE_ID, destinationTableId));
        }
    }

    @Test
    public void testReplayNameBasedRenameMigratesColdLocalState() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRenameTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, NEW_TABLE_NAME);

        new RefreshManager().replayRefreshTable(log);

        assertColdRenameMigrated();
    }

    private void seedAndEvictTableObject() {
        TestExternalTable table = new TestExternalTable(TABLE_ID, TABLE_NAME, TABLE_NAME, catalog, database);
        database.addTableForTest(table);
        database.evictTableObjectForTest(TABLE_NAME);
        Assertions.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
    }

    private void disableDatabaseObjectCacheWithTtlZero() {
        disableDatabaseObjectCacheWithTtlZero(catalog, database);
    }

    private void disableDatabaseObjectCacheWithTtlZero(
            ExternalCatalog targetCatalog, ExternalDatabase<? extends ExternalTable> targetDatabase) {
        FeMetaCacheEntry<String, ExternalDatabase<? extends ExternalTable>> disabledDatabases = new FeMetaCacheEntry<>(
                "ttl_zero_databases",
                ignored -> {
                    databaseObjectLoadCalls.incrementAndGet();
                    return targetDatabase;
                },
                CacheSpec.of(true, 0L, 10L),
                Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
                false);
        Deencapsulation.setField(targetCatalog, "databases", disabledDatabases);
        Assertions.assertFalse(targetCatalog.getDbForReplay(DATABASE_NAME).isPresent());
    }

    private Connector usePluginCatalogWithDisabledDatabaseObjectCache() {
        PluginCatalogFixture fixture = usePluginCatalog();
        disableDatabaseObjectCacheWithTtlZero(fixture.catalog, fixture.database);
        return fixture.connector;
    }

    private PluginCatalogFixture usePluginCatalog() {
        return usePluginCatalog(0);
    }

    private PluginCatalogFixture usePluginCatalog(int lowerCaseTableNames) {
        Connector connector = Mockito.mock(Connector.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "test");
        properties.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, Integer.toString(lowerCaseTableNames));
        PluginDrivenExternalCatalog pluginCatalog = new PluginDrivenExternalCatalog(
                CATALOG_ID, CATALOG_NAME, null, properties, "", connector);
        pluginCatalog.setInitializedForTest(true);
        PluginDatabase pluginDatabase =
                new PluginDatabase(pluginCatalog, DATABASE_ID, DATABASE_NAME, DATABASE_NAME);
        pluginDatabase.setInitializedForTest(true);
        pluginCatalog.addDatabaseForTest(pluginDatabase);
        testingCatalogMgr.setCatalog(pluginCatalog);
        return new PluginCatalogFixture(connector, pluginCatalog, pluginDatabase);
    }

    private void assertColdTableInvalidatedByName() {
        Assertions.assertEquals(1, engineCache.invalidateTableCalls.get());
        Assertions.assertEquals(CATALOG_ID, engineCache.lastCatalogId);
        Assertions.assertEquals(DATABASE_NAME, engineCache.lastDatabaseName);
        Assertions.assertEquals(TABLE_NAME, engineCache.lastTableName);
        Assertions.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
        Assertions.assertEquals(0, database.buildTableCalls.get());
    }

    private void assertColdRenameMigrated() {
        Assertions.assertEquals(CATALOG_ID, engineCache.lastCatalogId);
        Assertions.assertEquals(DATABASE_NAME, engineCache.lastDatabaseName);
        Assertions.assertEquals(NEW_TABLE_NAME, engineCache.lastTableName);
        Assertions.assertNull(database.getCachedTableNameByIdForTest(TABLE_ID));
        Assertions.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
        Assertions.assertNull(database.getCachedTableNamesForTest());
        Assertions.assertEquals(0, database.buildTableCalls.get());
        Assertions.assertEquals(1, constraintManager.renameTableCalls.get());
        Assertions.assertEquals("test_catalog", constraintManager.oldTableName.getCtl());
        Assertions.assertEquals(DATABASE_NAME, constraintManager.oldTableName.getDb());
        Assertions.assertEquals(TABLE_NAME, constraintManager.oldTableName.getTbl());
        Assertions.assertEquals("test_catalog", constraintManager.newTableName.getCtl());
        Assertions.assertEquals(DATABASE_NAME, constraintManager.newTableName.getDb());
        Assertions.assertEquals(NEW_TABLE_NAME, constraintManager.newTableName.getTbl());
    }

    private void useMutableCatalog() {
        useMutableCatalog(0);
    }

    private void useMutableCatalog(int lowerCaseTableNames) {
        MutableCatalogProvider.putTable("old_column");
        Map<String, String> properties = new HashMap<>();
        properties.put("catalog_provider.class", MutableCatalogProvider.class.getName());
        properties.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, Integer.toString(lowerCaseTableNames));
        catalog = new TestExternalCatalog(CATALOG_ID, CATALOG_NAME, "", properties, "");
        catalog.setInitializedForTest(true);
        database = new CountingDatabase(catalog, DATABASE_ID, DATABASE_NAME, DATABASE_NAME);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        testingCatalogMgr.setCatalog(catalog);
    }

    public static class MutableCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private static final Map<String, Map<String, List<Column>>> METADATA = new HashMap<>();

        static void putTable(String columnName) {
            putTable(TABLE_NAME, columnName);
        }

        static void putTable(String tableName, String columnName) {
            METADATA.computeIfAbsent(DATABASE_NAME, ignored -> new HashMap<>())
                    .put(tableName, Collections.singletonList(new Column(columnName, PrimitiveType.INT)));
        }

        static void removeTable() {
            METADATA.get(DATABASE_NAME).remove(TABLE_NAME);
        }

        static void reset() {
            METADATA.clear();
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return METADATA;
        }
    }

    public static class EmptyCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Collections.emptyMap();
        }
    }

    private static class CountingDatabase extends TestExternalDatabase {
        private final AtomicInteger buildTableCalls = new AtomicInteger();

        CountingDatabase(ExternalCatalog catalog, long id, String name, String remoteName) {
            super(catalog, id, name, remoteName);
        }

        @Override
        public TestExternalTable buildTableInternal(String remoteTableName, String localTableName, long tableId,
                org.apache.doris.datasource.ExternalCatalog externalCatalog, ExternalDatabase db) {
            buildTableCalls.incrementAndGet();
            return super.buildTableInternal(remoteTableName, localTableName, tableId, externalCatalog, db);
        }
    }

    private static class PluginCatalogFixture {
        private final Connector connector;
        private final PluginDrivenExternalCatalog catalog;
        private final PluginDatabase database;

        PluginCatalogFixture(
                Connector connector, PluginDrivenExternalCatalog catalog, PluginDatabase database) {
            this.connector = connector;
            this.catalog = catalog;
            this.database = database;
        }
    }

    private static class PluginDatabase extends ExternalDatabase<ExternalTable> {
        private final AtomicInteger buildTableCalls = new AtomicInteger();

        PluginDatabase(ExternalCatalog catalog, long id, String name, String remoteName) {
            super(catalog, id, name, remoteName);
        }

        @Override
        public ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tableId,
                ExternalCatalog catalog, ExternalDatabase db) {
            buildTableCalls.incrementAndGet();
            return new ExternalTable(tableId, localTableName, remoteTableName, catalog, db,
                    TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        }
    }

    private static class TestingCatalogMgr extends CatalogMgr {
        private CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;

        TestingCatalogMgr(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
            this.catalog = catalog;
        }

        void setCatalog(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
            this.catalog = catalog;
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(long id) {
            return catalog != null && id == catalog.getId() ? catalog : null;
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(String name) {
            return catalog != null && name.equals(catalog.getName()) ? catalog : null;
        }
    }

    private static class TestingEnv extends Env {
        private final CatalogMgr catalogMgr;
        private final ExternalMetaCacheMgr metaCacheMgr;
        private final ConstraintManager constraintManager;

        TestingEnv(CatalogMgr catalogMgr, ExternalMetaCacheMgr metaCacheMgr,
                ConstraintManager constraintManager) {
            super(true);
            this.catalogMgr = catalogMgr;
            this.metaCacheMgr = metaCacheMgr;
            this.constraintManager = constraintManager;
        }

        @Override
        public CatalogMgr getCatalogMgr() {
            return catalogMgr;
        }

        @Override
        public ExternalMetaCacheMgr getExtMetaCacheMgr() {
            return metaCacheMgr;
        }

        @Override
        public ConstraintManager getConstraintManager() {
            return constraintManager;
        }
    }

    private static class RecordingConstraintManager extends ConstraintManager {
        private final AtomicInteger renameTableCalls = new AtomicInteger();
        private TableNameInfo oldTableName;
        private TableNameInfo newTableName;

        @Override
        public void renameTable(TableNameInfo oldTableInfo, TableNameInfo newTableInfo) {
            oldTableName = oldTableInfo;
            newTableName = newTableInfo;
            renameTableCalls.incrementAndGet();
        }
    }

    private static class RecordingExternalMetaCache implements ExternalMetaCache {
        private final AtomicInteger invalidateDbCalls = new AtomicInteger();
        private final AtomicInteger invalidateTableCalls = new AtomicInteger();
        private boolean failDbInvalidation;
        private long lastCatalogId;
        private String lastDatabaseName;
        private String lastTableName;

        @Override
        public String engine() {
            return "default";
        }

        @Override
        public List<String> aliases() {
            return Collections.emptyList();
        }

        @Override
        public void initCatalog(long catalogId, Map<String, String> catalogProperties) {
        }

        @Override
        public <K, V> MetaCache<K, V> entry(
                long catalogId, String entryName, Class<K> keyType, Class<V> valueType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCatalogInitialized(long catalogId) {
        }

        @Override
        public boolean isCatalogInitialized(long catalogId) {
            return true;
        }

        @Override
        public void invalidateCatalog(long catalogId) {
        }

        @Override
        public void invalidateDb(long catalogId, String dbName) {
            lastCatalogId = catalogId;
            lastDatabaseName = dbName;
            invalidateDbCalls.incrementAndGet();
            if (failDbInvalidation) {
                throw new IllegalStateException("database metadata invalidation failed");
            }
        }

        @Override
        public void invalidateTable(long catalogId, String dbName, String tableName) {
            lastCatalogId = catalogId;
            lastDatabaseName = dbName;
            lastTableName = tableName;
            invalidateTableCalls.incrementAndGet();
        }

        @Override
        public void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions) {
        }

        @Override
        public Map<String, MetaCacheEntryStats> stats(long catalogId) {
            return Collections.emptyMap();
        }

        @Override
        public void close() {
        }
    }
}
