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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
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
import org.apache.doris.datasource.metacache.ExternalMetaCacheRegistry;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.persist.EditLog;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
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
    private TestingCatalogMgr testingCatalogMgr;

    @Before
    public void setUp() {
        Map<String, String> properties = Collections.singletonMap(
                "catalog_provider.class", EmptyCatalogProvider.class.getName());
        catalog = new TestExternalCatalog(CATALOG_ID, CATALOG_NAME, "", properties, "");
        catalog.setInitializedForTest(true);

        database = new CountingDatabase(catalog, DATABASE_ID, DATABASE_NAME, DATABASE_NAME);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);

        engineCache = new RecordingExternalMetaCache();
        metaCacheMgr = Mockito.spy(new ExternalMetaCacheMgr(true));
        ExternalMetaCacheRegistry cacheRegistry = Deencapsulation.getField(metaCacheMgr, "cacheRegistry");
        cacheRegistry.resetForTest(Collections.singletonList(engineCache));
        constraintManager = new RecordingConstraintManager();
        testingCatalogMgr = new TestingCatalogMgr(catalog);
        TestingEnv testingEnv = new TestingEnv(testingCatalogMgr, metaCacheMgr, constraintManager);
        editLog = Mockito.mock(EditLog.class);
        testingEnv.setEditLog(editLog);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(testingEnv);
    }

    @After
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
            mockedEnv = null;
        }
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

        Assert.assertThrows(IllegalStateException.class, () -> new RefreshManager().replayRefreshDb(log));

        InOrder order = Mockito.inOrder(metaCacheMgr);
        order.verify(metaCacheMgr).invalidateDbMetadataCache(CATALOG_ID, DATABASE_NAME);
        order.verify(metaCacheMgr).invalidateDbRowCountCache(CATALOG_ID, DATABASE_ID);
    }

    @Test
    public void testReplayNameBasedRefreshInvalidatesColdTableByLogName() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L);
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();

        new RefreshManager().replayRefreshTable(log);

        assertColdTableInvalidatedByName();
        Assert.assertNotEquals(
                constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
        Mockito.verify(metaCacheMgr).invalidateTable(
                CATALOG_ID, DATABASE_ID, DATABASE_NAME, TABLE_ID, TABLE_NAME);
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
            Assert.assertTrue(finishConnectorInvalidation.await(3L, TimeUnit.SECONDS));
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
                Assert.assertTrue(connectorInvalidationStarted.await(3L, TimeUnit.SECONDS));
                try {
                    return rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, TABLE_ID, false);
                } finally {
                    finishConnectorInvalidation.countDown();
                }
            });
            new RefreshManager().refreshTableInternal(fixture.database, table, 123L);

            Assert.assertEquals(100L, loadDuringConnectorInvalidation.get(3L, TimeUnit.SECONDS).longValue());
            Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    rowCountCache.getCachedRowCountIfPresent(CATALOG_ID, DATABASE_ID, TABLE_ID));
            Assert.assertEquals(200L,
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

        Assert.assertThrows(IllegalStateException.class,
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

        IllegalStateException thrown = Assert.assertThrows(IllegalStateException.class,
                () -> refreshManager.refreshTableAfterExternalMutation(table));
        Assert.assertSame(refreshFailure, thrown);

        InOrder order = Mockito.inOrder(editLog, refreshManager);
        ArgumentCaptor<ExternalObjectLog> logCaptor = ArgumentCaptor.forClass(ExternalObjectLog.class);
        order.verify(editLog).logRefreshExternalTable(logCaptor.capture());
        ExternalObjectLog log = logCaptor.getValue();
        Assert.assertEquals(CATALOG_ID, log.getCatalogId());
        Assert.assertEquals(DATABASE_NAME, log.getDbName());
        Assert.assertEquals(TABLE_NAME, log.getTableName());
        order.verify(refreshManager).refreshTableInternal(database, table, log.getLastUpdateTime());
    }

    @Test
    public void testReplayRefreshDbKeepsLocalBarrierWhenConnectorInvalidationFails() {
        PluginCatalogFixture fixture = usePluginCatalog();
        Mockito.doThrow(new IllegalStateException("connector invalidation failed"))
                .when(fixture.connector).invalidateDb(DATABASE_NAME);

        Assert.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().replayRefreshDb(
                        ExternalObjectLog.createForRefreshDb(CATALOG_ID, DATABASE_NAME)));

        Assert.assertFalse(fixture.database.isInitialized());
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
            Assert.assertEquals(100L,
                    rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, sourceTableId, false));
            Assert.assertEquals(200L,
                    rowCountCache.getCachedRowCount(CATALOG_ID, DATABASE_ID, destinationTableId, false));

            database.addTableForTest(
                    new TestExternalTable(sourceTableId, TABLE_NAME, TABLE_NAME, catalog, database));
            database.addTableForTest(
                    new TestExternalTable(destinationTableId, NEW_TABLE_NAME, NEW_TABLE_NAME, catalog, database));
            Assert.assertNotNull(database.getCachedTableForTest(NEW_TABLE_NAME));
            new RefreshManager().replayRefreshTable(ExternalObjectLog.createForRenameTable(
                    CATALOG_ID, DATABASE_NAME, TABLE_NAME, NEW_TABLE_NAME));

            Assert.assertNull(database.getCachedTableForTest(TABLE_NAME));
            Assert.assertNull(database.getCachedTableForTest(NEW_TABLE_NAME));
            Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                    rowCountCache.getCachedRowCountIfPresent(CATALOG_ID, DATABASE_ID, sourceTableId));
            Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
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
        Assert.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
    }

    private PluginCatalogFixture usePluginCatalog() {
        Connector connector = Mockito.mock(Connector.class);
        Map<String, String> properties = Collections.singletonMap("type", "test");
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
        Assert.assertEquals(1, engineCache.invalidateTableCalls.get());
        Assert.assertEquals(CATALOG_ID, engineCache.lastCatalogId);
        Assert.assertEquals(DATABASE_NAME, engineCache.lastDatabaseName);
        Assert.assertEquals(TABLE_NAME, engineCache.lastTableName);
        Assert.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
        Assert.assertEquals(0, database.buildTableCalls.get());
    }

    private void assertColdRenameMigrated() {
        Assert.assertEquals(CATALOG_ID, engineCache.lastCatalogId);
        Assert.assertEquals(DATABASE_NAME, engineCache.lastDatabaseName);
        Assert.assertEquals(NEW_TABLE_NAME, engineCache.lastTableName);
        Assert.assertNull(database.getCachedTableNameByIdForTest(TABLE_ID));
        Assert.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
        Assert.assertNull(database.getCachedTableNamesForTest());
        Assert.assertEquals(0, database.buildTableCalls.get());
        Assert.assertEquals(1, constraintManager.renameTableCalls.get());
        Assert.assertEquals("test_catalog", constraintManager.oldTableName.getCtl());
        Assert.assertEquals(DATABASE_NAME, constraintManager.oldTableName.getDb());
        Assert.assertEquals(TABLE_NAME, constraintManager.oldTableName.getTbl());
        Assert.assertEquals("test_catalog", constraintManager.newTableName.getCtl());
        Assert.assertEquals(DATABASE_NAME, constraintManager.newTableName.getDb());
        Assert.assertEquals(NEW_TABLE_NAME, constraintManager.newTableName.getTbl());
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
        public <K, V> MetaCacheEntry<K, V> entry(
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
