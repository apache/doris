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
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.ExternalMetaCacheRegistry;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RefreshManagerTest {
    private static final long CATALOG_ID = 10L;
    private static final long DATABASE_ID = 20L;
    private static final long TABLE_ID = 30L;
    private static final String DATABASE_NAME = "db1";
    private static final String TABLE_NAME = "tbl1";
    private static final String NEW_TABLE_NAME = "tbl2";

    private Env originalEnv;
    private TestExternalCatalog catalog;
    private CountingDatabase database;
    private RecordingExternalMetaCache engineCache;
    private RecordingConstraintManager constraintManager;

    @Before
    public void setUp() throws Exception {
        Map<String, String> properties = Collections.singletonMap(
                "catalog_provider.class", EmptyCatalogProvider.class.getName());
        catalog = new TestExternalCatalog(CATALOG_ID, "test_catalog", "", properties, "");
        catalog.setInitializedForTest(true);

        database = new CountingDatabase(catalog, DATABASE_ID, DATABASE_NAME, DATABASE_NAME);
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);

        engineCache = new RecordingExternalMetaCache();
        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalMetaCacheRegistry cacheRegistry = Deencapsulation.getField(metaCacheMgr, "cacheRegistry");
        cacheRegistry.resetForTest(Collections.singletonList(engineCache));
        constraintManager = new RecordingConstraintManager();
        originalEnv = replaceEnvSingleton(
                new TestingEnv(new TestingCatalogMgr(catalog), metaCacheMgr, constraintManager));
    }

    @After
    public void tearDown() throws Exception {
        if (originalEnv != null) {
            replaceEnvSingleton(originalEnv);
            originalEnv = null;
        }
    }

    @Test
    public void testReplayNameBasedRefreshInvalidatesColdTableByLogName() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L);

        new RefreshManager().replayRefreshTable(log);

        assertColdTableInvalidatedByName();
    }

    @Test
    public void testReplayIdOnlyRefreshInvalidatesColdTableByRetainedName() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, 123L);
        log.setDbName(null);
        log.setTableName(null);
        log.setDbId(DATABASE_ID);
        log.setTableId(TABLE_ID);

        new RefreshManager().replayRefreshTable(log);

        assertColdTableInvalidatedByName();
    }

    @Test
    public void testReplayNameBasedRenameMigratesColdLocalState() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRenameTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, NEW_TABLE_NAME);

        new RefreshManager().replayRefreshTable(log);

        assertColdRenameMigrated();
    }

    @Test
    public void testReplayIdOnlyRenameMigratesColdLocalState() {
        seedAndEvictTableObject();
        ExternalObjectLog log = ExternalObjectLog.createForRenameTable(
                CATALOG_ID, DATABASE_NAME, TABLE_NAME, NEW_TABLE_NAME);
        log.setDbName(null);
        log.setTableName(null);
        log.setDbId(DATABASE_ID);
        log.setTableId(TABLE_ID);

        new RefreshManager().replayRefreshTable(log);

        assertColdRenameMigrated();
    }

    private void seedAndEvictTableObject() {
        TestExternalTable table = new TestExternalTable(TABLE_ID, TABLE_NAME, TABLE_NAME, catalog, database);
        database.addTableForTest(table);
        database.evictTableObjectForTest(TABLE_NAME);
        Assert.assertFalse(database.getTableForReplay(TABLE_NAME).isPresent());
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
        Assert.assertEquals(1, engineCache.invalidateTableCalls.get());
        Assert.assertEquals(CATALOG_ID, engineCache.lastCatalogId);
        Assert.assertEquals(DATABASE_NAME, engineCache.lastDatabaseName);
        Assert.assertEquals(TABLE_NAME, engineCache.lastTableName);
        Assert.assertFalse(database.getTableNameForReplay(TABLE_ID).isPresent());
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

    private static Env replaceEnvSingleton(Env newInstance) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Class<?> singletonHolder = Class.forName("org.apache.doris.catalog.Env$SingletonHolder");
        Field instanceField = singletonHolder.getDeclaredField("INSTANCE");
        Method staticFieldOffset = unsafeClass.getMethod("staticFieldOffset", Field.class);
        Method staticFieldBase = unsafeClass.getMethod("staticFieldBase", Field.class);
        Method getObject = unsafeClass.getMethod("getObject", Object.class, long.class);
        Method putObject = unsafeClass.getMethod("putObject", Object.class, long.class, Object.class);
        long offset = (long) staticFieldOffset.invoke(unsafe, instanceField);
        Object base = staticFieldBase.invoke(unsafe, instanceField);
        Env old = (Env) getObject.invoke(unsafe, base, offset);
        putObject.invoke(unsafe, base, offset, newInstance);
        return old;
    }

    public static class EmptyCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Collections.emptyMap();
        }
    }

    private static class CountingDatabase extends TestExternalDatabase {
        private final AtomicInteger buildTableCalls = new AtomicInteger();

        CountingDatabase(TestExternalCatalog catalog, long id, String name, String remoteName) {
            super(catalog, id, name, remoteName);
        }

        @Override
        public TestExternalTable buildTableInternal(String remoteTableName, String localTableName, long tableId,
                org.apache.doris.datasource.ExternalCatalog externalCatalog, ExternalDatabase db) {
            buildTableCalls.incrementAndGet();
            return super.buildTableInternal(remoteTableName, localTableName, tableId, externalCatalog, db);
        }
    }

    private static class TestingCatalogMgr extends CatalogMgr {
        private final CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;

        TestingCatalogMgr(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
            this.catalog = catalog;
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(long id) {
            return id == catalog.getId() ? catalog : null;
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
        private final AtomicInteger invalidateTableCalls = new AtomicInteger();
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
