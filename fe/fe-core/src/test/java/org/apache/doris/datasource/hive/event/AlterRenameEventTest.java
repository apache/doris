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

package org.apache.doris.datasource.hive.event;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.NameCacheValue;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class AlterRenameEventTest {
    private static final long CATALOG_ID = 8100L;
    private static final String CATALOG_NAME = "rename_event_catalog";
    private static final String TABLE_DB_NAME = "db_tables";
    private static final String OLD_TABLE_NAME = "old_tbl";
    private static final String NEW_TABLE_NAME = "new_tbl";
    private static final long OLD_DB_ID = 8201L;
    private static final String OLD_DB_NAME = "db_old";
    private static final String NEW_DB_NAME = "db_new";

    private Env originalEnv;
    private RenameEventCatalog catalog;
    private RecordingExternalMetaCacheMgr metaCacheMgr;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        catalog = new RenameEventCatalog(CATALOG_ID, CATALOG_NAME);
        catalog.setInitializedForTest(true);
        metaCacheMgr = new RecordingExternalMetaCacheMgr();
        originalEnv = replaceEnvSingleton(new TestingEnv(new TestingCatalogMgr(catalog), metaCacheMgr));
    }

    @After
    public void tearDown() throws Exception {
        if (originalEnv != null) {
            replaceEnvSingleton(originalEnv);
            originalEnv = null;
        }
    }

    @Test
    public void testAlterTableRenameClearsOldStateWhenTargetCold() throws Exception {
        RenameEventDatabase db = createWarmDatabase(8200L, TABLE_DB_NAME);
        long oldTableId = Util.genIdByName(catalog.getName(), db.getFullName(), OLD_TABLE_NAME);
        HMSExternalTable oldTable = db.buildTableForInit(
                OLD_TABLE_NAME, OLD_TABLE_NAME, oldTableId, catalog, db, false);
        int tableInvalidationsBefore = metaCacheMgr.invalidateTableCalls;

        // Warm the old table's names/object/id state so the event must actively clean every local index.
        db.forceUpdateTableCache(oldTable);

        buildRenameTableEvent(TABLE_DB_NAME, OLD_TABLE_NAME, NEW_TABLE_NAME).process();

        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(OLD_TABLE_NAME));
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(NEW_TABLE_NAME));
        Assert.assertNull(db.getCachedTableForTest(OLD_TABLE_NAME));
        Assert.assertNull(db.getCachedTableForTest(NEW_TABLE_NAME));
        Assert.assertNull(db.getCachedTableNameByIdForTest(oldTableId));
        long newTableId = Util.genIdByName(catalog.getName(), db.getFullName(), NEW_TABLE_NAME);
        Assert.assertEquals(NEW_TABLE_NAME, db.getCachedTableNameByIdForTest(newTableId));
        Assert.assertEquals(tableInvalidationsBefore + 1, metaCacheMgr.invalidateTableCalls);
        Assert.assertEquals(catalog.getId(), metaCacheMgr.lastCatalogId);
        Assert.assertEquals(TABLE_DB_NAME, metaCacheMgr.lastDatabaseName);
        Assert.assertEquals(OLD_TABLE_NAME, metaCacheMgr.lastTableName);
    }

    @Test
    public void testAlterTableRenameClearsOldStateWhenTargetAlreadyHot() throws Exception {
        RenameEventDatabase db = createWarmDatabase(8202L, TABLE_DB_NAME);
        long oldTableId = Util.genIdByName(catalog.getName(), db.getFullName(), OLD_TABLE_NAME);
        long newTableId = Util.genIdByName(catalog.getName(), db.getFullName(), NEW_TABLE_NAME);
        HMSExternalTable oldTable = db.buildTableForInit(
                OLD_TABLE_NAME, OLD_TABLE_NAME, oldTableId, catalog, db, false);
        HMSExternalTable warmedTarget = db.buildTableForInit(
                NEW_TABLE_NAME, NEW_TABLE_NAME, newTableId, catalog, db, false);
        int tableInvalidationsBefore = metaCacheMgr.invalidateTableCalls;

        // Warm both names to reproduce the race where a normal query loads the target before the event arrives.
        db.forceUpdateTableCache(oldTable);
        db.forceUpdateTableCache(warmedTarget);

        buildRenameTableEvent(TABLE_DB_NAME, OLD_TABLE_NAME, NEW_TABLE_NAME).process();

        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(OLD_TABLE_NAME));
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(NEW_TABLE_NAME));
        Assert.assertNull(db.getCachedTableForTest(OLD_TABLE_NAME));
        Assert.assertNull(db.getCachedTableNameByIdForTest(oldTableId));
        Assert.assertNotNull(db.getCachedTableForTest(NEW_TABLE_NAME));
        Assert.assertEquals(NEW_TABLE_NAME, db.getCachedTableNameByIdForTest(newTableId));
        Assert.assertEquals(tableInvalidationsBefore + 1, metaCacheMgr.invalidateTableCalls);
        Assert.assertEquals(OLD_TABLE_NAME, metaCacheMgr.lastTableName);
    }

    @Test
    public void testAlterDatabaseRenameClearsOldStateWhenTargetCold() throws Exception {
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, OLD_DB_NAME);
        int dbInvalidationsBefore = metaCacheMgr.invalidateDbCalls;
        int invalidatedDbNamesBefore = metaCacheMgr.invalidatedDbNames.size();

        buildRenameDatabaseEvent(OLD_DB_NAME, NEW_DB_NAME).process();

        long newDbId = Util.genIdByName(catalog.getName(), NEW_DB_NAME);
        Assert.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName(OLD_DB_NAME));
        Assert.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName(NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseForTest(OLD_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseForTest(NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertEquals(NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
        Assert.assertTrue(metaCacheMgr.invalidateDbCalls >= dbInvalidationsBefore + 1);
        Assert.assertEquals(catalog.getId(), metaCacheMgr.lastCatalogId);
        Assert.assertTrue(metaCacheMgr.invalidatedDbNames.subList(
                invalidatedDbNamesBefore, metaCacheMgr.invalidatedDbNames.size()).contains(OLD_DB_NAME));
    }

    @Test
    public void testAlterDatabaseRenameClearsOldStateWhenTargetAlreadyHot() throws Exception {
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, OLD_DB_NAME);
        long newDbId = Util.genIdByName(CATALOG_NAME, NEW_DB_NAME);
        RenameEventDatabase warmedTarget = createWarmDatabase(newDbId, NEW_DB_NAME);
        int dbInvalidationsBefore = metaCacheMgr.invalidateDbCalls;
        int invalidatedDbNamesBefore = metaCacheMgr.invalidatedDbNames.size();

        buildRenameDatabaseEvent(OLD_DB_NAME, NEW_DB_NAME).process();

        Assert.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName(OLD_DB_NAME));
        Assert.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName(NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseForTest(OLD_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertNotNull(catalog.getCachedDatabaseForTest(NEW_DB_NAME));
        Assert.assertEquals(warmedTarget.getFullName(), catalog.getCachedDatabaseForTest(NEW_DB_NAME).getFullName());
        Assert.assertEquals(NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
        Assert.assertTrue(metaCacheMgr.invalidateDbCalls >= dbInvalidationsBefore + 1);
        Assert.assertTrue(metaCacheMgr.invalidatedDbNames.subList(
                invalidatedDbNamesBefore, metaCacheMgr.invalidatedDbNames.size()).contains(OLD_DB_NAME));
    }

    // Build a hot database entry so rename events can operate against real local cache state.
    private RenameEventDatabase createWarmDatabase(long dbId, String dbName) {
        RenameEventDatabase db = new RenameEventDatabase(catalog, dbId, dbName, dbName);
        db.setInitializedForTest(true);
        catalog.forceUpdateDatabaseCache(db);
        return db;
    }

    // Build a synthetic rename event so the test can exercise the actual process() implementation.
    private AlterTableEvent buildRenameTableEvent(String dbName, String oldTableName, String newTableName) {
        AlterTableEvent event = new AlterTableEvent(1L, CATALOG_NAME, dbName, oldTableName, true, false);
        Table before = new Table();
        before.setDbName(dbName);
        before.setTableName(oldTableName);
        Table after = new Table();
        after.setDbName(dbName);
        after.setTableName(newTableName);
        Deencapsulation.setField(event, "tableBefore", before);
        Deencapsulation.setField(event, "tableAfter", after);
        Deencapsulation.setField(event, "tblNameAfter", newTableName);
        return event;
    }

    // Build a synthetic rename event so the test can exercise the actual process() implementation.
    private AlterDatabaseEvent buildRenameDatabaseEvent(String oldDbName, String newDbName) {
        AlterDatabaseEvent event = new AlterDatabaseEvent(2L, CATALOG_NAME, oldDbName, true);
        Database before = new Database();
        before.setName(oldDbName);
        Database after = new Database();
        after.setName(newDbName);
        Deencapsulation.setField(event, "dbBefore", before);
        Deencapsulation.setField(event, "dbAfter", after);
        Deencapsulation.setField(event, "dbNameAfter", newDbName);
        return event;
    }

    // Replace the singleton so the event code sees the isolated testing catalog manager and meta cache manager.
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

    private static class TestingEnv extends Env {
        private final CatalogMgr catalogMgr;
        private final ExternalMetaCacheMgr metaCacheMgr;

        TestingEnv(CatalogMgr catalogMgr, ExternalMetaCacheMgr metaCacheMgr) {
            super(true);
            this.catalogMgr = catalogMgr;
            this.metaCacheMgr = metaCacheMgr;
        }

        @Override
        public CatalogMgr getCatalogMgr() {
            return catalogMgr;
        }

        @Override
        public ExternalMetaCacheMgr getExtMetaCacheMgr() {
            return metaCacheMgr;
        }
    }

    private static class TestingCatalogMgr extends CatalogMgr {
        private final RenameEventCatalog catalog;

        TestingCatalogMgr(RenameEventCatalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(String catalogName) {
            return catalog.getName().equals(catalogName) ? catalog : null;
        }

        @Override
        public void unregisterExternalTable(String dbName, String tableName, String catalogName, boolean ignoreIfExists)
                throws DdlException {
            RenameEventDatabase db = (RenameEventDatabase) catalog.getDbNullable(dbName);
            if (db == null) {
                if (!ignoreIfExists) {
                    throw new DdlException("Database " + dbName + " does not exist in catalog " + catalogName);
                }
                return;
            }
            db.unregisterTable(tableName);
        }

        @Override
        public void registerExternalTableFromEvent(String dbName, String tableName,
                String catalogName, long updateTime, boolean ignoreIfExists) throws DdlException {
            RenameEventDatabase db = (RenameEventDatabase) catalog.getDbNullable(dbName);
            if (db == null) {
                if (!ignoreIfExists) {
                    throw new DdlException("Database " + dbName + " does not exist in catalog " + catalogName);
                }
                return;
            }
            long tableId = Util.genIdByName(catalogName, db.getFullName(), tableName);
            HMSExternalTable table = db.buildTableForInit(tableName, tableName, tableId, catalog, db, false);
            table.setUpdateTime(updateTime);
            db.registerTable(table);
        }

        @Override
        public void unregisterExternalDatabase(String dbName, String catalogName) throws DdlException {
            catalog.unregisterDatabase(dbName);
        }

        @Override
        public void registerExternalDatabaseFromEvent(String dbName, String catalogName) throws DdlException {
            catalog.registerDatabase(Util.genIdByName(catalogName, dbName), dbName);
        }
    }

    private static class RecordingExternalMetaCacheMgr extends ExternalMetaCacheMgr {
        private int invalidateDbCalls;
        private int invalidateTableCalls;
        private long lastCatalogId;
        private String lastDatabaseName;
        private String lastTableName;
        private final List<String> invalidatedDbNames = new ArrayList<>();

        RecordingExternalMetaCacheMgr() {
            super(true);
        }

        @Override
        public void invalidateDb(long catalogId, String dbName) {
            invalidateDbCalls++;
            lastCatalogId = catalogId;
            lastDatabaseName = dbName;
            invalidatedDbNames.add(dbName);
        }

        @Override
        public void invalidateTable(long catalogId, String dbName, String tableName) {
            invalidateTableCalls++;
            lastCatalogId = catalogId;
            lastDatabaseName = dbName;
            lastTableName = tableName;
        }
    }

    private static class RenameEventCatalog extends HMSExternalCatalog {
        RenameEventCatalog(long id, String name) {
            super(id, name, null, buildCatalogProps(), "");
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
                long dbId, InitCatalogLog.Type logType, boolean checkExists) {
            String effectiveDbName = localDbName != null ? localDbName : remoteDbName;
            RenameEventDatabase db = new RenameEventDatabase(this, dbId, effectiveDbName, effectiveDbName);
            db.setInitializedForTest(true);
            return db;
        }

        // Expose the force-update helper so the test can seed hot database names/object/id state.
        void forceUpdateDatabaseCache(RenameEventDatabase db) {
            try {
                Method method = ExternalCatalog.class.getDeclaredMethod("updateDatabaseCache",
                        String.class, String.class, ExternalDatabase.class, boolean.class);
                method.setAccessible(true);
                method.invoke(this, db.getRemoteName(), db.getFullName(), db, true);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        // Expose the current hot names snapshot so the test can assert rename cleanup precisely.
        NameCacheValue getCachedDatabaseNamesForTest() {
            MetaCacheEntry<String, NameCacheValue> namesEntry = Deencapsulation.getField(this, "databaseNames");
            return namesEntry == null ? null : namesEntry.getIfPresent("");
        }

        // Expose the current hot database object cache so the test can assert rename cleanup precisely.
        ExternalDatabase<? extends ExternalTable> getCachedDatabaseForTest(String localDbName) {
            MetaCacheEntry<String, ExternalDatabase<? extends ExternalTable>> dbEntry =
                    Deencapsulation.getField(this, "databases");
            return dbEntry == null ? null : dbEntry.getIfPresent(localDbName);
        }

        // Expose the current id-to-name index so the test can assert rename cleanup precisely.
        String getCachedDatabaseNameByIdForTest(long dbId) {
            ConcurrentMap<Long, String> dbIdToName = Deencapsulation.getField(this, "dbIdToName");
            return dbIdToName.get(dbId);
        }

        private static Map<String, String> buildCatalogProps() {
            Map<String, String> props = Maps.newHashMap();
            props.put("type", "hms");
            props.put("hive.metastore.uris", "thrift://localhost:9083");
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, "0");
            return props;
        }
    }

    private static class RenameEventDatabase extends HMSExternalDatabase {
        RenameEventDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }

        // Expose the force-update helper so the test can seed hot table names/object/id state.
        void forceUpdateTableCache(HMSExternalTable table) {
            updateTableCache(table, table.getRemoteName(), table.getName(), true);
        }
    }
}
