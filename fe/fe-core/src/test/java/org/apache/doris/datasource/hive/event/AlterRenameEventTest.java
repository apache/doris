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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AlterRenameEventTest {
    private static final long CATALOG_ID = 8100L;
    private static final String CATALOG_NAME = "rename_event_catalog";
    private static final String TABLE_DB_NAME = "db_tables";
    private static final String OLD_TABLE_NAME = "old_tbl";
    private static final String NEW_TABLE_NAME = "new_tbl";
    private static final String MODED_OLD_TABLE_NAME = "OldTbl";
    private static final String MODED_NEW_TABLE_NAME = "NewTbl";
    private static final long OLD_DB_ID = 8201L;
    private static final String OLD_DB_NAME = "db_old";
    private static final String NEW_DB_NAME = "db_new";
    private static final String MODED_OLD_DB_NAME = "OldDb";
    private static final String MODED_NEW_DB_NAME = "NewDb";
    private static final String MAPPED_OLD_TABLE_NAME = "RemoteOldTbl";
    private static final String MAPPED_NEW_TABLE_NAME = "RemoteNewTbl";
    private static final String MAPPED_OLD_TABLE_ALIAS = "local_old_tbl";
    private static final String MAPPED_NEW_TABLE_ALIAS = "local_new_tbl";

    private Env originalEnv;
    private CatalogMgr catalogMgr;
    private RenameEventCatalog catalog;
    private RecordingExternalMetaCacheMgr metaCacheMgr;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        originalEnv = Env.getCurrentEnv();
        catalogMgr = new CatalogMgr();
        activateCatalog(0, 0);
    }

    @After
    public void tearDown() throws Exception {
        removeActiveCatalog();
        if (originalEnv != null) {
            replaceEnvSingleton(originalEnv);
            originalEnv = null;
        }
        metaCacheMgr = null;
        catalogMgr = null;
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
        long newDbId = Util.genIdByName(catalog.getName(), NEW_DB_NAME);
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

    @Test
    public void testAlterTableRenameModeOneCanonicalizesLowercaseKeysWhenTargetCold() throws Exception {
        activateCatalog(1, 0);
        RenameEventDatabase db = createWarmDatabase(8300L, TABLE_DB_NAME);
        assertTableRenameResult(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME, false);
        assertTableRenameCanonicalNames(db, MODED_NEW_TABLE_NAME.toLowerCase(Locale.ROOT));
        Assert.assertEquals(MODED_OLD_TABLE_NAME.toLowerCase(Locale.ROOT), metaCacheMgr.lastTableName);
    }

    @Test
    public void testAlterTableRenameModeOneCanonicalizesLowercaseKeysWhenTargetHot() throws Exception {
        activateCatalog(1, 0);
        RenameEventDatabase db = createWarmDatabase(8301L, TABLE_DB_NAME);
        assertTableRenameResult(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME, true);
        assertTableRenameCanonicalNames(db, MODED_NEW_TABLE_NAME.toLowerCase(Locale.ROOT));
        Assert.assertEquals(MODED_OLD_TABLE_NAME.toLowerCase(Locale.ROOT), metaCacheMgr.lastTableName);
    }

    @Test
    public void testAlterDatabaseRenameModeOneCanonicalizesLowercaseKeysWhenTargetCold() throws Exception {
        activateCatalog(0, 1);
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, MODED_OLD_DB_NAME);
        buildRenameDatabaseEvent(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME).process();

        String oldLocalDbName = catalog.localDatabaseNameForTest(MODED_OLD_DB_NAME);
        String newLocalDbName = catalog.localDatabaseNameForTest(MODED_NEW_DB_NAME);
        Assert.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName(oldLocalDbName));
        Assert.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName(newLocalDbName));
        Assert.assertNull(catalog.getCachedDatabaseForTest(oldLocalDbName));
        Assert.assertNull(catalog.getCachedDatabaseForTest(newLocalDbName));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertEquals(oldLocalDbName, metaCacheMgr.invalidatedDbNames.get(metaCacheMgr.invalidatedDbNames.size() - 1));
        Assert.assertEquals(1, catalog.getCachedDatabaseNamesForTest().localNames().size());
    }

    @Test
    public void testAlterDatabaseRenameModeOneCanonicalizesLowercaseKeysWhenTargetHot() throws Exception {
        activateCatalog(0, 1);
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, MODED_OLD_DB_NAME);
        String newLocalDbName = catalog.localDatabaseNameForTest(MODED_NEW_DB_NAME);
        RenameEventDatabase warmedTarget = createWarmDatabase(OLD_DB_ID + 100L, MODED_NEW_DB_NAME);

        buildRenameDatabaseEvent(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME).process();

        Assert.assertNull(catalog.getCachedDatabaseForTest(catalog.localDatabaseNameForTest(MODED_OLD_DB_NAME)));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertNotNull(catalog.getCachedDatabaseForTest(newLocalDbName));
        Assert.assertEquals(warmedTarget.getFullName(), catalog.getCachedDatabaseForTest(newLocalDbName).getFullName());
        Assert.assertEquals(1, catalog.getCachedDatabaseNamesForTest().localNames().size());
        Assert.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName(MODED_NEW_DB_NAME));
    }

    @Test
    public void testAlterTableRenameModeTwoKeepsCanonicalRemoteCaseWhenTargetCold() throws Exception {
        activateCatalog(2, 0);
        RenameEventDatabase db = createWarmDatabase(8302L, TABLE_DB_NAME);
        assertTableRenameResult(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME, false);
        assertModeTwoTableNameVisibility(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME);
        Assert.assertEquals(MODED_OLD_TABLE_NAME, metaCacheMgr.lastTableName);
    }

    @Test
    public void testAlterTableRenameModeTwoKeepsCanonicalRemoteCaseWhenTargetHot() throws Exception {
        activateCatalog(2, 0);
        RenameEventDatabase db = createWarmDatabase(8303L, TABLE_DB_NAME);
        assertTableRenameResult(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME, true);
        assertModeTwoTableNameVisibility(db, MODED_OLD_TABLE_NAME, MODED_NEW_TABLE_NAME);
        Assert.assertSame(db.getCachedTableForTest(MODED_NEW_TABLE_NAME), db.getTableForReplay("newtbl").orElse(null));
    }

    @Test
    public void testAlterDatabaseRenameModeTwoKeepsCanonicalRemoteCaseWhenTargetCold() throws Exception {
        activateCatalog(0, 2);
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, MODED_OLD_DB_NAME);

        buildRenameDatabaseEvent(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME).process();

        long newDbId = Util.genIdByName(catalog.getName(), MODED_NEW_DB_NAME);
        Assert.assertNull(catalog.getCachedDatabaseForTest(MODED_NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertEquals(MODED_NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
        assertModeTwoDatabaseNameVisibility(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME);
        Assert.assertEquals(MODED_OLD_DB_NAME, metaCacheMgr.invalidatedDbNames.get(metaCacheMgr.invalidatedDbNames.size() - 1));
    }

    @Test
    public void testAlterDatabaseRenameModeTwoKeepsCanonicalRemoteCaseWhenTargetHot() throws Exception {
        activateCatalog(0, 2);
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, MODED_OLD_DB_NAME);
        long newDbId = Util.genIdByName(catalog.getName(), MODED_NEW_DB_NAME);
        RenameEventDatabase warmedTarget = createWarmDatabase(newDbId, MODED_NEW_DB_NAME);

        buildRenameDatabaseEvent(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME).process();

        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertNotNull(catalog.getDbForReplay("newdb").orElse(null));
        Assert.assertEquals(warmedTarget.getFullName(), catalog.getDbForReplay("newdb").orElseThrow().getFullName());
        Assert.assertEquals(MODED_NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
        assertModeTwoDatabaseNameVisibility(MODED_OLD_DB_NAME, MODED_NEW_DB_NAME);
    }

    @Test
    public void testAlterTableRenameCustomNamingHookUsesMappedKeysWhenTargetCold() throws Exception {
        activateCatalog(0, 0, buildTableMappings());
        RenameEventDatabase db = createWarmDatabase(8304L, TABLE_DB_NAME);
        assertTableRenameResult(db, MAPPED_OLD_TABLE_NAME, MAPPED_NEW_TABLE_NAME, false);
        assertMappingTableRenameState(db, false);
    }

    @Test
    public void testAlterTableRenameCustomNamingHookUsesMappedKeysWhenTargetHot() throws Exception {
        activateCatalog(0, 0, buildTableMappings());
        RenameEventDatabase db = createWarmDatabase(8305L, TABLE_DB_NAME);
        assertTableRenameResult(db, MAPPED_OLD_TABLE_NAME, MAPPED_NEW_TABLE_NAME, true);
        assertMappingTableRenameState(db, true);
    }

    @Test
    public void testRenameEventsKeepColdNameEntriesColdWhileUpdatingIds() throws Exception {
        activateCatalog(0, 0);
        RenameEventDatabase tableDb = createColdDatabase(8400L, TABLE_DB_NAME);
        HMSExternalTable oldTable = createTable(tableDb, OLD_TABLE_NAME);
        tableDb.addTableForTest(oldTable);

        buildRenameTableEvent(TABLE_DB_NAME, OLD_TABLE_NAME, NEW_TABLE_NAME).process();

        long newTableId = Util.genIdByName(catalog.getName(), tableDb.getFullName(), NEW_TABLE_NAME);
        Assert.assertNull(tableDb.getCachedTableNamesForTest());
        Assert.assertNull(tableDb.getCachedTableForTest(OLD_TABLE_NAME));
        Assert.assertNull(tableDb.getCachedTableForTest(NEW_TABLE_NAME));
        Assert.assertNull(tableDb.getCachedTableNameByIdForTest(oldTable.getId()));
        Assert.assertEquals(NEW_TABLE_NAME, tableDb.getCachedTableNameByIdForTest(newTableId));

        activateCatalog(0, 0);
        RenameEventDatabase oldDb = createColdDatabase(OLD_DB_ID, OLD_DB_NAME);
        catalog.addDatabaseForTest(oldDb);

        buildRenameDatabaseEvent(OLD_DB_NAME, NEW_DB_NAME).process();

        long newDbId = Util.genIdByName(catalog.getName(), NEW_DB_NAME);
        Assert.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assert.assertNull(catalog.getCachedDatabaseForTest(OLD_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseForTest(NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertEquals(NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
    }

    @Test
    public void testUnregisterDatabaseDoesNotCallNamingHookWhenCatalogUninitialized() throws Exception {
        TrackingDatabaseNamingCatalog uninitializedCatalog = new TrackingDatabaseNamingCatalog(
                CATALOG_ID + 100L, CATALOG_NAME + "_uninitialized", 0, 0, Maps.newHashMap());
        installCatalog(uninitializedCatalog, false);

        uninitializedCatalog.unregisterDatabase(MODED_OLD_DB_NAME);

        Assert.assertEquals(0, uninitializedCatalog.getFromRemoteDatabaseNameCallCount());
        Assert.assertFalse(uninitializedCatalog.isInitialized());
        Assert.assertNull(uninitializedCatalog.getCachedDatabaseNamesForTest());
        Assert.assertNull(uninitializedCatalog.getCachedDatabaseForTest(MODED_OLD_DB_NAME));
        Assert.assertEquals(1, metaCacheMgr.invalidateDbCalls);
        Assert.assertEquals(uninitializedCatalog.getId(), metaCacheMgr.lastCatalogId);
        Assert.assertEquals(MODED_OLD_DB_NAME, metaCacheMgr.lastDatabaseName);
    }

    @Test
    public void testRenameEventsFenceInFlightColdNameLoads() throws Exception {
        assertTableRenameFencesInFlightLoad();
        assertDatabaseRenameFencesInFlightLoad();
    }

    @Test
    public void testRenameEventsAreIdempotent() throws Exception {
        activateCatalog(0, 0);
        RenameEventDatabase db = createWarmDatabase(8500L, TABLE_DB_NAME);
        HMSExternalTable oldTable = createTable(db, OLD_TABLE_NAME);
        db.forceUpdateTableCache(oldTable);
        AlterTableEvent tableEvent = buildRenameTableEvent(TABLE_DB_NAME, OLD_TABLE_NAME, NEW_TABLE_NAME);

        tableEvent.process();
        tableEvent.process();

        long newTableId = Util.genIdByName(catalog.getName(), db.getFullName(), NEW_TABLE_NAME);
        Assert.assertEquals(1, db.getCachedTableNamesForTest().localNames().size());
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(NEW_TABLE_NAME));
        Assert.assertNull(db.getCachedTableNameByIdForTest(oldTable.getId()));
        Assert.assertEquals(NEW_TABLE_NAME, db.getCachedTableNameByIdForTest(newTableId));

        activateCatalog(0, 0);
        RenameEventDatabase oldDb = createWarmDatabase(OLD_DB_ID, OLD_DB_NAME);
        AlterDatabaseEvent dbEvent = buildRenameDatabaseEvent(OLD_DB_NAME, NEW_DB_NAME);

        dbEvent.process();
        dbEvent.process();

        long newDbId = Util.genIdByName(catalog.getName(), NEW_DB_NAME);
        Assert.assertEquals(1, catalog.getCachedDatabaseNamesForTest().localNames().size());
        Assert.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName(NEW_DB_NAME));
        Assert.assertNull(catalog.getCachedDatabaseNameByIdForTest(oldDb.getId()));
        Assert.assertEquals(NEW_DB_NAME, catalog.getCachedDatabaseNameByIdForTest(newDbId));
    }

    // Install a fresh testing catalog so each scenario exercises its own lower-case and naming-hook policy.
    private void activateCatalog(int lowerCaseTableNames, int lowerCaseDatabaseNames) throws Exception {
        activateCatalog(lowerCaseTableNames, lowerCaseDatabaseNames, Maps.newHashMap());
    }

    // Install a fresh testing catalog so each scenario exercises its own lower-case and naming-hook policy.
    private void activateCatalog(int lowerCaseTableNames, int lowerCaseDatabaseNames,
            Map<String, Map<String, String>> tableMappings) throws Exception {
        RenameEventCatalog newCatalog = new RenameEventCatalog(
                CATALOG_ID + lowerCaseTableNames * 10L + lowerCaseDatabaseNames,
                CATALOG_NAME + "_" + lowerCaseTableNames + "_" + lowerCaseDatabaseNames,
                lowerCaseTableNames, lowerCaseDatabaseNames, tableMappings);
        installCatalog(newCatalog, true);
    }

    // Swap the active catalog inside the real CatalogMgr without going through addCatalog().
    private void installCatalog(RenameEventCatalog newCatalog, boolean initialized) throws Exception {
        removeActiveCatalog();
        catalog = newCatalog;
        if (initialized) {
            catalog.setInitializedForTest(true);
        }
        metaCacheMgr = new RecordingExternalMetaCacheMgr();
        Map<String, CatalogIf> nameToCatalog = Deencapsulation.getField(catalogMgr, "nameToCatalog");
        nameToCatalog.put(catalog.getName(), catalog);
        catalogMgr.getIdToCatalog().put(catalog.getId(), catalog);
        replaceEnvSingleton(new TestingEnv(catalogMgr, metaCacheMgr));
    }

    // Remove the previously injected catalog so repeated activateCatalog() calls never leak state.
    private void removeActiveCatalog() {
        if (catalog == null || catalogMgr == null) {
            return;
        }
        Map<String, CatalogIf> nameToCatalog = Deencapsulation.getField(catalogMgr, "nameToCatalog");
        nameToCatalog.remove(catalog.getName());
        catalogMgr.getIdToCatalog().remove(catalog.getId());
        catalog = null;
    }

    // Build a hot database entry so rename events can operate against real local cache state.
    private RenameEventDatabase createWarmDatabase(long dbId, String dbName) {
        return createDatabase(dbId, dbName, true);
    }

    // Build a cold database entry so rename events only publish ID state without warming the names snapshot.
    private RenameEventDatabase createColdDatabase(long dbId, String dbName) {
        return createDatabase(dbId, dbName, false);
    }

    // Build a database using the active catalog's production local-name rules.
    private RenameEventDatabase createDatabase(long dbId, String remoteDbName, boolean warmNames) {
        RenameEventDatabase db = catalog.buildDatabaseForTest(dbId, remoteDbName);
        if (warmNames) {
            catalog.forceUpdateDatabaseCache(db);
        } else {
            catalog.addDatabaseForTest(db);
        }
        return db;
    }

    // Build a table using the database's production canonical local-name rules.
    private HMSExternalTable createTable(RenameEventDatabase db, String remoteTableName) {
        String localTableName = db.localTableNameForTest(remoteTableName);
        long tableId = Util.genIdByName(catalog.getName(), db.getFullName(), localTableName);
        return db.buildTableForInit(remoteTableName, localTableName, tableId, catalog, db, false);
    }

    // Exercise the real event path and assert old/new table state under the active naming policy.
    private void assertTableRenameResult(RenameEventDatabase db, String oldRemoteTableName,
            String newRemoteTableName, boolean warmTarget) throws Exception {
        String oldLocalTableName = db.localTableNameForTest(oldRemoteTableName);
        String newLocalTableName = db.localTableNameForTest(newRemoteTableName);
        HMSExternalTable oldTable = createTable(db, oldRemoteTableName);
        db.forceUpdateTableCache(oldTable);
        if (warmTarget) {
            db.forceUpdateTableCache(createTable(db, newRemoteTableName));
        }

        buildRenameTableEvent(db.getRemoteName(), oldRemoteTableName, newRemoteTableName).process();

        long newTableId = Util.genIdByName(catalog.getName(), db.getFullName(), newLocalTableName);
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(oldLocalTableName));
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(newLocalTableName));
        Assert.assertNull(db.getCachedTableForTest(oldLocalTableName));
        if (warmTarget) {
            Assert.assertNotNull(db.getCachedTableForTest(newLocalTableName));
        } else {
            Assert.assertNull(db.getCachedTableForTest(newLocalTableName));
        }
        Assert.assertNull(db.getCachedTableNameByIdForTest(oldTable.getId()));
        Assert.assertEquals(newLocalTableName, db.getCachedTableNameByIdForTest(newTableId));
        Assert.assertEquals(catalog.localDatabaseNameForTest(db.getRemoteName()), metaCacheMgr.lastDatabaseName);
    }

    // Verify lower-case storage does not leak mixed-case duplicates after rename.
    private void assertTableRenameCanonicalNames(RenameEventDatabase db, String expectedLocalTableName) {
        Assert.assertEquals(1, db.getCachedTableNamesForTest().localNames().size());
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(expectedLocalTableName));
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(MODED_NEW_TABLE_NAME));
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(MODED_OLD_TABLE_NAME));
    }

    // Verify mode-2 keeps the remote case as the canonical cache key while preserving case-insensitive lookup.
    private void assertModeTwoTableNameVisibility(RenameEventDatabase db, String oldRemoteTableName,
            String newRemoteTableName) {
        Assert.assertEquals(newRemoteTableName,
                db.getCachedTableNamesForTest().remoteNameForCaseInsensitiveLookup(newRemoteTableName.toLowerCase(Locale.ROOT)));
        Assert.assertEquals(newRemoteTableName,
                db.getCachedTableNamesForTest().remoteNameForCaseInsensitiveLookup(newRemoteTableName.toUpperCase(Locale.ROOT)));
        Assert.assertNull(db.getCachedTableNamesForTest()
                .remoteNameForCaseInsensitiveLookup(oldRemoteTableName.toLowerCase(Locale.ROOT)));
    }

    // Verify mode-2 keeps the remote database case while preserving case-insensitive lookup.
    private void assertModeTwoDatabaseNameVisibility(String oldRemoteDbName, String newRemoteDbName) {
        Assert.assertEquals(newRemoteDbName,
                catalog.getCachedDatabaseNamesForTest().remoteNameForCaseInsensitiveLookup(newRemoteDbName.toLowerCase(Locale.ROOT)));
        Assert.assertEquals(newRemoteDbName,
                catalog.getCachedDatabaseNamesForTest().remoteNameForCaseInsensitiveLookup(newRemoteDbName.toUpperCase(Locale.ROOT)));
        Assert.assertNull(catalog.getCachedDatabaseNamesForTest()
                .remoteNameForCaseInsensitiveLookup(oldRemoteDbName.toLowerCase(Locale.ROOT)));
    }

    // Verify mapped aliases replace the raw remote table names in every local cache index.
    private void assertMappingTableRenameState(RenameEventDatabase db, boolean warmTarget) {
        long oldTableId = Util.genIdByName(catalog.getName(), db.getFullName(), MAPPED_OLD_TABLE_ALIAS);
        long newTableId = Util.genIdByName(catalog.getName(), db.getFullName(), MAPPED_NEW_TABLE_ALIAS);
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(MAPPED_OLD_TABLE_ALIAS));
        Assert.assertTrue(db.getCachedTableNamesForTest().containsLocalName(MAPPED_NEW_TABLE_ALIAS));
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(MAPPED_OLD_TABLE_NAME));
        Assert.assertFalse(db.getCachedTableNamesForTest().containsLocalName(MAPPED_NEW_TABLE_NAME));
        Assert.assertNull(db.getCachedTableForTest(MAPPED_OLD_TABLE_ALIAS));
        if (warmTarget) {
            Assert.assertNotNull(db.getCachedTableForTest(MAPPED_NEW_TABLE_ALIAS));
        } else {
            Assert.assertNull(db.getCachedTableForTest(MAPPED_NEW_TABLE_ALIAS));
        }
        Assert.assertNull(db.getCachedTableNameByIdForTest(oldTableId));
        Assert.assertEquals(MAPPED_NEW_TABLE_ALIAS, db.getCachedTableNameByIdForTest(newTableId));
        Assert.assertEquals(MAPPED_OLD_TABLE_ALIAS, metaCacheMgr.lastTableName);
    }

    // Build the fixed table alias mapping used by the custom naming-hook scenarios.
    private Map<String, Map<String, String>> buildTableMappings() {
        Map<String, Map<String, String>> mappings = Maps.newHashMap();
        Map<String, String> tableMapping = Maps.newHashMap();
        tableMapping.put(MAPPED_OLD_TABLE_NAME, MAPPED_OLD_TABLE_ALIAS);
        tableMapping.put(MAPPED_NEW_TABLE_NAME, MAPPED_NEW_TABLE_ALIAS);
        mappings.put(TABLE_DB_NAME, tableMapping);
        return mappings;
    }

    // Prove table rename events fence an in-flight stale names load and keep a cold names entry cold.
    private void assertTableRenameFencesInFlightLoad() throws Exception {
        activateCatalog(0, 0);
        RenameEventDatabase db = createColdDatabase(8600L, TABLE_DB_NAME);
        HMSExternalTable oldTable = createTable(db, OLD_TABLE_NAME);
        db.addTableForTest(oldTable);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch loaderStarted = new CountDownLatch(1);
            AtomicInteger loadCount = new AtomicInteger();
            MetaCacheEntry<String, NameCacheValue> namesEntry = new MetaCacheEntry<>(
                    "rename_event_table_names_fence",
                    ignored -> {
                        if (loadCount.incrementAndGet() == 1) {
                            loaderStarted.countDown();
                            awaitLatch(releaseLoader);
                            return namesSnapshot(Pair.of(OLD_TABLE_NAME, OLD_TABLE_NAME));
                        }
                        return namesSnapshot(Pair.of(NEW_TABLE_NAME, NEW_TABLE_NAME));
                    },
                    org.apache.doris.datasource.metacache.CacheSpec.of(
                            true, org.apache.doris.datasource.metacache.CacheSpec.CACHE_NO_TTL, 1L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            setTableNamesEntryForTest(db, namesEntry);

            Future<Set<String>> staleLoad = queryExecutor.submit(db::getTableNamesWithLock);
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<?> rename = eventExecutor.submit(
                    () -> buildRenameTableEvent(TABLE_DB_NAME, OLD_TABLE_NAME, NEW_TABLE_NAME).process());

            rename.get(3L, TimeUnit.SECONDS);
            Assert.assertNull(db.getCachedTableNamesForTest());
            releaseLoader.countDown();

            Assert.assertEquals(OLD_TABLE_NAME, staleLoad.get(3L, TimeUnit.SECONDS).iterator().next());
            Assert.assertNull(db.getCachedTableNamesForTest());
            Assert.assertTrue(db.getTableNamesWithLock().contains(NEW_TABLE_NAME));
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseLoader.countDown();
            refreshExecutor.shutdownNow();
            queryExecutor.shutdownNow();
            eventExecutor.shutdownNow();
        }
    }

    // Prove database rename events fence an in-flight stale names load and keep a cold names entry cold.
    private void assertDatabaseRenameFencesInFlightLoad() throws Exception {
        activateCatalog(0, 0);
        RenameEventDatabase oldDb = createColdDatabase(OLD_DB_ID, OLD_DB_NAME);
        catalog.addDatabaseForTest(oldDb);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch loaderStarted = new CountDownLatch(1);
            AtomicInteger loadCount = new AtomicInteger();
            MetaCacheEntry<String, NameCacheValue> namesEntry = new MetaCacheEntry<>(
                    "rename_event_db_names_fence",
                    ignored -> {
                        if (loadCount.incrementAndGet() == 1) {
                            loaderStarted.countDown();
                            awaitLatch(releaseLoader);
                            return namesSnapshot(Pair.of(OLD_DB_NAME, OLD_DB_NAME));
                        }
                        return namesSnapshot(Pair.of(NEW_DB_NAME, NEW_DB_NAME));
                    },
                    org.apache.doris.datasource.metacache.CacheSpec.of(
                            true, org.apache.doris.datasource.metacache.CacheSpec.CACHE_NO_TTL, 1L),
                    refreshExecutor,
                    false,
                    MetaCacheEntry.singleKeyStripeCount());
            setDatabaseNamesEntryForTest(catalog, namesEntry);

            Future<List<String>> staleLoad = queryExecutor.submit(catalog::getDbNames);
            Assert.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<?> rename = eventExecutor.submit(() -> buildRenameDatabaseEvent(OLD_DB_NAME, NEW_DB_NAME).process());

            rename.get(3L, TimeUnit.SECONDS);
            Assert.assertNull(catalog.getCachedDatabaseNamesForTest());
            releaseLoader.countDown();

            Assert.assertEquals(OLD_DB_NAME, staleLoad.get(3L, TimeUnit.SECONDS).get(0));
            Assert.assertNull(catalog.getCachedDatabaseNamesForTest());
            Assert.assertTrue(catalog.getDbNames().contains(NEW_DB_NAME));
            Assert.assertEquals(2, loadCount.get());
        } finally {
            releaseLoader.countDown();
            refreshExecutor.shutdownNow();
            queryExecutor.shutdownNow();
            eventExecutor.shutdownNow();
        }
    }

    // Build an immutable snapshot for tests that need deterministic names-loader results.
    @SafeVarargs
    private final NameCacheValue namesSnapshot(Pair<String, String>... names) {
        List<Pair<String, String>> pairs = new ArrayList<>();
        for (Pair<String, String> name : names) {
            pairs.add(name);
        }
        return NameCacheValue.of(pairs);
    }

    // Wait for the synthetic loader to advance so fencing tests can control publication order.
    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // Install a synthetic table names entry so the test can model a blocked stale load precisely.
    private void setTableNamesEntryForTest(RenameEventDatabase db, MetaCacheEntry<String, NameCacheValue> namesEntry)
            throws Exception {
        Field namesField = ExternalDatabase.class.getDeclaredField("tableNames");
        namesField.setAccessible(true);
        namesField.set(db, namesEntry);
    }

    // Install a synthetic database names entry so the test can model a blocked stale load precisely.
    private void setDatabaseNamesEntryForTest(RenameEventCatalog renameCatalog,
            MetaCacheEntry<String, NameCacheValue> namesEntry) throws Exception {
        Field namesField = ExternalCatalog.class.getDeclaredField("databaseNames");
        namesField.setAccessible(true);
        namesField.set(renameCatalog, namesEntry);
    }

    // Build a synthetic rename event so the test can exercise the actual process() implementation.
    private AlterTableEvent buildRenameTableEvent(String dbName, String oldTableName, String newTableName) {
        AlterTableEvent event = new AlterTableEvent(1L, catalog.getName(), dbName, oldTableName, true, false);
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
        AlterDatabaseEvent event = new AlterDatabaseEvent(2L, catalog.getName(), oldDbName, true);
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
        private final Map<String, Map<String, String>> tableMappings;

        RenameEventCatalog(long id, String name, int lowerCaseTableNames, int lowerCaseDatabaseNames,
                Map<String, Map<String, String>> tableMappings) {
            super(id, name, null, buildCatalogProps(lowerCaseTableNames, lowerCaseDatabaseNames,
                    tableMappings), "");
            this.tableMappings = Maps.newHashMap();
            for (Map.Entry<String, Map<String, String>> entry : tableMappings.entrySet()) {
                this.tableMappings.put(entry.getKey(), Maps.newHashMap(entry.getValue()));
            }
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            return tableMappings.getOrDefault(remoteDatabaseName, Maps.newHashMap())
                    .getOrDefault(remoteTableName, remoteTableName);
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
                long dbId, InitCatalogLog.Type logType, boolean checkExists) {
            String effectiveRemoteDbName = remoteDbName != null ? remoteDbName : localDbName;
            String effectiveDbName = localDbName != null ? localDbName : localDatabaseNameForTest(effectiveRemoteDbName);
            RenameEventDatabase db = new RenameEventDatabase(this, dbId, effectiveDbName, effectiveRemoteDbName);
            db.setInitializedForTest(true);
            return db;
        }

        // Build a database with the same local-name rules used by the production event path.
        RenameEventDatabase buildDatabaseForTest(long dbId, String remoteDbName) {
            return (RenameEventDatabase) buildDbForInit(remoteDbName, null, dbId, InitCatalogLog.Type.HMS, false);
        }

        // Expose the active local database key so assertions can stay aligned with production naming rules.
        String localDatabaseNameForTest(String remoteDbName) {
            String localDbName = fromRemoteDatabaseName(remoteDbName);
            if (getLowerCaseDatabaseNames() == 1) {
                return localDbName.toLowerCase(Locale.ROOT);
            }
            if (getLowerCaseDatabaseNames() == 2) {
                return remoteDbName;
            }
            return localDbName;
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

        private static Map<String, String> buildCatalogProps(int lowerCaseTableNames, int lowerCaseDatabaseNames,
                Map<String, Map<String, String>> tableMappings) {
            Map<String, String> props = Maps.newHashMap();
            props.put("type", "hms");
            props.put("hive.metastore.uris", "thrift://localhost:9083");
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, String.valueOf(lowerCaseTableNames));
            props.put(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, String.valueOf(lowerCaseDatabaseNames));
            return props;
        }
    }

    private static class TrackingDatabaseNamingCatalog extends RenameEventCatalog {
        private final AtomicInteger fromRemoteDatabaseNameCalls = new AtomicInteger();

        TrackingDatabaseNamingCatalog(long id, String name, int lowerCaseTableNames, int lowerCaseDatabaseNames,
                Map<String, Map<String, String>> tableMappings) {
            super(id, name, lowerCaseTableNames, lowerCaseDatabaseNames, tableMappings);
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            fromRemoteDatabaseNameCalls.incrementAndGet();
            throw new AssertionError("fromRemoteDatabaseName should stay unused on an uninitialized catalog");
        }

        int getFromRemoteDatabaseNameCallCount() {
            return fromRemoteDatabaseNameCalls.get();
        }
    }

    private static class RenameEventDatabase extends HMSExternalDatabase {
        RenameEventDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }

        // Expose the active local table key so assertions can stay aligned with production naming rules.
        String localTableNameForTest(String remoteTableName) {
            String localTableName = getCatalog().fromRemoteTableName(getRemoteName(), remoteTableName);
            if (getCatalog().getLowerCaseTableNames() == 1) {
                return localTableName.toLowerCase(Locale.ROOT);
            }
            if (getCatalog().getLowerCaseTableNames() == 2) {
                return remoteTableName;
            }
            return localTableName;
        }

        // Expose the force-update helper so the test can seed hot table names/object/id state.
        void forceUpdateTableCache(HMSExternalTable table) {
            updateTableCache(table, table.getRemoteName(), table.getName(), true);
        }
    }
}
