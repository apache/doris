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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalDatabaseTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @Test
    public void testGetTableForReplayByIdIsCacheOnlyOnObjectMiss() throws Exception {
        InspectableCatalog catalog = new InspectableCatalog();
        InspectableDatabase db = new InspectableDatabase(catalog, 100L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(101L, "tbl_hit", "tbl_hit", catalog, db);
        db.addTableForTest(table);

        // Keep the ID map entry but clear the object entry to verify replay-by-id stays cache-only.
        extractTablesEntry(db).invalidateKey("tbl_hit");

        Assertions.assertTrue(db.getTableForReplay(101L).isEmpty());
        Assertions.assertEquals(0, db.getBuildTableCallCount());
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

    private MetaCacheEntry<String, TestExternalTable> extractTablesEntry(InspectableDatabase db) throws Exception {
        Field tablesField = ExternalDatabase.class.getDeclaredField("tables");
        tablesField.setAccessible(true);
        return (MetaCacheEntry<String, TestExternalTable>) tablesField.get(db);
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
}
