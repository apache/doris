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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.NameCacheValue;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalCatalogTest extends TestWithFeService {
    private Env env;
    private CatalogMgr mgr;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        mgr = Env.getCurrentEnv().getCatalogMgr();
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // 1. create test catalog
        String createStmt = "create catalog test1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_database_list\" = \"db1\"\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test2 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test3 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test4 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\"\n"
                        + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test5 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test6 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    @Test
    public void testExternalCatalogAutoAnalyze() throws Exception {
        HMSExternalCatalog catalog = new HMSExternalCatalog();
        Assertions.assertFalse(catalog.enableAutoAnalyze());

        HashMap<String, String> prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "false");
        catalog.modifyCatalogProps(prop);
        Assertions.assertFalse(catalog.enableAutoAnalyze());

        prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "true");
        catalog.modifyCatalogProps(prop);
        Assertions.assertTrue(catalog.enableAutoAnalyze());

        prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "TRUE");
        catalog.modifyCatalogProps(prop);
        Assertions.assertTrue(catalog.enableAutoAnalyze());
    }

    @Test
    public void testShowCreateCatalogMasksSensitiveProperties() throws Exception {
        String createStmt = "create catalog mask_iceberg_rest properties(\n"
                + "    \"type\" = \"iceberg\",\n"
                + "    \"iceberg.catalog.type\" = \"rest\",\n"
                + "    \"iceberg.rest.uri\" = \"http://localhost:8181\",\n"
                + "    \"warehouse\" = \"test_db\",\n"
                + "    \"iceberg.rest.security.type\" = \"oauth2\",\n"
                + "    \"iceberg.rest.oauth2.credential\" = \"super-secret-pat\",\n"
                + "    \"iceberg.rest.oauth2.server-uri\" = \"http://localhost:8181/v1/oauth/tokens\",\n"
                + "    \"iceberg.rest.oauth2.scope\" = \"session:role:TEST_ROLE\"\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        Assertions.assertTrue(logicalPlan instanceof CreateCatalogCommand);
        ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);

        List<List<String>> rows = mgr.showCreateCatalog("mask_iceberg_rest");
        Assertions.assertEquals(1, rows.size());
        String ddl = rows.get(0).get(1);
        Assertions.assertTrue(ddl.contains("\"iceberg.rest.oauth2.credential\" = \""
                + DatasourcePrintableMap.PASSWORD_MASK + "\""));
        Assertions.assertFalse(ddl.contains("super-secret-pat"));

        String createTokenStmt = "create catalog mask_iceberg_rest_token properties(\n"
                + "    \"type\" = \"iceberg\",\n"
                + "    \"iceberg.catalog.type\" = \"rest\",\n"
                + "    \"iceberg.rest.uri\" = \"http://localhost:8181\",\n"
                + "    \"warehouse\" = \"test_db\",\n"
                + "    \"iceberg.rest.security.type\" = \"oauth2\",\n"
                + "    \"iceberg.rest.oauth2.token\" = \"super-secret-token\"\n"
                + ");";

        logicalPlan = nereidsParser.parseSingle(createTokenStmt);
        Assertions.assertTrue(logicalPlan instanceof CreateCatalogCommand);
        ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);

        rows = mgr.showCreateCatalog("mask_iceberg_rest_token");
        Assertions.assertEquals(1, rows.size());
        ddl = rows.get(0).get(1);
        Assertions.assertTrue(ddl.contains("\"iceberg.rest.oauth2.token\" = \""
                + DatasourcePrintableMap.PASSWORD_MASK + "\""));
        Assertions.assertFalse(ddl.contains("super-secret-token"));
    }

    @Test
    public void testExternalCatalogFilteredDatabase() throws Exception {
        TestExternalCatalog ctl = (TestExternalCatalog) mgr.getCatalog("test1");
        List<String> dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(3, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db2"));

        ctl = (TestExternalCatalog) mgr.getCatalog("test2");
        // before get dbnames
        String useDb = "use test2.db3";
        StmtExecutor stmtExecutor = new StmtExecutor(rootCtx, useDb);
        stmtExecutor.execute();
        Assertions.assertTrue(rootCtx.getState().getErrorMessage().contains("Unknown database 'db3'"));

        dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(3, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db1"));

        ctl = (TestExternalCatalog) mgr.getCatalog("test3");
        dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(2, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db1"));
        Assertions.assertTrue(!dbNames.contains("db2"));

        // use non exist db
        useDb = "use test2.db3";
        stmtExecutor = new StmtExecutor(rootCtx, useDb);
        stmtExecutor.execute();
        Assertions.assertTrue(rootCtx.getState().getErrorMessage().contains("Unknown database 'db3'"));

        // use exist db
        useDb = "use test2.db2";
        stmtExecutor = new StmtExecutor(rootCtx, useDb);
        stmtExecutor.execute();
        Assertions.assertEquals(MysqlStateType.OK, rootCtx.getState().getStateType());

        ctl = (TestExternalCatalog) mgr.getCatalog("test4");
        dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(3, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db2"));

        ctl = (TestExternalCatalog) mgr.getCatalog("test5");
        dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(3, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db1"));

        ctl = (TestExternalCatalog) mgr.getCatalog("test6");
        dbNames = ctl.getDbNames();
        System.out.println(dbNames);
        Assertions.assertEquals(2, dbNames.size());
        Assertions.assertTrue(!dbNames.contains("db1"));
        Assertions.assertTrue(!dbNames.contains("db2"));

        // use non exist db
        useDb = "use test5.db3";
        stmtExecutor = new StmtExecutor(rootCtx, useDb);
        stmtExecutor.execute();
        Assertions.assertTrue(rootCtx.getState().getErrorMessage().contains("Unknown database 'db3'"));

        // use exist db
        useDb = "use test5.db2";
        stmtExecutor = new StmtExecutor(rootCtx, useDb);
        stmtExecutor.execute();
        Assertions.assertEquals(MysqlStateType.OK, rootCtx.getState().getStateType());
    }

    @Test
    public void testGetIncludeTableMap() throws Exception {
        NereidsParser nereidsParser = new NereidsParser();

        // Test 1: Empty include_table_list
        String createStmt = "create catalog test_include_table_empty properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\"\n"
                + ");";
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        TestExternalCatalog ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_empty");
        Map<String, List<String>> includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertTrue(includeTableMap.isEmpty());

        // Test 2: Single table
        createStmt = "create catalog test_include_table_single properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \"db1.tbl1\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_single");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(1, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertEquals(1, includeTableMap.get("db1").size());
        Assertions.assertEquals("tbl1", includeTableMap.get("db1").get(0));

        // Test 3: Multiple tables in same database
        createStmt = "create catalog test_include_table_same_db properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \"db1.tbl1,db1.tbl2,db1.tbl3\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_same_db");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(1, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertEquals(3, includeTableMap.get("db1").size());
        Assertions.assertTrue(includeTableMap.get("db1").contains("tbl1"));
        Assertions.assertTrue(includeTableMap.get("db1").contains("tbl2"));
        Assertions.assertTrue(includeTableMap.get("db1").contains("tbl3"));

        // Test 4: Multiple tables in different databases
        createStmt = "create catalog test_include_table_diff_db properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \"db1.tbl1,db2.tbl2,db3.tbl3\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_diff_db");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(3, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertTrue(includeTableMap.containsKey("db2"));
        Assertions.assertTrue(includeTableMap.containsKey("db3"));
        Assertions.assertEquals(1, includeTableMap.get("db1").size());
        Assertions.assertEquals(1, includeTableMap.get("db2").size());
        Assertions.assertEquals(1, includeTableMap.get("db3").size());
        Assertions.assertEquals("tbl1", includeTableMap.get("db1").get(0));
        Assertions.assertEquals("tbl2", includeTableMap.get("db2").get(0));
        Assertions.assertEquals("tbl3", includeTableMap.get("db3").get(0));

        // Test 5: Invalid format (should be ignored)
        createStmt = "create catalog test_include_table_invalid properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \"db1.tbl1,invalid_format,db2.tbl2,too.many.dots\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_invalid");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(2, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertTrue(includeTableMap.containsKey("db2"));
        Assertions.assertFalse(includeTableMap.containsKey("invalid_format"));
        Assertions.assertFalse(includeTableMap.containsKey("too"));

        // Test 6: With whitespace (should be trimmed)
        createStmt = "create catalog test_include_table_whitespace properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \" db1.tbl1 , db2.tbl2 \"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_whitespace");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(2, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertTrue(includeTableMap.containsKey("db2"));

        // Test 7: Mixed valid and invalid with multiple tables in same db
        createStmt = "create catalog test_include_table_mixed properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                + "    \"include_table_list\" = \"db1.tbl1,db1.tbl2,invalid,db2.tbl3\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        ctl = (TestExternalCatalog) mgr.getCatalog("test_include_table_mixed");
        includeTableMap = ctl.getIncludeTableMap();
        Assertions.assertEquals(2, includeTableMap.size());
        Assertions.assertTrue(includeTableMap.containsKey("db1"));
        Assertions.assertTrue(includeTableMap.containsKey("db2"));
        Assertions.assertEquals(2, includeTableMap.get("db1").size());
        Assertions.assertTrue(includeTableMap.get("db1").contains("tbl1"));
        Assertions.assertTrue(includeTableMap.get("db1").contains("tbl2"));
        Assertions.assertEquals(1, includeTableMap.get("db2").size());
        Assertions.assertTrue(includeTableMap.get("db2").contains("tbl3"));
    }

    @Test
    public void testIncrementalDatabaseRegisterKeepsEntriesColdAndUpdatesIdMap() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);

        TestExternalDatabase db = new TestExternalDatabase(catalog, 100L, "db_new", "db_new");
        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("db_new"));
        Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(100L));

        catalog.simulateIncrementalRegisterDatabase(db);

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("db_new"));
        Assertions.assertEquals("db_new", catalog.getCachedDatabaseNameByIdForTest(100L));
        Assertions.assertTrue(catalog.getDbForReplay("db_new").isEmpty());
    }

    @Test
    public void testIncrementalDatabaseUnregisterClearsColdIdMap() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);

        TestExternalDatabase db = new TestExternalDatabase(catalog, 101L, "db_drop", "db_drop");
        catalog.simulateIncrementalRegisterDatabase(db);
        Assertions.assertEquals("db_drop", catalog.getCachedDatabaseNameByIdForTest(101L));

        catalog.unregisterDatabase("db_drop");

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("db_drop"));
        Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(101L));
    }

    @Test
    public void testCaseInsensitiveDatabaseUnregisterClearsCanonicalHotEntries() {
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("MixedDb");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            ExternalDatabase<? extends ExternalTable> db = catalog.getDbNullable("mixeddb");
            Assertions.assertNotNull(db);
            Assertions.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName("MixedDb"));
            Assertions.assertSame(db, catalog.getCachedDatabaseForTest("MixedDb"));
            Assertions.assertEquals("MixedDb", catalog.getCachedDatabaseNameByIdForTest(db.getId()));

            catalog.unregisterDatabase("mixeddb");

            Assertions.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName("MixedDb"));
            Assertions.assertNull(catalog.getCachedDatabaseForTest("MixedDb"));
            Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(db.getId()));
        } finally {
            NameMissCatalogProvider.reset();
        }
    }

    @Test
    public void testLowerCaseDatabaseUnregisterUsesLowerCaseInvalidationKey() {
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("MixedDb");
            NameMissCatalog catalog = new NameMissCatalog(1);
            catalog.setInitializedForTest(true);

            // Load the lower-case local key first so unregister must clean names/object/id through that key.
            ExternalDatabase<? extends ExternalTable> db = catalog.getDbNullable("mixeddb");
            Assertions.assertNotNull(db);
            Assertions.assertTrue(catalog.getCachedDatabaseNamesForTest().containsLocalName("mixeddb"));
            Assertions.assertSame(db, catalog.getCachedDatabaseForTest("mixeddb"));
            Assertions.assertEquals("mixeddb", catalog.getCachedDatabaseNameByIdForTest(db.getId()));

            catalog.unregisterDatabase("MixedDb");

            Assertions.assertFalse(catalog.getCachedDatabaseNamesForTest().containsLocalName("mixeddb"));
            Assertions.assertNull(catalog.getCachedDatabaseForTest("mixeddb"));
            Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(db.getId()));
        } finally {
            NameMissCatalogProvider.reset();
        }
    }

    @Test
    public void testCaseInsensitiveDatabaseUnregisterFallsBackToHotObjectWhenNamesAreCold() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog(2);
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 103L, "MixedDb", "MixedDb");

        // Seed only the object/id cache so unregister must resolve the canonical key from the hot object entry.
        catalog.addDatabaseForTest(db);

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertSame(db, catalog.getCachedDatabaseForTest("MixedDb"));
        Assertions.assertEquals("MixedDb", catalog.getCachedDatabaseNameByIdForTest(103L));

        catalog.unregisterDatabase("mixeddb");

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("MixedDb"));
        Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(103L));
    }

    @Test
    public void testCaseInsensitiveDatabaseUnregisterClearsCanonicalColdIdMap() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog(2);
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 102L, "MixedDb", "MixedDb");
        catalog.simulateIncrementalRegisterDatabase(db);

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("MixedDb"));
        Assertions.assertEquals("MixedDb", catalog.getCachedDatabaseNameByIdForTest(102L));

        catalog.unregisterDatabase("mixeddb");

        Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        Assertions.assertNull(catalog.getCachedDatabaseForTest("MixedDb"));
        Assertions.assertNull(catalog.getCachedDatabaseNameByIdForTest(102L));
    }

    @Test
    public void testGetDbNullableByIdLoadsColdObjectEntry() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);
        long dbId = Util.genIdByName(catalog.getName(), "db_by_id");
        TestExternalDatabase eventDb = new TestExternalDatabase(catalog, dbId, "db_by_id", "db_by_id");
        catalog.simulateIncrementalRegisterDatabase(eventDb);

        Assertions.assertNull(catalog.getCachedDatabaseForTest("db_by_id"));

        ExternalDatabase<? extends ExternalTable> loadedDb = catalog.getDbNullable(dbId);

        Assertions.assertNotNull(loadedDb);
        Assertions.assertEquals(dbId, loadedDb.getId());
        Assertions.assertSame(loadedDb, catalog.getCachedDatabaseForTest("db_by_id"));
    }

    @Test
    public void testGetDbForReplayByIdReturnsEmptyWhenCatalogIsUninitialized() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();

        // Replay-by-ID must stay cache-only even before the catalog finishes initialization.
        Assertions.assertTrue(catalog.getDbForReplay(9999L).isEmpty());
        Assertions.assertEquals(0, catalog.getBuildDatabaseCallCount());
    }

    @Test
    public void testGetDbForReplayByIdIsCacheOnlyAcrossIdMapStates() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 404L, "db_replay", "db_replay");

        // Verify replay-by-ID handles ID misses, cold object entries, and hot object hits without remote loading.
        Assertions.assertTrue(catalog.getDbForReplay(404L).isEmpty());
        Assertions.assertEquals(0, catalog.getBuildDatabaseCallCount());

        catalog.simulateIncrementalRegisterDatabase(db);
        Assertions.assertEquals("db_replay", catalog.getCachedDatabaseNameByIdForTest(404L));
        Assertions.assertNull(catalog.getCachedDatabaseForTest("db_replay"));
        Assertions.assertTrue(catalog.getDbForReplay(404L).isEmpty());
        Assertions.assertEquals(0, catalog.getBuildDatabaseCallCount());

        catalog.addDatabaseForTest(db);
        Assertions.assertSame(db, catalog.getDbForReplay(404L).orElse(null));
        Assertions.assertEquals(0, catalog.getBuildDatabaseCallCount());
    }

    @Test
    public void testIncrementalDatabaseRegisterReplacesExistingIdMapping() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);

        // The event object's actual ID must replace any stale navigation entry for that ID.
        catalog.seedDatabaseIdNameForTest(110L, "stale_db");
        TestExternalDatabase db = new TestExternalDatabase(catalog, 110L, "db_hot", "db_hot");
        catalog.simulateIncrementalRegisterDatabase(db);

        Assertions.assertEquals("db_hot", catalog.getCachedDatabaseNameByIdForTest(110L));
    }

    @Test
    public void testColdDatabaseNameEventsFenceInFlightLoads() throws Exception {
        assertColdDatabaseNameEventFencesInFlightLoad(true);
        assertColdDatabaseNameEventFencesInFlightLoad(false);
    }

    @Test
    public void testIgnoredExternalTableDropCleansColdModeTwoEventState() throws Exception {
        assertIgnoredExternalTableDropCleansColdEventState(
                new EventHmsCatalog(2200L, "event_hms_mode_two", 2, false), "Foo", "foo", "Foo");
    }

    @Test
    public void testIgnoredExternalTableDropCleansColdMappedEventState() throws Exception {
        assertIgnoredExternalTableDropCleansColdEventState(
                new EventHmsCatalog(2201L, "event_hms_mapped", 0, true),
                "remote_table", "remote_table", "local_remote_table");
    }

    @Test
    public void testExternalTableCreateEventReplacesHotObjectWithoutLoadThrough() throws Exception {
        EventHmsCatalog catalog = new EventHmsCatalog(2202L, "event_hms_hot_object", 0, false);
        EventHmsDatabase db = new EventHmsDatabase(catalog, 221L, "db1", "db1");
        catalog.setDatabase(db);
        catalog.setInitializedForTest(true);
        db.setInitializedForTest(true);

        long tableId = Util.genIdByName(catalog.getName(), db.getFullName(), "tbl1");
        HMSExternalTable oldTable = db.buildTableForInit("tbl1", "tbl1", tableId, catalog, db, false);
        oldTable.setUpdateTime(1L);
        db.addTableForTest(oldTable);

        @SuppressWarnings("unchecked")
        Map<String, CatalogIf> nameToCatalog = Deencapsulation.getField(mgr, "nameToCatalog");
        nameToCatalog.put(catalog.getName(), catalog);
        mgr.getIdToCatalog().put(catalog.getId(), catalog);
        try {
            mgr.registerExternalTableFromEvent("db1", "tbl1", catalog.getName(), 123L, true);

            HMSExternalTable eventTable = db.getCachedTableForTest("tbl1");
            Assertions.assertNotNull(eventTable);
            Assertions.assertNotSame(oldTable, eventTable);
            Assertions.assertEquals(123L, eventTable.getUpdateTime());
            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertEquals("tbl1", db.getCachedTableNameByIdForTest(tableId));
            Assertions.assertEquals(0, db.getTableLookupCount());
        } finally {
            nameToCatalog.remove(catalog.getName());
            mgr.getIdToCatalog().remove(catalog.getId());
        }
    }

    private void assertIgnoredExternalTableDropCleansColdEventState(EventHmsCatalog catalog,
            String remoteTableName, String dropTableName, String localTableName) throws Exception {
        EventHmsDatabase db = new EventHmsDatabase(catalog, 220L, "db_ci", "db_ci");
        catalog.setDatabase(db);
        catalog.setInitializedForTest(true);
        db.setInitializedForTest(true);

        @SuppressWarnings("unchecked")
        Map<String, CatalogIf> nameToCatalog = Deencapsulation.getField(mgr, "nameToCatalog");
        nameToCatalog.put(catalog.getName(), catalog);
        mgr.getIdToCatalog().put(catalog.getId(), catalog);
        try {
            mgr.registerExternalTableFromEvent("db_ci", remoteTableName, catalog.getName(), 1L, true);
            long tableId = Util.genIdByName(catalog.getName(), db.getFullName(), localTableName);

            Assertions.assertEquals(0, db.getTableLookupCount());
            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertNull(db.getCachedTableForTest(localTableName));
            Assertions.assertEquals(localTableName, db.getCachedTableNameByIdForTest(tableId));

            // The remote table has already disappeared when DROP_TABLE is delivered. The ignored event path must
            // still resolve the canonical local key without a load-through lookup and remove the cold ID mapping.
            mgr.unregisterExternalTable("db_ci", dropTableName, catalog.getName(), true);

            Assertions.assertEquals(0, db.getTableLookupCount());
            Assertions.assertNull(db.getCachedTableNamesForTest());
            Assertions.assertNull(db.getCachedTableForTest(localTableName));
            Assertions.assertNull(db.getCachedTableNameByIdForTest(tableId));
        } finally {
            nameToCatalog.remove(catalog.getName());
            mgr.getIdToCatalog().remove(catalog.getId());
        }
    }

    @Test
    public void testIncrementalTableRegisterKeepsEntriesColdAndUpdatesIdMap() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);

        IncrementalUpdateDatabase db = new IncrementalUpdateDatabase(catalog, 200L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(300L, "tbl_new", "tbl_new", catalog, db);

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("tbl_new"));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(300L));

        db.registerTable(table);

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("tbl_new"));
        Assertions.assertEquals("tbl_new", db.getCachedTableNameByIdForTest(300L));
        Assertions.assertTrue(db.getTableForReplay("tbl_new").isEmpty());
    }

    @Test
    public void testIncrementalTableUnregisterClearsColdIdMap() {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);

        IncrementalUpdateDatabase db = new IncrementalUpdateDatabase(catalog, 201L, "db1", "db1");
        db.setInitializedForTest(true);
        TestExternalTable table = new TestExternalTable(301L, "tbl_drop", "tbl_drop", catalog, db);
        db.registerTable(table);
        Assertions.assertEquals("tbl_drop", db.getCachedTableNameByIdForTest(301L));

        db.unregisterTable("tbl_drop");

        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("tbl_drop"));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(301L));
    }

    @Test
    public void testMetaCacheEntryStripeCountByEntryType() throws Exception {
        IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
        catalog.setInitializedForTest(true);
        IncrementalUpdateDatabase db = new IncrementalUpdateDatabase(catalog, 200L, "db1", "db1");
        db.setInitializedForTest(true);

        // Keep single-key names entries minimal and let object entries share the configured default stripe count.
        Assertions.assertEquals(1, extractStripeCount(catalog.databaseNames));
        Assertions.assertEquals(Config.external_meta_cache_object_entry_lock_stripes, extractStripeCount(catalog.databases));
        Assertions.assertEquals(1, extractStripeCount(extractMetaCacheEntry(db, "tableNames")));
        Assertions.assertEquals(Config.external_meta_cache_object_entry_lock_stripes,
                extractStripeCount(extractMetaCacheEntry(db, "tables")));
    }

    @Test
    public void testCaseInsensitiveIsTableExistUsesRemoteNameLookup() {
        // Mode 2 should resolve the remote table name from the names snapshot before probing the catalog.
        rootCtx.setThreadLocalInfo();
        CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 400L, "db_ci", "db_ci");
        db.setInitializedForTest(true);

        Assertions.assertTrue(db.getTableNamesWithLock().contains("Foo"));
        Assertions.assertTrue(db.isTableExist("foo"));
        Assertions.assertFalse(db.isTableExist("missing"));
    }

    @Test
    public void testGetTableForReplayPrefersExactObjectCacheHit() {
        CaseInsensitiveCatalog catalog = new CaseInsensitiveCatalog();
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 401L, "db_ci", "db_ci");
        db.setInitializedForTest(true);

        // Warm and then invalidate only the names snapshot so replay must fall back to the object cache.
        Assertions.assertTrue(db.getTableNamesWithLock().contains("Foo"));
        TestExternalTable table = new TestExternalTable(402L, "Foo", "Foo", catalog, db);
        db.addTableForTest(table);
        db.resetMetaCacheNames();

        Assertions.assertTrue(db.getTableForReplay("Foo").isPresent());
        Assertions.assertEquals(table, db.getTableForReplay("Foo").orElse(null));
    }

    @Test
    public void testGetDbForReplayFallsBackToCaseInsensitiveHotObjectKeyWhenNamesAreCold() {
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            // Warm names first so resetMetaCacheNames() clears only the names snapshot.
            Assertions.assertNotNull(catalog.getDbNullable("dbbase"));
            TestExternalDatabase db = new TestExternalDatabase(catalog, 403L, "DbKeep", "DbKeep");
            catalog.addDatabaseForTest(db);
            catalog.resetMetaCacheNames();

            Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
            Assertions.assertSame(db, catalog.getDbForReplay("dbkeep").orElse(null));
            Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
        } finally {
            NameMissCatalogProvider.reset();
        }
    }

    @Test
    public void testDatabaseNameMissRefreshDisabledSkipsHotSnapshotReload() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = false;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            Assertions.assertNotNull(catalog.getDbNullable("dbbase"));
            Assertions.assertEquals(1, catalog.getListDatabaseNamesCount());

            NameMissCatalogProvider.putDatabase("DbHot");
            Assertions.assertNull(catalog.getDbNullable("dbhot"));
            Assertions.assertEquals(1, catalog.getListDatabaseNamesCount());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testDatabaseNameMissRefreshEnabledReloadsHotSnapshot() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            Assertions.assertNotNull(catalog.getDbNullable("dbbase"));
            Assertions.assertEquals(1, catalog.getListDatabaseNamesCount());

            NameMissCatalogProvider.putDatabase("DbHot");
            Assertions.assertNotNull(catalog.getDbNullable("dbhot"));
            Assertions.assertEquals(2, catalog.getListDatabaseNamesCount());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testDatabaseReplayMissSkipsNameRefresh() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            Assertions.assertTrue(catalog.getDbForReplay("missing").isEmpty());
            Assertions.assertEquals(0, catalog.getListDatabaseNamesCount());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testColdDatabaseMissLoadsNamesOnlyOnceWhenRefreshEnabled() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            Assertions.assertNull(catalog.getDbNullable("missing"));
            Assertions.assertEquals(1, catalog.getListDatabaseNamesCount());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testBuildDbForInitColdMissLoadsNamesOnlyOnceForModeZeroAndOne() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        try {
            // Cover mode-0 and mode-1 object-loader existence checks on cold snapshot misses.
            for (int mode : new int[] {0, 1}) {
                NameMissCatalogProvider.reset();
                try {
                    NameMissCatalogProvider.putDatabase(remoteBaseDbNameForMode(mode));
                    NameMissCatalog catalog = new NameMissCatalog(mode);
                    catalog.setInitializedForTest(true);

                    Assertions.assertNull(catalog.getDbNullable(missingDbLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListDatabaseNamesCount(), "mode=" + mode);
                } finally {
                    NameMissCatalogProvider.reset();
                }
            }
        } finally {
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testBuildDbForInitResolvesRemoteNameInLowerCaseModeOne() {
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("MixedDb");
            NameMissCatalog catalog = new NameMissCatalog(1);
            catalog.setInitializedForTest(true);

            ExternalDatabase<? extends ExternalTable> db = catalog.getDbNullable("mixeddb");

            Assertions.assertNotNull(db);
            Assertions.assertEquals("mixeddb", db.getFullName());
            Assertions.assertEquals("MixedDb", db.getRemoteName());
        } finally {
            NameMissCatalogProvider.reset();
        }
    }

    @Test
    public void testBuildDbForInitHotMissHonorsMutableRefreshConfigForModeZeroAndOne() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        try {
            // Use the same catalog instance to prove the mutable config takes effect after the snapshot is already hot.
            for (int mode : new int[] {0, 1}) {
                NameMissCatalogProvider.reset();
                try {
                    Config.enable_external_meta_cache_name_miss_refresh = false;
                    NameMissCatalogProvider.putDatabase(remoteBaseDbNameForMode(mode));
                    NameMissCatalog catalog = new NameMissCatalog(mode);
                    catalog.setInitializedForTest(true);

                    Assertions.assertNotNull(catalog.getDbNullable(baseDbLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListDatabaseNamesCount(), "mode=" + mode);

                    NameMissCatalogProvider.putDatabase(remoteHotDbNameForMode(mode));
                    Assertions.assertNull(catalog.getDbNullable(hotDbLookupNameForMode(mode)));
                    Assertions.assertEquals(1, catalog.getListDatabaseNamesCount(), "mode=" + mode);

                    Config.enable_external_meta_cache_name_miss_refresh = true;
                    Assertions.assertNotNull(catalog.getDbNullable(hotDbLookupNameForMode(mode)));
                    Assertions.assertEquals(2, catalog.getListDatabaseNamesCount(), "mode=" + mode);
                } finally {
                    NameMissCatalogProvider.reset();
                }
            }
        } finally {
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testRepeatedMissingDatabaseQueriesReloadNamesWhenEnabled() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            Assertions.assertNotNull(catalog.getDbNullable("dbbase"));
            Assertions.assertNull(catalog.getDbNullable("missing"));
            Assertions.assertNull(catalog.getDbNullable("missing"));
            Assertions.assertEquals(3, catalog.getListDatabaseNamesCount());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    @Test
    public void testDatabaseReplayHotSnapshotMissSkipsReloadAndKeepsSnapshot() {
        boolean original = Config.enable_external_meta_cache_name_miss_refresh;
        Config.enable_external_meta_cache_name_miss_refresh = true;
        NameMissCatalogProvider.reset();
        try {
            NameMissCatalogProvider.putDatabase("DbBase");
            NameMissCatalog catalog = new NameMissCatalog();
            catalog.setInitializedForTest(true);

            // Warm the names snapshot first so replay hits the hot-snapshot negative-lookup branch.
            Assertions.assertNotNull(catalog.getDbNullable("dbbase"));
            NameCacheValue namesSnapshot = catalog.getCachedDatabaseNamesForTest();

            Assertions.assertNotNull(namesSnapshot);
            Assertions.assertTrue(catalog.getDbForReplay("missing").isEmpty());
            Assertions.assertEquals(1, catalog.getListDatabaseNamesCount());
            Assertions.assertSame(namesSnapshot, catalog.getCachedDatabaseNamesForTest());
        } finally {
            NameMissCatalogProvider.reset();
            Config.enable_external_meta_cache_name_miss_refresh = original;
        }
    }

    private void assertColdDatabaseNameEventFencesInFlightLoad(boolean createEvent) throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            CountDownLatch loaderStarted = new CountDownLatch(1);
            AtomicInteger loadCount = new AtomicInteger();
            IncrementalUpdateCatalog catalog = new IncrementalUpdateCatalog();
            catalog.setInitializedForTest(true);
            TestExternalDatabase eventDb = new TestExternalDatabase(
                    catalog, 120L, createEvent ? "db_create" : "db_drop", createEvent ? "db_create" : "db_drop");
            if (!createEvent) {
                catalog.simulateIncrementalRegisterDatabase(eventDb);
            }

            NameCacheValue staleSnapshot = createEvent
                    ? namesSnapshot("db_base") : namesSnapshot("db_drop");
            NameCacheValue currentSnapshot = createEvent
                    ? namesSnapshot("db_base", "db_create") : NameCacheValue.empty();
            MetaCacheEntry<String, NameCacheValue> namesEntry = new MetaCacheEntry<>(
                    "database_names_event_test",
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
            catalog.setDatabaseNamesEntryForTest(namesEntry);

            Future<List<String>> staleLoad = queryExecutor.submit(catalog::getDbNames);
            Assertions.assertTrue(loaderStarted.await(3L, TimeUnit.SECONDS));
            Future<?> event = eventExecutor.submit(() -> {
                if (createEvent) {
                    catalog.simulateIncrementalRegisterDatabase(eventDb);
                } else {
                    catalog.unregisterDatabase(eventDb.getFullName());
                }
            });

            // Incremental events must not wait for the slow names loader and must not materialize a cold snapshot.
            event.get(3L, TimeUnit.SECONDS);
            Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
            Assertions.assertEquals(createEvent ? eventDb.getFullName() : null,
                    catalog.getCachedDatabaseNameByIdForTest(eventDb.getId()));
            releaseLoader.countDown();

            Assertions.assertEquals(staleSnapshot.localNames(), staleLoad.get(3L, TimeUnit.SECONDS));
            Assertions.assertNull(catalog.getCachedDatabaseNamesForTest());
            Assertions.assertEquals(currentSnapshot.localNames(), catalog.getDbNames());
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

    public static class RefreshCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            // db1
            tblSchemaMap1.put("tbl11", Lists.newArrayList(
                    new Column("a11", PrimitiveType.BIGINT),
                    new Column("a12", PrimitiveType.STRING),
                    new Column("a13", PrimitiveType.FLOAT)));
            tblSchemaMap1.put("tbl12", Lists.newArrayList(
                    new Column("b21", PrimitiveType.BIGINT),
                    new Column("b22", PrimitiveType.STRING),
                    new Column("b23", PrimitiveType.FLOAT)));
            MOCKED_META.put("db1", tblSchemaMap1);
            // db2
            Map<String, List<Column>> tblSchemaMap2 = Maps.newHashMap();
            tblSchemaMap2.put("tbl21", Lists.newArrayList(
                    new Column("c11", PrimitiveType.BIGINT),
                    new Column("c12", PrimitiveType.STRING),
                    new Column("c13", PrimitiveType.FLOAT)));
            MOCKED_META.put("db2", tblSchemaMap2);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    public static class IncrementalCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            Map<String, Map<String, List<Column>>> metadata = Maps.newHashMap();
            metadata.put("db_by_id", Maps.newHashMap());
            return metadata;
        }
    }

    private static class IncrementalUpdateCatalog extends TestExternalCatalog {
        private final AtomicInteger buildDatabaseCallCount = new AtomicInteger();

        IncrementalUpdateCatalog() {
            this(0);
        }

        IncrementalUpdateCatalog(int lowerCaseDatabaseNames) {
            super(1000L, "incremental_test", "",
                    buildIncrementalCatalogProps(lowerCaseDatabaseNames), "");
        }

        void simulateIncrementalRegisterDatabase(TestExternalDatabase db) {
            // Route database cache publication through the production helper so tests cover the same id source.
            updateDatabaseCache(db.getRemoteName(), db.getFullName(), db);
        }

        void seedDatabaseIdNameForTest(long dbId, String localDbName) {
            dbIdToName.put(dbId, localDbName);
        }

        void setDatabaseNamesEntryForTest(MetaCacheEntry<String, NameCacheValue> namesEntry) {
            databaseNames = namesEntry;
        }

        NameCacheValue getCachedDatabaseNamesForTest() {
            return databaseNames == null ? null : databaseNames.getIfPresent("");
        }

        ExternalDatabase<? extends ExternalTable> getCachedDatabaseForTest(String localDbName) {
            return databases == null ? null : databases.getIfPresent(localDbName);
        }

        String getCachedDatabaseNameByIdForTest(long dbId) {
            return dbIdToName.get(dbId);
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
                long dbId, InitCatalogLog.Type logType, boolean checkExists) {
            buildDatabaseCallCount.incrementAndGet();
            return super.buildDbForInit(remoteDbName, localDbName, dbId, logType, checkExists);
        }

        int getBuildDatabaseCallCount() {
            return buildDatabaseCallCount.get();
        }

        private static Map<String, String> buildIncrementalCatalogProps(int lowerCaseDatabaseNames) {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", IncrementalCatalogProvider.class.getName());
            props.put(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, String.valueOf(lowerCaseDatabaseNames));
            return props;
        }
    }

    private static class IncrementalUpdateDatabase extends TestExternalDatabase {
        IncrementalUpdateDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }
    }

    private static class EventHmsCatalog extends HMSExternalCatalog {
        private EventHmsDatabase database;
        private final boolean mapTableNames;

        EventHmsCatalog(long id, String name, int lowerCaseTableNames, boolean mapTableNames) {
            super(id, name, null, buildEventCatalogProps(lowerCaseTableNames), "");
            this.mapTableNames = mapTableNames;
        }

        void setDatabase(EventHmsDatabase database) {
            this.database = database;
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
            return database != null && database.getFullName().equalsIgnoreCase(dbName) ? database : null;
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            return mapTableNames ? "local_" + remoteTableName : remoteTableName;
        }

        private static Map<String, String> buildEventCatalogProps(int lowerCaseTableNames) {
            Map<String, String> props = Maps.newHashMap();
            props.put("type", "hms");
            props.put("hive.metastore.uris", "thrift://localhost:9083");
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, String.valueOf(lowerCaseTableNames));
            return props;
        }
    }

    private static class EventHmsDatabase extends HMSExternalDatabase {
        private final AtomicInteger tableLookupCount = new AtomicInteger();

        EventHmsDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
            super(extCatalog, id, name, remoteName);
        }

        @Override
        public HMSExternalTable getTableNullable(String tableName) {
            tableLookupCount.incrementAndGet();
            return null;
        }

        int getTableLookupCount() {
            return tableLookupCount.get();
        }
    }

    public static class CaseInsensitiveCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tableSchemaMap = Maps.newHashMap();
            tableSchemaMap.put("Foo", Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
            MOCKED_META.put("db_ci", tableSchemaMap);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    private static class CaseInsensitiveCatalog extends TestExternalCatalog {
        CaseInsensitiveCatalog() {
            super(2000L, "case_insensitive_test", "", buildCaseInsensitiveCatalogProps(), "");
        }

        private static Map<String, String> buildCaseInsensitiveCatalogProps() {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", CaseInsensitiveCatalogProvider.class.getName());
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, "2");
            return props;
        }
    }

    public static class NameMissCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private static final Map<String, Map<String, List<Column>>> MOCKED_META = Maps.newLinkedHashMap();

        static void reset() {
            MOCKED_META.clear();
        }

        static void putDatabase(String remoteDbName) {
            MOCKED_META.put(remoteDbName, Maps.newHashMap());
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    private static class NameMissCatalog extends TestExternalCatalog {
        private final AtomicInteger listDatabaseNamesCount = new AtomicInteger();

        NameMissCatalog() {
            this(2);
        }

        NameMissCatalog(int mode) {
            super(3000L + mode, "name_miss_catalog_" + mode, "", buildNameMissCatalogProps(mode), "");
        }

        @Override
        protected List<String> listDatabaseNames() {
            listDatabaseNamesCount.incrementAndGet();
            return Lists.newArrayList(NameMissCatalogProvider.MOCKED_META.keySet());
        }

        int getListDatabaseNamesCount() {
            return listDatabaseNamesCount.get();
        }

        // Expose the hot names snapshot so replay tests can verify it is preserved on misses.
        NameCacheValue getCachedDatabaseNamesForTest() {
            return databaseNames == null ? null : databaseNames.getIfPresent("");
        }

        ExternalDatabase<? extends ExternalTable> getCachedDatabaseForTest(String localDbName) {
            return databases == null ? null : databases.getIfPresent(localDbName);
        }

        String getCachedDatabaseNameByIdForTest(long dbId) {
            return dbIdToName.get(dbId);
        }

        private static Map<String, String> buildNameMissCatalogProps(int mode) {
            Map<String, String> props = Maps.newHashMap();
            props.put("catalog_provider.class", NameMissCatalogProvider.class.getName());
            props.put(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, String.valueOf(mode));
            return props;
        }
    }

    // Keep lookup strings explicit so mode-0 and mode-1 tests exercise the intended object-loader branch.
    private String remoteBaseDbNameForMode(int mode) {
        return mode == 0 ? "db_base" : "DbBase";
    }

    private String remoteHotDbNameForMode(int mode) {
        return mode == 0 ? "db_hot" : "DbHot";
    }

    private String baseDbLookupNameForMode(int mode) {
        return mode == 0 ? "db_base" : "dbbase";
    }

    private String hotDbLookupNameForMode(int mode) {
        return mode == 0 ? "db_hot" : "dbhot";
    }

    private String missingDbLookupNameForMode(int mode) {
        return mode == 0 ? "db_missing" : "dbmissing";
    }

    private int extractStripeCount(MetaCacheEntry<?, ?> entry) throws Exception {
        Field stripeCountField = MetaCacheEntry.class.getDeclaredField("stripeCount");
        stripeCountField.setAccessible(true);
        return stripeCountField.getInt(entry);
    }

    private MetaCacheEntry<?, ?> extractMetaCacheEntry(Object owner, String fieldName) throws Exception {
        Class<?> current = owner.getClass();
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (MetaCacheEntry<?, ?>) field.get(owner);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
