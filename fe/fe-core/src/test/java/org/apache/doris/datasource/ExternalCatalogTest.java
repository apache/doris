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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
