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
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshCatalogCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IncludeTableListTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
    }

    @Override
    protected void beforeCluster() {
        FeConstants.runningUnitTest = true;
    }

    private void createCatalog(String catalogName, String providerClass, String includeTableList) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("create catalog ").append(catalogName).append(" properties(\n");
        sb.append("    \"type\" = \"test\",\n");
        sb.append("    \"catalog_provider.class\" = \"").append(providerClass).append("\"");
        if (includeTableList != null) {
            sb.append(",\n    \"").append(ExternalCatalog.INCLUDE_TABLE_LIST)
                    .append("\" = \"").append(includeTableList).append("\"");
        }
        sb.append("\n);");

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sb.toString());
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    private void dropCatalog(String catalogName) throws Exception {
        rootCtx.setThreadLocalInfo();
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop catalog " + catalogName);
        if (logicalPlan instanceof DropCatalogCommand) {
            ((DropCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    private void refreshCatalog(String catalogName) {
        RefreshCatalogCommand refreshCatalogCommand = new RefreshCatalogCommand(catalogName, null);
        try {
            refreshCatalogCommand.run(connectContext, null);
        } catch (Exception e) {
            // Do nothing
        }
    }

    private static final String PROVIDER_CLASS =
            "org.apache.doris.datasource.IncludeTableListTest$IncludeTableListProvider";

    // ==================== Basic filtering tests ====================

    /**
     * When include_table_list is not configured, all tables should be visible.
     */
    @Test
    public void testNoIncludeTableList() throws Exception {
        String catalogName = "test_no_include";
        createCatalog(catalogName, PROVIDER_CLASS, null);
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> tableNames = db1.getTableNamesWithLock();
            // All 3 tables in db1 should be visible
            Assertions.assertEquals(3, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl1"));
            Assertions.assertTrue(tableNames.contains("tbl2"));
            Assertions.assertTrue(tableNames.contains("tbl3"));

            ExternalDatabase<?> db2 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db2");
            Assertions.assertNotNull(db2);
            Set<String> db2TableNames = db2.getTableNamesWithLock();
            Assertions.assertEquals(2, db2TableNames.size());
            Assertions.assertTrue(db2TableNames.contains("tbl_a"));
            Assertions.assertTrue(db2TableNames.contains("tbl_b"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list specifies a single table in one db, only that table should be visible
     * in that db; other dbs should show all tables.
     */
    @Test
    public void testSingleTableInclude() throws Exception {
        String catalogName = "test_single_include";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl1");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> tableNames = db1.getTableNamesWithLock();
            // Only tbl1 should be visible in db1
            Assertions.assertEquals(1, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl1"));
            Assertions.assertFalse(tableNames.contains("tbl2"));
            Assertions.assertFalse(tableNames.contains("tbl3"));

            // db2 should still show all tables (not in include_table_list)
            ExternalDatabase<?> db2 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db2");
            Assertions.assertNotNull(db2);
            Set<String> db2TableNames = db2.getTableNamesWithLock();
            Assertions.assertEquals(2, db2TableNames.size());
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list specifies multiple tables in same db.
     */
    @Test
    public void testMultipleTablesInSameDb() throws Exception {
        String catalogName = "test_multi_same_db";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl1,db1.tbl3");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> tableNames = db1.getTableNamesWithLock();
            // Only tbl1 and tbl3 should be visible
            Assertions.assertEquals(2, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl1"));
            Assertions.assertFalse(tableNames.contains("tbl2"));
            Assertions.assertTrue(tableNames.contains("tbl3"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list specifies tables across multiple dbs.
     */
    @Test
    public void testMultipleTablesAcrossDbs() throws Exception {
        String catalogName = "test_multi_cross_db";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl2,db2.tbl_a");
        try {
            refreshCatalog(catalogName);

            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> db1Tables = db1.getTableNamesWithLock();
            Assertions.assertEquals(1, db1Tables.size());
            Assertions.assertTrue(db1Tables.contains("tbl2"));

            ExternalDatabase<?> db2 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db2");
            Assertions.assertNotNull(db2);
            Set<String> db2Tables = db2.getTableNamesWithLock();
            Assertions.assertEquals(1, db2Tables.size());
            Assertions.assertTrue(db2Tables.contains("tbl_a"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    // ==================== Error / edge case tests ====================

    /**
     * When include_table_list specifies a table that does NOT exist in the remote source,
     * the table name should still appear in listTableNames but getTableNullable should return null.
     */
    @Test
    public void testNonExistentTableInIncludeList() throws Exception {
        String catalogName = "test_nonexist_tbl";
        // "nonexistent_table" does not exist in the provider's db1
        createCatalog(catalogName, PROVIDER_CLASS, "db1.nonexistent_table");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            // The include list overrides remote listing, so it reports "nonexistent_table"
            Set<String> tableNames = db1.getTableNamesWithLock();
            Assertions.assertEquals(1, tableNames.size());
            Assertions.assertTrue(tableNames.contains("nonexistent_table"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * Mix of existing and non-existing tables in include_table_list.
     * Existing table should be accessible; non-existing table should return null.
     */
    @Test
    public void testMixExistentAndNonExistentTables() throws Exception {
        String catalogName = "test_mix_exist";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl1,db1.no_such_table");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> tableNames = db1.getTableNamesWithLock();
            Assertions.assertEquals(2, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl1"));
            Assertions.assertTrue(tableNames.contains("no_such_table"));

            // Existing table should be accessible
            Assertions.assertNotNull(db1.getTableNullable("tbl1"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list refers to a non-existent database, that db entry
     * in the include map is simply ignored (the db won't appear).
     */
    @Test
    public void testNonExistentDbInIncludeList() throws Exception {
        String catalogName = "test_nonexist_db";
        // "no_such_db" does not exist in the provider
        createCatalog(catalogName, PROVIDER_CLASS, "no_such_db.tbl1,db1.tbl1");
        try {
            refreshCatalog(catalogName);
            // db1 should still work with filtered table
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> db1Tables = db1.getTableNamesWithLock();
            Assertions.assertEquals(1, db1Tables.size());
            Assertions.assertTrue(db1Tables.contains("tbl1"));

            // The non-existent db should return null
            Assertions.assertNull(
                    env.getCatalogMgr().getCatalog(catalogName).getDbNullable("no_such_db"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list contains entries with invalid format (no dot separator),
     * those entries are silently ignored.
     */
    @Test
    public void testInvalidFormatInIncludeList() throws Exception {
        String catalogName = "test_invalid_fmt";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl1,bad_format,db2.tbl_a,too.many.dots");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> db1Tables = db1.getTableNamesWithLock();
            // Only "db1.tbl1" is a valid entry for db1
            Assertions.assertEquals(1, db1Tables.size());
            Assertions.assertTrue(db1Tables.contains("tbl1"));

            ExternalDatabase<?> db2 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db2");
            Assertions.assertNotNull(db2);
            Set<String> db2Tables = db2.getTableNamesWithLock();
            Assertions.assertEquals(1, db2Tables.size());
            Assertions.assertTrue(db2Tables.contains("tbl_a"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list entries have extra whitespace, they should be trimmed properly.
     */
    @Test
    public void testWhitespaceInIncludeList() throws Exception {
        String catalogName = "test_whitespace";
        createCatalog(catalogName, PROVIDER_CLASS, " db1.tbl1 , db1.tbl2 ");
        try {
            refreshCatalog(catalogName);
            ExternalDatabase<?> db1 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db1");
            Assertions.assertNotNull(db1);
            Set<String> tableNames = db1.getTableNamesWithLock();
            Assertions.assertEquals(2, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl1"));
            Assertions.assertTrue(tableNames.contains("tbl2"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * When include_table_list is set but all entries are for one db,
     * another db should show all its tables (unaffected).
     */
    @Test
    public void testUnaffectedDbShowsAllTables() throws Exception {
        String catalogName = "test_unaffected_db";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl1");
        try {
            refreshCatalog(catalogName);
            // db2 is not mentioned in include_table_list, so all tables should be visible
            ExternalDatabase<?> db2 = (ExternalDatabase<?>) env.getCatalogMgr()
                    .getCatalog(catalogName).getDbNullable("db2");
            Assertions.assertNotNull(db2);
            Set<String> db2Tables = db2.getTableNamesWithLock();
            Assertions.assertEquals(2, db2Tables.size());
            Assertions.assertTrue(db2Tables.contains("tbl_a"));
            Assertions.assertTrue(db2Tables.contains("tbl_b"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    /**
     * Test that listTableNames (the catalog-level API) returns the included list directly
     * when include_table_list is configured for that db.
     */
    @Test
    public void testListTableNamesAPI() throws Exception {
        String catalogName = "test_api_list";
        createCatalog(catalogName, PROVIDER_CLASS, "db1.tbl2,db1.tbl3");
        try {
            ExternalCatalog catalog = (ExternalCatalog) env.getCatalogMgr().getCatalog(catalogName);
            List<String> tableNames = catalog.listTableNames(null, "db1");
            Assertions.assertEquals(2, tableNames.size());
            Assertions.assertTrue(tableNames.contains("tbl2"));
            Assertions.assertTrue(tableNames.contains("tbl3"));
            Assertions.assertFalse(tableNames.contains("tbl1"));

            // db2 not in include map → returns all remote tables
            List<String> db2Names = catalog.listTableNames(null, "db2");
            Assertions.assertEquals(2, db2Names.size());
            Assertions.assertTrue(db2Names.contains("tbl_a"));
            Assertions.assertTrue(db2Names.contains("tbl_b"));
        } finally {
            dropCatalog(catalogName);
        }
    }

    // ==================== Mock data provider ====================

    public static class IncludeTableListProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();

            // db1 with 3 tables
            Map<String, List<Column>> db1Tables = Maps.newHashMap();
            db1Tables.put("tbl1", Lists.newArrayList(
                    new Column("id", PrimitiveType.INT),
                    new Column("name", PrimitiveType.VARCHAR)));
            db1Tables.put("tbl2", Lists.newArrayList(
                    new Column("id", PrimitiveType.INT),
                    new Column("value", PrimitiveType.BIGINT)));
            db1Tables.put("tbl3", Lists.newArrayList(
                    new Column("key", PrimitiveType.VARCHAR),
                    new Column("data", PrimitiveType.STRING)));
            MOCKED_META.put("db1", db1Tables);

            // db2 with 2 tables
            Map<String, List<Column>> db2Tables = Maps.newHashMap();
            db2Tables.put("tbl_a", Lists.newArrayList(
                    new Column("col1", PrimitiveType.INT),
                    new Column("col2", PrimitiveType.FLOAT)));
            db2Tables.put("tbl_b", Lists.newArrayList(
                    new Column("x", PrimitiveType.BIGINT),
                    new Column("y", PrimitiveType.DOUBLE)));
            MOCKED_META.put("db2", db2Tables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
