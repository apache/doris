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

package org.apache.doris.datasource.lowercase;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabasesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL-level tests for SHOW TABLES/DATABASES and SELECT query resolution
 * under different per-catalog lower_case_table_names / lower_case_database_names modes (0/1/2).
 *
 * Mode 0: case-sensitive (exact match required)
 * Mode 1: stored as lowercase (remote already lowercase; any-case input lowered for lookup)
 * Mode 2: case-insensitive comparison (original case preserved; any-case input matches)
 */
public class ShowAndSelectLowercaseTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void beforeCluster() {
        Config.lower_case_table_names = 0;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // Mode 0: mixed-case remote names, case-sensitive
        createCatalog("test_mode0",
                "org.apache.doris.datasource.lowercase.ShowAndSelectLowercaseTest$MixedCaseProvider",
                0, 0);
        // Mode 1: lowercase remote names (real-world: remote stores lowercase)
        createCatalog("test_mode1",
                "org.apache.doris.datasource.lowercase.ShowAndSelectLowercaseTest$LowercaseProvider",
                1, 1);
        // Mode 2: mixed-case remote names, case-insensitive comparison
        createCatalog("test_mode2",
                "org.apache.doris.datasource.lowercase.ShowAndSelectLowercaseTest$MixedCaseProvider",
                2, 2);
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        dropCatalog("test_mode0");
        dropCatalog("test_mode1");
        dropCatalog("test_mode2");
    }

    // ==================== Helper methods ====================

    private void createCatalog(String name, String providerClass,
            int lowerCaseTableNames, int lowerCaseDatabaseNames) throws Exception {
        String createStmt = "create catalog " + name + " properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" = \"" + providerClass + "\",\n"
                + "    \"" + ExternalCatalog.LOWER_CASE_TABLE_NAMES + "\" = \"" + lowerCaseTableNames + "\",\n"
                + "    \"" + ExternalCatalog.LOWER_CASE_DATABASE_NAMES + "\" = \"" + lowerCaseDatabaseNames + "\"\n"
                + ");";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(createStmt);
        if (plan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) plan).run(rootCtx, null);
        }
    }

    private void dropCatalog(String name) throws Exception {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle("drop catalog " + name);
        if (plan instanceof DropCatalogCommand) {
            ((DropCatalogCommand) plan).run(rootCtx, null);
        }
    }

    private List<String> getShowDatabaseNames(String catalog) throws Exception {
        rootCtx.setThreadLocalInfo();
        ShowDatabasesCommand cmd = new ShowDatabasesCommand(catalog, null, null);
        ShowResultSet resultSet = cmd.doRun(rootCtx, new StmtExecutor(rootCtx, ""));
        return resultSet.getResultRows().stream()
                .map(row -> row.get(0))
                .filter(n -> !n.equalsIgnoreCase("information_schema") && !n.equalsIgnoreCase("mysql"))
                .collect(Collectors.toList());
    }

    private List<String> getShowTableNames(String db, String catalog) throws Exception {
        rootCtx.setThreadLocalInfo();
        ShowTableCommand cmd = new ShowTableCommand(db, catalog, false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = cmd.doRun(rootCtx, new StmtExecutor(rootCtx, ""));
        return resultSet.getResultRows().stream()
                .map(row -> row.get(0))
                .collect(Collectors.toList());
    }

    private List<String> getShowTableNamesLike(String db, String catalog, String pattern) throws Exception {
        rootCtx.setThreadLocalInfo();
        ShowTableCommand cmd = new ShowTableCommand(db, catalog, false, pattern, null, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = cmd.doRun(rootCtx, new StmtExecutor(rootCtx, ""));
        return resultSet.getResultRows().stream()
                .map(row -> row.get(0))
                .collect(Collectors.toList());
    }

    private String getPlanOrError(String sql) throws Exception {
        rootCtx.setThreadLocalInfo();
        return getSQLPlanOrErrorMsg(rootCtx, "EXPLAIN " + sql, false);
    }

    private boolean hasNameResolutionError(String result) {
        return result.contains("does not exist")
                || result.contains("Unknown column")
                || result.contains("unknown qualifier")
                || result.contains("unknown database");
    }

    private void assertSqlSuccess(String sql) throws Exception {
        String result = getPlanOrError(sql);
        Assertions.assertFalse(hasNameResolutionError(result),
                "Expected success but got name resolution error: " + result);
    }

    private void assertSqlError(String sql) throws Exception {
        String result = getPlanOrError(sql);
        Assertions.assertTrue(hasNameResolutionError(result),
                "Expected name resolution error but got: " + result);
    }

    // ==================== SHOW DATABASES tests ====================

    @Test
    public void testShowDatabasesMode0() throws Exception {
        List<String> dbs = getShowDatabaseNames("test_mode0");
        Assertions.assertTrue(dbs.contains("DB_UPPER"), "Expected DB_UPPER, got: " + dbs);
        Assertions.assertTrue(dbs.contains("MixedDb"), "Expected MixedDb, got: " + dbs);
    }

    @Test
    public void testShowDatabasesMode1() throws Exception {
        List<String> dbs = getShowDatabaseNames("test_mode1");
        Assertions.assertTrue(dbs.contains("db_upper"), "Expected db_upper, got: " + dbs);
        Assertions.assertTrue(dbs.contains("mixeddb"), "Expected mixeddb, got: " + dbs);
    }

    @Test
    public void testShowDatabasesMode2() throws Exception {
        List<String> dbs = getShowDatabaseNames("test_mode2");
        Assertions.assertTrue(dbs.contains("DB_UPPER"), "Expected DB_UPPER, got: " + dbs);
        Assertions.assertTrue(dbs.contains("MixedDb"), "Expected MixedDb, got: " + dbs);
    }

    // ==================== SHOW TABLES tests ====================

    @Test
    public void testShowTablesMode0() throws Exception {
        List<String> tables = getShowTableNames("DB_UPPER", "test_mode0");
        Assertions.assertTrue(tables.contains("TBL_UPPER"), "Expected TBL_UPPER, got: " + tables);
        Assertions.assertTrue(tables.contains("MixedTbl"), "Expected MixedTbl, got: " + tables);
    }

    @Test
    public void testShowTablesMode1() throws Exception {
        List<String> tables = getShowTableNames("db_upper", "test_mode1");
        Assertions.assertTrue(tables.contains("tbl_upper"), "Expected tbl_upper, got: " + tables);
        Assertions.assertTrue(tables.contains("mixedtbl"), "Expected mixedtbl, got: " + tables);
    }

    @Test
    public void testShowTablesMode2() throws Exception {
        List<String> tables = getShowTableNames("db_upper", "test_mode2");
        Assertions.assertTrue(tables.contains("TBL_UPPER"), "Expected TBL_UPPER, got: " + tables);
        Assertions.assertTrue(tables.contains("MixedTbl"), "Expected MixedTbl, got: " + tables);
    }

    // ==================== SHOW TABLES DB case-sensitivity tests ====================

    @Test
    public void testShowTablesMode0WrongDbCase() {
        Assertions.assertThrows(Exception.class, () -> {
            getShowTableNames("db_upper", "test_mode0");
        });
    }

    @Test
    public void testShowTablesMode1AnyDbCase() throws Exception {
        // Mode 1 lowercases input, so DB_UPPER -> db_upper which exists
        List<String> tables = getShowTableNames("DB_UPPER", "test_mode1");
        Assertions.assertTrue(tables.contains("tbl_upper"), "Expected tbl_upper, got: " + tables);
        Assertions.assertTrue(tables.contains("mixedtbl"), "Expected mixedtbl, got: " + tables);
    }

    @Test
    public void testShowTablesMode2AnyDbCase() throws Exception {
        // Mode 2 does case-insensitive lookup, so Db_Upper finds DB_UPPER
        List<String> tables = getShowTableNames("Db_Upper", "test_mode2");
        Assertions.assertTrue(tables.contains("TBL_UPPER"), "Expected TBL_UPPER, got: " + tables);
        Assertions.assertTrue(tables.contains("MixedTbl"), "Expected MixedTbl, got: " + tables);
    }

    // ==================== SHOW TABLES LIKE tests ====================

    @Test
    public void testShowTablesLikeMode0CaseSensitive() throws Exception {
        // Mode 0: LIKE is case-sensitive, "tbl%" should NOT match "TBL_UPPER"
        List<String> tables = getShowTableNamesLike("DB_UPPER", "test_mode0", "tbl%");
        Assertions.assertEquals(0, tables.size(), "Mode 0 LIKE should be case-sensitive, got: " + tables);
    }

    @Test
    public void testShowTablesLikeMode0CaseSensitiveMatch() throws Exception {
        List<String> tables = getShowTableNamesLike("DB_UPPER", "test_mode0", "TBL%");
        Assertions.assertEquals(1, tables.size(), "Expected 1 match for TBL%, got: " + tables);
        Assertions.assertTrue(tables.contains("TBL_UPPER"));
    }

    @Test
    public void testShowTablesLikeMode1CaseInsensitive() throws Exception {
        // Mode 1: LIKE is case-insensitive, "TBL%" matches "tbl_upper"
        List<String> tables = getShowTableNamesLike("db_upper", "test_mode1", "TBL%");
        Assertions.assertEquals(1, tables.size(), "Expected 1 match for TBL%, got: " + tables);
        Assertions.assertTrue(tables.contains("tbl_upper"));
    }

    @Test
    public void testShowTablesLikeMode2CaseInsensitive() throws Exception {
        // Mode 2: LIKE is case-insensitive, "tbl%" matches "TBL_UPPER"
        List<String> tables = getShowTableNamesLike("db_upper", "test_mode2", "tbl%");
        Assertions.assertEquals(1, tables.size(), "Expected 1 match for tbl%, got: " + tables);
        Assertions.assertTrue(tables.contains("TBL_UPPER"));
    }

    // ==================== SELECT tests — FROM clause ====================

    @Test
    public void testSelectExactCaseMode0() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectWrongTableCaseMode0() throws Exception {
        assertSqlError("SELECT * FROM test_mode0.DB_UPPER.tbl_upper");
    }

    @Test
    public void testSelectWrongDbCaseMode0() throws Exception {
        assertSqlError("SELECT * FROM test_mode0.db_upper.TBL_UPPER");
    }

    @Test
    public void testSelectAnyCaseMode1() throws Exception {
        // Mode 1: DB_UPPER -> db_upper, TBL_UPPER -> tbl_upper (both exist in lowercase provider)
        assertSqlSuccess("SELECT * FROM test_mode1.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectLowerCaseMode1() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode1.db_upper.tbl_upper");
    }

    @Test
    public void testSelectAnyCaseMode2() throws Exception {
        // Mode 2: db_upper finds DB_UPPER, tbl_upper finds TBL_UPPER
        assertSqlSuccess("SELECT * FROM test_mode2.db_upper.tbl_upper");
    }

    @Test
    public void testSelectMixedCaseMode2() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode2.Db_Upper.Tbl_Upper");
    }

    // ==================== A. SELECT list — qualified column references ====================

    @Test
    public void testSelectListTableQualMode0() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.id, TBL_UPPER.name FROM test_mode0.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectListTableQualMode0WrongCase() throws Exception {
        assertSqlError("SELECT tbl_upper.id FROM test_mode0.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectListTableQualMode1() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.id FROM test_mode1.db_upper.tbl_upper");
    }

    @Test
    public void testSelectListTableQualMode2() throws Exception {
        assertSqlSuccess("SELECT tbl_upper.id FROM test_mode2.db_upper.TBL_UPPER");
    }

    // ==================== B. SELECT list — qualified star ====================

    @Test
    public void testSelectStarTableQualMode0() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.* FROM test_mode0.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectStarTableQualMode0WrongCase() throws Exception {
        assertSqlError("SELECT tbl_upper.* FROM test_mode0.DB_UPPER.TBL_UPPER");
    }

    @Test
    public void testSelectStarTableQualMode1() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.* FROM test_mode1.db_upper.tbl_upper");
    }

    @Test
    public void testSelectStarDbTableQualMode1() throws Exception {
        assertSqlSuccess("SELECT DB_UPPER.TBL_UPPER.* FROM test_mode1.db_upper.tbl_upper");
    }

    @Test
    public void testSelectStarTableQualMode2() throws Exception {
        assertSqlSuccess("SELECT tbl_upper.* FROM test_mode2.db_upper.TBL_UPPER");
    }

    // ==================== C. WHERE clause — filter predicates ====================

    @Test
    public void testWhereTableQualMode0() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER WHERE TBL_UPPER.id = 1");
    }

    @Test
    public void testWhereTableQualMode0WrongCase() throws Exception {
        assertSqlError("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER WHERE tbl_upper.id = 1");
    }

    @Test
    public void testWhereTableQualMode1() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode1.db_upper.tbl_upper WHERE TBL_UPPER.id = 1");
    }

    @Test
    public void testWhereTableQualMode2() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode2.db_upper.TBL_UPPER WHERE tbl_upper.id = 1");
    }

    @Test
    public void testWhereDbTableQualMode0() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER WHERE DB_UPPER.TBL_UPPER.id = 1");
    }

    @Test
    public void testWhereDbTableQualMode0WrongCase() throws Exception {
        assertSqlError("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER WHERE db_upper.tbl_upper.id = 1");
    }

    @Test
    public void testWhereDbTableQualMode1() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode1.db_upper.tbl_upper WHERE DB_UPPER.TBL_UPPER.id = 1");
    }

    @Test
    public void testWhereDbTableQualMode2() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode2.db_upper.TBL_UPPER WHERE db_upper.tbl_upper.id = 1");
    }

    // ==================== D. JOIN — table reference + ON clause qualifiers ====================

    @Test
    public void testJoinMode0() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.id FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "JOIN test_mode0.DB_UPPER.MixedTbl ON TBL_UPPER.id = MixedTbl.k1");
    }

    @Test
    public void testJoinMode0WrongCase() throws Exception {
        assertSqlError("SELECT tbl_upper.id FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "JOIN test_mode0.DB_UPPER.MixedTbl ON tbl_upper.id = mixedtbl.k1");
    }

    @Test
    public void testJoinMode1() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.id FROM test_mode1.db_upper.tbl_upper "
                + "JOIN test_mode1.db_upper.mixedtbl ON TBL_UPPER.id = MixedTbl.k1");
    }

    @Test
    public void testJoinMode2() throws Exception {
        assertSqlSuccess("SELECT tbl_upper.id FROM test_mode2.db_upper.TBL_UPPER "
                + "JOIN test_mode2.db_upper.MixedTbl ON tbl_upper.id = mixedtbl.k1");
    }

    // ==================== E. GROUP BY — qualified column reference ====================

    @Test
    public void testGroupByMode0() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.name, count(*) FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "GROUP BY TBL_UPPER.name");
    }

    @Test
    public void testGroupByMode0WrongCase() throws Exception {
        assertSqlError("SELECT TBL_UPPER.name, count(*) FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "GROUP BY tbl_upper.name");
    }

    @Test
    public void testGroupByMode1() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.name, count(*) FROM test_mode1.db_upper.tbl_upper "
                + "GROUP BY TBL_UPPER.name");
    }

    @Test
    public void testGroupByMode2() throws Exception {
        assertSqlSuccess("SELECT tbl_upper.name, count(*) FROM test_mode2.db_upper.TBL_UPPER "
                + "GROUP BY tbl_upper.name");
    }

    // ==================== F. ORDER BY — qualified column reference ====================

    @Test
    public void testOrderByMode0() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER ORDER BY TBL_UPPER.id");
    }

    @Test
    public void testOrderByMode0WrongCase() throws Exception {
        assertSqlError("SELECT * FROM test_mode0.DB_UPPER.TBL_UPPER ORDER BY tbl_upper.id");
    }

    @Test
    public void testOrderByMode1() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode1.db_upper.tbl_upper ORDER BY TBL_UPPER.id");
    }

    @Test
    public void testOrderByMode2() throws Exception {
        assertSqlSuccess("SELECT * FROM test_mode2.db_upper.TBL_UPPER ORDER BY tbl_upper.id");
    }

    // ==================== G. HAVING — qualified column in aggregate filter ====================

    @Test
    public void testHavingMode0() throws Exception {
        assertSqlSuccess("SELECT TBL_UPPER.name FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "GROUP BY TBL_UPPER.name HAVING count(TBL_UPPER.id) > 0");
    }

    @Test
    public void testHavingMode0WrongCase() throws Exception {
        assertSqlError("SELECT TBL_UPPER.name FROM test_mode0.DB_UPPER.TBL_UPPER "
                + "GROUP BY TBL_UPPER.name HAVING count(tbl_upper.id) > 0");
    }

    @Test
    public void testHavingMode1() throws Exception {
        assertSqlSuccess("SELECT tbl_upper.name FROM test_mode1.db_upper.tbl_upper "
                + "GROUP BY tbl_upper.name HAVING count(TBL_UPPER.id) > 0");
    }

    // ==================== H. Subquery — table reference inside subquery ====================

    @Test
    public void testSubqueryMode0() throws Exception {
        assertSqlSuccess("SELECT * FROM (SELECT TBL_UPPER.id FROM test_mode0.DB_UPPER.TBL_UPPER) t");
    }

    @Test
    public void testSubqueryMode0WrongCase() throws Exception {
        assertSqlError("SELECT * FROM (SELECT id FROM test_mode0.DB_UPPER.tbl_upper) t");
    }

    @Test
    public void testSubqueryMode1() throws Exception {
        assertSqlSuccess("SELECT * FROM (SELECT id FROM test_mode1.DB_UPPER.TBL_UPPER) t");
    }

    @Test
    public void testSubqueryMode2() throws Exception {
        assertSqlSuccess("SELECT * FROM (SELECT id FROM test_mode2.Db_Upper.Tbl_Upper) t");
    }

    // ==================== I. Cross-database JOIN ====================

    @Test
    public void testCrossDbJoinMode1() throws Exception {
        assertSqlSuccess("SELECT a.id FROM test_mode1.DB_UPPER.TBL_UPPER a "
                + "JOIN test_mode1.MIXEDDB.another_tbl b ON a.id = b.x");
    }

    @Test
    public void testCrossDbJoinMode2() throws Exception {
        assertSqlSuccess("SELECT a.id FROM test_mode2.db_upper.tbl_upper a "
                + "JOIN test_mode2.mixeddb.another_tbl b ON a.id = b.x");
    }

    // ==================== Mock data providers ====================

    /**
     * Mixed-case provider for mode 0 (case-sensitive) and mode 2 (case-insensitive comparison).
     * Remote names preserve original casing.
     */
    public static class MixedCaseProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();

            Map<String, List<Column>> dbUpperTables = Maps.newHashMap();
            dbUpperTables.put("TBL_UPPER", Lists.newArrayList(
                    new Column("id", PrimitiveType.INT),
                    new Column("name", PrimitiveType.VARCHAR)));
            dbUpperTables.put("MixedTbl", Lists.newArrayList(
                    new Column("k1", PrimitiveType.INT),
                    new Column("k2", PrimitiveType.VARCHAR)));
            MOCKED_META.put("DB_UPPER", dbUpperTables);

            Map<String, List<Column>> mixedDbTables = Maps.newHashMap();
            mixedDbTables.put("another_tbl", Lists.newArrayList(
                    new Column("x", PrimitiveType.INT),
                    new Column("y", PrimitiveType.VARCHAR)));
            MOCKED_META.put("MixedDb", mixedDbTables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }

    /**
     * Lowercase provider for mode 1 (stored as lowercase).
     * In real-world mode 1, the remote system stores names in lowercase.
     * The test verifies that any-case SQL input gets lowered and resolves correctly.
     */
    public static class LowercaseProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();

            Map<String, List<Column>> dbUpperTables = Maps.newHashMap();
            dbUpperTables.put("tbl_upper", Lists.newArrayList(
                    new Column("id", PrimitiveType.INT),
                    new Column("name", PrimitiveType.VARCHAR)));
            dbUpperTables.put("mixedtbl", Lists.newArrayList(
                    new Column("k1", PrimitiveType.INT),
                    new Column("k2", PrimitiveType.VARCHAR)));
            MOCKED_META.put("db_upper", dbUpperTables);

            Map<String, List<Column>> mixedDbTables = Maps.newHashMap();
            mixedDbTables.put("another_tbl", Lists.newArrayList(
                    new Column("x", PrimitiveType.INT),
                    new Column("y", PrimitiveType.VARCHAR)));
            MOCKED_META.put("mixeddb", mixedDbTables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
