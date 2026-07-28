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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExternalDatabaseRegisterTableTest extends TestWithFeService {

    private static final String CATALOG_NAME = "test_register_tbl_catalog";
    private static final String DB_NAME = "db1";

    private static final DynamicProvider PROVIDER = new DynamicProvider();

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = createDefaultCtx();
        String createStmt = "create catalog " + CATALOG_NAME + " properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.ExternalDatabaseRegisterTableTest$DynamicProvider\",\n"
                + "    \"include_database_list\" = \"db1\"\n"
                + ");";
        createCatalog(ctx, createStmt);
    }

    private void createCatalog(ConnectContext ctx, String stmt) throws Exception {
        org.apache.doris.nereids.parser.NereidsParser parser = new org.apache.doris.nereids.parser.NereidsParser();
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan plan = parser.parseSingle(stmt);
        if (plan instanceof org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand) {
            ((org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand) plan).run(ctx, null);
        }
    }

    private ExternalDatabase<? extends ExternalTable> getDb() {
        TestExternalCatalog catalog = (TestExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(CATALOG_NAME);
        Assertions.assertNotNull(catalog);
        ExternalDatabase<? extends ExternalTable> db = (ExternalDatabase<? extends ExternalTable>)
                catalog.getDbNullable(DB_NAME);
        Assertions.assertNotNull(db, "Database should exist after catalog init");
        return db;
    }

    @Test
    public void testRegisterTableFromCreateMakesTableVisible() {
        String tblName = "tbl_newly_created";
        PROVIDER.addTable(DB_NAME, tblName, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        Assertions.assertNull(db.getTableNullable(tblName),
                "Table should not exist before registerTableFromCreate");

        db.registerTableFromCreate(tblName);

        ExternalTable table = db.getTableNullable(tblName);
        Assertions.assertNotNull(table, "Table should be visible after registerTableFromCreate");
        Assertions.assertEquals(tblName, table.getName());

        PROVIDER.removeTable(DB_NAME, tblName);
    }

    @Test
    public void testRegisterTableFromCreateUpdatesNamesCache() {
        String tblName = "tbl_names_cache";
        PROVIDER.addTable(DB_NAME, tblName, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        db.registerTableFromCreate(tblName);

        List<String> tableNames = Lists.newArrayList(db.getTableNamesWithLock());
        Assertions.assertTrue(tableNames.contains(tblName),
                "New table name should appear in table name list");

        PROVIDER.removeTable(DB_NAME, tblName);
    }

    @Test
    public void testRegisterTableFromCreateIdempotent() {
        String tblName = "tbl_idempotent";
        PROVIDER.addTable(DB_NAME, tblName, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        db.registerTableFromCreate(tblName);
        db.registerTableFromCreate(tblName);

        ExternalTable table = db.getTableNullable(tblName);
        Assertions.assertNotNull(table, "Table should still be visible after duplicate register");

        PROVIDER.removeTable(DB_NAME, tblName);
    }

    @Test
    public void testRegisterTableFromCreateMultipleTables() {
        String table1 = "tbl_multi_a";
        String table2 = "tbl_multi_b";
        PROVIDER.addTable(DB_NAME, table1, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));
        PROVIDER.addTable(DB_NAME, table2, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        db.registerTableFromCreate(table1);
        db.registerTableFromCreate(table2);

        Assertions.assertNotNull(db.getTableNullable(table1));
        Assertions.assertNotNull(db.getTableNullable(table2));

        List<String> tableNames = Lists.newArrayList(db.getTableNamesWithLock());
        Assertions.assertTrue(tableNames.contains(table1));
        Assertions.assertTrue(tableNames.contains(table2));

        PROVIDER.removeTable(DB_NAME, table1);
        PROVIDER.removeTable(DB_NAME, table2);
    }

    @Test
    public void testGetTableForReplayAfterRegister() {
        String tblName = "tbl_replay";
        PROVIDER.addTable(DB_NAME, tblName, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        db.registerTableFromCreate(tblName);

        Optional<? extends ExternalTable> tableOpt = db.getTableForReplay(tblName);
        Assertions.assertTrue(tableOpt.isPresent(),
                "Table should be retrievable via getTableForReplay after registerTableFromCreate");

        PROVIDER.removeTable(DB_NAME, tblName);
    }

    @Test
    public void testRegisterTableFromCreateDoesNotAffectOtherTables() {
        String tbl1 = "tbl_persist_a";
        String tbl2 = "tbl_persist_b";
        PROVIDER.addTable(DB_NAME, tbl1, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));
        PROVIDER.addTable(DB_NAME, tbl2, Lists.newArrayList(
                new Column("col1", PrimitiveType.INT)));

        ExternalDatabase<? extends ExternalTable> db = getDb();

        db.registerTableFromCreate(tbl1);

        Assertions.assertNotNull(db.getTableNullable(tbl1));
        Assertions.assertNull(db.getTableNullable(tbl2),
                "Other table should not be visible until registered");

        db.registerTableFromCreate(tbl2);
        Assertions.assertNotNull(db.getTableNullable(tbl2));

        PROVIDER.removeTable(DB_NAME, tbl1);
        PROVIDER.removeTable(DB_NAME, tbl2);
    }

    public static class DynamicProvider implements TestExternalCatalog.TestCatalogProvider {
        private final Map<String, Map<String, List<Column>>> metadata = Maps.newConcurrentMap();

        public DynamicProvider() {
            metadata.put(DB_NAME, Maps.newConcurrentMap());
        }

        public void addTable(String dbName, String tblName, List<Column> cols) {
            metadata.get(dbName).put(tblName, cols);
        }

        public void removeTable(String dbName, String tblName) {
            metadata.get(dbName).remove(tblName);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return metadata;
        }
    }
}
