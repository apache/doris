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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
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

public class ExternalDatabaseNameStoredLowercaseMetaCacheFalseTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // 1. create test catalog with lower_case_database_names = 1
        String createStmt = "create catalog test1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.lowercase.ExternalDatabaseNameStoredLowercaseMetaCacheFalseTest$ExternalDatabaseNameStoredLowercaseProvider\",\n"
                + "    \"" + ExternalCatalog.LOWER_CASE_DATABASE_NAMES + "\" = \"1\"\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    @Override
    protected void beforeCluster() {
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop catalog test1");
        if (logicalPlan instanceof DropCatalogCommand) {
            ((DropCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    @Test
    public void testGlobalVariable() {
        ExternalCatalog catalog = (ExternalCatalog) env.getCatalogMgr().getCatalog("test1");
        Assertions.assertEquals(1, catalog.getLowerCaseDatabaseNames());
    }

    @Test
    public void testGetDbWithOutList() {
        RefreshCatalogCommand refreshCatalogCommand = new RefreshCatalogCommand("test1", null);
        try {
            refreshCatalogCommand.run(connectContext, null);
        } catch (Exception e) {
            // Do nothing
        }
        // Query with uppercase, should retrieve lowercase
        ExternalDatabase db = (ExternalDatabase) env.getCatalogMgr().getCatalog("test1")
                .getDbNullable("DATABASE1");
        Assertions.assertNotNull(db);
        Assertions.assertEquals("database1", db.getFullName());

        // Query with mixed case
        ExternalDatabase db2 = (ExternalDatabase) env.getCatalogMgr().getCatalog("test1")
                .getDbNullable("Database1");
        Assertions.assertNotNull(db2);
        Assertions.assertEquals("database1", db2.getFullName());
    }

    @Test
    public void testDatabaseNameLowerCase() {
        List<String> dbNames = env.getCatalogMgr().getCatalog("test1").getDbNames();
        Assertions.assertTrue(dbNames.contains("database1"));
        Assertions.assertTrue(dbNames.contains("database2"));
        Assertions.assertTrue(dbNames.contains("database3"));
        Assertions.assertFalse(dbNames.contains("Database1"));
        Assertions.assertFalse(dbNames.contains("DATABASE2"));
    }

    public static class ExternalDatabaseNameStoredLowercaseProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tables = Maps.newHashMap();
            tables.put("table1", Lists.newArrayList(new Column("k1", PrimitiveType.INT)));

            // Test databases with mixed case in remote system
            MOCKED_META.put("Database1", tables);
            MOCKED_META.put("DATABASE2", tables);
            MOCKED_META.put("database3", tables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
