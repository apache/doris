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

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalTableNameComparedLowercaseMetaCacheTrueTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // 1. create test catalog
        CreateCatalogStmt testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt("create catalog test1 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"use_meta_cache\" = \"true\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.lowercase.ExternalTableNameComparedLowercaseMetaCacheTrueTest$ExternalTableNameComparedLowercaseProvider\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);
    }

    @Override
    protected void beforeCluster() {
        Config.lower_case_table_names = 2;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        DropCatalogStmt stmt = (DropCatalogStmt) parseAndAnalyzeStmt("drop catalog test1");
        env.getCatalogMgr().dropCatalog(stmt);
    }


    @Test
    public void testGlobalVariable() {
        Assertions.assertEquals(2, GlobalVariable.lowerCaseTableNames);
    }

    @Test
    public void testGetTableWithOutList() {
        RefreshCatalogStmt refreshCatalogStmt = new RefreshCatalogStmt("test1", null);
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshCatalogStmt);
        } catch (Exception e) {
            // Do nothing
        }
        String tblName = env.getCatalogMgr().getCatalog("test1").getDbNullable("db1").getTableNullable("table1").getName();
        Assertions.assertEquals("TABLE1", tblName);
        String tblName2 = env.getCatalogMgr().getCatalog("test1").getDbNullable("db1").getTableNullable("table2").getName();
        Assertions.assertEquals("TABLE2", tblName2);
    }

    @Test
    public void testTableNameLowerCase() {
        Set<String> tableNames = env.getCatalogMgr().getCatalog("test1").getDbNullable("db1").getTableNamesWithLock();
        Assertions.assertEquals(2, tableNames.size());
        Assertions.assertTrue(tableNames.contains("TABLE1"));
        Assertions.assertTrue(tableNames.contains("TABLE2"));
    }

    private void switchTest() throws Exception {
        SwitchStmt switchTest = (SwitchStmt) parseAndAnalyzeStmt("switch test1;");
        Env.getCurrentEnv().changeCatalog(connectContext, switchTest.getCatalogName());
    }

    public static class ExternalTableNameComparedLowercaseProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            // db1
            tblSchemaMap1.put("TABLE1", Lists.newArrayList(
                    new Column("siteid", PrimitiveType.INT),
                    new Column("citycode", PrimitiveType.SMALLINT),
                    new Column("username", PrimitiveType.VARCHAR),
                    new Column("pv", PrimitiveType.BIGINT)));
            tblSchemaMap1.put("TABLE2", Lists.newArrayList(
                    new Column("k1", PrimitiveType.INT),
                    new Column("k2", PrimitiveType.VARCHAR),
                    new Column("k3", PrimitiveType.VARCHAR),
                    new Column("k4", PrimitiveType.INT),
                    new Column("k5", PrimitiveType.LARGEINT)));
            MOCKED_META.put("db1", tblSchemaMap1);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
