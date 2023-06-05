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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.TestExternalTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RefreshTableTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // 1. create test catalog
        CreateCatalogStmt testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt("create catalog test1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.catalog.RefreshTableTest$RefreshTableProvider\"\n"
                + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        DropCatalogStmt stmt = (DropCatalogStmt) parseAndAnalyzeStmt("drop catalog test1");
        env.getCatalogMgr().dropCatalog(stmt);
    }

    @Test
    public void testRefreshCatalog() throws Exception {
        CatalogIf test1 = env.getCatalogMgr().getCatalog("test1");
        TestExternalTable table = (TestExternalTable) test1.getDbNullable("db1").getTable("tbl11").get();
        Assertions.assertFalse(table.isObjectCreated());
        table.makeSureInitialized();
        Assertions.assertTrue(table.isObjectCreated());
        RefreshCatalogStmt refreshCatalogStmt = new RefreshCatalogStmt("test1", null);
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshCatalogStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertTrue(table.isObjectCreated());
        test1.getDbNullable("db1").getTables();
        Assertions.assertFalse(table.isObjectCreated());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshCatalogStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertFalse(((ExternalCatalog) test1).isInitialized());
        table.makeSureInitialized();
        Assertions.assertTrue(((ExternalCatalog) test1).isInitialized());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshCatalogStmt);
        } catch (Exception e) {
            // Do nothing
        }
    }

    @Test
    public void testRefreshDatabase() throws Exception {
        CatalogIf test1 = env.getCatalogMgr().getCatalog("test1");
        TestExternalTable table = (TestExternalTable) test1.getDbNullable("db1").getTable("tbl11").get();
        Assertions.assertFalse(table.isObjectCreated());
        table.makeSureInitialized();
        Assertions.assertTrue(table.isObjectCreated());
        RefreshDbStmt refreshDbStmt = new RefreshDbStmt("test1", "db1", null);
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertTrue(table.isObjectCreated());
        test1.getDbNullable("db1").getTables();
        Assertions.assertFalse(table.isObjectCreated());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertFalse(((ExternalDatabase) test1.getDbNullable("db1")).isInitialized());
        table.makeSureInitialized();
        Assertions.assertTrue(((ExternalDatabase) test1.getDbNullable("db1")).isInitialized());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
    }

    @Test
    public void testRefreshTable() throws Exception {
        CatalogIf test1 = env.getCatalogMgr().getCatalog("test1");
        TestExternalTable table = (TestExternalTable) test1.getDbNullable("db1").getTable("tbl11").get();
        Assertions.assertFalse(table.isObjectCreated());
        table.makeSureInitialized();
        Assertions.assertTrue(table.isObjectCreated());
        RefreshTableStmt refreshTableStmt = new RefreshTableStmt(new TableName("test1", "db1", "tbl11"));
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshTableStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertFalse(table.isObjectCreated());
    }

    public static class RefreshTableProvider implements TestExternalCatalog.TestCatalogProvider {
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
