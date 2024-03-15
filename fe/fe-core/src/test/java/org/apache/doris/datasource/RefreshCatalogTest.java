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

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RefreshCatalogTest extends TestWithFeService {
    private static Auth auth;
    private static Env env;
    private CatalogMgr mgr;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        mgr = Env.getCurrentEnv().getCatalogMgr();
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        auth = env.getAuth();
        // 1. create test catalog
        CreateCatalogStmt testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test1 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"metadata_refresh_interval_sec\" = \"1\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\"\n"
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
        List<String> dbNames1 = test1.getDbNames();
        // there are test1.db1 , test1.db2, information_schema
        Assertions.assertEquals(3, dbNames1.size());
        // 1.simulate ExternalCatalog adds a new table
        RefreshCatalogProvider.addData();
        // 2.wait for the refresh time of the catalog
        Thread.sleep(5000);
        // there are test1.db1 , test1.db2 , test1.db3, information_schema
        List<String> dbNames2 = test1.getDbNames();
        Assertions.assertEquals(4, dbNames2.size());
        ExternalInfoSchemaDatabase infoDb = (ExternalInfoSchemaDatabase) test1.getDb(InfoSchemaDb.DATABASE_NAME).get();
        Assertions.assertEquals(30, infoDb.getTables().size());
        TestExternalDatabase testDb = (TestExternalDatabase) test1.getDb("db1").get();
        Assertions.assertEquals(2, testDb.getTables().size());

        String json = GsonUtils.GSON.toJson(env.getCatalogMgr());
        System.out.println(json);
        CatalogMgr mgr2 = GsonUtils.GSON.fromJson(json, CatalogMgr.class);
        test1 = mgr2.getCatalog("test1");
        infoDb = (ExternalInfoSchemaDatabase) test1.getDb(InfoSchemaDb.DATABASE_NAME).get();
        Assertions.assertEquals(30, infoDb.getTables().size());
        testDb = (TestExternalDatabase) test1.getDb("db1").get();
        Assertions.assertEquals(2, testDb.getTables().size());
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

        public static void addData() {
            // db3
            Map<String, List<Column>> tblSchemaMap3 = Maps.newHashMap();
            tblSchemaMap3.put("tbl31", Lists.newArrayList(
                    new Column("c11", PrimitiveType.BIGINT),
                    new Column("c12", PrimitiveType.STRING),
                    new Column("c13", PrimitiveType.FLOAT)));
            MOCKED_META.put("db3", tblSchemaMap3);
        }
    }
}
