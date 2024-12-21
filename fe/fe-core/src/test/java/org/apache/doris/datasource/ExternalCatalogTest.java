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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.Files;
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
        CreateCatalogStmt testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test1 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test2 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test3 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        // use_meta_cache=false
        testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test4 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"use_meta_cache\" = \"false\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test5 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"use_meta_cache\" = \"false\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test6 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"use_meta_cache\" = \"false\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\",\n"
                        + "    \"include_database_list\" = \"db1\",\n"
                        + "    \"exclude_database_list\" = \"db1\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);
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
        // 1. use_meta_cache=true
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

        // 2. use_meta_cache=false
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

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./external_catalog_persist_test.dat");
        file.createNewFile();
        try {
            DataOutputStream dos = new DataOutputStream(Files.newOutputStream(file.toPath()));

            TestExternalCatalog ctl = (TestExternalCatalog) mgr.getCatalog("test1");
            ctl.write(dos);
            dos.flush();
            dos.close();

            // 2. Read objects from file
            DataInputStream dis = new DataInputStream(Files.newInputStream(file.toPath()));

            String json = Text.readString(dis);
            TestExternalCatalog ctl2 = GsonUtils.GSON.fromJson(json, TestExternalCatalog.class);
            Configuration conf = ctl2.getConfiguration();
            Assertions.assertNotNull(conf);

            // 3. delete files
            dis.close();
        } finally {
            file.delete();
        }
    }
}
