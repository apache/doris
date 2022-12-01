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

import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.catalog.external.EsExternalTable;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class CatalogMgrTest extends TestWithFeService {
    private static final String MY_CATALOG = "my_catalog";
    private static PaloAuth auth;
    private static Env env;
    private static UserIdentity user1;
    private static UserIdentity user2;
    private CatalogMgr mgr;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_multi_catalog = true;
        FeConstants.runningUnitTest = true;
        mgr = Env.getCurrentEnv().getCatalogMgr();

        ConnectContext rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        auth = env.getAuth();

        // grant with no catalog is switched, internal catalog works.
        CreateRoleStmt createRole1 = (CreateRoleStmt) parseAndAnalyzeStmt("create role role1;", rootCtx);
        auth.createRole(createRole1);
        GrantStmt grantRole1 = (GrantStmt) parseAndAnalyzeStmt("grant grant_priv on tpch.* to role 'role1';", rootCtx);
        auth.grant(grantRole1);
        // grant with ctl.db.tbl. grant can succeed even if the catalog does not exist
        GrantStmt grantRole1WithCtl = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on testc.testdb.* to role 'role1';", rootCtx);
        auth.grant(grantRole1WithCtl);
        // user1 can't switch to hive
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user1'@'%' identified by 'pwd1' default role 'role1';", rootCtx));
        user1 = new UserIdentity("user1", "%");
        user1.analyze(SystemInfoService.DEFAULT_CLUSTER);
        // user1 has the privileges of testc which is granted by ctl.db.tbl format.
        Assert.assertTrue(auth.getDbPrivTable().hasPrivsOfCatalog(user1, "testc"));

        // create catalog
        CreateCatalogStmt hiveCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hive properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(hiveCatalog);
        CreateCatalogStmt iceBergCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog iceberg properties('type' = 'hms', 'iceberg.hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(iceBergCatalog);

        // create es catalog
        CreateCatalogStmt esCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog es properties('type' = 'es', 'elasticsearch.hosts' = 'http://192.168.0.1',"
                        + " 'elasticsearch.username' = 'user1');",
                rootCtx);
        env.getCatalogMgr().createCatalog(esCatalog);

        createDbAndTableForCatalog(env.getCatalogMgr().getCatalog("hive"));
        createDbAndTableForCatalog(env.getCatalogMgr().getCatalog("es"));

        // switch to hive.
        SwitchStmt switchHive = (SwitchStmt) parseAndAnalyzeStmt("switch hive;", rootCtx);
        env.changeCatalog(rootCtx, switchHive.getCatalogName());
        CreateRoleStmt createRole2 = (CreateRoleStmt) parseAndAnalyzeStmt("create role role2;", rootCtx);
        auth.createRole(createRole2);
        GrantStmt grantRole2 = (GrantStmt) parseAndAnalyzeStmt("grant grant_priv on tpch.customer to role 'role2';",
                rootCtx);
        auth.grant(grantRole2);
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user2'@'%' identified by 'pwd2' default role 'role2';", rootCtx));
        user2 = new UserIdentity("user2", "%");
        user2.analyze(SystemInfoService.DEFAULT_CLUSTER);
    }

    private void createDbAndTableForCatalog(CatalogIf catalog) {
        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
            HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hive_db1");
            HMSExternalTable tbl = new HMSExternalTable(10001, "hive_tbl1", "hive_db1", hmsCatalog);
            tbl.setNewFullSchema(schema);
            db.addTableForTest(tbl);
            hmsCatalog.addDatabaseForTest(db);
        } else if (catalog instanceof EsExternalCatalog) {
            EsExternalCatalog esCatalog = (EsExternalCatalog) catalog;
            EsExternalDatabase db = new EsExternalDatabase(esCatalog, 10002, "es_db1");
            EsExternalTable tbl = new EsExternalTable(10003, "es_tbl1", "es_tbl1", esCatalog);
            tbl.setNewFullSchema(schema);
            db.addTableForTest(tbl);
            esCatalog.addDatabaseForTest(db);
        }
    }

    @Test
    public void testNormalCase() throws Exception {
        String createCatalogSql = "CREATE CATALOG hms_catalog "
                + "properties( \"type\" = \"hms\", \"hive.metastore.uris\"=\"thrift://localhost:9083\" )";
        CreateCatalogStmt createStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        mgr.createCatalog(createStmt);

        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(5, showResultSet.getResultRows().size());

        //test result order
        Assertions.assertEquals("es", showResultSet.getResultRows().get(0).get(1));
        Assertions.assertEquals("internal", showResultSet.getResultRows().get(4).get(1));

        showCatalogSql = "SHOW CATALOGS LIKE 'hms%'";
        showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(1, showResultSet.getResultRows().size());

        String alterCatalogNameSql = "ALTER CATALOG hms_catalog RENAME " + MY_CATALOG + ";";
        AlterCatalogNameStmt alterNameStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameSql);
        mgr.alterCatalogName(alterNameStmt);

        // test modify property
        String alterCatalogProps = "ALTER CATALOG " + MY_CATALOG + " SET PROPERTIES"
                + " (\"type\" = \"hms\", \"hive.metastore.uris\" = \"thrift://172.16.5.9:9083\");";
        AlterCatalogPropertyStmt alterPropStmt = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProps);
        mgr.alterCatalogProps(alterPropStmt);

        CatalogIf catalog = env.getCatalogMgr().getCatalog(MY_CATALOG);
        Assert.assertEquals(2, catalog.getProperties().size());
        Assert.assertEquals("thrift://172.16.5.9:9083", catalog.getProperties().get("hive.metastore.uris"));

        // test add property
        Map<String, String> alterProps2 = Maps.newHashMap();
        alterProps2.put("dfs.nameservices", "service1");
        alterProps2.put("dfs.ha.namenodes.service1", "nn1,nn2");
        AlterCatalogPropertyStmt alterStmt = new AlterCatalogPropertyStmt(MY_CATALOG, alterProps2);
        mgr.alterCatalogProps(alterStmt);
        catalog = env.getCatalogMgr().getCatalog(MY_CATALOG);
        Assert.assertEquals(4, catalog.getProperties().size());
        Assert.assertEquals("service1", catalog.getProperties().get("dfs.nameservices"));

        String showDetailCatalog = "SHOW CATALOG my_catalog";
        ShowCatalogStmt showDetailStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showDetailCatalog);
        showResultSet = mgr.showCatalogs(showDetailStmt);

        Assert.assertEquals(4, showResultSet.getResultRows().size());
        for (List<String> row : showResultSet.getResultRows()) {
            Assertions.assertEquals(2, row.size());
            if (row.get(0).equalsIgnoreCase("type")) {
                Assertions.assertEquals("hms", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("dfs.ha.namenodes.service1")) {
                Assertions.assertEquals("nn1,nn2", row.get(1));
            }
        }

        testCatalogMgrPersist();

        String dropCatalogSql = "DROP CATALOG " + MY_CATALOG;
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) parseAndAnalyzeStmt(dropCatalogSql);
        mgr.dropCatalog(dropCatalogStmt);

        showCatalogSql = "SHOW CATALOGS";
        showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(4, showResultSet.getResultRows().size());
    }

    private void testCatalogMgrPersist() throws Exception {
        File file = new File("./CatalogMgrTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        mgr.write(dos);
        dos.flush();
        dos.close();

        CatalogIf internalCatalog = mgr.getCatalog(InternalCatalog.INTERNAL_DS_ID);
        CatalogIf internalCatalog2 = mgr.getInternalCatalog();
        Assert.assertTrue(internalCatalog == internalCatalog2);
        CatalogIf myCatalog = mgr.getCatalog(MY_CATALOG);
        Assert.assertNotNull(myCatalog);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        CatalogMgr mgr2 = CatalogMgr.read(dis);

        Assert.assertEquals(5, mgr2.listCatalogs().size());
        Assert.assertEquals(myCatalog.getId(), mgr2.getCatalog(MY_CATALOG).getId());
        Assert.assertEquals(0, mgr2.getInternalCatalog().getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalCatalog.INTERNAL_DS_ID).getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME).getId());

        EsExternalCatalog esExternalCatalog = (EsExternalCatalog) mgr2.getCatalog("es");
        Assert.assertNotNull(esExternalCatalog);
        Map<String, String> properties = esExternalCatalog.getCatalogProperty().getProperties();
        Assert.assertEquals("user1", properties.get(EsExternalCatalog.PROP_USERNAME));
        Assert.assertEquals("http://192.168.0.1", properties.get(EsExternalCatalog.PROP_HOSTS));
        Assert.assertEquals("user1", esExternalCatalog.getUsername());
        Assert.assertEquals("http://192.168.0.1", esExternalCatalog.getNodes()[0]);

        CatalogIf hms = mgr2.getCatalog(MY_CATALOG);
        properties = hms.getProperties();
        Assert.assertEquals(4, properties.size());
        Assert.assertEquals("hms", properties.get("type"));
        Assert.assertEquals("thrift://172.16.5.9:9083", properties.get("hive.metastore.uris"));

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testSwitchCommand() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        // user1 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalCatalog.INTERNAL_CATALOG_NAME + ";", user1Ctx);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user1Ctx.getDefaultCatalog());
        // user1 can't switch to hive
        try {
            parseAndAnalyzeStmt("switch hive;", user1Ctx);
            Assert.fail("user1 switch to hive with no privilege.");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user1' to catalog 'hive'");
        }
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user1Ctx.getDefaultCatalog());

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        // user2 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalCatalog.INTERNAL_CATALOG_NAME + ";", user2Ctx);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user2Ctx.getDefaultCatalog());
        // user2 can switch to hive
        SwitchStmt switchHive = (SwitchStmt) parseAndAnalyzeStmt("switch hive;", user2Ctx);
        env.changeCatalog(user2Ctx, switchHive.getCatalogName());
        Assert.assertEquals(user2Ctx.getDefaultCatalog(), "hive");
        // user2 can grant select_priv to tpch.customer
        GrantStmt user2GrantHiveTable = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on tpch.customer to 'user2'@'%';", user2Ctx);
        auth.grant(user2GrantHiveTable);
    }

    @Test
    public void testShowCatalogStmtWithPrivileges() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        ShowCatalogStmt user1Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs;", user1Ctx);
        List<List<String>> user1ShowResult = env.getCatalogMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(1), InternalCatalog.INTERNAL_CATALOG_NAME);
        Assert.assertEquals(user1ShowResult.get(0).get(0), String.valueOf(InternalCatalog.INTERNAL_DS_ID));

        // have privilege and match
        user1Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs like 'inter%';", user1Ctx);
        user1ShowResult = env.getCatalogMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(1), InternalCatalog.INTERNAL_CATALOG_NAME);
        Assert.assertEquals(user1ShowResult.get(0).get(0), String.valueOf(InternalCatalog.INTERNAL_DS_ID));

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        ShowCatalogStmt user2Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs;", user2Ctx);
        List<List<String>> user2ShowResult = env.getCatalogMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 2);
        Assert.assertTrue(user2ShowResult.stream().map(l -> l.get(1)).anyMatch(c -> c.equals("hive")));

        // have privilege but not match
        user2Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs like 'ice%';", user2Ctx);
        user2ShowResult = env.getCatalogMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 0);

        // access denied
        ShowCatalogStmt user2ShowHive = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalog hive;", user2Ctx);
        List<List<String>> user2ShowHiveResult = env.getCatalogMgr().showCatalogs(user2ShowHive).getResultRows();
        Assert.assertTrue(
                user2ShowHiveResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive.metastore.uris")));
        try {
            env.getCatalogMgr()
                    .showCatalogs((ShowCatalogStmt) parseAndAnalyzeStmt("show catalog iceberg;", user2Ctx));
            Assert.fail("");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user2' to catalog 'iceberg'");
        }
    }

}
