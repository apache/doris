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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

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

public class DatasourceMgrTest extends TestWithFeService {
    private DataSourceMgr mgr;
    private static final String MY_CATALOG = "my_catalog";

    private static PaloAuth auth;
    private static Env env;
    private static UserIdentity user1;
    private static UserIdentity user2;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_multi_catalog = true;
        FeConstants.runningUnitTest = true;
        mgr = Env.getCurrentEnv().getDataSourceMgr();

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
        env.getDataSourceMgr().createCatalog(hiveCatalog);
        CreateCatalogStmt iceBergCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog iceberg properties('type' = 'hms', 'iceberg.hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getDataSourceMgr().createCatalog(iceBergCatalog);

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

    @Test
    public void testNormalCase() throws Exception {
        String createCatalogSql = "CREATE CATALOG hms_catalog "
                + "properties( \"type\" = \"hms\", \"hive.metastore.uris\"=\"thrift://localhost:9083\" )";
        CreateCatalogStmt createStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        mgr.createCatalog(createStmt);

        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(4, showResultSet.getResultRows().size());

        String alterCatalogNameSql = "ALTER CATALOG hms_catalog RENAME " + MY_CATALOG + ";";
        AlterCatalogNameStmt alterNameStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameSql);
        mgr.alterCatalogName(alterNameStmt);

        String alterCatalogProps = "ALTER CATALOG " + MY_CATALOG + " SET PROPERTIES"
                + " (\"type\" = \"hms\", \"k\" = \"v\");";
        AlterCatalogPropertyStmt alterPropStmt = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProps);
        mgr.alterCatalogProps(alterPropStmt);

        String showDetailCatalog = "SHOW CATALOG my_catalog";
        ShowCatalogStmt showDetailStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showDetailCatalog);
        showResultSet = mgr.showCatalogs(showDetailStmt);

        for (List<String> row : showResultSet.getResultRows()) {
            Assertions.assertEquals(2, row.size());
            if (row.get(0).equalsIgnoreCase("type")) {
                Assertions.assertEquals("hms", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("k")) {
                Assertions.assertEquals("v", row.get(1));
            } else {
                Assertions.fail();
            }
        }

        testDataSourceMgrPersist();

        String dropCatalogSql = "DROP CATALOG " + MY_CATALOG;
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) parseAndAnalyzeStmt(dropCatalogSql);
        mgr.dropCatalog(dropCatalogStmt);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(3, showResultSet.getResultRows().size());
    }

    private void testDataSourceMgrPersist() throws Exception {
        File file = new File("./CatalogMgrTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        mgr.write(dos);
        dos.flush();
        dos.close();

        DataSourceIf internalCatalog = mgr.getCatalog(InternalDataSource.INTERNAL_DS_ID);
        DataSourceIf internalCatalog2 = mgr.getInternalDataSource();
        Assert.assertTrue(internalCatalog == internalCatalog2);
        DataSourceIf myCatalog = mgr.getCatalog(MY_CATALOG);
        Assert.assertNotNull(myCatalog);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        DataSourceMgr mgr2 = DataSourceMgr.read(dis);

        Assert.assertEquals(4, mgr2.listCatalogs().size());
        Assert.assertEquals(myCatalog.getId(), mgr2.getCatalog(MY_CATALOG).getId());
        Assert.assertEquals(0, mgr2.getInternalDataSource().getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalDataSource.INTERNAL_DS_ID).getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalDataSource.INTERNAL_DS_NAME).getId());

        DataSourceIf hms = mgr2.getCatalog(MY_CATALOG);
        Map<String, String> properties = hms.getProperties();
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals("hms", properties.get("type"));
        Assert.assertEquals("v", properties.get("k"));

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testSwitchCommand() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        // user1 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalDataSource.INTERNAL_DS_NAME + ";", user1Ctx);
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user1Ctx.getDefaultCatalog());
        // user1 can't switch to hive
        try {
            parseAndAnalyzeStmt("switch hive;", user1Ctx);
            Assert.fail("user1 switch to hive with no privilege.");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user1' to catalog 'hive'");
        }
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user1Ctx.getDefaultCatalog());

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        // user2 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalDataSource.INTERNAL_DS_NAME + ";", user2Ctx);
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user2Ctx.getDefaultCatalog());
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
        List<List<String>> user1ShowResult = env.getDataSourceMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(1), InternalDataSource.INTERNAL_DS_NAME);
        Assert.assertEquals(user1ShowResult.get(0).get(0), String.valueOf(InternalDataSource.INTERNAL_DS_ID));

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        ShowCatalogStmt user2Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs;", user2Ctx);
        List<List<String>> user2ShowResult = env.getDataSourceMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 2);
        Assert.assertTrue(user2ShowResult.stream().map(l -> l.get(1)).anyMatch(c -> c.equals("hive")));

        // access denied
        ShowCatalogStmt user2ShowHive = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalog hive;", user2Ctx);
        List<List<String>> user2ShowHiveResult = env.getDataSourceMgr().showCatalogs(user2ShowHive).getResultRows();
        Assert.assertTrue(
                user2ShowHiveResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive.metastore.uris")));
        try {
            env.getDataSourceMgr()
                    .showCatalogs((ShowCatalogStmt) parseAndAnalyzeStmt("show catalog iceberg;", user2Ctx));
            Assert.fail("");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user2' to catalog 'iceberg'");
        }
    }
}
