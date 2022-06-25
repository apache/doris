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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalDataSource;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class SwitchStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;
    private static String clusterName = "default_cluster";

    private static PaloAuth auth;
    private static Catalog catalog;
    private static UserIdentity user1;
    private static UserIdentity user2;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_multi_catalog = true;
        UtFrameUtils.createDorisCluster(runningDir);

        // use root to initialize.
        ConnectContext rootCtx = UtFrameUtils.createDefaultCtx();
        catalog = Catalog.getCurrentCatalog();
        auth = catalog.getAuth();

        // grant with no catalog is switched, internal catalog works.
        CreateRoleStmt createRole1 = (CreateRoleStmt) UtFrameUtils.parseAndAnalyzeStmt("create role role1;", rootCtx);
        auth.createRole(createRole1);
        GrantStmt grantRole1 = (GrantStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "grant grant_priv on tpch.* to role 'role1';", rootCtx);
        auth.grant(grantRole1);
        // user1 can't switch to hive
        auth.createUser((CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "create user 'user1'@'%' identified by 'pwd1' default role 'role1';", rootCtx));
        user1 = new UserIdentity("user1", "%");
        user1.analyze(clusterName);

        // create catalog
        CreateCatalogStmt hiveCatalog = (CreateCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "create catalog hive properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        catalog.getDataSourceMgr().createCatalog(hiveCatalog);
        CreateCatalogStmt iceBergCatalog = (CreateCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "create catalog iceberg properties('type' = 'hms', 'iceberg.hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        catalog.getDataSourceMgr().createCatalog(iceBergCatalog);

        // switch to hive.
        SwitchStmt switchHive = (SwitchStmt) UtFrameUtils.parseAndAnalyzeStmt("switch hive;", rootCtx);
        catalog.changeCatalog(rootCtx, switchHive.getCatalogName());
        CreateRoleStmt createRole2 = (CreateRoleStmt) UtFrameUtils.parseAndAnalyzeStmt("create role role2;", rootCtx);
        auth.createRole(createRole2);
        GrantStmt grantRole2 = (GrantStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "grant grant_priv on tpch.customer to role 'role2';", rootCtx);
        auth.grant(grantRole2);
        auth.createUser((CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "create user 'user2'@'%' identified by 'pwd2' default role 'role2';", rootCtx));
        user2 = new UserIdentity("user2", "%");
        user2.analyze(clusterName);
    }

    @Test
    public void testSwitchCommand() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = UtFrameUtils.createDefaultCtx(user1, "127.0.0.1");
        // user1 can switch to internal catalog
        UtFrameUtils.parseAndAnalyzeStmt("switch " + InternalDataSource.INTERNAL_DS_NAME + ";", user1Ctx);
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user1Ctx.getDefaultCatalog());
        // user1 can't switch to hive
        try {
            UtFrameUtils.parseAndAnalyzeStmt("switch hive;", user1Ctx);
            Assert.fail("user1 switch to hive with no privilege.");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user1' to catalog 'hive'");
        }
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user1Ctx.getDefaultCatalog());

        // mock the login of user2
        ConnectContext user2Ctx = UtFrameUtils.createDefaultCtx(user2, "127.0.0.1");
        // user2 can switch to internal catalog
        UtFrameUtils.parseAndAnalyzeStmt("switch " + InternalDataSource.INTERNAL_DS_NAME + ";", user2Ctx);
        Assert.assertEquals(InternalDataSource.INTERNAL_DS_NAME, user2Ctx.getDefaultCatalog());
        // user2 can switch to hive
        SwitchStmt switchHive = (SwitchStmt) UtFrameUtils.parseAndAnalyzeStmt("switch hive;", user2Ctx);
        catalog.changeCatalog(user2Ctx, switchHive.getCatalogName());
        Assert.assertEquals(user2Ctx.getDefaultCatalog(), "hive");
        // user2 can grant select_priv to tpch.customer
        GrantStmt user2GrantHiveTable = (GrantStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "grant select_priv on tpch.customer to 'user2'@'%';", user2Ctx);
        auth.grant(user2GrantHiveTable);
    }

    @Test
    public void testShowCatalogStmtWithPrivileges() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = UtFrameUtils.createDefaultCtx(user1, "127.0.0.1");
        ShowCatalogStmt user1Show = (ShowCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt("show catalogs;", user1Ctx);
        List<List<String>> user1ShowResult = catalog.getDataSourceMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(0), InternalDataSource.INTERNAL_DS_NAME);

        // mock the login of user1
        ConnectContext user2Ctx = UtFrameUtils.createDefaultCtx(user2, "127.0.0.1");
        ShowCatalogStmt user2Show = (ShowCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt("show catalogs;", user2Ctx);
        List<List<String>> user2ShowResult = catalog.getDataSourceMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 2);
        Assert.assertTrue(user2ShowResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive")));

        // access denied
        ShowCatalogStmt user2ShowHive = (ShowCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt("show catalog hive;",
                user2Ctx);
        List<List<String>> user2ShowHiveResult = catalog.getDataSourceMgr().showCatalogs(user2ShowHive).getResultRows();
        Assert.assertTrue(
                user2ShowHiveResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive.metastore.uris")));
        try {
            catalog.getDataSourceMgr().showCatalogs(
                    (ShowCatalogStmt) UtFrameUtils.parseAndAnalyzeStmt("show catalog iceberg;", user2Ctx));
            Assert.fail("");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'default_cluster:user2' to catalog 'iceberg'");
        }
    }
}
