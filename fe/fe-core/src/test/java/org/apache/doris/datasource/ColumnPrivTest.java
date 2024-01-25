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
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.analysis.ShowTableStatusStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog.TestCatalogProvider;
import org.apache.doris.mysql.privilege.AccessControllerFactory;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

// when `select` suppport `col auth`,will open ColumnPrivTest
@Disabled
public class ColumnPrivTest extends TestWithFeService {
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
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.ColumnPrivTest$MockedCatalogProvider\",\n"
                        + "    \"access_controller.class\" "
                        + "= \"org.apache.doris.datasource.ColumnPrivTest$TestAccessControllerFactory\",\n"
                        + "    \"access_controller.properties.key1\" = \"val1\",\n"
                        + "    \"access_controller.properties.key2\" = \"val2\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);

        CreateCatalogStmt testCatalog2 = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog test2 properties(\n"
                        + "    \"type\" = \"test\",\n"
                        + "    \"catalog_provider.class\" "
                        + "= \"org.apache.doris.datasource.ColumnPrivTest$MockedCatalogProvider\",\n"
                        + "    \"access_controller.properties.key1\" = \"val1\",\n"
                        + "    \"access_controller.properties.key2\" = \"val2\"\n"
                        + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog2);

        // 2. create internal db and tbl
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt("create database innerdb1");
        env.createDb(createDbStmt);
        createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt("create database innerdb2");
        env.createDb(createDbStmt);

        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(
                "create table innerdb1.innertbl11\n"
                        + "(\n"
                        + "    col1 int, \n"
                        + "    col2 string\n"
                        + ")\n"
                        + "distributed by hash(col1) buckets 1\n"
                        + "properties(\"replication_num\" = \"1\");", rootCtx);
        env.createTable(createTableStmt);

        createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(
                "create table innerdb1.innertbl12\n"
                        + "(\n"
                        + "    col3 int, \n"
                        + "    col4 string\n"
                        + ")\n"
                        + "distributed by hash(col3) buckets 1\n"
                        + "properties(\"replication_num\" = \"1\");", rootCtx);
        env.createTable(createTableStmt);

        createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(
                "create table innerdb2.innertbl21\n"
                        + "(\n"
                        + "    col5 int, \n"
                        + "    col6 string\n"
                        + ")\n"
                        + "distributed by hash(col5) buckets 1\n"
                        + "properties(\"replication_num\" = \"1\");", rootCtx);
        env.createTable(createTableStmt);
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        Assert.assertTrue(env.getAccessManager().checkIfAccessControllerExist("test1"));
        DropCatalogStmt stmt = (DropCatalogStmt) parseAndAnalyzeStmt("drop catalog test1");
        env.getCatalogMgr().dropCatalog(stmt);
        Assert.assertFalse(env.getAccessManager().checkIfAccessControllerExist("test1"));
    }

    @Test
    public void testColumnPrivs() throws Exception {
        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(3, showResultSet.getResultRows().size());

        CreateRoleStmt createRole1 = (CreateRoleStmt) parseAndAnalyzeStmt("create role role1;", rootCtx);
        auth.createRole(createRole1);
        GrantStmt grantRole = (GrantStmt) parseAndAnalyzeStmt("grant select_priv on test1.*.* to role 'role1';",
                rootCtx);
        auth.grant(grantRole);
        grantRole = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on internal.innerdb1.innertbl11 to role 'role1';", rootCtx);
        auth.grant(grantRole);
        grantRole = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on internal.innerdb1.v1 to role 'role1';", rootCtx);
        auth.grant(grantRole);
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user1'@'%' identified by 'pwd1' default role 'role1';", rootCtx));
        // create a view
        CreateViewStmt viewStmt = (CreateViewStmt) parseAndAnalyzeStmt(
                "create view innerdb1.v1 as select * from test1.db1.tbl11", rootCtx);
        env.createView(viewStmt);

        // Now we have
        // internal.innerdb1
        //      innertbl11: col1(int), col2(string)
        //      innertbl12: col2(int), col4(string)
        // internal.innerdb2
        //      innertbl21: col5(int), col6(string)
        // test1.db1
        //      tbl11:  a11(bigint), a12(string), a13(float)
        //      tbl12:  b21(bigint), b22(string), b23(float)
        // test1.db2
        //      tbl21:  c11(bigint), c12(string), c13(float)

        UserIdentity user1 = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");

        // 1. query inner table
        testSql(user1Ctx, "select * from innerdb1.innertbl11", "0:VOlapScanNode");
        // 2. query external table, without a11 column priv
        testSql(user1Ctx, "select * from test1.db1.tbl11", "Access deny to column a11");
        // 3. query external table, not query column a12
        testSql(user1Ctx, "select a12 from test1.db1.tbl11", "TABLE: tbl11");
        // change to test1.db1
        user1Ctx.changeDefaultCatalog("test1");
        user1Ctx.setDatabase("db1");
        testSql(user1Ctx, "select a12 from tbl11 where a11 > 0;", "Access deny to column a11");
        testSql(user1Ctx, "select sum(a13) from db1.tbl11 group by a11;", "Access deny to column a11");
        testSql(user1Ctx, "select sum(a13) x from test1.db1.tbl11 group by a11 having x > 0;",
                "Access deny to column a11");
        testSql(user1Ctx, "select a12 from tbl11 where abs(a11) > 0;", "Access deny to column a11");
        // TODO: how to handle count(*) when setting column privilege?
        // testSql(user1Ctx, "select count(*) from tbl11;", "Access deny to column a11");

        // change to internal.innerdb1
        user1Ctx.changeDefaultCatalog("internal");
        user1Ctx.setDatabase("innerdb1");
        testSql(user1Ctx, "select sum(a13) x from test1.db1.tbl11 group by a11 having x > 0;",
                "Access deny to column a11");
        testSql(user1Ctx, "with cte1 as (select a11 from test1.db1.tbl11) select * from cte1;",
                "Access deny to column a11");
        testSql(user1Ctx, "select a12 from (select * from test1.db1.tbl11) x", "TABLE: tbl11");
        testSql(user1Ctx, "select * from v1", "Access deny to column a11");

        testSql(user1Ctx, "select * from numbers(\"number\" = \"1\");", "0:VDataGenScanNode");
    }

    @Test
    public void testShowTableStatusPrivs() throws Exception {
        ConnectContext root = createCtx(UserIdentity.ROOT, "127.0.0.1");
        CreateUserStmt createUserStmt = (CreateUserStmt) parseAndAnalyzeStmt("create user show_table_status"
                + " identified by '123456'", root);
        auth.createUser(createUserStmt);
        GrantStmt grant = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on test2.*.* to show_table_status;", root);
        auth.grant(grant);

        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("show_table_status", "%");
        ConnectContext userCtx = createCtx(user, "127.0.0.1");

        ShowTableStatusStmt stmt = (ShowTableStatusStmt) parseAndAnalyzeStmt(
                "show table status from test2.db1 LIKE \"%tbl%\";");
        ShowExecutor executor = new ShowExecutor(userCtx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals(2, resultSet.getResultRows().size());
    }

    private void testSql(ConnectContext ctx, String sql, String expectedMsg) throws Exception {
        String res = getSQLPlanOrErrorMsg(ctx, "explain " + sql, false);
        System.out.println(res);
        Assert.assertTrue(res.contains(expectedMsg));
    }

    private void testShow(ConnectContext ctx, String sql, String expectedMsg) throws Exception {
        String res = getSQLPlanOrErrorMsg(ctx, "explain " + sql, false);
        System.out.println(res);
        Assert.assertTrue(res.contains(expectedMsg));
    }

    public static class TestAccessControllerFactory implements AccessControllerFactory {
        @Override
        public CatalogAccessController createAccessController(Map<String, String> prop) {
            return new TestAccessController(prop);
        }

        public static class TestAccessController implements CatalogAccessController {
            private Map<String, String> prop;

            public TestAccessController(Map<String, String> prop) {
                this.prop = prop;
            }

            @Override
            public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
                return false;
            }

            @Override
            public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
                return false;
            }

            @Override
            public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl,
                    PrivPredicate wanted) {
                if (ClusterNamespace.getNameFromFullName(currentUser.getQualifiedUser()).equals("user1")) {
                    if (ctl.equals("test1")) {
                        if (ClusterNamespace.getNameFromFullName(db).equals("db1")) {
                            if (tbl.equals("tbl11")) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }

            @Override
            public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
                    PrivPredicate wanted) throws AuthorizationException {
                if (currentUser.getQualifiedUser().contains("user1")) {
                    if (ctl.equals("test1")) {
                        if (db.equals("db1")) {
                            if (tbl.equals("tbl11")) {
                                if (cols.contains("a11")) {
                                    throw new AuthorizationException("Access deny to column a11");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static class MockedCatalogProvider implements TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            // db1
            tblSchemaMap1.put("tbl11", Lists.newArrayList(
                    new Column("a11", PrimitiveType.BIGINT),
                    new Column("a12", PrimitiveType.STRING),
                    new Column("a13", PrimitiveType.FLOAT)
            ));
            tblSchemaMap1.put("tbl12", Lists.newArrayList(
                    new Column("b21", PrimitiveType.BIGINT),
                    new Column("b22", PrimitiveType.STRING),
                    new Column("b23", PrimitiveType.FLOAT)
            ));
            MOCKED_META.put("db1", tblSchemaMap1);
            // db2
            Map<String, List<Column>> tblSchemaMap2 = Maps.newHashMap();
            tblSchemaMap2.put("tbl21", Lists.newArrayList(
                    new Column("c11", PrimitiveType.BIGINT),
                    new Column("c12", PrimitiveType.STRING),
                    new Column("c13", PrimitiveType.FLOAT)
            ));
            MOCKED_META.put("db2", tblSchemaMap2);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
