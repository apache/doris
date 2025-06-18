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
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ResourceTypeEnum;
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
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.trees.plans.commands.ShowTableStatusCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    public void testShowTableStatusPrivs() throws Exception {
        ConnectContext root = createCtx(UserIdentity.ROOT, "127.0.0.1");
        CreateUserStmt createUserStmt = (CreateUserStmt) parseAndAnalyzeStmt("create user show_table_status"
                + " identified by '123456'", root);
        auth.createUser(createUserStmt);
        GrantStmt grant = (GrantStmt) parseAndAnalyzeStmt(
                "grant select_priv on test2.*.* to show_table_status;", root);
        auth.grant(grant);

        // show table status from test2.db1 LIKE "%tbl%
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("show_table_status", "%");
        ConnectContext userCtx = createCtx(user, "127.0.0.1");
        ShowTableStatusCommand command = new ShowTableStatusCommand("db1", "test2", "%tbl%", null);
        ShowResultSet resultSet = command.doRun(userCtx, new StmtExecutor(userCtx, ""));
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
            public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
                return false;
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
            public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
                return false;
            }

            @Override
            public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName,
                    PrivPredicate wanted) {
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

            @Override
            public boolean checkCloudPriv(UserIdentity currentUser, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
                return false;
            }

            @Override
            public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName,
                    PrivPredicate wanted) {
                return false;
            }

            @Override
            public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db,
                    String tbl, String col) {
                return Optional.empty();
            }

            @Override
            public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl,
                    String db, String tbl) {
                return null;
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
