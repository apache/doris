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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableStatusCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
        String createStmt = "create catalog test1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.ColumnPrivTest$MockedCatalogProvider\",\n"
                + "    \"access_controller.class\" "
                + "= \"org.apache.doris.datasource.ColumnPrivTest$TestAccessControllerFactory\",\n"
                + "    \"access_controller.properties.key1\" = \"val1\",\n"
                + "    \"access_controller.properties.key2\" = \"val2\"\n"
                + ");";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        createStmt = "create catalog test2 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.ColumnPrivTest$MockedCatalogProvider\",\n"
                + "    \"access_controller.properties.key1\" = \"val1\",\n"
                + "    \"access_controller.properties.key2\" = \"val2\"\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }

        // 2. create internal db and tbl
        String createDbStmtStr = "create database innerdb1";
        logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

        createDbStmtStr = "create database innerdb2";
        logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

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
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop catalog test1");
        if (logicalPlan instanceof DropCatalogCommand) {
            ((DropCatalogCommand) logicalPlan).run(rootCtx, null);
        }
        Assert.assertFalse(env.getAccessManager().checkIfAccessControllerExist("test1"));
    }

    @Test
    public void testShowTableStatusPrivs() throws Exception {
        ConnectContext root = createCtx(UserIdentity.ROOT, "127.0.0.1");

        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create user show_table_status identified by '123456'";
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(root, sql);
        if (parsed instanceof CreateUserCommand) {
            ((CreateUserCommand) parsed).run(root, stmtExecutor);
        }

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle("grant select_priv on test2.*.* to show_table_status;");
        Assertions.assertTrue(logicalPlan1 instanceof GrantTablePrivilegeCommand);
        GrantTablePrivilegeCommand command1 = (GrantTablePrivilegeCommand) logicalPlan1;
        command1.validate();
        auth.grantTablePrivilegeCommand(command1);

        // show table status from test2.db1 LIKE "%tbl%
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("show_table_status", "%");
        ConnectContext userCtx = createCtx(user, "127.0.0.1");
        ShowTableStatusCommand command = new ShowTableStatusCommand("db1", "test2", "%tbl%", null);
        ShowResultSet resultSet = command.doRun(userCtx, new StmtExecutor(userCtx, ""));
        Assert.assertEquals(2, resultSet.getResultRows().size());
    }
}
