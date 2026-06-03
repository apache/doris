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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

/*
 * Author: Chenmingyu
 * Date: Feb 16, 2020
 */

public class CreateFunctionTest {

    private static String runningDir = "fe/mocked/CreateFunctionTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        FeConstants.runningUnitTest = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        Env.getCurrentEnv().getWorkloadGroupMgr().createNormalWorkloadGroupForUT();
    }

    @AfterClass
    public static void teardown() {
        File file = new File("fe/mocked/CreateFunctionTest/");
        file.delete();
    }

    @Test
    public void test() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // create database db1
        createDatabase(ctx, "create database db1;");

        createTable("create table db1.tbl1(k1 int, k2 bigint, k3 varchar(10), k4 char(5)) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');", ctx);

        dorisAssert = new DorisAssert(ctx);
        dorisAssert.useDatabase("db1");

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db1");
        Assert.assertNotNull(db);

        // create alias function
        String createFuncStr
                = "create alias function db1.id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        createFunction(createFuncStr, ctx);

        List<Function> functions = db.getFunctions();
        Assert.assertEquals(1, functions.size());

        String queryStr = "select db1.id_masking(13888888888);";
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Planner planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        PlanFragment fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        UnionNode unionNode = (UnionNode) fragment.getPlanRoot();
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "materializedConstExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);

        queryStr = "select db1.id_masking(k1) from db1.tbl1";
        Assert.assertTrue(containsIgnoreCase(dorisAssert.query(queryStr).explainQuery(),
                "concat(left(CAST(CAST(k1 as BIGINT) AS VARCHAR(65533)), 3), '****',"
                        + " right(CAST(CAST(k1 AS BIGINT) AS VARCHAR(65533)), 4))"));

        String pythonUdfSql = "create function db1.py_stable(int) returns int "
                + "properties('type'='PYTHON_UDF', 'symbol'='evaluate', "
                + "'runtime_version'='3.10.2', 'volatility'='stable');";
        createFunction(pythonUdfSql, ctx);
        Assert.assertEquals(2, db.getFunctions().size());
        Function pythonFn = findFunction(db, "py_stable");
        Assert.assertEquals(FunctionVolatility.STABLE, pythonFn.getVolatility());
        Assert.assertTrue(FunctionToSqlConverter.toSql(pythonFn, false).contains("\"VOLATILITY\"=\"stable\""));

        String defaultVolatileSql = "create function db1.py_default(int) returns int "
                + "properties('type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2');";
        createFunction(defaultVolatileSql, ctx);
        Assert.assertEquals(FunctionVolatility.VOLATILE, findFunction(db, "py_default").getVolatility());

        String defaultImmutableUdafSql = "create aggregate function db1.py_agg_default(int) returns int "
                + "properties('type'='PYTHON_UDF', 'symbol'='Agg', 'runtime_version'='3.10.2');";
        createFunction(defaultImmutableUdafSql, ctx);
        Assert.assertEquals(FunctionVolatility.IMMUTABLE, findFunction(db, "py_agg_default").getVolatility());

        String stableUdtfSql = "create tables function db1.py_table_stable(int) returns array<int> "
                + "properties('type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2', "
                + "'volatility'='stable');";
        createFunction(stableUdtfSql, ctx);
        Assert.assertEquals(FunctionVolatility.STABLE, findFunction(db, "py_table_stable").getVolatility());
    }

    @Test
    public void testCreatePythonFunctionRejectsObjectTypes() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        createDatabase(ctx, "create database py_obj_type_db;");
        dorisAssert = new DorisAssert(ctx);
        dorisAssert.useDatabase("py_obj_type_db");

        assertCreateFunctionAnalysisException(ctx, "create function py_obj_type_db.py_bitmap_arg(bitmap) returns int "
                + "properties('type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2');",
                "PYTHON_UDF does not support argument 1 type bitmap");
        assertCreateFunctionAnalysisException(ctx, "create function py_obj_type_db.j_bitmap_arg(bitmap) returns int "
                + "properties('type'='JAVA_UDF', 'symbol'='evaluate');",
                "JAVA_UDF does not support argument 1 type bitmap");
        assertCreateFunctionAnalysisException(ctx, "create function py_obj_type_db.py_hll_ret(int) returns hll "
                + "properties('type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2');",
                "PYTHON_UDF does not support return type hll");
        assertCreateFunctionAnalysisException(ctx, "create aggregate function py_obj_type_db.py_quantile_arg"
                + "(quantile_state) returns int properties('type'='PYTHON_UDF', 'symbol'='Agg', "
                + "'runtime_version'='3.10.2');",
                "PYTHON_UDF does not support argument 1 type quantile_state");
        assertCreateFunctionAnalysisException(ctx, "create aggregate function py_obj_type_db.j_quantile_arg"
                + "(quantile_state) returns int properties('type'='JAVA_UDF', 'symbol'='Agg');",
                "JAVA_UDF does not support argument 1 type quantile_state");
        assertCreateFunctionAnalysisException(ctx, "create tables function py_obj_type_db.py_bitmap_table(int) "
                + "returns array<bitmap> properties('type'='PYTHON_UDF', 'symbol'='evaluate', "
                + "'runtime_version'='3.10.2');",
                "ARRAY unsupported sub-type: bitmap");
    }

    @Test
    public void testCreateGlobalFunction() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableFoldConstantByBe(false);

        // 1. create database db2
        createDatabase(ctx, "create database db2;");

        createTable("create table db2.tbl1(k1 int, k2 bigint, k3 varchar(10), k4 char(5)) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');", ctx);

        dorisAssert = new DorisAssert(ctx);
        dorisAssert.useDatabase("db2");

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db2");
        Assert.assertNotNull(db);

        // 2. create global function

        String createFuncStr
                = "create global alias function id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        createFunction(createFuncStr, ctx);
        List<Function> functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assert.assertEquals(1, functions.size());

        String queryStr = "select id_masking(13888888888);";
        testFunctionQuery(ctx, queryStr, true);

        queryStr = "select id_masking(k1) from db2.tbl1";
        Assert.assertTrue(containsIgnoreCase(dorisAssert.query(queryStr).explainQuery(),
                "concat(left(CAST(CAST(k1 as BIGINT) AS VARCHAR(65533)), 3), '****',"
                        + " right(CAST(CAST(k1 AS BIGINT) AS VARCHAR(65533)), 4))"));
    }

    private void testFunctionQuery(ConnectContext ctx, String queryStr, Boolean isStringLiteral) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Planner planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        PlanFragment fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        UnionNode unionNode = (UnionNode) fragment.getPlanRoot();
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "materializedConstExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        if (isStringLiteral) {
            Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);
        } else {
            Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);
        }
    }

    private void createTable(String sql, ConnectContext connectContext) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setStatementContext(new StatementContext());
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private void createDatabase(ConnectContext ctx, String createDbStmtStr) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(ctx, stmtExecutor);
        }

        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
    }

    private void createFunction(String sql, ConnectContext connectContext) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setStatementContext(new StatementContext());
        if (parsed instanceof CreateFunctionCommand) {
            ((CreateFunctionCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private void assertCreateFunctionAnalysisException(ConnectContext ctx, String sql, String message) {
        Exception exception = Assert.assertThrows(Exception.class, () -> createFunction(sql, ctx));
        Assert.assertTrue("Expected error to contain: " + message + ", actual: " + exception.getMessage(),
                exception.getMessage().contains(message));
    }

    private boolean containsIgnoreCase(String str, String sub) {
        return str.toLowerCase().contains(sub.toLowerCase());
    }

    private Function findFunction(Database db, String functionName) {
        for (Function function : db.getFunctions()) {
            if (functionName.equals(function.functionName())) {
                return function;
            }
        }
        throw new AssertionError("function not found: " + functionName);
    }
}
