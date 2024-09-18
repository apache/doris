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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
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
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');");

        dorisAssert = new DorisAssert(ctx);
        dorisAssert.useDatabase("db1");

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db1");
        Assert.assertNotNull(db);

        // create alias function
        String createFuncStr
                = "create alias function db1.id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr,
                ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

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
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);

        queryStr = "select db1.id_masking(k1) from db1.tbl1";
        Assert.assertTrue(containsIgnoreCase(dorisAssert.query(queryStr).explainQuery(),
                "concat(left(CAST(CAST(k1 as BIGINT) AS VARCHAR(65533)), 3), '****',"
                        + " right(CAST(CAST(k1 AS BIGINT) AS VARCHAR(65533)), 4))"));
    }

    @Test
    public void testCreateGlobalFunction() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableFoldConstantByBe(false);

        // 1. create database db2
        createDatabase(ctx, "create database db2;");

        createTable("create table db2.tbl1(k1 int, k2 bigint, k3 varchar(10), k4 char(5)) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');");

        dorisAssert = new DorisAssert(ctx);
        dorisAssert.useDatabase("db2");

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db2");
        Assert.assertNotNull(db);

        // 2. create global function

        String createFuncStr
                = "create global alias function id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr,
                ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        List<Function> functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assert.assertEquals(1, functions.size());

        String queryStr = "select id_masking(13888888888);";
        testFunctionQuery(ctx, queryStr, false);

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
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        if (isStringLiteral) {
            Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);
        } else {
            Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);
        }
    }

    private void createTable(String createTblStmtStr) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr,
                connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private void createDatabase(ConnectContext ctx, String createDbStmtStr) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
    }

    private boolean containsIgnoreCase(String str, String sub) {
        return str.toLowerCase().contains(sub.toLowerCase());
    }
}
