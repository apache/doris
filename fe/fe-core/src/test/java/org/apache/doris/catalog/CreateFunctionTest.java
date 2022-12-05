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
import org.apache.doris.common.Config;
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
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        String createTblStmtStr = "create table db1.tbl1(k1 int, k2 bigint, k3 varchar(10), k4 char(5)) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);

        dorisAssert = new DorisAssert();
        dorisAssert.useDatabase("db1");

        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:db1");
        Assert.assertNotNull(db);

        String createFuncStr = "create function db1.my_add(VARCHAR(1024)) RETURNS BOOLEAN properties\n"
                + "(\n"
                + "\"symbol\" =  \"_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_9StringValE\",\n"
                + "\"prepare_fn\" = \"_ZN9doris_udf13AddUdfPrepareEPNS_15FunctionContextENS0_18FunctionStateScopeE\",\n"
                + "\"close_fn\" = \"_ZN9doris_udf11AddUdfCloseEPNS_15FunctionContextENS0_18FunctionStateScopeE\",\n"
                + "\"object_file\" = \"http://127.0.0.1:8008/libcmy_udf.so\"\n"
                + ");";

        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        List<Function> functions = db.getFunctions();
        Assert.assertEquals(1, functions.size());
        Assert.assertTrue(functions.get(0).isUdf());

        String queryStr = "select db1.my_add(null)";
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Planner planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        PlanFragment fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        UnionNode unionNode =  (UnionNode) fragment.getPlanRoot();
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);

        // create alias function
        createFuncStr = "create alias function db1.id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        functions = db.getFunctions();
        Assert.assertEquals(2, functions.size());

        queryStr = "select db1.id_masking(13888888888);";
        ctx.getState().reset();
        stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        unionNode =  (UnionNode) fragment.getPlanRoot();
        constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);

        queryStr = "select db1.id_masking(k1) from db1.tbl1";
        Assert.assertTrue(dorisAssert.query(queryStr).explainQuery().contains("concat(left(`k1`, 3), '****', right(`k1`, 4))"));

        // create alias function with cast
        // cast any type to decimal with specific precision and scale
        createFuncStr = "create alias function db1.decimal(all, int, int) with parameter(col, precision, scale)"
                + " as cast(col as decimal(precision, scale));";
        createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        functions = db.getFunctions();
        Assert.assertEquals(3, functions.size());

        queryStr = "select db1.decimal(333, 4, 1);";
        ctx.getState().reset();
        stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        unionNode =  (UnionNode) fragment.getPlanRoot();
        constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        System.out.println(constExprLists.get(0).get(0));
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);

        queryStr = "select db1.decimal(k3, 4, 1) from db1.tbl1;";
        if (Config.enable_decimal_conversion) {
            Assert.assertTrue(dorisAssert.query(queryStr).explainQuery().contains("CAST(`k3` AS DECIMALV3(4,1))"));
        } else {
            Assert.assertTrue(dorisAssert.query(queryStr).explainQuery().contains("CAST(`k3` AS DECIMAL(4,1))"));
        }

        // cast any type to varchar with fixed length
        createFuncStr = "create alias function db1.varchar(all, int) with parameter(text, length) as "
                + "cast(text as varchar(length));";
        createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        functions = db.getFunctions();
        Assert.assertEquals(4, functions.size());

        queryStr = "select db1.varchar(333, 4);";
        ctx.getState().reset();
        stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        unionNode =  (UnionNode) fragment.getPlanRoot();
        constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);

        queryStr = "select db1.varchar(k1, 4) from db1.tbl1;";
        Assert.assertTrue(dorisAssert.query(queryStr).explainQuery().contains("CAST(`k1` AS CHARACTER)"));

        // cast any type to char with fixed length
        createFuncStr = "create alias function db1.char(all, int) with parameter(text, length) as "
                + "cast(text as char(length));";
        createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        functions = db.getFunctions();
        Assert.assertEquals(5, functions.size());

        queryStr = "select db1.char(333, 4);";
        ctx.getState().reset();
        stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assert.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        planner = stmtExecutor.planner();
        Assert.assertEquals(1, planner.getFragments().size());
        fragment = planner.getFragments().get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        unionNode =  (UnionNode) fragment.getPlanRoot();
        constExprLists = Deencapsulation.getField(unionNode, "constExprLists");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof StringLiteral);

        queryStr = "select db1.char(k1, 4) from db1.tbl1;";
        Assert.assertTrue(dorisAssert.query(queryStr).explainQuery().contains("CAST(`k1` AS CHARACTER)"));
    }
}
