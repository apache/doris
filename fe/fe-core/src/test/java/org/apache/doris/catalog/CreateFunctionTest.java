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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
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

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        FeConstants.runningUnitTest = true;
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
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        System.out.println(Catalog.getCurrentCatalog().getDbNames());

        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:db1");
        Assert.assertNotNull(db);

        String createFuncStr = "create function db1.my_add(VARCHAR(1024)) RETURNS BOOLEAN properties\n" +
                "(\n" +
                "\"symbol\" =  \"_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_9StringValE\",\n" +
                "\"prepare_fn\" = \"_ZN9doris_udf13AddUdfPrepareEPNS_15FunctionContextENS0_18FunctionStateScopeE\",\n" +
                "\"close_fn\" = \"_ZN9doris_udf11AddUdfCloseEPNS_15FunctionContextENS0_18FunctionStateScopeE\",\n" +
                "\"object_file\" = \"http://127.0.0.1:8008/libcmy_udf.so\"\n" +
                ");";
        
        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr, ctx);
        Catalog.getCurrentCatalog().createFunction(createFunctionStmt);

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
        UnionNode unionNode =  (UnionNode)fragment.getPlanRoot();
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "constExprLists_");
        Assert.assertEquals(1, constExprLists.size());
        Assert.assertEquals(1, constExprLists.get(0).size());
        Assert.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);
    }
}
