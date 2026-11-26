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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.commands.CreateEncryptkeyCommand;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateEncryptKeyTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void test() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // create database db1
        String createDbStmtStr = "create database db1;";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, createDbStmtStr);
        createDatabaseWithSql(createDbStmtStr);

        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db1");
        Assertions.assertNotNull(db);

        String createFuncStr = "create encryptkey db1.my_key as \"beijing\";";

        CreateEncryptkeyCommand createFunctionStmt
                = (CreateEncryptkeyCommand) UtFrameUtils.parseStmt(createFuncStr, ctx);
        createFunctionStmt.run(ctx, stmtExecutor);

        List<EncryptKey> encryptKeys = db.getEncryptKeys();
        Assertions.assertEquals(1, encryptKeys.size());
        Assertions.assertEquals("beijing", encryptKeys.get(0).getKeyString());

        String queryStr = "SELECT HEX(AES_ENCRYPT(\"Doris is Great\", key db1.my_key));";
        ctx.getState().reset();
        stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Planner planner = stmtExecutor.planner();
        Assertions.assertEquals(1, planner.getFragments().size());
        PlanFragment fragment = planner.getFragments().get(0);
        Assertions.assertTrue(fragment.getPlanRoot() instanceof UnionNode);
        UnionNode unionNode =  (UnionNode) fragment.getPlanRoot();
        List<List<Expr>> constExprLists = Deencapsulation.getField(unionNode, "materializedConstExprLists");
        Assertions.assertEquals(1, constExprLists.size());
        Assertions.assertEquals(1, constExprLists.get(0).size());
        Assertions.assertTrue(constExprLists.get(0).get(0) instanceof FunctionCallExpr);
    }
}
