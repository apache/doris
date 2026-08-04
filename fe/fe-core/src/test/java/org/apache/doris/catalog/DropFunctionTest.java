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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.DropFunctionCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DropFunctionTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDropGlobalFunction() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // 1. create database db1
        createDatabaseWithSql("create database db1;");

        String createFuncStr
                = "create global alias function id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        createFunction(createFuncStr, ctx);

        List<Function> functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assertions.assertEquals(1, functions.size());
        // drop global function
        String dropFuncStr = "drop global function id_masking(bigint)";

        dropFunction(dropFuncStr, ctx);

        functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assertions.assertEquals(0, functions.size());
    }

    private void createFunction(String sql, ConnectContext ctx) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        ctx.setStatementContext(new StatementContext());
        if (parsed instanceof CreateFunctionCommand) {
            ((CreateFunctionCommand) parsed).run(ctx, stmtExecutor);
        }
    }

    private void dropFunction(String sql, ConnectContext ctx) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        ctx.setStatementContext(new StatementContext());
        if (parsed instanceof DropFunctionCommand) {
            ((DropFunctionCommand) parsed).run(ctx, stmtExecutor);
        }
    }
}
