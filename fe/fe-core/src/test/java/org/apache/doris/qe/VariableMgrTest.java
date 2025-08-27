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

package org.apache.doris.qe;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class VariableMgrTest {
    private static String runningDir = "fe/mocked/VariableMgrTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database db1;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(ctx, stmtExecutor);
        }
    }

    @Test
    public void testGlobalVariablePersist() throws Exception {
        Config.edit_log_roll_num = 1;
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set global exec_mem_limit=5678", ctx);
        stmt.run(ctx, null);
        Assert.assertEquals(5678, VariableMgr.newSessionVariable().getMaxExecMemByte());
        // the session var is also changed.
        Assert.assertEquals(5678, ctx.getSessionVariable().getMaxExecMemByte());

        Config.edit_log_roll_num = 100;
        stmt = (SetOptionsCommand) UtFrameUtils.parseStmt("set global exec_mem_limit=7890", ctx);
        stmt.run(ctx, null);
        Assert.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());

        // Get currentCatalog first
        Env currentEnv = Env.getCurrentEnv();
        // Save real ckptThreadId
        long ckptThreadId = currentEnv.getCheckpointer().getId();
        try {
            // set checkpointThreadId to current thread id, so that when do checkpoint manually here,
            // the Catalog.isCheckpointThread() will return true.
            Deencapsulation.setField(Env.class, "checkpointThreadId", Thread.currentThread().getId());
            currentEnv.getCheckpointer().doCheckpoint();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            // Restore the ckptThreadId
            Deencapsulation.setField(Env.class, "checkpointThreadId", ckptThreadId);
        }
        Assert.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());
    }

    @Test(expected = DdlException.class)
    public void testReadOnly() throws DdlException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "version_comment", null);
        VariableMgr.setVar(null, setVar);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testVariableCallback() throws Exception {
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set session_context='trace_id:123'", ctx);
        stmt.run(ctx, null);
        Assert.assertEquals("123", ctx.traceId());
    }

    @Test
    public void testSetGlobalDefault() throws Exception {
        // Set global variable with default value
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set global enable_profile = default", ctx);
        stmt.run(ctx, null);
        SessionVariable defaultSessionVar = new SessionVariable();
        Assert.assertEquals(defaultSessionVar.enableProfile(), VariableMgr.newSessionVariable().enableProfile());
    }

    @Test
    public void testAutoCommitConvert() throws Exception {
        // boolean var with ConvertBoolToLongMethod annotation
        VariableExpr desc = new VariableExpr("autocommit");
        SessionVariable var = new SessionVariable();
        VariableMgr.fillValue(var, desc);
        Assert.assertTrue(desc.getLiteralExpr() instanceof IntLiteral);
        Assert.assertEquals(Type.BIGINT, desc.getType());

        // normal boolean var
        desc = new VariableExpr("enable_bucket_shuffle_join");
        VariableMgr.fillValue(var, desc);
        Assert.assertTrue(desc.getLiteralExpr() instanceof BoolLiteral);
        Assert.assertEquals(Type.BOOLEAN, desc.getType());
    }

    // @@auto_commit's type should be BIGINT
    @Test
    public void testAutoCommitType() throws AnalysisException {
        // Old planner
        SessionVariable sv = new SessionVariable();
        VariableExpr desc = new VariableExpr(SessionVariable.AUTO_COMMIT);
        VariableMgr.fillValue(sv, desc);
        Assert.assertEquals(Type.BIGINT, desc.getType());
        // Nereids
        sv = new SessionVariable();
        String name = SessionVariable.AUTO_COMMIT;
        SetType setType = SetType.SESSION;
        Literal l = VariableMgr.getLiteral(sv, name, setType);
        Assert.assertEquals(BigIntType.INSTANCE, l.getDataType());
    }

    @Test
    public void testCheckSqlConvertorFeatures() throws DdlException {
        // set wrong var
        SetVar setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral("wrong"));
        SessionVariable var = new SessionVariable();
        try {
            VariableMgr.setVar(var, setVar);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown sql convertor feature: wrong"));
        }

        // set one var
        Assert.assertEquals(new String[] {""}, var.getSqlConvertorFeatures());
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral("ctas"));
        VariableMgr.setVar(var, setVar);
        Assert.assertEquals(new String[] {"ctas"}, var.getSqlConvertorFeatures());

        // set multiple var
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral("ctas,delete_all_comment"));
        VariableMgr.setVar(var, setVar);
        Assert.assertEquals(new String[] {"ctas", "delete_all_comment"}, var.getSqlConvertorFeatures());

        // set to empty
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral(""));
        VariableMgr.setVar(var, setVar);
        Assert.assertEquals(new String[] {""}, var.getSqlConvertorFeatures());
    }
}
