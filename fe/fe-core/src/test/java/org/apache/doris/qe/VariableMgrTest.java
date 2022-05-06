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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SysVariableDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
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
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    @Test
    public void testNormal() throws Exception {
        SessionVariable var = VariableMgr.newSessionVariable();
        long originExecMemLimit = var.getMaxExecMemByte();
        boolean originEnableProfile = var.enableProfile();
        long originQueryTimeOut = var.getQueryTimeoutS();

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, var, null);
        Assert.assertTrue(rows.size() > 5);
        for (List<String> row : rows) {
            if (row.get(0).equalsIgnoreCase("exec_mem_limit")) {
                Assert.assertEquals(String.valueOf(originExecMemLimit), row.get(1));
            } else if (row.get(0).equalsIgnoreCase("is_report_success")) {
                Assert.assertEquals(String.valueOf(originEnableProfile), row.get(1));
            } else if (row.get(0).equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals(String.valueOf(originQueryTimeOut), row.get(1));
            } else if (row.get(0).equalsIgnoreCase("sql_mode")) {
                Assert.assertEquals("", row.get(1));
            }
        }

        // Set global variable
        SetStmt stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global exec_mem_limit=1234", ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(originExecMemLimit, var.getMaxExecMemByte());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(1234L, var.getMaxExecMemByte());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global parallel_fragment_exec_instance_num=5", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(1L, var.getParallelExecInstanceNum());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());

        // Test checkTimeZoneValidAndStandardize
        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global time_zone='+8:00'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global time_zone='Asia/Shanghai'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global time_zone='CST'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("CST", var.getTimeZone());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global time_zone='8:00'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global time_zone='-8:00'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("-08:00", VariableMgr.newSessionVariable().getTimeZone());

        // Set session variable
        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set exec_mem_limit=1234", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(1234L, ctx.getSessionVariable().getMaxExecMemByte());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set time_zone='Asia/Jakarta'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals("Asia/Jakarta", ctx.getSessionVariable().getTimeZone());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set sql_mode='PIPES_AS_CONCAT'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(2L, ctx.getSessionVariable().getSqlMode());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set runtime_filter_type ='BLOOM_FILTER'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(2L, ctx.getSessionVariable().getRuntimeFilterType());

        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set runtime_filter_type ='IN_OR_BLOOM_FILTER'", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(8L, ctx.getSessionVariable().getRuntimeFilterType());

        // Get from name
        SysVariableDesc desc = new SysVariableDesc("exec_mem_limit");
        Assert.assertEquals(var.getMaxExecMemByte() + "", VariableMgr.getValue(var, desc));
    }

    @Test
    public void testGlobalVariablePersist() throws Exception {
        Config.edit_log_roll_num = 1;
        SetStmt stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global exec_mem_limit=5678", ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(5678, VariableMgr.newSessionVariable().getMaxExecMemByte());

        Config.edit_log_roll_num = 100;
        stmt = (SetStmt) UtFrameUtils.parseAndAnalyzeStmt("set global exec_mem_limit=7890", ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());

        // Get currentCatalog first
        Catalog currentCatalog = Catalog.getCurrentCatalog();
        // Save real ckptThreadId
        long ckptThreadId = currentCatalog.getCheckpointer().getId();
        try {
            // set checkpointThreadId to current thread id, so that when do checkpoint manually here,
            // the Catalog.isCheckpointThread() will return true.
            Deencapsulation.setField(Catalog.class, "checkpointThreadId", Thread.currentThread().getId());
            currentCatalog.getCheckpointer().doCheckpoint();
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        } finally {
            // Restore the ckptThreadId
            Deencapsulation.setField(Catalog.class, "checkpointThreadId", ckptThreadId);
        }
        Assert.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());
    }

    @Test(expected = UserException.class)
    public void testInvalidType() throws UserException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "exec_mem_limit", new StringLiteral("abc"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = UserException.class)
    public void testInvalidTimeZoneRegion() throws UserException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("Hongkong"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = UserException.class)
    public void testInvalidTimeZoneOffset() throws UserException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("+15:00"));
        try {
            setVar.analyze(null);
        } catch (Exception e) {
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testReadOnly() throws AnalysisException, DdlException {
        SysVariableDesc desc = new SysVariableDesc("version_comment");

        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "version_comment", null);
        VariableMgr.setVar(null, setVar);
        Assert.fail("No exception throws.");
    }
}

