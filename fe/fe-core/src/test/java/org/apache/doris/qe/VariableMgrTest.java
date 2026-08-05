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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariableMgrTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("db1");
    }

    @Test
    public void testGlobalVariablePersist() throws Exception {
        Config.edit_log_roll_num = 1;
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set global exec_mem_limit=5678", connectContext);
        stmt.run(connectContext, null);
        Assertions.assertEquals(5678, VariableMgr.newSessionVariable().getMaxExecMemByte());
        // the session var is also changed.
        Assertions.assertEquals(5678, connectContext.getSessionVariable().getMaxExecMemByte());

        Config.edit_log_roll_num = 100;
        stmt = (SetOptionsCommand) UtFrameUtils.parseStmt("set global exec_mem_limit=7890", connectContext);
        stmt.run(connectContext, null);
        Assertions.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());

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
            Assertions.fail(e.getMessage());
        } finally {
            // Restore the ckptThreadId
            Deencapsulation.setField(Env.class, "checkpointThreadId", ckptThreadId);
        }
        Assertions.assertEquals(7890, VariableMgr.newSessionVariable().getMaxExecMemByte());
    }

    @Test
    public void testReadOnly() {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "version_comment", null);
        Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(null, setVar));
    }

    @Test
    public void testVariableCallback() throws Exception {
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set session_context='trace_id:123'", connectContext);
        stmt.run(connectContext, null);
        Assertions.assertEquals("123", connectContext.traceId());
    }

    @Test
    public void testSetGlobalDefault() throws Exception {
        // Set global variable with default value
        SetOptionsCommand stmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                "set global enable_profile = default", connectContext);
        stmt.run(connectContext, null);
        SessionVariable defaultSessionVar = new SessionVariable();
        Assertions.assertEquals(defaultSessionVar.enableProfile(), VariableMgr.newSessionVariable().enableProfile());
    }

    @Test
    public void testAutoCommitConvert() throws Exception {
        // boolean var with ConvertBoolToLongMethod annotation
        VariableExpr desc = new VariableExpr("autocommit");
        SessionVariable var = new SessionVariable();
        VariableMgr.fillValue(var, desc);
        Assertions.assertTrue(desc.getLiteralExpr() instanceof IntLiteral);
        Assertions.assertEquals(Type.BIGINT, desc.getType());

        // normal boolean var
        desc = new VariableExpr("enable_bucket_shuffle_join");
        VariableMgr.fillValue(var, desc);
        Assertions.assertTrue(desc.getLiteralExpr() instanceof BoolLiteral);
        Assertions.assertEquals(Type.BOOLEAN, desc.getType());
    }

    // @@auto_commit's type should be BIGINT
    @Test
    public void testAutoCommitType() throws AnalysisException {
        // Old planner
        SessionVariable sv = new SessionVariable();
        VariableExpr desc = new VariableExpr(SessionVariable.AUTO_COMMIT);
        VariableMgr.fillValue(sv, desc);
        Assertions.assertEquals(Type.BIGINT, desc.getType());
        // Nereids
        sv = new SessionVariable();
        String name = SessionVariable.AUTO_COMMIT;
        SetType setType = SetType.SESSION;
        Literal l = VariableMgr.getLiteral(sv, name, setType);
        Assertions.assertEquals(BigIntType.INSTANCE, l.getDataType());
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
            Assertions.assertTrue(e.getMessage().contains("Unknown sql convertor feature: wrong"));
        }

        // set one var
        Assertions.assertArrayEquals(new String[] {""}, var.getSqlConvertorFeatures());
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral("ctas"));
        VariableMgr.setVar(var, setVar);
        Assertions.assertArrayEquals(new String[] {"ctas"}, var.getSqlConvertorFeatures());

        // set multiple var
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral("ctas,delete_all_comment"));
        VariableMgr.setVar(var, setVar);
        Assertions.assertArrayEquals(new String[] {"ctas", "delete_all_comment"}, var.getSqlConvertorFeatures());

        // set to empty
        setVar = new SetVar(SetType.SESSION, SessionVariable.ENABLE_SQL_CONVERTOR_FEATURES,
                new StringLiteral(""));
        VariableMgr.setVar(var, setVar);
        Assertions.assertArrayEquals(new String[] {""}, var.getSqlConvertorFeatures());
    }

    @Test
    public void testAdaptiveBatchSizeDefaultsToThrift() {
        SessionVariable var = new SessionVariable();
        TQueryOptions options = var.toThrift();

        Assertions.assertEquals(8160, var.batchSize);
        Assertions.assertEquals(8160, options.getBatchSize());
        Assertions.assertEquals(8388608L, options.getPreferredBlockSizeBytes());
    }

    @Test
    public void testAdaptiveBatchSizeSessionVariables() throws Exception {
        SessionVariable var = new SessionVariable();

        VariableMgr.setVar(var, new SetVar(SetType.SESSION, SessionVariable.BATCH_SIZE,
                new StringLiteral("12345")));
        VariableMgr.setVar(var, new SetVar(SetType.SESSION, SessionVariable.PREFERRED_BLOCK_SIZE_BYTES,
                new StringLiteral("1048576")));

        TQueryOptions options = var.toThrift();
        Assertions.assertEquals(12345, var.batchSize);
        Assertions.assertEquals(1048576L, var.preferredBlockSizeBytes);
        Assertions.assertEquals(12345, options.getBatchSize());
        Assertions.assertEquals(1048576L, options.getPreferredBlockSizeBytes());
    }

    @Test
    public void testAdaptiveBatchSizeRejectsTinyNonZeroBytes() {
        SessionVariable var = new SessionVariable();
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(var,
                new SetVar(SetType.SESSION, SessionVariable.PREFERRED_BLOCK_SIZE_BYTES,
                        new StringLiteral("1"))));
        Assertions.assertTrue(exception.getMessage().contains("preferred_block_size_bytes"));
    }

    @Test
    public void testAdaptiveBatchSizeRejectsZeroByteValues() {
        SessionVariable var = new SessionVariable();

        DdlException blockSizeException = Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(var,
                new SetVar(SetType.SESSION, SessionVariable.PREFERRED_BLOCK_SIZE_BYTES,
                        new StringLiteral("0"))));
        Assertions.assertTrue(blockSizeException.getMessage().contains("preferred_block_size_bytes"));
    }
}
