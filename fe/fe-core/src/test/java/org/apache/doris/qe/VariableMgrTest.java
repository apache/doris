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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsetVariableCommand;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.GlobalVarPersistInfo;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.ImmutableMap;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Optional;

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

    // A negative audit_plugin_max_sql_length dropped audit events (AuditLogHelperTest), so SET rejects it.
    @Test
    public void testNegativeAuditMaxSqlLengthIsRejected() throws Exception {
        int original = GlobalVariable.auditPluginMaxSqlLength;
        try {
            SetOptionsCommand negative = (SetOptionsCommand) UtFrameUtils.parseStmt(
                    "set global audit_plugin_max_sql_length = -1", connectContext);
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> negative.run(connectContext, null));
            Assertions.assertEquals("Variable 'audit_plugin_max_sql_length' can't be set to '-1', "
                    + "the value must not be negative", e.getDetailMessage());
            Assertions.assertEquals(original, GlobalVariable.auditPluginMaxSqlLength);
            // A follower applies a forwarded SET GLOBAL to its own copy whether or not the master accepted it.
            Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVarForNonMasterFE(
                    connectContext.getSessionVariable(), new SetVar(SetType.SESSION,
                            GlobalVariable.AUDIT_PLUGIN_MAX_SQL_LENGTH, new StringLiteral("-1"))));
            Assertions.assertEquals(original, GlobalVariable.auditPluginMaxSqlLength);

            SetOptionsCommand zero = (SetOptionsCommand) UtFrameUtils.parseStmt(
                    "set global audit_plugin_max_sql_length = 0", connectContext);
            zero.run(connectContext, null);
            Assertions.assertEquals(0, GlobalVariable.auditPluginMaxSqlLength);
        } finally {
            GlobalVariable.auditPluginMaxSqlLength = original;
        }
    }

    // query_audit_always_on: SET rejects every value that weakens query auditing, globally or for the session,
    // and leaves the variable as it was.
    @Test
    public void testAlwaysOnRejectsWeakerAuditVariables() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            for (String sql : new String[] {
                    "set global enable_audit_plugin = false",
                    "set enable_prepared_stmt_audit_log = false",
                    "set global enable_prepared_stmt_audit_log = false",
                    // Its default is false.
                    "set enable_prepared_stmt_audit_log = default",
                    "set global audit_plugin_max_sql_length = 100",
                    "set global audit_plugin_max_sql_length = -1",
                    "set global audit_plugin_max_insert_stmt_length = 100",
                    "set global audit_plugin_load_timeout = 10",
                    "set global audit_plugin_max_batch_bytes = 1",
                    "set global audit_plugin_max_batch_interval_sec = 3600"}) {
                DdlException e = Assertions.assertThrows(DdlException.class, () -> runSet(sql), sql);
                Assertions.assertTrue(e.getDetailMessage().contains("because query_audit_always_on is enabled"),
                        sql + ": " + e.getMessage());
            }
            Assertions.assertTrue(GlobalVariable.enableAuditLoader);
            Assertions.assertFalse(VariableMgr.getDefaultSessionVariable().isEnablePreparedStmtAuditLog());
            Assertions.assertEquals(2097152, GlobalVariable.auditPluginMaxSqlLength);
            Assertions.assertEquals(Integer.MAX_VALUE, GlobalVariable.auditPluginMaxInsertStmtLength);
            Assertions.assertEquals(600, GlobalVariable.auditPluginLoadTimeoutS);
            Assertions.assertEquals(52428800L, GlobalVariable.auditPluginMaxBatchBytes);
            Assertions.assertEquals(60L, GlobalVariable.auditPluginMaxBatchInternalSec);
        });
    }

    // UNSET and the SET_VAR hint go through VariableMgr.setVar too.
    @Test
    public void testAlwaysOnRejectsUnsetAndSetVarHint() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            UnsetVariableCommand unset = (UnsetVariableCommand) UtFrameUtils.parseStmt(
                    "unset variable enable_prepared_stmt_audit_log", connectContext);
            connectContext.getState().reset();
            unset.run(connectContext, null);
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("query_audit_always_on"),
                    connectContext.getState().getErrorMessage());
            connectContext.getState().reset();

            // A connection of its own: the hint marks the session for a per-statement revert.
            ConnectContext hintContext = new ConnectContext();
            SelectHintSetVar hint = new SelectHintSetVar("set_var",
                    ImmutableMap.of(SessionVariable.ENABLE_PREPARED_STMT_AUDIT_LOG, Optional.of("false")));
            org.apache.doris.nereids.exceptions.AnalysisException e = Assertions.assertThrows(
                    org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> hint.setVarOnceInSql(new StatementContext(hintContext, new OriginStatement("select 1", 0))));
            Assertions.assertTrue(e.getCause().getMessage().contains("query_audit_always_on"), e.getMessage());
        });
    }

    // A follower syncs a forwarded SET GLOBAL into its own copy through setVarForNonMasterFE, also after the master
    // rejected it, so the follower must reject the value as well.
    @Test
    public void testAlwaysOnFollowerKeepsValuesTheMasterRejects() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            for (String[] change : new String[][] {
                    {GlobalVariable.ENABLE_AUDIT_PLUGIN, "false"},
                    {GlobalVariable.AUDIT_PLUGIN_MAX_SQL_LENGTH, "100"},
                    {GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_BYTES, "1"},
                    {SessionVariable.ENABLE_PREPARED_STMT_AUDIT_LOG, "false"}}) {
                DdlException e = Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVarForNonMasterFE(
                        connectContext.getSessionVariable(),
                        new SetVar(SetType.SESSION, change[0], new StringLiteral(change[1]))), change[0]);
                Assertions.assertTrue(e.getDetailMessage().contains("query_audit_always_on"), e.getMessage());
            }
            Assertions.assertTrue(GlobalVariable.enableAuditLoader);
            Assertions.assertEquals(2097152, GlobalVariable.auditPluginMaxSqlLength);
            Assertions.assertEquals(52428800L, GlobalVariable.auditPluginMaxBatchBytes);
        });
    }

    @Test
    public void testAlwaysOnAllowsStrongerAuditVariables() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            runSet("set global enable_audit_plugin = true");
            runSet("set enable_prepared_stmt_audit_log = true");
            Assertions.assertTrue(connectContext.getSessionVariable().isEnablePreparedStmtAuditLog());
            runSet("set global enable_prepared_stmt_audit_log = true");
            Assertions.assertTrue(VariableMgr.getDefaultSessionVariable().isEnablePreparedStmtAuditLog());
            runSet("set global audit_plugin_max_sql_length = 4194304");
            Assertions.assertEquals(4194304, GlobalVariable.auditPluginMaxSqlLength);
            runSet("set global audit_plugin_load_timeout = 1200");
            Assertions.assertEquals(1200, GlobalVariable.auditPluginLoadTimeoutS);
            runSet("set global audit_plugin_max_batch_bytes = 52428800");
            runSet("set global audit_plugin_max_batch_interval_sec = default");
            runSet("set global audit_plugin_max_insert_stmt_length = 2147483647");
        });
    }

    // UNSET ALL resets the other variables and keeps the ones whose default query_audit_always_on rejects.
    @Test
    public void testAlwaysOnUnsetAllKeepsRejectedDefaults() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            SessionVariable sessionVariable = VariableMgr.newSessionVariable();
            sessionVariable.enablePreparedStmtAuditLog = true;
            int defaultQueryTimeout = sessionVariable.getQueryTimeoutS();
            sessionVariable.setQueryTimeoutS(defaultQueryTimeout + 1);
            VariableMgr.setAllVarsToDefaultValue(sessionVariable, SetType.SESSION);
            Assertions.assertTrue(sessionVariable.isEnablePreparedStmtAuditLog());
            Assertions.assertEquals(defaultQueryTimeout, sessionVariable.getQueryTimeoutS());
        });
    }

    @Test
    public void testAuditVariablesFollowSetWhenAlwaysOnIsOff() throws Throwable {
        withAuditVariablesRestored(false, () -> {
            runSet("set global enable_audit_plugin = false");
            Assertions.assertFalse(GlobalVariable.enableAuditLoader);
            runSet("set enable_prepared_stmt_audit_log = default");
            Assertions.assertFalse(connectContext.getSessionVariable().isEnablePreparedStmtAuditLog());
            runSet("set global audit_plugin_max_sql_length = 100");
            Assertions.assertEquals(100, GlobalVariable.auditPluginMaxSqlLength);
            runSet("set global audit_plugin_max_batch_bytes = 1");
            Assertions.assertEquals(1L, GlobalVariable.auditPluginMaxBatchBytes);
            SessionVariable sessionVariable = VariableMgr.newSessionVariable();
            sessionVariable.enablePreparedStmtAuditLog = true;
            VariableMgr.setAllVarsToDefaultValue(sessionVariable, SetType.SESSION);
            Assertions.assertFalse(sessionVariable.isEnablePreparedStmtAuditLog());
        });
    }

    // A master taking over rewrites the persisted global values query_audit_always_on does not allow, and logs each
    // rewrite for the followers to replay; allowed values stay, and nothing changes with the policy off.
    @Test
    public void testAlwaysOnRewritesPersistedGlobalValues() throws Throwable {
        withAuditVariablesRestored(true, () -> {
            GlobalVariable.enableAuditLoader = false;
            VariableMgr.getDefaultSessionVariable().enablePreparedStmtAuditLog = false;
            GlobalVariable.auditPluginMaxSqlLength = 100;
            GlobalVariable.auditPluginMaxBatchBytes = 1;
            GlobalVariable.auditPluginLoadTimeoutS = 1200;

            Env env = Env.getCurrentEnv();
            EditLog editLog = env.getEditLog();
            EditLog spiedEditLog = Mockito.spy(editLog);
            Deencapsulation.setField(env, "editLog", spiedEditLog);
            try {
                VariableMgr.applyQueryAuditPolicyToGlobalValues();
            } finally {
                Deencapsulation.setField(env, "editLog", editLog);
            }

            Assertions.assertTrue(GlobalVariable.enableAuditLoader);
            Assertions.assertTrue(VariableMgr.newSessionVariable().isEnablePreparedStmtAuditLog());
            Assertions.assertEquals(2097152, GlobalVariable.auditPluginMaxSqlLength);
            Assertions.assertEquals(52428800L, GlobalVariable.auditPluginMaxBatchBytes);
            Assertions.assertEquals(1200, GlobalVariable.auditPluginLoadTimeoutS);

            ArgumentCaptor<GlobalVarPersistInfo> logged = ArgumentCaptor.forClass(GlobalVarPersistInfo.class);
            Mockito.verify(spiedEditLog, Mockito.times(4)).logGlobalVariableV2(logged.capture());
            JSONObject replayed = new JSONObject();
            for (GlobalVarPersistInfo info : logged.getAllValues()) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                info.write(new DataOutputStream(bytes));
                GlobalVarPersistInfo read = GlobalVarPersistInfo.read(
                        new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
                replayed.putAll((JSONObject) JSONValue.parse(read.getPersistJsonString()));
            }
            Assertions.assertEquals(ImmutableMap.of(GlobalVariable.ENABLE_AUDIT_PLUGIN, true,
                    SessionVariable.ENABLE_PREPARED_STMT_AUDIT_LOG, true,
                    GlobalVariable.AUDIT_PLUGIN_MAX_SQL_LENGTH, 2097152L,
                    GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_BYTES, 52428800L), replayed);
        });
        withAuditVariablesRestored(false, () -> {
            GlobalVariable.enableAuditLoader = false;
            VariableMgr.applyQueryAuditPolicyToGlobalValues();
            Assertions.assertFalse(GlobalVariable.enableAuditLoader);
        });
    }

    // A renamed variable would silently lose its protection: every name the policy protects must resolve.
    @Test
    public void testQueryAuditPolicyNamesAreVariables() {
        for (String name : QueryAuditPolicy.protectedVariableNames()) {
            VariableMgr.VarContext ctx = VariableMgr.getVarContext(name);
            Assertions.assertNotNull(ctx, name);
            Assertions.assertEquals(name, ctx.getField().getAnnotation(VarAttrDef.VarAttr.class).name());
        }
    }

    private void runSet(String sql) throws Exception {
        ((SetOptionsCommand) UtFrameUtils.parseStmt(sql, connectContext)).run(connectContext, null);
    }

    // Runs body with query_audit_always_on set as given, then restores it and every audit variable body may change.
    private void withAuditVariablesRestored(boolean alwaysOn, Executable body) throws Throwable {
        boolean originalAlwaysOn = Config.query_audit_always_on;
        boolean enableAuditLoader = GlobalVariable.enableAuditLoader;
        int maxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int maxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        int loadTimeoutS = GlobalVariable.auditPluginLoadTimeoutS;
        long maxBatchBytes = GlobalVariable.auditPluginMaxBatchBytes;
        long maxBatchIntervalSec = GlobalVariable.auditPluginMaxBatchInternalSec;
        boolean globalPreparedAudit = VariableMgr.getDefaultSessionVariable().enablePreparedStmtAuditLog;
        boolean sessionPreparedAudit = connectContext.getSessionVariable().enablePreparedStmtAuditLog;
        try {
            Config.query_audit_always_on = alwaysOn;
            body.execute();
        } finally {
            Config.query_audit_always_on = originalAlwaysOn;
            GlobalVariable.enableAuditLoader = enableAuditLoader;
            GlobalVariable.auditPluginMaxSqlLength = maxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = maxInsertStmtLength;
            GlobalVariable.auditPluginLoadTimeoutS = loadTimeoutS;
            GlobalVariable.auditPluginMaxBatchBytes = maxBatchBytes;
            GlobalVariable.auditPluginMaxBatchInternalSec = maxBatchIntervalSec;
            VariableMgr.getDefaultSessionVariable().enablePreparedStmtAuditLog = globalPreparedAudit;
            connectContext.getSessionVariable().enablePreparedStmtAuditLog = sessionPreparedAudit;
        }
    }

    @Test
    public void testReadOnly() {
        // Set global variable
        SetVar setVar = new SetVar(SetType.SESSION, "version_comment", null);
        Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(null, setVar));
    }

    @Test
    public void testDynamicPartitionDropProtectionIsGlobalOnly() throws Exception {
        try {
            VariableMgr.setGlobalSessionVariableForImage(
                    SessionVariable.ENABLE_DYNAMIC_PARTITION_DROP_PROTECTION, "false");
            Assertions.assertFalse(VariableMgr.isDynamicPartitionDropProtectionEnabled());

            SetOptionsCommand sessionStmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                    "set enable_dynamic_partition_drop_protection=true", connectContext);
            Assertions.assertThrows(DdlException.class, () -> sessionStmt.run(connectContext, null));

            VariableMgr.setGlobalSessionVariableForImage(
                    SessionVariable.ENABLE_DYNAMIC_PARTITION_DROP_PROTECTION, "true");
            Assertions.assertTrue(VariableMgr.isDynamicPartitionDropProtectionEnabled());
            Assertions.assertTrue(VariableMgr.newSessionVariable().isEnableDynamicPartitionDropProtection());

            SetOptionsCommand globalStmt = (SetOptionsCommand) UtFrameUtils.parseStmt(
                    "set global enable_dynamic_partition_drop_protection=false", connectContext);
            globalStmt.run(connectContext, null);
            Assertions.assertFalse(VariableMgr.isDynamicPartitionDropProtectionEnabled());
        } finally {
            VariableMgr.setGlobalSessionVariableForImage(
                    SessionVariable.ENABLE_DYNAMIC_PARTITION_DROP_PROTECTION, "false");
        }
    }

    @Test
    public void testAuthenticationPolicyIsTheReadOnlyMysqlDefault() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        VariableExpr variableExpr = new VariableExpr(GlobalVariable.AUTHENTICATION_POLICY);
        VariableMgr.fillValue(sessionVariable, variableExpr);
        Assertions.assertEquals("*,,", VariableMgr.getValue(sessionVariable, variableExpr));

        SetVar setVar = new SetVar(SetType.GLOBAL, GlobalVariable.AUTHENTICATION_POLICY, new StringLiteral("*,"));
        Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(sessionVariable, setVar));
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
    public void testVariableVersionAfterUpgrade() {
        SessionVariable defaultSessionVariable = VariableMgr.getDefaultSessionVariable();
        boolean originalEnableSqlCache = defaultSessionVariable.isEnableSqlCache();
        boolean originalAnsiBehavior = GlobalVariable.enable_ansi_query_organization_behavior;
        boolean originalTypeCoercionBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        int originalVersion = GlobalVariable.variableVersion;
        try {
            GlobalVariable.variableVersion = GlobalVariable.VARIABLE_VERSION_300;

            VariableMgr.forceUpdateVariables();

            Assertions.assertEquals(GlobalVariable.CURRENT_VARIABLE_VERSION, GlobalVariable.variableVersion);
        } finally {
            defaultSessionVariable.setEnableSqlCache(originalEnableSqlCache);
            GlobalVariable.enable_ansi_query_organization_behavior = originalAnsiBehavior;
            GlobalVariable.enableNewTypeCoercionBehavior = originalTypeCoercionBehavior;
            GlobalVariable.variableVersion = originalVersion;
        }
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
    public void testRemovedNereidsTraceVariablesRemainCompatible() throws Exception {
        for (String name : new String[] {"enable_nereids_trace", "nereids_trace_event_mode"}) {
            SessionVariable sessionVariable = new SessionVariable();
            SetVar setVar = new SetVar(SetType.SESSION, name.toUpperCase(), new StringLiteral("ignored"));

            Assertions.assertDoesNotThrow(() -> VariableMgr.setVar(sessionVariable, setVar));
            Assertions.assertDoesNotThrow(() -> VariableMgr.setVarForNonMasterFE(sessionVariable, setVar));

            VariableExpr variableExpr = new VariableExpr(name.toUpperCase());
            VariableMgr.fillValue(sessionVariable, variableExpr);
            Assertions.assertEquals(Type.VARCHAR, variableExpr.getType());
            Assertions.assertEquals("", VariableMgr.getValue(sessionVariable, variableExpr));

            Literal literal = VariableMgr.getLiteral(sessionVariable, name.toUpperCase(), SetType.SESSION);
            Assertions.assertNotNull(literal);
            Assertions.assertEquals("", literal.getStringValue());
        }
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

    @Test
    public void testExternalScanConsistentHashSpreadVariable() throws Exception {
        SessionVariable var = new SessionVariable();
        VariableMgr.setVar(var, new SetVar(SetType.SESSION,
                SessionVariable.USE_CONSISTENT_HASHING_FOR_EXTERNAL_SCAN, new StringLiteral("true")));
        SessionVariable hashForwarded = new SessionVariable();
        hashForwarded.setForwardedSessionVariables(var.getForwardVariables());
        Assertions.assertTrue(hashForwarded.getUseConsistentHashForExternalScan());

        Assertions.assertEquals(1, var.getExternalScanConsistentHashSpreadNum());
        for (String value : new String[] {"0", "1", "2", "3", "2147483647"}) {
            VariableMgr.setVar(var, new SetVar(SetType.SESSION,
                    SessionVariable.EXTERNAL_SCAN_CONSISTENT_HASH_SPREAD_NUM, new StringLiteral(value)));
            Assertions.assertEquals(Integer.parseInt(value), var.getExternalScanConsistentHashSpreadNum());
        }
        for (String value : new String[] {"-1", "2147483648", "abc"}) {
            Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(var, new SetVar(SetType.SESSION,
                    SessionVariable.EXTERNAL_SCAN_CONSISTENT_HASH_SPREAD_NUM, new StringLiteral(value))));
            Assertions.assertEquals(Integer.MAX_VALUE, var.getExternalScanConsistentHashSpreadNum());
        }
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(var.getForwardVariables());
        Assertions.assertEquals(Integer.MAX_VALUE, forwarded.getExternalScanConsistentHashSpreadNum());
        VariableMgr.setVar(var, new SetVar(SetType.SESSION,
                SessionVariable.EXTERNAL_SCAN_CONSISTENT_HASH_SPREAD_NUM, null));
        Assertions.assertEquals(1, var.getExternalScanConsistentHashSpreadNum());
    }

}
