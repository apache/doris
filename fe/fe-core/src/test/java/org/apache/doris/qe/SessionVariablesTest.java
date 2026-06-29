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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.rewrite.eageraggregation.EagerAggHints.Action;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class SessionVariablesTest extends TestWithFeService {

    private SessionVariable sessionVariable;
    private int numOfForwardVars;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test_d");
        useDatabase("test_d");
        // Skip creating an OLAP table because these cases only validate session-variable behavior and parsing.

        sessionVariable = new SessionVariable();
        Field[] fields = SessionVariable.class.getDeclaredFields();
        for (Field f : fields) {
            VarAttrDef.VarAttr varAttr = f.getAnnotation(VarAttrDef.VarAttr.class);
            if (varAttr == null || !(varAttr.needForward() || varAttr.affectQueryResultInPlan()
                    || varAttr.affectQueryResultInExecution())) {
                continue;
            }
            numOfForwardVars++;
        }
    }

    @Test
    public void testForwardSessionVariables() {
        Map<String, String> vars = sessionVariable.getForwardVariables();
        Assertions.assertTrue(numOfForwardVars >= 6);
        Assertions.assertEquals(numOfForwardVars, vars.size());

        vars.put(SessionVariable.ENABLE_PROFILE, "true");
        vars.put(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, "ERROR");
        sessionVariable.setForwardedSessionVariables(vars);
        Assertions.assertTrue(sessionVariable.enableProfile);
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVariable.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVariable.getInsertVisibleTimeoutReturnModeEnum());
    }

    @Test
    public void testInsertVisibleTimeoutReturnMode() throws Exception {
        connectContext.setThreadLocalInfo();
        SessionVariable sessionVar = connectContext.getSessionVariable();

        VariableMgr.setVar(sessionVar, new SetVar(SetType.SESSION,
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, new StringLiteral("ERROR")));
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVar.getInsertVisibleTimeoutReturnModeEnum());
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getForwardVariables().get(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE));

        SessionVariable restored = new SessionVariable();
        restored.readFromJson("{\"insert_visible_timeout_return_mode\":\"ERROR\"}");
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                restored.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                restored.getInsertVisibleTimeoutReturnModeEnum());

        // Verify map restore keeps accepting canonical string tokens without extra normalization hooks.
        Map<String, String> restoredMap = new HashMap<>();
        restoredMap.put(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, "ERROR");
        restored.readFromMap(restoredMap);
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                restored.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                restored.getInsertVisibleTimeoutReturnModeEnum());

        Map<String, String> forwardVars = sessionVar.getForwardVariables();
        forwardVars.put(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, "ERROR");
        restored.setForwardedSessionVariables(forwardVars);
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                restored.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                restored.getInsertVisibleTimeoutReturnModeEnum());

        Field field = SessionVariable.class.getDeclaredField("insertVisibleTimeoutReturnMode");
        VarAttrDef.VarAttr varAttr = field.getAnnotation(VarAttrDef.VarAttr.class);
        Assertions.assertArrayEquals(new String[] {
                "控制普通内表 INSERT 在 publish timeout 时返回给客户端的状态。",
                "Controls the status returned to the client when a normal internal-table INSERT times out "
                        + "while waiting for publish visibility."
        }, varAttr.description());
        Assertions.assertArrayEquals(new String[] {
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_COMMITTED,
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR
        }, varAttr.options());

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "insertVisibleTimeoutReturnMode value is invalid",
                () -> VariableMgr.setVar(sessionVar, new SetVar(SetType.SESSION,
                        SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, new StringLiteral("unexpected"))));
    }

    @Test
    public void testInsertVisibleTimeoutReturnModeDefaultsAndCheckerBranches() {
        // Cover the default branch and the helper methods used by setter/checker paths.
        SessionVariable sessionVar = new SessionVariable();
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_COMMITTED,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertFalse(sessionVar.isInsertVisibleTimeoutReturnError());

        // Verify setter normalization is case-insensitive and stores the canonical lowercase value.
        sessionVar.setInsertVisibleTimeoutReturnMode("ErRoR");
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVar.getInsertVisibleTimeoutReturnModeEnum());
        Assertions.assertTrue(sessionVar.isInsertVisibleTimeoutReturnError());

        ExceptionChecker.expectThrowsWithMsg(UnsupportedOperationException.class,
                "insertVisibleTimeoutReturnMode value is empty",
                () -> sessionVar.checkInsertVisibleTimeoutReturnMode(""));
    }

    @Test
    public void testRuntimeFilterBroadcastJoinProducerNumDescription() throws Exception {
        SessionVariable sessionVar = new SessionVariable();
        Assertions.assertEquals(3, sessionVar.getRuntimeFilterBroadcastJoinProducerNum());

        Field field = SessionVariable.class.getDeclaredField("runtimeFilterBroadcastJoinProducerNum");
        VarAttrDef.VarAttr varAttr = field.getAnnotation(VarAttrDef.VarAttr.class);
        Assertions.assertArrayEquals(new String[] {
                "控制 Nereids 分布式规划中每个 broadcast join runtime filter 的生产 BE 数量。"
                        + "设置为小于等于 0 时不限制。Legacy Coordinator 路径保持原行为。",
                "Controls the number of producer BEs for each broadcast join runtime filter in "
                        + "the Nereids distributed planner. Values less than or equal to 0 disable the limit. "
                        + "The legacy Coordinator path keeps the existing behavior."
        }, varAttr.description());
    }

    @Test
    public void testForceEagerAggHintParseWhenSetSessionVariable() throws Exception {
        SessionVariable sessionVar = new SessionVariable();

        VariableMgr.setVar(sessionVar, new SetVar(SetType.SESSION,
                "force_eager_agg_hint", new StringLiteral("sum:t1.a=push; count:*=nopush")));
        Assertions.assertEquals("sum:t1.a=push; count:*=nopush", sessionVar.forceEagerAggHint);
        Assertions.assertEquals(Action.PUSH, sessionVar.getForceEagerAggHintMap().get("sum:t1.a"));
        Assertions.assertEquals(Action.NOPUSH, sessionVar.getForceEagerAggHintMap().get("count:*"));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Invalid force_eager_agg_hint",
                () -> VariableMgr.setVar(sessionVar, new SetVar(SetType.SESSION,
                        "force_eager_agg_hint", new StringLiteral("sum:t1.a=unknown"))));
        Assertions.assertEquals("sum:t1.a=push; count:*=nopush", sessionVar.forceEagerAggHint);
        Assertions.assertEquals(Action.PUSH, sessionVar.getForceEagerAggHintMap().get("sum:t1.a"));

        SessionVariable restored = new SessionVariable();
        restored.readFromJson("{\"force_eager_agg_hint\":\"sum:t2.b=no_push\"}");
        Assertions.assertEquals(Action.NOPUSH, restored.getForceEagerAggHintMap().get("sum:t2.b"));
    }

    @Test
    public void testSetVarInHint() {
        String sql = "insert into test_t1 select /*+ set_var(enable_nereids_dml_with_pipeline=false)*/ * from test_t1 where enable_nereids_dml_with_pipeline=true";
        new NereidsParser().parseSQL(sql);
        Assertions.assertEquals(false, connectContext.getSessionVariable().enableNereidsDmlWithPipeline);
    }

    @Test
    public void testAiSessionVariableChecker() throws Exception {
        SessionVariable sv = new SessionVariable();

        VariableMgr.setVar(sv, new SetVar(SetType.SESSION, SessionVariable.EMBED_MAX_BATCH_SIZE,
                new IntLiteral(1)));
        Assertions.assertEquals(1, sv.embedMaxBatchSize);
        DdlException embedException = Assertions.assertThrows(DdlException.class,
                () -> VariableMgr.setVar(sv, new SetVar(SetType.SESSION,
                        SessionVariable.EMBED_MAX_BATCH_SIZE, new IntLiteral(0))));
        Assertions.assertTrue(embedException.getMessage().contains(SessionVariable.EMBED_MAX_BATCH_SIZE));
        Assertions.assertEquals(1, sv.embedMaxBatchSize);

        VariableMgr.setVar(sv, new SetVar(SetType.SESSION, SessionVariable.AI_CONTEXT_WINDOW_SIZE,
                new IntLiteral(1)));
        Assertions.assertEquals(1, sv.aiContextWindowSize);
        DdlException contextException = Assertions.assertThrows(DdlException.class,
                () -> VariableMgr.setVar(sv, new SetVar(SetType.SESSION,
                        SessionVariable.AI_CONTEXT_WINDOW_SIZE, new IntLiteral(-1))));
        Assertions.assertTrue(contextException.getMessage().contains(SessionVariable.AI_CONTEXT_WINDOW_SIZE));
        Assertions.assertEquals(1, sv.aiContextWindowSize);
    }

    @Test
    public void testMorValuePredicatePushdownEnabled() {
        SessionVariable sv = new SessionVariable();

        // default empty string — disabled for all tables
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));

        // wildcard enables all tables
        sv.enableMorValuePredicatePushdownTables = "*";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled(null, "tbl1"));

        // single table name without db — matches any database
        sv.enableMorValuePredicatePushdownTables = "tbl1";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db2", "tbl1"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db1", "tbl2"));

        // table name with db prefix — must match both
        sv.enableMorValuePredicatePushdownTables = "db1.tbl1";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db2", "tbl1"));

        // multiple tables comma-separated
        sv.enableMorValuePredicatePushdownTables = "db1.tbl1,tbl2";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db2", "tbl1"));
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db2", "tbl2"));

        // case-insensitive matching
        sv.enableMorValuePredicatePushdownTables = "DB1.TBL1";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));

        // whitespace handling
        sv.enableMorValuePredicatePushdownTables = " tbl1 , db2.tbl2 ";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db2", "tbl2"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db1", "tbl2"));

        // null dbName — matches table-only entries, not db-qualified entries
        sv.enableMorValuePredicatePushdownTables = "tbl1,db2.tbl2";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled(null, "tbl1"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled(null, "tbl2"));

        // consecutive commas / empty entries
        sv.enableMorValuePredicatePushdownTables = "tbl1,,tbl2";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl2"));

        // ctl.db.table format — matches on db and table components
        sv.enableMorValuePredicatePushdownTables = "ctl1.db1.tbl1";
        Assertions.assertTrue(sv.isMorValuePredicatePushdownEnabled("db1", "tbl1"));
        Assertions.assertFalse(sv.isMorValuePredicatePushdownEnabled("db2", "tbl1"));
    }

    @Test
    public void testEnableStrictConsistencyDmlDefaultsToFalseInCloudMode() {
        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);
            SessionVariable sv = new SessionVariable();
            // In cloud mode, enable_strict_consistency_dml should always return false
            // because store-compute separation has no multi-replica consistency concern.
            Assertions.assertFalse(sv.isEnableStrictConsistencyDml());
            // Even if the field is set to true, cloud mode overrides it.
            sv.enableStrictConsistencyDml = true;
            Assertions.assertFalse(sv.isEnableStrictConsistencyDml());
        }
    }

    @Test
    public void testEnableStrictConsistencyDmlDefaultsTrueInNonCloudMode() {
        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(false);
            SessionVariable sv = new SessionVariable();
            // In non-cloud mode, default is true (multi-replica consistency is needed).
            Assertions.assertTrue(sv.isEnableStrictConsistencyDml());
            // Users can disable it.
            sv.enableStrictConsistencyDml = false;
            Assertions.assertFalse(sv.isEnableStrictConsistencyDml());
        }
    }

    @Test
    public void testEnablePreloadExternalMetadata() throws DdlException {
        Assertions.assertFalse(sessionVariable.isEnablePreloadExternalMetadata());

        // Verify the new preload switch can be changed through the standard session variable path.
        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.ENABLE_PRELOAD_EXTERNAL_METADATA,
                new StringLiteral("true")));

        Assertions.assertTrue(sessionVariable.isEnablePreloadExternalMetadata());
    }

    @Test
    public void testAnnSessionVariableChecker() throws Exception {
        SessionVariable sv = new SessionVariable();

        // hnsw_ef_search: valid value accepted
        VariableMgr.setVar(sv, new SetVar(SetType.SESSION, SessionVariable.HNSW_EF_SEARCH,
                new IntLiteral(1)));
        Assertions.assertEquals(1, sv.hnswEFSearch);

        // hnsw_ef_search: zero rejected
        DdlException hnswException = Assertions.assertThrows(DdlException.class,
                () -> VariableMgr.setVar(sv, new SetVar(SetType.SESSION,
                        SessionVariable.HNSW_EF_SEARCH, new IntLiteral(0))));
        Assertions.assertTrue(hnswException.getMessage().contains("hnsw_ef_search must be >= 1"));
        Assertions.assertEquals(1, sv.hnswEFSearch);

        // ivf_nprobe: valid value accepted
        VariableMgr.setVar(sv, new SetVar(SetType.SESSION, SessionVariable.IVF_NPROBE,
                new IntLiteral(2)));
        Assertions.assertEquals(2, sv.ivfNprobe);

        // ivf_nprobe: zero rejected
        DdlException nprobeException = Assertions.assertThrows(DdlException.class,
                () -> VariableMgr.setVar(sv, new SetVar(SetType.SESSION,
                        SessionVariable.IVF_NPROBE, new IntLiteral(0))));
        Assertions.assertTrue(nprobeException.getMessage().contains("ivf_nprobe must be >= 1"));
        Assertions.assertEquals(2, sv.ivfNprobe);
    }
}
