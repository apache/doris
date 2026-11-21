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

package org.apache.doris.nereids.spm;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class SPMHintReplayTest {
    private ConnectContext context;

    @BeforeEach
    void setUp() {
        context = new ConnectContext();
        context.setSessionVariable(new SessionVariable());
        context.getSessionVariable().setSqlMode(0L);
        context.getSessionVariable().setTimeZone("+00:00");
        context.setStatementContext(new StatementContext(context, null));
        context.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    private static List<SelectHint> hints(Plan plan) {
        List<SelectHint> result = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof LogicalSelectHint) {
                result.addAll(((LogicalSelectHint<?>) node).getHints());
            }
        });
        return result;
    }

    @Test
    void nestedSqlModeHintMatchesCreationReloadAndForwarding() throws Exception {
        String sql = "SELECT s.a || s.b AS c FROM"
                + " (SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ a, b FROM t) s";
        BaselinePlan baseline = SpmPlannerTestAccess.buildBaseline(new SPMPlanner(), sql, sql);
        Assertions.assertEquals(0L, baseline.getCreatorSqlMode());
        VariableMgr.revertSessionValue(context.getSessionVariable());
        context.getSessionVariable().clearSessionOriginValue();
        context.setStatementContext(new StatementContext(context, null));
        LogicalPlan caller = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(SPMPlanTreeSupport.check(baseline.getParameterizedBindPlan(), caller,
                new HashMap<>()));
        Assertions.assertEquals(baseline.getBindSqlDigest(), SPMPlanner.queryMatchKey(context, sql).first);
        Pair<LogicalPlan, LogicalPlan> rebuilt = SPMPlanner.rebuildParameterizedTrees(
                sql, sql, baseline.getCreatorSqlMode(), baseline.getPlanSqlMode());
        Assertions.assertTrue(SPMPlanTreeSupport.check(rebuilt.first, caller, new HashMap<>()));
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                context.getSessionVariable().getSqlMode(), "stored parsing must leave the live mode untouched");
    }

    @Test
    void replayTakesSetVarFromCallerAndPreservesPlanHints() {
        String bindSql = "SELECT /*+ SET_VAR(time_zone='+00:00') */ from_unixtime(k) AS c FROM t";
        String planSql = "SELECT /*+ SET_VAR(time_zone='+08:00') ORDERED */ from_unixtime(k) AS c FROM t";
        LogicalPlan bind = (LogicalPlan) SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(bindSql, 0L);
        LogicalPlan plan = (LogicalPlan) SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(planSql, 0L);
        LogicalPlan caller = new NereidsParser().parseSingle(bindSql);
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, caller, new HashMap<>()));
        LogicalPlan replay = SPMPlanTreeSupport.attachCallerSetVarHints(
                SPMPlanTreeSupport.stripSetVarHints(plan), caller);
        List<SelectHint> replayHints = hints(replay);
        Assertions.assertEquals(2, replayHints.size());
        Assertions.assertTrue(replayHints.stream().anyMatch(SelectHintOrdered.class::isInstance));
        SelectHintSetVar setVar = (SelectHintSetVar) replayHints.stream()
                .filter(SelectHintSetVar.class::isInstance).findFirst().orElseThrow();
        Assertions.assertEquals("+00:00", setVar.getParameters().get("time_zone").orElseThrow());
        setVar.applyToStatement(context.getStatementContext());
        Assertions.assertEquals("+00:00", context.getSessionVariable().getTimeZone());
    }

    @Test
    void planOnlySetVarCannotReachAnUnhintedCallerThroughAScalarSubquery() {
        String bindSql = "SELECT (SELECT from_unixtime(k) FROM t) AS c";
        String planSql = "SELECT (SELECT /*+ SET_VAR(time_zone='+08:00') ORDERED */"
                + " from_unixtime(k) FROM t) AS c";
        LogicalPlan plan = (LogicalPlan) SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(planSql, 0L);
        Assertions.assertEquals("+00:00", context.getSessionVariable().getTimeZone());
        LogicalPlan caller = new NereidsParser().parseSingle(bindSql);
        LogicalPlan stripped = SPMPlanTreeSupport.stripSetVarHints(plan);
        LogicalPlan replay = SPMPlanTreeSupport.attachCallerSetVarHints(stripped, caller);
        Assertions.assertSame(stripped, replay);
        Assertions.assertEquals(1, hints(replay).size());
        Assertions.assertTrue(hints(replay).get(0) instanceof SelectHintOrdered);
    }

    @Test
    void callerSetVarIsAttachedWhenTheCapturedPlanHasNone() {
        LogicalPlan caller = new NereidsParser().parseSingle(
                "SELECT /*+ SET_VAR(time_zone='+08:00') */ from_unixtime(k) AS c FROM t");
        LogicalPlan plan = new NereidsParser().parseSingle("SELECT from_unixtime(k) AS c FROM t");
        LogicalPlan replay = SPMPlanTreeSupport.attachCallerSetVarHints(plan, caller);
        Assertions.assertEquals(1, hints(replay).size());
        Assertions.assertEquals("+08:00", ((SelectHintSetVar) hints(replay).get(0))
                .getParameters().get("time_zone").orElseThrow());
    }

    @Test
    void repeatedStatementApplicationRetainsTheOriginalValueForCleanup() throws Exception {
        LogicalPlan caller = new NereidsParser().parseSingle(
                "SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ k FROM t");
        SelectHintSetVar setVar = (SelectHintSetVar) hints(caller).get(0);
        setVar.applyToStatement(context.getStatementContext());
        setVar.applyToStatement(context.getStatementContext());
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                context.getSessionVariable().getSqlMode());
        Assertions.assertEquals(0L, context.getStatementContext().getSpmOriginSqlMode());
        VariableMgr.revertSessionValue(context.getSessionVariable());
        Assertions.assertEquals(0L, context.getSessionVariable().getSqlMode());
    }

    @Test
    void protectedPlanHintsFailCreationInsteadOfBeingStoredAsRawFallback() {
        for (String assignment : List.of("enable_nereids_rules=''", "topn_lazy_materialization_threshold=1",
                "enable_cte_materialize=true", "inline_cte_referenced_threshold=0", "cte_inline_mode=0")) {
            String sql = "SELECT /*+ SET_VAR(" + assignment + ") */ 1 AS c";
            AnalysisException failure = Assertions.assertThrows(AnalysisException.class,
                    () -> new SPMPlanner().buildBaselineFromSql(context, sql, sql, false));
            Assertions.assertTrue(failure.getMessage().contains("SPM-protected session variable"),
                    failure.getMessage());
        }
    }
}
