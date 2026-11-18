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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
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
import java.util.List;

class SqlModeHintParsingTest {
    private ConnectContext context;

    @BeforeEach
    void setUp() {
        context = new ConnectContext();
        context.setSessionVariable(new SessionVariable());
        context.setThreadLocalInfo();
        reset(0L);
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    private void reset(long mode) {
        context.getSessionVariable().clearSessionOriginValue();
        context.getSessionVariable().setIsSingleSetVar(false);
        context.getSessionVariable().setSqlMode(mode);
        context.setStatementContext(new StatementContext(context, null));
    }

    private static void assertPipeOperators(Plan plan, long mode) {
        List<Expression> operators = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            for (Expression expression : node.getExpressions()) {
                operators.addAll(expression.collectToList(expr -> expr instanceof Or
                        || expr instanceof UnboundFunction && ((UnboundFunction) expr).getName().equals("concat")));
            }
        });
        Assertions.assertEquals(2, operators.size(), plan.treeString());
        for (Expression operator : operators) {
            Assertions.assertEquals(mode == 0L, operator instanceof Or, plan.treeString());
        }
    }

    @Test
    void nestedHintsCannotChangeTheModeOfLaterExpressions() throws Exception {
        for (long mode : List.of(0L, SqlModeHelper.MODE_PIPES_AS_CONCAT)) {
            String hint = mode == 0L ? "PIPES_AS_CONCAT" : "";
            List<String> statements = List.of(
                    "SELECT s.a || s.b AS c FROM (SELECT /*+ SET_VAR(sql_mode='" + hint
                            + "') */ a || b AS x, a, b FROM t) s",
                    "WITH c AS (SELECT /*+ SET_VAR(sql_mode='" + hint
                            + "') */ a || b AS x, a, b FROM t) SELECT a || b AS c FROM c",
                    "SELECT a || b AS c, (SELECT /*+ SET_VAR(sql_mode='" + hint
                            + "') */ x || y FROM u) AS d FROM t",
                    "SELECT /*+ SET_VAR(sql_mode='" + hint
                            + "') */ a || b AS c FROM t UNION ALL SELECT a || b AS c FROM u");
            for (String sql : statements) {
                reset(mode);
                Plan plan = ((LogicalPlanAdapter) new NereidsParser().parseSQL(sql).get(0)).getLogicalPlan();
                assertPipeOperators(plan, mode);
                Assertions.assertEquals(mode == 0L ? SqlModeHelper.MODE_PIPES_AS_CONCAT : 0L,
                        context.getSessionVariable().getSqlMode());
                Assertions.assertEquals(mode, context.getStatementContext().getSpmOriginSqlMode());
                VariableMgr.revertSessionValue(context.getSessionVariable());
                Assertions.assertEquals(mode, context.getSessionVariable().getSqlMode());
            }
        }
    }

    @Test
    void cachedParsingUsesTheSameModeOnMissAndHit() {
        String sql = "SELECT s.a || s.b AS c FROM (SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */"
                + " a || b AS x, a, b FROM t) s";
        for (int pass = 0; pass < 2; pass++) {
            reset(0L);
            assertPipeOperators(new NereidsParser().parseSingleCached(sql), 0L);
            Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                    context.getSessionVariable().getSqlMode());
        }
    }

    @Test
    void nestedHintsCannotChangeStringLiteralDecoding() {
        for (long mode : List.of(0L, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES)) {
            reset(mode);
            String hintedMode = mode == 0L ? "NO_BACKSLASH_ESCAPES" : "";
            Plan plan = new NereidsParser().parseSingle("SELECT 'a\\nb' AS c FROM"
                    + " (SELECT /*+ SET_VAR(sql_mode='" + hintedMode + "') */ k FROM t) s");
            List<StringLikeLiteral> literals = new ArrayList<>();
            SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
                for (Expression expression : node.getExpressions()) {
                    literals.addAll(expression.collectToList(StringLikeLiteral.class::isInstance));
                }
            });
            Assertions.assertEquals(1, literals.size());
            Assertions.assertEquals(mode == 0L ? "a\nb" : "a\\nb", literals.get(0).getStringValue());
        }
    }

    @Test
    void statementBatchRetainsTheActualParseModeForEachForwardedStatement() {
        String sql = "SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ a || b, b || a FROM t;"
                + "SELECT a || b, b || a FROM u";
        new NereidsParser().parseMultiple(sql).forEach(parsed -> {
            assertPipeOperators(parsed.first, 0L);
            Assertions.assertEquals(0L, parsed.second.getSpmOriginSqlMode());
        });
    }

    @Test
    void viewParsingUsesOneModeForAllBlocks() {
        String sql = "SELECT s.a || s.b AS c FROM (SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */"
                + " a || b AS x, a, b FROM t) s";
        assertPipeOperators(new NereidsParser().parseForCreateView(sql), 0L);
    }

    @Test
    void scalarSubqueryHintsBelongOnlyToTheirOwnBlock() {
        String sql = "SELECT (SELECT /*+ SET_VAR(time_zone='+08:00') ORDERED */ k FROM t) AS c";
        for (int pass = 0; pass < 2; pass++) {
            Plan plan = new NereidsParser().parseSingleCached(sql);
            List<LogicalSelectHint<?>> hintedBlocks = new ArrayList<>();
            SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
                if (node instanceof LogicalSelectHint) {
                    hintedBlocks.add((LogicalSelectHint<?>) node);
                }
            });
            Assertions.assertEquals(1, hintedBlocks.size(), plan.treeString());
            Assertions.assertEquals(2, hintedBlocks.get(0).getHints().size());
        }
    }

    @Test
    void multipleHintsOfOneBlockApplyInSqlTextOrder() {
        String sql = "SELECT /*+ SET_VAR(time_zone='+08:00') */"
                + "       /*+ SET_VAR(time_zone='+00:00') */ k FROM t";
        for (int pass = 0; pass < 2; pass++) {
            Plan plan = new NereidsParser().parseSingleCached(sql);
            LogicalSelectHint<?> hintedBlock = (LogicalSelectHint<?>) plan.child(0);
            List<String> zones = new ArrayList<>();
            hintedBlock.getHints().forEach(hint -> zones.add(
                    ((SelectHintSetVar) hint).getParameters().get("time_zone").orElseThrow()));
            Assertions.assertEquals(List.of("+08:00", "+00:00"), zones);
            Assertions.assertEquals("+00:00", context.getSessionVariable().getTimeZone());
        }
    }

    @Test
    void modeOverrideSurvivesNestedParsingFailureWithoutRestoringTheLiveSession() {
        SqlModeHelper.withSqlMode(0L, () -> {
            context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
            Assertions.assertThrows(RuntimeException.class, () -> new NereidsParser().parseSingle(
                    "SELECT CAST(s.a AS DECIMAL(99, 1)) FROM"
                            + " (SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ a FROM t) s"));
            Assertions.assertEquals(0L, SqlModeHelper.currentMode());
            return null;
        });
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT, SqlModeHelper.currentMode());
    }

    @Test
    void parseTimeSetVarSuppressionRestoresItsStateAfterNestedFailure() {
        String sql = "SELECT /*+ SET_VAR(time_zone='+09:30') */ k FROM t";
        String before = context.getSessionVariable().getTimeZone();
        SelectHintSetVar.withoutParseTimeSetVar(() -> {
            Assertions.assertTrue(SelectHintSetVar.isParseTimeSetVarSuppressed());
            Assertions.assertThrows(IllegalStateException.class,
                    () -> SelectHintSetVar.withoutParseTimeSetVar(() -> {
                        throw new IllegalStateException("nested parser failure");
                    }));
            Assertions.assertTrue(SelectHintSetVar.isParseTimeSetVarSuppressed());
            new NereidsParser().parseSingle(sql);
            Assertions.assertEquals(before, context.getSessionVariable().getTimeZone());
            return null;
        });
        Assertions.assertFalse(SelectHintSetVar.isParseTimeSetVarSuppressed());
        new NereidsParser().parseSingle(sql);
        Assertions.assertEquals("+09:30", context.getSessionVariable().getTimeZone());
    }
}
