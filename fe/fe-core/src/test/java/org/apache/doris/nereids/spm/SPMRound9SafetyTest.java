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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ninth review round: matching-safety / frozen-SQL / reload tests.
 *
 * Covered here:
 *  - the USING join's out-of-band MATCH_CONDITION is scanned for unreplaced placeholders by
 *    BOTH residue visitors (a plan-only interval id used to survive into the analyzer)
 *  - concrete string literals (e.g. a generator delimiter) are escaped so the frozen text
 *    re-parses to the SAME value
 *  - a SessionVarGuardExpr wrapper renders its child through the mapped visitor (the
 *    guard's own toSql() bypasses the ExprId -> column mapping)
 *  - every nested LogicalSelectHint is stripped from the in-memory fallback tree
 *  - reserved keywords are quoted when registered as identifiers
 *  - replay-time context expressions (current_user() / database() / ...) are detected and
 *    rejected before optimization can freeze the creator's value
 *  - the persisted creation sql_mode drives the bind-tree rebuild (PIPES_AS_CONCAT stays
 *    concat instead of degrading to a boolean Or)
 *  - MARK_CONDITION on a MARK join is routed by the parsed join type (the grammar's shared
 *    MATCH_CONDITION ( ... ) text), and duplicate MARK_SLOTs are rejected
 *  - a two-part relation is catalog-qualified even when no database is selected
 */
public class SPMRound9SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static int countSelectHints(Plan plan) {
        AtomicInteger count = new AtomicInteger();
        try {
            SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
                if (node instanceof LogicalSelectHint) {
                    count.incrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            throw e;
        }
        return count.get();
    }

    private static LogicalUsingJoin<LogicalPlan, LogicalPlan> usingJoinWithCondition(
            Expression matchCondition) {
        LogicalPlan left = parse("SELECT k FROM t1");
        LogicalPlan right = parse("SELECT k FROM t2");
        return new LogicalUsingJoin<>(JoinType.ASOF_LEFT_INNER_JOIN, left, right,
                List.of(), Optional.of(matchCondition), new DistributeHint(DistributeType.NONE));
    }

    // ==================== #4: USING join MATCH_CONDITION residue ====================

    @Test
    public void testUsingJoinMatchConditionIsScannedForResidue() {
        // bind-side residue: an SPM placeholder left inside the ASOF boundary
        LogicalUsingJoin<LogicalPlan, LogicalPlan> bindResidue =
                usingJoinWithCondition(new EqualTo(new IntegerLiteral(1), new SpmConstVar(9,
                        new IntegerLiteral(1))));
        Assertions.assertTrue(SPMPlanTreeSupport.containsPlaceholder(bindResidue),
                "containsPlaceholder must scan the out-of-band MATCH_CONDITION");

        // frozen-side residue: an unsubstituted _spm_const_var(id) call
        LogicalUsingJoin<LogicalPlan, LogicalPlan> frozenResidue =
                usingJoinWithCondition(new EqualTo(new IntegerLiteral(1),
                        new UnboundFunction("_spm_const_var", List.of(new IntegerLiteral(9)))));
        Assertions.assertTrue(SPMPlanTreeSupport.containsFrozenPlaceholder(frozenResidue),
                "containsFrozenPlaceholder must scan the out-of-band MATCH_CONDITION");

        // a clean condition stays clean
        LogicalUsingJoin<LogicalPlan, LogicalPlan> clean =
                usingJoinWithCondition(new EqualTo(new IntegerLiteral(1), new IntegerLiteral(1)));
        Assertions.assertFalse(SPMPlanTreeSupport.containsPlaceholder(clean));
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(clean));
    }

    // ==================== #5: generator string literal escaping ====================

    @Test
    public void testGeneratorStringLiteralIsEscaped() {
        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();
        String rendered = builder.print(new VarcharLiteral("a'b\\c"), new SQLRelation());
        Assertions.assertEquals("'a''b\\\\c'", rendered,
                "a concrete string literal must be escaped for the default sql mode");

        Expression reparsed = new NereidsParser().parseExpression(rendered);
        Assertions.assertTrue(reparsed instanceof StringLikeLiteral, reparsed.toString());
        Assertions.assertEquals("a'b\\c", ((StringLikeLiteral) reparsed).getStringValue(),
                "the frozen text must re-parse to the same value");
    }

    // ==================== #8: SessionVarGuardExpr through the mapped visitor ====================

    @Test
    public void testSessionVarGuardRendersThroughTheColumnMapping() {
        SlotReference slot = new SlotReference(new ExprId(1), "id", IntegerType.INSTANCE,
                true, List.of("t2"));
        SQLRelation relation = new SQLRelation();
        relation.registerRef(slot.getExprId(), "t2.id");

        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();
        Expression guarded = new SessionVarGuardExpr(slot, Map.of("enable_decimal256", "true"));
        Assertions.assertEquals("t2.id", builder.print(guarded, relation),
                "the guard's child must be rendered through the mapped visitor, not by the guard's"
                        + " own toSql() (which would emit the ambiguous bare id)");

        // and without the guard the rendering must be identical
        Assertions.assertEquals(builder.print(slot, relation), builder.print(guarded, relation));
    }

    // ==================== #7: nested SET_VAR hints are stripped ====================

    @Test
    public void testStripSelectHintsRemovesNestedHints() {
        LogicalPlan rootHint = parse(
                "SELECT /*+ SET_VAR(parallel_pipeline_task_num=4) */ k FROM t1");
        Assertions.assertTrue(countSelectHints(rootHint) >= 1);
        Assertions.assertEquals(0, countSelectHints(SPMPlanTreeSupport.stripSelectHints(rootHint)),
                "the root hint must be removed");

        LogicalPlan nested = parse("SELECT * FROM (SELECT /*+ SET_VAR(time_zone='+08:00') */ k"
                + " FROM t1) x JOIN t2 ON x.k = t2.k");
        Assertions.assertTrue(countSelectHints(nested) >= 1,
                "a hint inside a nested query block must be present in the parsed tree");
        Assertions.assertEquals(0, countSelectHints(SPMPlanTreeSupport.stripSelectHints(nested)),
                "every nested hint must be stripped (a root-only peel left the inner SET_VAR"
                        + " applied during replay analysis)");

        LogicalPlan plain = parse("SELECT k FROM t1");
        Assertions.assertEquals(0, countSelectHints(SPMPlanTreeSupport.stripSelectHints(plain)));
    }

    // ==================== #11: reserved identifiers are quoted ====================

    @Test
    public void testReservedIdentifiersAreQuoted() {
        Assertions.assertEquals("`from`",
                org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder.quoteIdentifier("from"));
        Assertions.assertEquals("`select`".toUpperCase(java.util.Locale.ROOT),
                org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder.quoteIdentifier("SELECT")
                        .toUpperCase(java.util.Locale.ROOT));
        Assertions.assertEquals("plain_name",
                org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder.quoteIdentifier("plain_name"));
        // the quoted form is a valid identifier reference
        Assertions.assertNotNull(new NereidsParser().parseSingle("SELECT `from` FROM t1"));
    }

    // ==================== #12: replay-time context expressions ====================

    @Test
    public void testReplayContextExpressionsAreDetected() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT current_user() AS u, k FROM t1 WHERE k = 1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT session_user() AS u FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT database() AS d FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT connection_id() AS c FROM t1")));
        // a nested subquery must be scanned too
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k IN (SELECT k FROM t2 WHERE k = 1"
                        + " AND current_user() IS NOT NULL)")));
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k = 1")));
    }

    // ==================== #13: persisted creation sql_mode ====================

    @Test
    public void testPersistedSqlModeDrivesBindRebuild() {
        String sql = "SELECT k FROM t1 WHERE a || b = 'x' ORDER BY k";
        Pair<LogicalPlan, LogicalPlan> concat = SPMPlanner.rebuildParameterizedTrees(
                sql, null, SqlModeHelper.MODE_PIPES_AS_CONCAT);
        Assertions.assertNotNull(concat.first);
        Assertions.assertTrue(concat.first.treeString().toLowerCase().contains("concat"),
                "PIPES_AS_CONCAT text must be rebuilt as concat(a, b): "
                        + concat.first.treeString());

        Pair<LogicalPlan, LogicalPlan> defaultMode = SPMPlanner.rebuildParameterizedTrees(
                sql, null, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertNotNull(defaultMode.first);
        Assertions.assertFalse(defaultMode.first.treeString().toLowerCase().contains("concat"),
                "the default mode rebuilds the same text as a boolean Or: "
                        + defaultMode.first.treeString());
    }

    // ==================== #14 / #6: MARK_CONDITION routing and duplicate MARK_SLOT ====================

    @Test
    public void testMarkConditionAloneBuildsAMarkJoin() {
        // the FIRST MARK_CONDITION (...) is parsed into the grammar's optional matchCondition
        // slot: it must be routed back to the mark conjuncts by the parsed join type instead
        // of being rejected as a non-ASOF MATCH_CONDITION
        LogicalPlan plan = parse("SELECT * FROM t1 LEFT SEMI MARK JOIN t2"
                + " MARK_CONDITION(t1.a = t2.b) MARK_SLOT m ON t1.k = t2.k");
        AtomicInteger markJoins = new AtomicInteger();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof org.apache.doris.nereids.trees.plans.logical.LogicalJoin
                    && ((org.apache.doris.nereids.trees.plans.logical.LogicalJoin<?, ?>) node).isMarkJoin()) {
                markJoins.incrementAndGet();
            }
        });
        Assertions.assertEquals(1, markJoins.get(), "the MARK join must build with mark conjuncts");
    }

    @Test
    public void testSecondMarkSlotIsRejected() {
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> parse(
                "SELECT * FROM t1 LEFT SEMI MARK JOIN t2 MARK_SLOT first MARK_SLOT second"
                        + " ON t1.k = t2.k"));
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains("at most one MARK_SLOT"),
                "a second MARK_SLOT must be rejected, not silently overwritten: " + e.getMessage());
    }

    // ==================== #15: two-part names are catalog-qualified ====================

    @Test
    public void testTwoPartRelationIsCatalogQualifiedWithoutDatabase() {
        List<List<String>> qualified = new ArrayList<>();
        LogicalPlan plan = parse("SELECT k FROM db1.t1 WHERE k = 1");
        LogicalPlan qualifiedPlan = SPMPlanTreeSupport.namespaceQualified(plan, "cat1", null);
        SPMPlanTreeSupport.<RuntimeException>walkPlans(qualifiedPlan, node -> {
            if (node instanceof UnboundRelation) {
                qualified.add(((UnboundRelation) node).getNameParts());
            }
        });
        Assertions.assertEquals(List.of(List.of("cat1", "db1", "t1")), qualified,
                "db.t is relative to the current CATALOG and must be prefixed even without a USE db");

        List<List<String>> onePart = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(
                SPMPlanTreeSupport.namespaceQualified(parse("SELECT k FROM t1"), "cat1", null),
                node -> {
                    if (node instanceof UnboundRelation) {
                        onePart.add(((UnboundRelation) node).getNameParts());
                    }
                });
        Assertions.assertEquals(List.of(List.of("t1")), onePart,
                "a one-part name cannot be made absolute without a database and stays verbatim");
    }
}
