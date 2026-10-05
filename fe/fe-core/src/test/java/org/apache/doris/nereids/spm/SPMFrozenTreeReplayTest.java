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

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.capture.AuditLogScanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * M3 milestone test: rewrite replays the FROZEN optimal plan (SR-aligned).
 *
 * After CREATE BASELINE (buildBaselineFromSql) the baseline's planSql is the frozen
 * optimal plan - decompiled from the SPM-optimized physical plan and carrying the
 * placeholder ids (_spm_const_var(id) / _spm_const_list(id)), the join
 * distribution hints ([BROADCAST] / [SHUFFLE]) and the pushed-down structure. On a
 * rewrite hit SPMPlanner re-parses that frozen text and substitutes the user values by
 * placeholder id (SPMFrozenTreeReplacer), so the rewritten tree starts from the
 * frozen optimal structure instead of the user's raw SQL structure.
 *
 * These tests hand-build a BaselinePlan with a frozen (placeholder-carrying) planSql -
 * the equivalent of what buildBaselineFromSql stores after a successful decompile - and
 * verify the rewrite path: scalar / CAST-wrapped placeholders, IN-list placeholders,
 * join distribution hint preservation, and the rejection of plan-only placeholders.
 */
public class SPMFrozenTreeReplayTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // ==================== frozen-tree rewrite (re-parse + id substitution) ====================

    /**
     * A scalar placeholder wrapped by the optimizer's CAST survives the replay: the
     * frozen text a > CAST(_spm_const_var(1) AS INT) is re-parsed, the placeholder
     * is replaced with the user value inside the CAST and no placeholder call remains.
     */
    @Test
    public void testFrozenScalarCastRewrite() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        // frozen planSql as produced by the decompiler (id-only placeholder, CAST kept)
        String frozenPlanSql =
                "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "structurally identical user query must hit the frozen-text baseline");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        String exprSqls = allExprSqls(rewritten);
        // the user value is substituted inside the preserved CAST wrapper
        Assertions.assertTrue(exprSqls.contains("CAST(42 AS INT)")
                        || exprSqls.contains("42"),
                "user value must be substituted into the frozen tree: " + exprSqls);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain in the rewritten tree");
        Assertions.assertFalse(exprSqls.contains("_spm_const_var"),
                "no placeholder may appear in the rewritten SQL: " + exprSqls);
    }

    /**
     * The frozen JOIN plan keeps its join distribution hint ([BROADCAST]) and its
     * subquery-nesting structure after the replay: the rewritten tree must carry the
     * hint so the later full optimization starts from the frozen join structure.
     */
    @Test
    public void testFrozenJoinHintPreserved() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.x WHERE t1.a > 100";
        // frozen planSql: filter pushed into a nested subquery + [BROADCAST] join hint
        String frozenPlanSql = "SELECT * FROM (SELECT * FROM t1 "
                + "WHERE (a > CAST(_spm_const_var(1) AS INT))) t_5 "
                + "INNER JOIN [BROADCAST] t2 ON (t1.a = t2.x)";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 JOIN t2 ON t1.a = t2.x WHERE t1.a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "frozen JOIN plan must be replayed on a hit");
        String tree = rewritten.treeString();
        Assertions.assertTrue(tree.contains("hint=[broadcast]"),
                "frozen join distribution hint must be preserved in the replay: " + tree);
        Assertions.assertTrue(allExprSqls(rewritten).contains("42"),
                "user value must be substituted: " + allExprSqls(rewritten));
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain: " + tree);
    }

    /**
     * An IN-list placeholder of the frozen text is replaced with the user's actual IN
     * list. The frozen text carries the type-coercion CAST wrapper around the list call
     * (exactly what the decompiler emits: b IN (CAST(_spm_const_list(1) AS INT))),
     * so the replacer must detect the list call THROUGH the CAST.
     */
    @Test
    public void testFrozenInListRewrite() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE b IN (1, 2, 3)";
        String frozenPlanSql =
                "SELECT * FROM t1 WHERE (b IN (CAST(_spm_const_list(1) AS INT)))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE b IN (10, 20)");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "IN-list query must be rewritten from the frozen text");
        String exprSqls = allExprSqls(rewritten);
        Assertions.assertTrue(exprSqls.contains("10"), "user IN value 10 missing: " + exprSqls);
        Assertions.assertTrue(exprSqls.contains("20"), "user IN value 20 missing: " + exprSqls);
        Assertions.assertFalse(exprSqls.contains("_spm_const_list"),
                "no list placeholder may remain: " + exprSqls);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain in the rewritten tree: " + exprSqls);
    }

    /**
     * A frozen placeholder with no user-extracted value (a plan-only placeholder) must
     * never reach the analyzer: the rewrite is rejected (falls back and returns null when
     * no other rewrite source exists).
     */
    @Test
    public void testFrozenPlanOnlyPlaceholderRejected() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        // frozen text carries an extra placeholder (id 2) with no bind-side counterpart
        String frozenPlanSql = "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT)) "
                + "AND (b < CAST(_spm_const_var(2) AS INT))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten,
                "a frozen plan-only placeholder (no user value) must reject the rewrite");
    }

    /**
     * round-41 #1: the caller-label realignment must not descend PAST the node that
     * produces the caller's rows. An aggregate-rooted query (GROUP BY without a
     * projection above the aggregate) has no root project, so the walk used to cross the
     * aggregate and land on the INNER derived-table project, renaming its items position
     * by position. The decompiler renders the frozen plan's own item order (here
     * (b, substring(..) AS cc)), which can differ from the caller's own text order (here
     * (cc, b)); the positional rename then SWAPPED the derived column names and the outer
     * references (GROUP BY cc, sum(b)) bound to the wrong side - the real TPCH q22 replay
     * grouped by c_acctbal and summed the country code. The walk must stop at the
     * aggregate (its outputs ARE the caller-visible list) and leave the inner block alone.
     */
    @Test
    public void testAggregateRootAlignKeepsInnerDerivedLabels() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT cc, count(*) AS n, sum(b) AS s "
                + "FROM (SELECT substr(p, 1, 2) AS cc, b FROM t1) t GROUP BY cc";
        // frozen text as the decompiler renders an aggregate-rooted query: the derived
        // projection is reordered (the computed column last) and the aggregate carries
        // the output list itself
        String frozenPlanSql = "SELECT cc, count(*) AS n, sum(b) AS s "
                + "FROM (SELECT b, substring(p, _spm_const_var(1), _spm_const_var(2)) AS cc "
                + "FROM t1) t GROUP BY cc";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "the aggregate-rooted baseline must replay");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        LogicalProject<?> derivedProject = findDerivedProject(rewritten);
        Assertions.assertNotNull(derivedProject,
                "the frozen replay must keep its derived projection: " + rewritten.treeString());
        Assertions.assertEquals(2, derivedProject.getProjects().size(),
                "the derived projection must keep both items: " + rewritten.treeString());
        // the inner query block's labels are NOT caller-visible output labels: the
        // alignment must leave them exactly as the frozen text spelled them
        Assertions.assertEquals("b", derivedProject.getProjects().get(0).getName(),
                "the derived item order must not be renamed: " + rewritten.treeString());
        Expression computed = derivedProject.getProjects().get(1);
        Assertions.assertTrue(computed instanceof UnboundAlias,
                "the computed column must stay an explicit alias: " + rewritten.treeString());
        Assertions.assertEquals("cc", ((UnboundAlias) computed).getAlias().orElse(null),
                "the computed derived column must keep its own label: "
                        + rewritten.treeString());
    }

    /**
     * round-41 #9: equal OUTER limits do not make a manual plan equivalent - its INNER cap
     * can truncate a DIFFERENT slice before the caller's own sort (ORDER BY ASC LIMIT 2
     * inside, then a descending outer sort). The candidate must be skipped although both
     * trees ask for LIMIT 2.
     */
    @Test
    public void testInnerCapOfAManualPlanIsCheckedForEqualOuterLimits() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT k FROM t1 WHERE k > 5 ORDER BY k DESC LIMIT 2";
        String frozenPlanSql = "SELECT k FROM (SELECT k FROM t1 WHERE k > "
                + "_spm_const_var(1) ORDER BY k ASC LIMIT 2) t_0 ORDER BY k DESC LIMIT 2";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten, "the manual plan's inner cap picks a different"
                + " slice than the caller's own plan; the candidate must be skipped");
    }

    /**
     * Control for round-41 #9: a frozen plan whose caps are exactly the caller's own (a
     * single top-level TopN) keeps hitting.
     */
    @Test
    public void testFrozenPlanWithOnlyTheCallersOwnCapStillHits() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT k FROM t1 WHERE k > 5 ORDER BY k DESC LIMIT 2";
        String frozenPlanSql = "SELECT k FROM t1 WHERE k > CAST(_spm_const_var(1) AS INT)"
                + " ORDER BY k DESC LIMIT 2";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "a frozen plan carrying exactly the caller's own cap must keep hitting");
    }

    /**
     * round-42 #5: matching the CAPTURED limit VALUE is not a contract. A manual plan
     * {@code 'SELECT k FROM t1'} freezes a text with NO limit node at all - mergeLimits
     * only replaces the VALUE of a cap that aligns positionally, it cannot ADD a missing
     * one - so a LIMIT 2 caller used to be answered with EVERY row. The candidate must be
     * skipped unless the replayed tree actually carries the caller's cap.
     */
    @Test
    public void testReplayWithoutTheCallersCapIsRejected() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT k FROM t1 ORDER BY k LIMIT 2";
        // the frozen manual plan has no cap the merge could align with
        String frozenPlanSql = "SELECT k FROM t1";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten,
                "a replay that cannot CARRY the caller's LIMIT must be skipped");
        Assertions.assertTrue(planner.getUsedBaselineId() <= 0,
                "no baseline may be reported as hit: " + planner.getUsedBaselineId());
    }

    /**
     * round-42 #11: the CALLER's own nested caps must survive the replay as well. A manual
     * plan that kept only the OUTER cap passed the one-directional containment
     * (replayed caps ⊆ caller caps) although the replayed tree answers with the outer
     * cap ALONE: the caller's INNER cap used to truncate a different slice first, so the
     * two plans return different rows.
     */
    @Test
    public void testCallerInnerCapMustSurviveTheReplay() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT k FROM (SELECT k FROM t1 ORDER BY k LIMIT 1) s"
                + " ORDER BY k LIMIT 2";
        String frozenPlanSql = "SELECT k FROM t1 ORDER BY k LIMIT 2";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten,
                "the replay lacks the caller's INNER cap: it returns more rows than the"
                        + " caller's own plan, so the candidate must be skipped");
        Assertions.assertTrue(planner.getUsedBaselineId() <= 0,
                "no baseline may be reported as hit: " + planner.getUsedBaselineId());
    }

    /**
     * round-42 #11: a cap's identity is (value, occurrence, AGGREGATE-BELOW, ORDER-BY
     * SLICE), not the value alone. A manual plan whose inner TOP-N sorts a DIFFERENT
     * direction truncates a different slice; the previous value-only key matched it and
     * answered an ascending caller with the descending slice.
     */
    @Test
    public void testCapSliceMustMatchNotOnlyTheValue() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT k FROM (SELECT k FROM t1 ORDER BY k DESC LIMIT 1) s";
        String frozenPlanSql = "SELECT k FROM (SELECT k FROM t1 ORDER BY k ASC LIMIT 1) s";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse(bindSql);
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten,
                "a cap over a DIFFERENT ORDER-BY slice picks other rows: same LIMIT value"
                        + " is not the same cap");
        Assertions.assertTrue(planner.getUsedBaselineId() <= 0,
                "no baseline may be reported as hit: " + planner.getUsedBaselineId());
    }

    /**
     * round-41 #2: a real global UDF named like the marker whose FIRST argument is not the
     * marker id must still have its OTHER arguments substituted. The old early return left
     * a genuine nested marker in the tree, and the residue scan then rejected a persisted
     * frozen baseline that has no parameterized-tree fallback.
     */
    @Test
    public void testMarkerNamedFunctionStillHasItsArgumentsSubstituted() throws Exception {
        installConnectContext();
        // the explicit alias keeps the DERIVED label (which carries the original text
        // verbatim as a plain name) out of the collected expression SQL
        LogicalPlan parsed = parse("SELECT _spm_const_var(a, _spm_const_var(1)) AS f FROM t1");
        java.util.Map<Long, Expression> values = new java.util.HashMap<>();
        values.put(1L, new org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral(42));
        SPMFrozenTreeReplacer replacer = new SPMFrozenTreeReplacer();
        LogicalPlan rewritten = SPMPlanTreeSupport.transform(parsed,
                expr -> expr.accept(replacer, values));
        String sql = allExprSqls(rewritten);
        Assertions.assertTrue(sql.contains("42"),
                "the nested marker must be substituted: " + sql);
        Assertions.assertFalse(sql.contains("_spm_const_var(1)"),
                "no marker may remain: " + sql);
        // the UnboundSlot first argument renders quoted ('a): the real call stays an
        // unqualified UDF with a NON-numeric first argument, so it is not a marker
        Assertions.assertTrue(sql.contains("_spm_const_var('a"),
                "the real UDF call itself must stay: " + sql);
    }

    // ==================== text classification: only REAL placeholder calls are frozen ====================

    @Test
    public void testFrozenClassificationIsTreeBased() {
        // real placeholder calls (anywhere in the tree) mark a stored text as frozen
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(
                "SELECT * FROM t1 WHERE a > _spm_const_var(1)"));
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(
                "SELECT * FROM t1 WHERE a IN (_spm_const_list(3))"));
        // ... but a mere SUBSTRING inside a literal / identifier / comment does not: the
        // fallback path stores the ORIGINAL planSql when the decompiler rejects a node,
        // and replaying that text would return the CAPTURED literals
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(
                "SELECT 'note _spm_const_var(1) here' AS v FROM t1"));
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql("SELECT _spm_const_list FROM t1"));
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(
                "SELECT * FROM t1 /* _spm_const_var(2) */ WHERE a > 100"));
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql("SELECT * FROM t1 WHERE a > 100"));
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(null));
        // the PERSISTED provenance overrides the text classifier in both directions:
        // a literal-free optimized join decompiles to text with NO placeholder call and
        // must still be frozen when the row says so; an explicit non-frozen flag wins
        // over a placeholder-shaped name in a literal
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(
                "SELECT * FROM [SHUFFLE] t1 INNER JOIN [BROADCAST] t2 ON (t1.a = t2.x)",
                Boolean.TRUE), "a marker-free decompiled text is frozen when the row says so");
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(
                "SELECT * FROM t1 WHERE a > _spm_const_var(1)", Boolean.FALSE),
                "an explicit non-frozen flag wins over a placeholder-shaped name");
    }

    /**
     * R11-8: a literal-free optimized plan (no spm_const* call anywhere) still replays
     * as frozen when the row is marked plan_frozen=true - an EMPTY substitution map is
     * legitimate. Before the fix the marker check recorded planFrozen=false for such a
     * decompile, every replay rejected the frozen text and fell back to the original
     * pre-optimization tree, losing exactly the stored join / distribution choice.
     */
    @Test
    public void testLiteralFreeFrozenJoinReplaysImmediately() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.x";
        // decompiled rendering of a literal-free hinted join: NO placeholder call
        String frozenPlanSql = "SELECT * FROM (SELECT * FROM t1) t_5 "
                + "INNER JOIN [BROADCAST] t2 ON (t1.a = t2.x)";
        BaselinePlan baseline = frozenBaseline(bindSql, frozenPlanSql);
        baseline.setPlanFrozen(Boolean.TRUE);
        manager.createBaseline(baseline);

        LogicalPlan userPlan = parse(bindSql);
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan,
                System.currentTimeMillis() + 5000);

        Assertions.assertNotNull(rewritten,
                "a marker-free FROZEN text must replay (the provenance is the decompile,"
                        + " not the placeholder presence)");
        Assertions.assertTrue(rewritten.treeString().contains("hint=[broadcast]"),
                "the frozen join distribution choice must survive: " + rewritten.treeString());
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "an empty substitution map is fine: no placeholder may remain");
    }

    /**
     * R11-8 (refresh / restart): the row is rebuilt from the persisted planFrozen=true +
     * the SAME marker-free decompiled text, and the rebuilt baseline replays the frozen
     * structure instead of flipping to the parameterized fallback tree.
     */
    @Test
    public void testLiteralFreeFrozenSurvivesReloadReplay() throws Exception {
        installConnectContext();
        String bindSql = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.x";
        String frozenPlanSql = "SELECT * FROM (SELECT * FROM t1) t_5 "
                + "INNER JOIN [BROADCAST] t2 ON (t1.a = t2.x)";
        String digest = parse(bindSql).toSpmDigest();
        long hash = SPMUtils.hashOf(digest);

        ResultRow row = new ResultRow(List.of(
                "77", bindSql, digest, String.valueOf(hash), frozenPlanSql, "", "1.0",
                "-1", "USER", "ENABLED", "2026-01-01 00:00:00", "2026-01-01 00:00:00",
                String.valueOf(SqlModeHelper.MODE_DEFAULT),
                String.valueOf(SqlModeHelper.MODE_DEFAULT), "true", ""));
        BaselinePlan rebuilt = BaselineManager.parsePersistedRowForTest(row);
        Assertions.assertNull(rebuilt.getParameterizedPlanPlan(),
                "a frozen row is replayed as text - no parameterized plan tree is built");
        manager.createBaseline(rebuilt);

        SPMPlanner planner = new SPMPlanner();
        LogicalPlan rewritten = planner.tryRewritePlan(parse(bindSql),
                System.currentTimeMillis() + 5000);
        Assertions.assertNotNull(rewritten, "the reloaded marker-free frozen row must replay");
        Assertions.assertTrue(rewritten.treeString().contains("hint=[broadcast]"),
                "the frozen join shape must survive the reload: " + rewritten.treeString());
    }

    /**
     * R11-8 companion: a STALE plan_frozen=false on a row whose planSql re-parses into
     * REAL placeholder calls (pre-provenance rows migrated with a default flag / an old
     * release that recorded false for a successful marker-free decompile) must not kill
     * the baseline. The parameterized fallback tree is rebuilt from an ALREADY
     * parameterized text, its reconstructed placeholder ids do not line up with the
     * values extracted from the bind tree, the residue safety net rejects the rewrite -
     * the row would silently never apply. The text is the authority here.
     */
    @Test
    public void testStaleNotFrozenFlagOnMarkerTextIsIgnored() throws Exception {
        installConnectContext();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        String frozenPlanSql = "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))";
        String digest = parse(bindSql).toSpmDigest();
        long hash = SPMUtils.hashOf(digest);
        ResultRow row = new ResultRow(List.of(
                "88", bindSql, digest, String.valueOf(hash), frozenPlanSql, "", "1.0",
                "-1", "USER", "ENABLED", "2026-01-01 00:00:00", "2026-01-01 00:00:00",
                String.valueOf(SqlModeHelper.MODE_DEFAULT),
                String.valueOf(SqlModeHelper.MODE_DEFAULT), "false", ""));
        BaselinePlan rebuilt = BaselineManager.parsePersistedRowForTest(row);
        Assertions.assertNull(rebuilt.getParameterizedPlanPlan(),
                "the stale flag is upgraded: the row is replayed as frozen text");
        manager.createBaseline(rebuilt);

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        LogicalPlan rewritten = new SPMPlanner().tryRewritePlan(userPlan,
                System.currentTimeMillis() + 5000);
        Assertions.assertNotNull(rewritten,
                "the stale-flag row must replay through its placeholder text");
        Assertions.assertTrue(allExprSqls(rewritten).contains("42"),
                "the user value must be substituted: " + allExprSqls(rewritten));
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder may remain after the replay");
    }

    /**
     * An ordinary fallback text whose literal merely CONTAINS a placeholder name must not
     * be replayed as frozen: the replacement substitutes nothing and the returned tree
     * would carry the CAPTURED literal values.
     */
    @Test
    public void testLiteralContainingPlaceholderNameIsNotReplayed() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        String ordinaryPlanSql = "SELECT * FROM t1 WHERE a > 100 AND 'x _spm_const_var(1) y' <> ''";
        manager.createBaseline(frozenBaseline(bindSql, ordinaryPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        // no parameterized plan tree was stored on this hand-built baseline, so a
        // correctly rejected replay simply yields no rewrite (the captured 100 is never
        // returned as a "frozen" tree)
        Assertions.assertNull(rewritten,
                "an ordinary text containing a placeholder NAME in a literal must not be"
                        + " replayed as a frozen plan");
    }

    /**
     * A frozen placeholder inside a SUBQUERY plan (IN / EXISTS / scalar / residual
     * NOT IN - the LEFT NULL_AWARE ANTI JOIN case) must be substituted: the generic
     * visitor only walks Expression.children(), so without the explicit
     * visitSubqueryExpr the residue scan finds the call and rejects the replay.
     */
    @Test
    public void testFrozenSubqueryPlaceholdersAreSubstituted() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        assertFrozenSubqueryRewrite(planner,
                "SELECT * FROM t1 WHERE a IN (SELECT x FROM t2 WHERE y > 100)",
                "SELECT * FROM t1 WHERE a IN (SELECT x FROM t2 WHERE y > _spm_const_var(1))",
                "SELECT * FROM t1 WHERE a IN (SELECT x FROM t2 WHERE y > 42)");
        assertFrozenSubqueryRewrite(planner,
                "SELECT * FROM t1 WHERE EXISTS (SELECT x FROM t2 WHERE y > 100)",
                "SELECT * FROM t1 WHERE EXISTS (SELECT x FROM t2 WHERE y > _spm_const_var(1))",
                "SELECT * FROM t1 WHERE EXISTS (SELECT x FROM t2 WHERE y > 42)");
        assertFrozenSubqueryRewrite(planner,
                "SELECT * FROM t1 WHERE a > (SELECT max(y) FROM t2 WHERE z > 100)",
                "SELECT * FROM t1 WHERE a > (SELECT max(y) FROM t2 WHERE z > _spm_const_var(1))",
                "SELECT * FROM t1 WHERE a > (SELECT max(y) FROM t2 WHERE z > 42)");
        assertFrozenSubqueryRewrite(planner,
                "SELECT * FROM t1 WHERE a NOT IN (SELECT x FROM t2 WHERE y > 100)",
                "SELECT * FROM t1 WHERE a NOT IN (SELECT x FROM t2 WHERE y > _spm_const_var(1))",
                "SELECT * FROM t1 WHERE a NOT IN (SELECT x FROM t2 WHERE y > 42)");
    }

    /** Asserts one frozen-subquery replay substitutes the user value everywhere. */
    private void assertFrozenSubqueryRewrite(SPMPlanner planner, String bindSql,
            String frozenPlanSql, String userSql) throws Exception {
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(parse(userSql), deadline);
        Assertions.assertNotNull(rewritten,
                "a frozen placeholder inside a subquery plan must be substituted: "
                        + frozenPlanSql);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain anywhere in the rewritten tree");
        String exprSqls = allExprSqlsDeep(rewritten);
        Assertions.assertTrue(exprSqls.contains("42"),
                "the user value must be substituted into the subquery: " + exprSqls);
    }

    /**
     * The captured session mode is recorded on the baseline and drives the reload's
     * re-parse of the stored bindSql: under MODE_DEFAULT "a || b" rebuilds as a boolean
     * Or, so a CONCAT-mode baseline would silently stop matching after a restart.
     */
    @Test
    public void testAuditSqlModeDrivesBuildAndReload() throws Exception {
        long concatMode = SqlModeHelper.MODE_PIPES_AS_CONCAT;
        Assertions.assertEquals(concatMode, AuditLogScanner.decodeAuditSqlMode("PIPES_AS_CONCAT"),
                "the audit_log mode text must decode back to the parser mode");

        String sql = "SELECT a || b FROM t1";
        BaselinePlan baseline = SqlModeHelper.withSqlMode(concatMode, () -> {
            try {
                return new SPMPlanner().buildBaseline(sql, sql);
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
        });
        Assertions.assertEquals(concatMode, baseline.getCreatorSqlMode(),
                "the build must RECORD the originating parser mode for the reload");

        LogicalPlan reloadedDefault = SPMPlanner.rebuildParameterizedTrees(
                sql, null, SqlModeHelper.MODE_DEFAULT).first;
        LogicalPlan reloadedCaptured = SPMPlanner.rebuildParameterizedTrees(
                sql, null, concatMode).first;
        Assertions.assertNotNull(reloadedCaptured);
        Assertions.assertNotEquals(reloadedDefault.toSpmDigest(),
                reloadedCaptured.toSpmDigest(),
                "CONCAT semantics must survive the reload (the parse is mode-dependent)");
    }

    // ==================== helpers ====================

    /**
     * Installs a minimal ConnectContext on this thread (join-hint / statement-context
     * parsing of the frozen text needs it, exactly like the real StmtExecutor path).
     */
    private static void installConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("SELECT 1", 0)));
    }

    /**
     * Hand-builds a baseline whose planSql is a frozen (placeholder-carrying) plan text -
     * the equivalent of what buildBaselineFromSql stores after a successful decompile.
     * The parameterized bind tree is produced by the same whole-tree parameterization the
     * real CREATE path runs, so the placeholder ids align with the frozen text.
     */
    private static BaselinePlan frozenBaseline(String bindSql, String frozenPlanSql)
            throws Exception {
        LogicalPlan bindPlan = parse(bindSql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));

        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql(bindSql);
        baseline.setBindSqlDigest(bindPlan.toSpmDigest());
        baseline.setBindSqlHash(SPMUtils.hashOf(bindPlan.toSpmDigest()));
        baseline.setPlanSql(frozenPlanSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setCost(0.0);
        return baseline;
    }

    /** Parses a single SELECT SQL into an unbound logical plan. */
    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The projection of the first derived table / subquery alias in the tree. */
    private static LogicalProject<?> findDerivedProject(Plan plan) {
        if (plan instanceof LogicalSubQueryAlias<?> && plan.child(0) instanceof LogicalProject) {
            return (LogicalProject<?>) plan.child(0);
        }
        for (Plan child : plan.children()) {
            LogicalProject<?> found = findDerivedProject(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Concatenates the SQL text of every expression, recursing into subquery plans. */
    private static String allExprSqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqls(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqls(Plan plan, StringBuilder sb) {
        for (Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
        }
        for (Plan child : plan.children()) {
            collectExprSqls(child, sb);
        }
    }

    /** Like {@link #allExprSqls} but also recurses into subquery PLANS. */
    private static String allExprSqlsDeep(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqlsDeep(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqlsDeep(Plan plan, StringBuilder sb) {
        for (Expression expr : plan.getExpressions()) {
            collectExprSql(expr, sb);
        }
        for (Plan child : plan.children()) {
            collectExprSqlsDeep(child, sb);
        }
    }

    private static void collectExprSql(Expression expr, StringBuilder sb) {
        sb.append(expr.toString()).append('\n');
        if (expr instanceof SubqueryExpr) {
            collectExprSqlsDeep(((SubqueryExpr) expr).getQueryPlan(), sb);
        }
        for (Expression child : expr.children()) {
            collectExprSql(child, sb);
        }
    }
}
