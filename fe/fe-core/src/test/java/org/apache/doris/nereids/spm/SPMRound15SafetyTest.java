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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * The bind / plan INPUT walks and the parse mode.
 *
 * - The function dependency walk must reach `* REPLACE` payloads (they live OUTSIDE
 *   children()) on the bind side AND cover the functions used only by the stored plan.
 * - The runtime-context guard must reject a parsed `@v` (an UnboundVariable, not the
 *   analyzed Variable), user() / current_catalog() / last_query_id() / version(), and
 *   the payloads of `* REPLACE`.
 * - `KEY db.key` parses straight to a bound EncryptKeyRef - the name-only
 *   UnboundFunction check never fired and the optimizer folded the secret into the
 *   frozen plan.
 * - Both CREATE texts must be parsed inside ONE captured sql_mode window, so a
 *   SET_VAR(sql_mode=...) hint of the first text cannot decide how the second one is
 *   read.
 */
public class SPMRound15SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** A resolvable-looking table identity for the fingerprint entries. */
    private static TableIf table(String name, long id, String column) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getBaseSchema()).thenReturn(
                List.of(new Column(column, Type.INT)));
        return table;
    }

    /** A session whose statement context resolves the bind-side relation to bind. */
    private static ConnectContext contextResolvingTo(TableIf bind) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenReturn(bind);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenReturn(bind);
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /** SQL of every expression in the tree (the plan tree is not flat). */
    private static String expressionSql(Plan plan) {
        StringBuilder sql = new StringBuilder();
        for (org.apache.doris.nereids.trees.expressions.Expression expr : plan.getExpressions()) {
            sql.append(expr.toSql()).append(' ');
        }
        for (Plan child : plan.children()) {
            sql.append(expressionSql(child));
        }
        return sql.toString().toLowerCase(java.util.Locale.ROOT);
    }

    // ==================== #4: `* REPLACE` payloads join the function walk ====================

    /**
     * The payload of `SELECT * REPLACE(f(k) AS k)` lives in UnboundStar.getReplacedAlias(),
     * OUTSIDE the expression children: a walk that only visits children() missed every
     * function referenced there, so redefining f after CREATE left the stored fingerprint
     * (and the bind SQL) unchanged while the frozen plan still inlined the OLD body.
     */
    @Test
    public void testStarReplaceFunctionJoinsTheFingerprint() {
        ConnectContext ctx = contextResolvingTo(table("t_bind", 7L, "k"));
        try {
            String control = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx,
                    parse("SELECT * FROM internal.spm_db.t_bind"), null, null);
            Assertions.assertFalse(control.contains("abs"),
                    "control: no function is referenced here: " + control);

            String replaced = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx,
                    parse("SELECT * REPLACE(abs(k) AS k) FROM internal.spm_db.t_bind"),
                    null, null);
            Assertions.assertTrue(replaced.contains("abs"),
                    "a function used by a * REPLACE payload must be part of the"
                            + " fingerprint: " + replaced);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #5: parsed runtime contexts are rejected ====================

    /**
     * The parsed shape of `@v` is an UnboundVariable - NOT the analyzed Variable the
     * guard knew - so `WHERE k = @v` slipped through and the creator-context
     * optimization froze the CREATOR's value into a global baseline. user() /
     * current_catalog() / last_query_id() / version() are the same creator /
     * environment values, and a `* REPLACE` payload sits outside children().
     */
    @Test
    public void testParsedRuntimeContextsAreDetected() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k = @v")),
                "a parsed @v is an UnboundVariable and must be rejected");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT user() AS u FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT version() AS v FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT current_catalog() AS c FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT last_query_id() AS q FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT * REPLACE(database() AS k) FROM t1")),
                "the payloads of * REPLACE sit outside children() and must be scanned");
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT abs(k) AS k FROM t1")),
                "a plain function call is not a replay-time context");
    }

    /** The CREATE entry must surface the rejection (and not freeze the value). */
    @Test
    public void testCreateRejectsParsedSessionVariable() {
        SPMPlanner planner = new SPMPlanner();
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> planner.buildBaseline("SELECT k FROM t1 WHERE k = @v",
                        "SELECT k FROM t1 WHERE k = @v"));
        Assertions.assertTrue(failure.getMessage().contains("replay-time context"),
                failure.getMessage());
    }

    /**
     * CLOCK FUNCTIONS are the same creator-time class of value. FE constant
     * folding evaluates now() / current_timestamp() from the CREATE statement's start time
     * (DateTimeAcquire#currentDateTime uses ConnectContext#getStartTimeInstant), and the
     * decompiler stores that literal in the frozen planFrozen SQL: a baseline for
     * SELECT now() AS ts FROM t1 returned the CREATE timestamp on every later
     * matching query, and WHERE event_time < now() replayed with a stale cutoff.
     */
    @Test
    public void testCreatorTimeClockFunctionsAreDetected() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT now() AS ts FROM t1")),
                "now() freezes the CREATE statement's start time");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT current_timestamp() AS ts FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT current_date() AS d FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT utc_timestamp() AS ts FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT localtime() AS ts FROM t1")));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k < unix_timestamp()")),
                "the argument-less unix_timestamp() is the statement clock as well");
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT unix_timestamp(k) AS ts FROM t1")),
                "unix_timestamp(expr) is a pure function of its argument");

        SPMPlanner planner = new SPMPlanner();
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> planner.buildBaseline("SELECT now() AS ts FROM t1",
                        "SELECT now() AS ts FROM t1"));
        Assertions.assertTrue(failure.getMessage().contains("replay-time context"),
                failure.getMessage());
    }

    /**
     * NULLABILITY is part of the schema fingerprint - SPM leaves
     * ELIMINATE_NOT_NULL enabled, so a NOT NULL column's "v IS NOT NULL" filter freezes
     * away, and ALTER TABLE ... MODIFY COLUMN v INT NULL (no name / type change) must
     * invalidate the baseline.
     */
    @Test
    public void testFingerprintIncludesColumnNullability() {
        Assertions.assertNotEquals(
                fingerprintOfTableDeclared(true), fingerprintOfTableDeclared(false),
                "a nullability change must invalidate a frozen NOT NULL elimination");
    }

    /**
     * upgrade compatibility: the nullability flag lives in its OWN entry
     * section, so an entry persisted before that section existed still matches the
     * current one - introducing the check must not invalidate every already persisted
     * baseline (a flag hashed INTO the column list would have changed every hash).
     * Two entries that BOTH carry the section must agree, so the check stays effective
     * for everything written since.
     */
    @Test
    public void testLegacyFingerprintWithoutNullabilitySectionIsTolerated() {
        String current = fingerprintOfTableDeclared(false);
        String legacy = current.substring(0, current.lastIndexOf("|nullable:"));
        Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                        legacy, current),
                "a pre-upgrade entry must still be contained: " + legacy);
        Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintEquivalent(legacy, current),
                "the post-plan comparison must tolerate a pre-upgrade row as well");

        // both sides carry the section -> a mismatch fails closed on BOTH paths
        String changed = fingerprintOfTableDeclared(true);
        Assertions.assertFalse(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                        changed, current),
                "a dropped nullability flag must no longer be contained");
        Assertions.assertFalse(SPMPlanTreeSupport.schemaFingerprintEquivalent(changed, current),
                "a dropped nullability flag must fail the post-plan comparison too");
        Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintEquivalent(current, current));
    }

    /** The fingerprint of a one-table mock whose single INT column has the given flag. */
    private static String fingerprintOfTableDeclared(boolean allowNull) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("t_nn");
        Mockito.when(table.getId()).thenReturn(7L);
        Column column = new Column("k", Type.INT);
        column.setIsAllowNull(allowNull);
        Mockito.when(table.getBaseSchema()).thenReturn(List.of(column));
        return SPMPlanTreeSupport.schemaFingerprintForCreate(
                contextResolvingTo(table), parse("SELECT * FROM internal.spm_db.t_nn"),
                null, null);
    }

    // ==================== #6: plan-side functions are fingerprinted ====================

    /**
     * The bind SQL `SELECT k FROM t` never mentions f while the STORED PLAN is
     * `SELECT f(k) AS k FROM t`: without plan-text function entries, redefining f
     * (x + 1 -> x + 2) left BOTH the fingerprint and the bind SQL unchanged, and the
     * replay guard accepted a frozen plan inlining the OLD body. The plan-side walk is
     * bound to the STORED TEXT because that text is the artifact every replay re-plans
     * (an optimized plan may legally pick an equivalent builtin with another name).
     */
    @Test
    public void testPlanTextFunctionsAreFingerprinted() {
        ConnectContext ctx = contextResolvingTo(table("t_bind", 7L, "k"));
        LogicalPlan bindPlan = parse("SELECT k FROM internal.spm_db.t_bind");
        try {
            String withAbs = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, null, "SELECT abs(k) AS k FROM internal.spm_db.t_bind");
            String withSqrt = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, null, "SELECT sqrt(k) AS k FROM internal.spm_db.t_bind");
            Assertions.assertTrue(withAbs.contains("abs"), withAbs);
            Assertions.assertTrue(withSqrt.contains("sqrt"), withSqrt);
            Assertions.assertNotEquals(withAbs, withSqrt,
                    "a function used ONLY by the stored plan text must be part of the"
                            + " fingerprint, otherwise its redefinition invalidates"
                            + " nothing");
            Assertions.assertEquals(withAbs, SPMPlanTreeSupport.schemaFingerprintForReplay(
                            ctx, bindPlan, null,
                            "SELECT abs(k) AS k FROM internal.spm_db.t_bind"),
                    "the revalidation must recompute the SAME entries from the SAME"
                            + " stored text");
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #8: the parsed KEY expression is rejected ====================

    /**
     * `KEY db.key` parses straight to a bound EncryptKeyRef (no UnboundFunction on the
     * path), so the name-only check never fired: the optimizer folded the named secret
     * into the frozen plan and replay served the CREATOR's key to other sessions.
     */
    @Test
    public void testEncryptKeyReferenceIsRejected() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectVolatileFunctionDependencies(
                        parse("SELECT k FROM t1 WHERE v = KEY k1"), null),
                "a parsed KEY reference is volatile and must fail the CREATE");
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectVolatileFunctionDependencies(
                        parse("SELECT k FROM t1"),
                        parse("SELECT v FROM t1 WHERE v = KEY k1")),
                "the secret may live ONLY in the plan text");
        // a normal pair still passes
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(
                parse("SELECT k FROM t1 WHERE k > 1"),
                parse("SELECT k FROM t1 WHERE k > 1"));
    }

    // ==================== #7: both CREATE texts parse under ONE mode ====================

    /**
     * The bind text carries a SET_VAR(sql_mode='PIPES_AS_CONCAT') hint and both texts
     * spell `a || b`: the plan text must still be read under the mode captured when the
     * CREATE started (here: no PIPES_AS_CONCAT -> a boolean Or), never under whatever a
     * hint of the first text may have done to the session in between.
     */
    @Test
    public void testCreateTextsParseUnderOneMode() {
        SPMPlanner planner = new SPMPlanner();
        String bindWithHint = "SELECT a || b AS x FROM t1"
                + " /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */";
        String planText = "SELECT a || b AS x FROM t1";

        BaselinePlan built = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
            try {
                return planner.buildBaseline(bindWithHint, planText);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        String planExprSql = expressionSql(built.getParameterizedPlanPlan());
        Assertions.assertFalse(planExprSql.contains("concat"),
                "the bind text's SET_VAR hint must not decide how the PLAN text is read:"
                        + " " + planExprSql);
        Assertions.assertTrue(planExprSql.contains("or"),
                "under the captured mode `||` stays a boolean Or: " + planExprSql);

        // control: the SAME text under PIPES_AS_CONCAT really is a concat (the window,
        // not the hint, decides)
        BaselinePlan piped = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                () -> {
                    try {
                        return planner.buildBaseline(planText, planText);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        String pipedExprSql = expressionSql(piped.getParameterizedPlanPlan());
        Assertions.assertTrue(pipedExprSql.contains("concat"),
                "PIPES_AS_CONCAT text must be read as a concat: " + pipedExprSql);
    }
}
