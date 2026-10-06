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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Pre-lock resolution, CTE scoping and the hidden payloads of the
 * metadata / context guards.
 *
 * - The view guard and the bind-side table fingerprint must be CTE-SCOPE aware: a WITH
 *   alias shadows a same-named catalog view / table, and resolving it against the
 *   catalog made every matching query exit at viewReferenced (the baseline never
 *   applied) or pinned a phantom table into the fingerprint;
 * - the view guard must NOT populate the planner's resolved-table cache before
 *   collectAndLockTable: a cached pre-lock TableIf survives a concurrent DROP / CREATE t;
 * - SELECT * REPLACE(...) hides subqueries in getReplacedAlias(): they must reach the
 *   view scan and the namespace qualification (db1/db2 cross-match);
 * - the runtime-context guard must see replay-context expressions inside expression
 *   subquery plans.
 */
public class SPMRound20SafetyTest {

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

    /** A session whose relation resolution reports the given names as CATALOG views. */
    private static ConnectContext ctxResolving(Set<String> viewNames) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    String name = qualifier.get(qualifier.size() - 1);
                    return viewNames.contains(name)
                            ? Mockito.mock(View.class) : Mockito.mock(TableIf.class);
                });
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    String name = qualifier.get(qualifier.size() - 1);
                    return viewNames.contains(name)
                            ? Mockito.mock(View.class) : Mockito.mock(TableIf.class);
                });
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /** A session resolving EVERY name to a table named after the last qualifier part. */
    private static ConnectContext ctxResolvingByName() {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    return table(qualifier.get(qualifier.size() - 1), 7L, "k");
                });
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    return table(qualifier.get(qualifier.size() - 1), 7L, "k");
                });
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /** Every UnboundRelation of a plan tree (children walk, subquery plans included). */
    private static void collectRelations(Plan plan, List<List<String>> out) {
        if (plan == null) {
            return;
        }
        if (plan instanceof UnboundRelation) {
            out.add(((UnboundRelation) plan).getNameParts());
        }
        for (Plan child : plan.children()) {
            collectRelations(child, out);
        }
        for (Expression expression : plan.getExpressions()) {
            collectSubqueryRelations(expression, out);
        }
    }

    private static void collectSubqueryRelations(Expression expression, List<List<String>> out) {
        if (expression instanceof SubqueryExpr) {
            collectRelations(((SubqueryExpr) expression).getQueryPlan(), out);
        }
        for (Expression child : expression.children()) {
            collectSubqueryRelations(child, out);
        }
    }

    /** The first UnboundStar of a plan tree. */
    private static UnboundStar firstStar(Plan plan) {
        if (plan == null) {
            return null;
        }
        for (Expression expression : plan.getExpressions()) {
            UnboundStar star = firstStarInExpression(expression);
            if (star != null) {
                return star;
            }
        }
        for (Plan child : plan.children()) {
            UnboundStar star = firstStar(child);
            if (star != null) {
                return star;
            }
        }
        return null;
    }

    private static UnboundStar firstStarInExpression(Expression expression) {
        if (expression instanceof UnboundStar) {
            return (UnboundStar) expression;
        }
        for (Expression child : expression.children()) {
            UnboundStar star = firstStarInExpression(child);
            if (star != null) {
                return star;
            }
        }
        return null;
    }

    /**
     * A WITH alias shadows a same-named CATALOG VIEW: the analyzer binds FROM c to the
     * CTE, but resolving "c" against the catalog reported a view, so CREATE stored a raw
     * fallback and every matching query exited at viewReferenced - the baseline never
     * applied. The shadowed view itself must still be detected.
     */
    @Test
    public void testViewGuardSkipsCteAliasesShadowingACatalogView() {
        try {
            Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("c")),
                    parse("WITH c AS (SELECT t.a AS x FROM cat.db.t AS t) SELECT c.x FROM c")),
                    "a FROM c bound by the WITH clause is not the catalog view c");
            Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("c")),
                    parse("SELECT c.x FROM cat.db.c AS c")),
                    "the catalog view itself must still be reported");
            // the scope is per point: a nested WITH extends it, the enclosing query keeps
            // resolving the same name against the catalog
            Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("c")),
                    parse("WITH c AS (SELECT t.a AS x FROM cat.db.t AS t)"
                            + " SELECT c.x FROM c, cat.db.c d")),
                    "the catalog view c is still referenced next to the WITH alias");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The bind-side table fingerprint must skip WITH aliases for the same reason: the
     * phantom entry of the same-named catalog relation also broke the pre-match
     * containment check of every correct bind fingerprint.
     */
    @Test
    public void testBindFingerprintSkipsCteAliases() {
        ConnectContext ctx = ctxResolvingByName();
        try {
            // the body relation is fully qualified: a one-part name needs a current
            // catalog / database the bare unit-test context does not have
            String fingerprint = SPMPlanTreeSupport.schemaFingerprint(ctx, parse(
                    "WITH cte_x AS (SELECT k FROM cat_x.db_x.t) SELECT k FROM cte_x"));
            Assertions.assertTrue(fingerprint.contains("t|7|"),
                    "the base table of the CTE body must be fingerprinted: " + fingerprint);
            Assertions.assertFalse(fingerprint.contains("cte_x|"),
                    "a WITH reference must not resolve against the catalog: " + fingerprint);
            Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprint(ctx,
                            parse("SELECT k FROM cat_x.db_x.t")).contains("t|7|"),
                    "control: the same table resolves through the normal path");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The view guard must resolve WITHOUT populating the planner's resolved-table cache:
     * it runs before collectAndLockTable, and a cached pre-lock TableIf would survive a
     * concurrent DROP / CREATE and be reused by CollectRelation / BindExpression.
     */
    @Test
    public void testViewGuardDoesNotPopulateTheResolverCache() {
        ConnectContext ctx = ctxResolving(Set.of("v"));
        StatementContext statementContext = ctx.getStatementContext();
        try {
            Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctx,
                    parse("SELECT v.a FROM cat.db.v AS v")));
            Mockito.verify(statementContext, Mockito.never())
                    .getAndCacheTable(Mockito.anyList(), Mockito.any(), Mockito.any());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * SELECT * REPLACE((SELECT max(v.a) FROM view_v AS v) AS k) hides the subquery in
     * UnboundStar.getReplacedAlias() - OUTSIDE children() - so the view scan saw only the
     * outer table and the optimizer could freeze the view's expanded base-table SQL
     * (replay then checked base-table privileges instead of the original view).
     */
    @Test
    public void testViewBehindStarReplacePayloadIsDetected() {
        try {
            Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("view_v")),
                    parse("SELECT * REPLACE((SELECT max(v.a) FROM cat.db.view_v AS v) AS k)"
                            + " FROM cat.db.t")),
                    "a view inside the * REPLACE payload must be reported");
            Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of()),
                    parse("SELECT * REPLACE((SELECT max(x.a) FROM cat.db.x AS x) AS k)"
                            + " FROM cat.db.t")),
                    "a payload over base tables is not a view reference");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The namespace qualification must rebuild the * REPLACE payloads too: creating a
     * baseline in db1 for
     * SELECT * REPLACE((SELECT MAX(v) FROM u) AS k) FROM t and running the same
     * text under db2 kept the raw "u", so digest and Level-3 matched while the frozen SQL
     * still read db1.u.
     */
    @Test
    public void testStarReplaceSubqueryIsNamespaceQualified() {
        LogicalPlan plan = parse(
                "SELECT * REPLACE((SELECT max(v) FROM u) AS k) FROM t");
        UnboundStar star = firstStar(plan);
        Assertions.assertNotNull(star, "the parsed tree carries the star payload");
        Assertions.assertEquals(1, star.getReplacedAlias().size());

        LogicalPlan qualified =
                SPMPlanTreeSupport.namespaceQualified(plan, "cat_x", "db_x");
        UnboundStar qualifiedStar = firstStar(qualified);
        Assertions.assertNotNull(qualifiedStar);
        Assertions.assertNotSame(star, qualifiedStar,
                "the payload must be rebuilt under the current namespace");
        List<List<String>> payloadRelations = new ArrayList<>();
        collectSubqueryRelations(qualifiedStar.getReplacedAlias().get(0), payloadRelations);
        collectRelations(qualified, payloadRelations);
        Assertions.assertTrue(payloadRelations.contains(List.of("cat_x", "db_x", "u")),
                "the payload subquery must be namespace-qualified: " + payloadRelations);
        Assertions.assertTrue(payloadRelations.contains(List.of("cat_x", "db_x", "t")),
                "the outer relation must keep being qualified: " + payloadRelations);
        // the payload (and its namespace) must take part in the L1/L2 digest key: the same
        // text under another database must not share the matching key
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(plan, "cat_x", "db_x").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(plan, "cat_x", "db_y").toSpmDigest(),
                "the REPLACE payload's namespace must be part of the digest");
    }

    /**
     * A payload subquery referencing a WITH alias must keep the CTE reference verbatim
     * (the alias is visible at that point) while base tables around it are qualified.
     */
    @Test
    public void testStarReplacePayloadKeepsVisibleCteAliases() {
        LogicalPlan plan = parse("WITH c AS (SELECT k FROM t) SELECT * REPLACE("
                + "(SELECT max(k) FROM c) AS kk) FROM t2");
        LogicalPlan qualified = SPMPlanTreeSupport.namespaceQualified(plan, "cat_x", "db_x");
        UnboundStar star = firstStar(qualified);
        Assertions.assertNotNull(star);
        List<List<String>> payloadRelations = new ArrayList<>();
        collectSubqueryRelations(star.getReplacedAlias().get(0), payloadRelations);
        Assertions.assertTrue(payloadRelations.contains(List.of("c")),
                "the CTE reference must stay verbatim: " + payloadRelations);
        Assertions.assertFalse(payloadRelations.contains(List.of("cat_x", "db_x", "c")),
                "the CTE alias must not be namespace-qualified: " + payloadRelations);
    }

    /**
     * After a different explicit database failed to match, the lookup must NOT fall back
     * to the bare table name - CREATE BASELINE bind db1.t WITH plan db2.t fingerprinted
     * db2.t under the bind's db1.t, and the next query's CORRECT bind fingerprint then
     * failed the pre-membership check (the new baseline never applied).
     */
    @Test
    public void testExplicitDatabaseMismatchDoesNotFallBackToTheBareName() {
        TableIf planned = table("t", 22L, "k");
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getFullName()).thenReturn("db2");
        Mockito.when(planned.getDatabase()).thenReturn(database);
        org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation relation =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation.class);
        Mockito.when(relation.getTable()).thenReturn(planned);

        TableIf actualBind = table("t", 11L, "k");
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenReturn(actualBind);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenReturn(actualBind);
        ctx.setStatementContext(statementContext);
        try {
            String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx,
                    parse("SELECT k FROM db1.t"), relation, null);
            Assertions.assertTrue(stored.contains("t|11|"),
                    "the actual bind table db1.t must be resolved: " + stored);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The runtime-context guard must see expressions held by SUBQUERY PLANS: a parsed
     * at-variable (UnboundVariable) or user() inside an expression subquery was
     * invisible to the bespoke visitor, and analysis inlined the creator's value into
     * the frozen SQL.
     */
    @Test
    public void testReplayContextInsideSubqueryPlansIsRejected() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT (SELECT max(@v) FROM t2) AS m FROM t1")),
                "a @v inside a scalar subquery plan must be rejected");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k = (SELECT max(@v) FROM t2)")),
                "a @v inside a WHERE subquery plan must be rejected");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT k FROM t1 WHERE k IN (SELECT user() FROM t2)")),
                "user() inside an IN subquery must be rejected");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("WITH c AS (SELECT max(@v) AS m FROM t2) SELECT m FROM t1, c")),
                "a @v inside a CTE body must be rejected");
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT (SELECT max(x) FROM t2) AS m FROM t1")),
                "a plain subquery is not a replay-context expression");
    }

    /**
     * The SPM fallback replans the SAME statement context: the abandoned rewritten
     * pass's short-circuit flag would make StmtExecutor select the PointQueryExecutor
     * for the original (non-point) statement, and its automatic
     * runtime_filter_wait_time_ms would leak into the fallback plan.
     */
    @Test
    public void testResetPlannerStateClearsShortCircuitAndRestoresTheWait() {
        StatementContext statementContext = new StatementContext();
        ConnectContext ctx = new ConnectContext();
        statementContext.setConnectContext(ctx);
        int originalWait = ctx.getSessionVariable().getRuntimeFilterWaitTimeMs();

        statementContext.setShortCircuitQuery(true);
        statementContext.setShortCircuitQueryContext(Mockito.mock(
                org.apache.doris.qe.ShortCircuitQueryContext.class));
        statementContext.recordRuntimeFilterWaitTimeBeforePlannerSet(originalWait);
        // first-wins: a later pass's pre-value is the previous assignment, not the
        // user's original state
        statementContext.recordRuntimeFilterWaitTimeBeforePlannerSet(50000);
        ctx.getSessionVariable().setRuntimeFilterWaitTimeMs(50000);

        statementContext.resetPlannerStateForReplan();
        Assertions.assertFalse(statementContext.isShortCircuitQuery(),
                "the abandoned pass's short-circuit flag must not survive");
        Assertions.assertNull(statementContext.getShortCircuitQueryContext(),
                "the abandoned pass's short-circuit context must be dropped");
        Assertions.assertEquals(originalWait,
                ctx.getSessionVariable().getRuntimeFilterWaitTimeMs(),
                "the abandoned pass's automatic wait must be restored");

        // idempotent: without a fresh record the second reset changes nothing
        ctx.getSessionVariable().setRuntimeFilterWaitTimeMs(12345);
        statementContext.resetPlannerStateForReplan();
        Assertions.assertEquals(12345, ctx.getSessionVariable().getRuntimeFilterWaitTimeMs(),
                "a reset without a recorded value must not clobber the session");
    }
}
