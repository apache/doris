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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.capture.AuditLogScanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Regression tests for the matching-safety / rendering fixes: every node state that
 * lives OUTSIDE expressions()/children() (subquery limits, TVF properties, positional
 * alias lists, CTE recursion, UnboundStar REPLACE payloads, window frames, MARK state)
 * must either take part in the Level 3 match exactly or the query must not match at all
 * - a false match would replay the captured value and change results.
 */
public class SPMMatchingSafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Two trees match only when their ONLY difference is a placeholder-able value. */
    private static boolean matches(String bindSql, String userSql) {
        LogicalPlan bind = parse(bindSql);
        LogicalPlan user = parse(userSql);
        // sanity: the coarse digest must not be trusted to reject these (it may or may
        // not differ); Level 3 is the authoritative check
        return SPMPlanTreeSupport.check(bind, user, new HashMap<Long, Expression>());
    }

    /** As {@link #matches}, but the bind side goes through the PRODUCTION
     * parameterization (a raw parse carries no placeholders at all). */
    private static boolean matchesParameterized(String bindSql, String userSql) throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(bindSql, bindSql);
        return SPMPlanTreeSupport.check(baseline.getParameterizedBindPlan(), parse(userSql),
                new HashMap<Long, Expression>());
    }

    // ==================== round-23 observations ====================

    @Test
    public void testUnorderedOrPairingBacktracksForALaterPlaceholderUse() throws Exception {
        String bind = "SELECT (a = 1 OR a = 2) AS c FROM t WHERE a = 1";
        Assertions.assertTrue(matchesParameterized(bind,
                "SELECT (a = 3 OR a = 4) AS c FROM t WHERE a = 4"),
                "a pairing that satisfies BOTH placeholder uses must be found");
        // round-32 #9: the SELECT-list literal and the WHERE literal of the bind tree are
        // INDEPENDENT placeholders (different projection-item / no-item scopes), so a
        // variant that changes each site independently is a legitimate match - the old
        // identity shared one id between the two sites and rejected it.
        Assertions.assertTrue(matchesParameterized(bind,
                "SELECT (a = 3 OR a = 4) AS c FROM t WHERE a = 5"),
                "the projection and the predicate occurrences are separate placeholders");
        // The one-id-one-value rule still rejects an INCONSISTENT assignment: two identical
        // repeated sub-expressions of the bind tree share their literal ids, so a variant
        // assigning different values to those same positions cannot match.
        String repeated = "SELECT * FROM t WHERE (a + 2) > 3 AND (a + 2) > 3";
        Assertions.assertTrue(matchesParameterized(repeated,
                "SELECT * FROM t WHERE (a + 5) > 3 AND (a + 5) > 3"),
                "the same value at every occurrence of the shared id matches");
        Assertions.assertFalse(matchesParameterized(repeated,
                "SELECT * FROM t WHERE (a + 5) > 3 AND (a + 6) > 3"),
                "no consistent global assignment exists here");
    }

    @Test
    public void testDerivedOutputLabelStaysMatchableAndIsRealignedAtReplay() throws Exception {
        String bind = "SELECT k + 1 FROM t";
        Assertions.assertTrue(matchesParameterized(bind, "SELECT k + 1 FROM t"),
                "identical derived labels match");
        Assertions.assertTrue(matchesParameterized(bind, "SELECT k + 2 FROM t"),
                "a value variant under a DERIVED label must stay matchable: the replay"
                        + " hands the caller's own label back (alignRootOutputLabels)");
        Assertions.assertTrue(matchesParameterized("SELECT k + 1 AS v FROM t",
                "SELECT k + 2 AS v FROM t"),
                "an EXPLICIT alias pins the header, so value variants stay matchable");
        // the replay side of the contract: the rewritten tree exposes the CALLER's
        // labels (the frozen text pinned the captured "k + 1"), position by position
        LogicalPlan rewritten = parse("SELECT (k + 2) AS `k + 1` FROM t");
        LogicalPlan user = parse("SELECT k + 2 FROM t");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        UnboundAlias alignedItem = findUnboundAlias(aligned);
        Assertions.assertEquals("k + 2", alignedItem.getAlias().orElse(null),
                "the caller's derived text must replace the captured label: " + aligned);
        Assertions.assertTrue(alignedItem.isNameFromChild(),
                "the caller's own label is derived, so the replacement stays derived");
        // an EXPLICIT caller alias wins as well (an alias already equal stays untouched)
        LogicalPlan explicitAligned = SPMPlanTreeSupport.alignRootOutputLabels(
                rewritten, parse("SELECT k + 2 AS total FROM t"));
        UnboundAlias explicitItem = findUnboundAlias(explicitAligned);
        Assertions.assertEquals("total", explicitItem.getAlias().orElse(null));
        Assertions.assertFalse(explicitItem.isNameFromChild());
        // arity mismatch (SELECT * keeps the star as ONE item) leaves the tree alone
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten, parse("SELECT * FROM t")));
    }

    @Test
    public void testManualPlanWithRenamedAliasIsRejected() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT * FROM t a WHERE a.k = 1",
                        "SELECT * FROM t b WHERE b.k = 1"));
        // the renamed alias is rejected by the round-44 divergence guard (the conjunct
        // texts no longer match) or - when that guard is not reached - by the placeholder
        // alignment; both messages say "cannot align"
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("align"),
                failure.getMessage());
    }

    // ==================== subquery LIMIT is part of the match ====================

    @Test
    public void testSubqueryLimitIsPartOfMatch() {
        String bind = "SELECT (SELECT sum(x) FROM t2 WHERE k = 1 LIMIT 1) FROM t1 WHERE a = 1";
        Assertions.assertTrue(matches(bind,
                "SELECT (SELECT sum(x) FROM t2 WHERE k = 1 LIMIT 1) FROM t1 WHERE a = 1"),
                "identical subquery limits must match");
        Assertions.assertFalse(matches(bind,
                "SELECT (SELECT sum(x) FROM t2 WHERE k = 1 LIMIT 2) FROM t1 WHERE a = 1"),
                "a different subquery LIMIT must not match (the captured limit would be replayed)");
        // a LIMIT that only differs in the offset is just as significant
        Assertions.assertFalse(matches(
                "SELECT (SELECT sum(x) FROM t2 WHERE k = 1 LIMIT 5 OFFSET 1) FROM t1 WHERE a = 1",
                "SELECT (SELECT sum(x) FROM t2 WHERE k = 1 LIMIT 5 OFFSET 2) FROM t1 WHERE a = 1"),
                "a different subquery OFFSET must not match");
    }

    // ==================== derived-table TopN is part of the match ====================

    /**
     * round-22 #6: a derived table's ORDER BY ... LIMIT (LogicalTopN) is not reachable by
     * the positional limit merge once the frozen join order differs from the user's - the
     * limited derived table pairs with the OTHER relation and the class-mismatch guard
     * leaves the captured slice. Nested TopN limits must therefore take part in the exact
     * match, like the nested LOGICAL LIMIT above.
     */
    @Test
    public void testDerivedTableTopNIsPartOfMatch() {
        String bind = "SELECT * FROM (SELECT k FROM t1 ORDER BY k LIMIT 1) d"
                + " JOIN t2 ON d.k = t2.k";
        Assertions.assertTrue(matches(bind,
                "SELECT * FROM (SELECT k FROM t1 ORDER BY k LIMIT 1) d"
                        + " JOIN t2 ON d.k = t2.k"),
                "identical derived-table limits must match");
        Assertions.assertFalse(matches(bind,
                "SELECT * FROM (SELECT k FROM t1 ORDER BY k LIMIT 2) d"
                        + " JOIN t2 ON d.k = t2.k"),
                "a different derived-table ORDER BY ... LIMIT must not match");
        Assertions.assertFalse(matches(
                "SELECT * FROM (SELECT k FROM t1 ORDER BY k LIMIT 5 OFFSET 1) d"
                        + " JOIN t2 ON d.k = t2.k",
                "SELECT * FROM (SELECT k FROM t1 ORDER BY k LIMIT 5 OFFSET 2) d"
                        + " JOIN t2 ON d.k = t2.k"),
                "a different derived-table OFFSET must not match");
    }

    // ==================== TVF properties are part of the match ====================

    @Test
    public void testTvfPropertiesArePartOfMatch() {
        Assertions.assertTrue(matches("SELECT * FROM numbers('number' = '100')",
                "SELECT * FROM numbers('number' = '100')"));
        Assertions.assertFalse(matches("SELECT * FROM numbers('number' = '100')",
                "SELECT * FROM numbers('number' = '10')"),
                "numbers() with different properties must not match (the frozen replay"
                        + " would run the captured properties)");
    }

    // ==================== positional subquery/CTE aliases are part of the match ====================

    @Test
    public void testSubQueryColumnAliasesArePartOfMatch() {
        // derived-table alias columns (s(x, y)) are not parse-supported in Doris; the
        // positional alias list exists on CTE definitions
        Assertions.assertTrue(matches("WITH s(x, y) AS (SELECT a, b FROM t) SELECT x FROM s",
                "WITH s(x, y) AS (SELECT a, b FROM t) SELECT x FROM s"));
        Assertions.assertFalse(matches("WITH s(x, y) AS (SELECT a, b FROM t) SELECT x FROM s",
                "WITH s(y, x) AS (SELECT a, b FROM t) SELECT x FROM s"),
                "swapped column aliases bind x to a different child column and must not match");
    }

    // ==================== CTE recursion mode is part of the match ====================

    @Test
    public void testCteRecursionIsPartOfMatch() {
        String recursive = "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL"
                + " SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r";
        String plain = "WITH r AS (SELECT 1 AS n UNION ALL"
                + " SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r";
        Assertions.assertTrue(matches(recursive, recursive));
        Assertions.assertFalse(matches(recursive, plain),
                "RECURSIVE changes how the inner r binds (work table vs base table)"
                        + " and must not match");
    }

    // ==================== UnboundStar REPLACE payload is part of the match ====================

    @Test
    public void testUnboundStarReplacePayloadIsPartOfMatch() {
        Assertions.assertTrue(matches("SELECT * REPLACE(k + 1 AS k) FROM t",
                "SELECT * REPLACE(k + 1 AS k) FROM t"));
        Assertions.assertFalse(matches("SELECT * REPLACE(k + 1 AS k) FROM t",
                "SELECT * REPLACE(k + 2 AS k) FROM t"),
                "a different REPLACE expression must not replay the captured projection");
        Assertions.assertFalse(matches("SELECT * REPLACE(k + 1 AS k) FROM t",
                "SELECT * REPLACE(k + 1 AS j) FROM t"),
                "a different REPLACE target must not match");
    }

    // ==================== window frame is part of the match ====================

    @Test
    public void testWindowFrameIsPartOfMatch() {
        String one = "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                + " AS s FROM t";
        String two = "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
                + " AS s FROM t";
        Assertions.assertTrue(matches(one, one));
        Assertions.assertFalse(matches(one, two),
                "a different frame bound must not match (WindowFrame is not an expression"
                        + " child, so the generic comparison cannot see it)");
    }

    // ==================== comparator is a total order (0 is a valid time) ====================

    @Test
    public void testComparatorTotalOrder() {
        BaselinePlan zero = plan(0, 5.0);
        BaselinePlan zeroOtherCost = plan(0, 1.0);
        BaselinePlan unknown = plan(-1, 3.0);
        // 0 vs 0 must compare equal (previously it returned -1 for both directions)
        Assertions.assertEquals(0, BaselineManager.compareCandidates(zero, zeroOtherCost));
        Assertions.assertEquals(0, BaselineManager.compareCandidates(zeroOtherCost, zero));
        // a known (>= 0) time is ordered LAST: the unknown (manual) baseline wins
        Assertions.assertTrue(BaselineManager.compareCandidates(zero, unknown) > 0);
        Assertions.assertTrue(BaselineManager.compareCandidates(unknown, zero) < 0);
        // both unknown -> lower cost wins
        Assertions.assertTrue(
                BaselineManager.compareCandidates(plan(-1, 1.0), plan(-1, 9.0)) < 0);
    }

    private static BaselinePlan plan(long queryTimeMs, double cost) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(1);
        plan.setQueryTimeMs(queryTimeMs);
        plan.setCost(cost);
        return plan;
    }

    // ==================== bitwise operators render as one token ====================

    @Test
    public void testBitwiseOperatorRendering() {
        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();
        SQLRelation relation = new SQLRelation();
        for (String[] probe : new String[][] {
                {"SELECT 1 FROM t WHERE (a & 1) = 1", "&"},
                {"SELECT 1 FROM t WHERE (a | 1) = 1", "|"},
                {"SELECT 1 FROM t WHERE (a ^ 1) = 1", "^"}}) {
            LogicalPlan parsed = parse(probe[0]);
            // parse wraps the query in a result sink - unwrap to the filter conjunct
            LogicalPlan project = (LogicalPlan) (parsed instanceof LogicalProject
                    ? parsed : parsed.child(0));
            LogicalFilter<?> filter = (LogicalFilter<?>) project.child(0);
            Expression conjunct = filter.getConjuncts().iterator().next();
            String sql = builder.print(conjunct, relation);
            Assertions.assertTrue(sql.contains(" " + probe[1] + " "),
                    "expected '" + probe[1] + "' in: " + sql);
            // the operand must appear exactly once (the old fallback returned the whole
            // expression as the operator token, producing "(a (a & 1) 1)")
            Assertions.assertEquals(1, countOccurrences(sql, "a"),
                    "operand rendered more than once: " + sql);
        }
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        int idx = text.indexOf(needle);
        while (idx >= 0) {
            count++;
            idx = text.indexOf(needle, idx + needle.length());
        }
        return count;
    }

    // ==================== catalog / database namespace is part of the match key ====================

    @Test
    public void testNamespaceIsPartOfMatchKey() {
        LogicalPlan plan = parse("SELECT * FROM t WHERE k = 1");
        String db1 = SPMPlanTreeSupport.namespaceQualified(plan, "internal", "db1").toSpmDigest();
        String db2 = SPMPlanTreeSupport.namespaceQualified(plan, "internal", "db2").toSpmDigest();
        Assertions.assertNotEquals(db1, db2,
                "an unqualified relation must be keyed by the effective catalog/db: the same"
                        + " text under two databases must not share the match key");
        Assertions.assertEquals(db1,
                SPMPlanTreeSupport.namespaceQualified(plan, "internal", "db1").toSpmDigest(),
                "the same namespace must produce the same key");
        // explicitly qualified references stay verbatim: they mean the same table everywhere
        LogicalPlan explicit = parse("SELECT * FROM db9.t WHERE k = 1");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(explicit, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(explicit, "internal", "db2").toSpmDigest(),
                "an explicitly qualified relation must not be re-qualified");
        // relations inside subquery expressions are qualified too
        LogicalPlan subquery = parse(
                "SELECT * FROM u WHERE x IN (SELECT y FROM t)");
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(subquery, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(subquery, "internal", "db2").toSpmDigest(),
                "subquery-owned relations must be namespace-qualified as well");
    }

    // ==================== CTE aliases are namespace-independent ====================

    @Test
    public void testCteReferencesAreNamespaceIndependent() {
        // the WITH alias binds inside the query itself: the same query means the same
        // thing in every database, so its match key must not depend on the database
        LogicalPlan pureCte = parse("WITH my_cte AS (SELECT 1 AS x) SELECT * FROM my_cte");
        String pureDb1 = SPMPlanTreeSupport.namespaceQualified(pureCte, "internal", "db1")
                .toSpmDigest();
        String pureDb2 = SPMPlanTreeSupport.namespaceQualified(pureCte, "internal", "db2")
                .toSpmDigest();
        Assertions.assertEquals(pureDb1, pureDb2,
                "a CTE reference must not be keyed by the current database");
        Assertions.assertFalse(pureDb1.contains("db1.my_cte"),
                "the CTE alias must stay verbatim in the digest: " + pureDb1);

        // ... but the same text WITHOUT its WITH clause refers to a real table and MUST
        // stay keyed by the database (otherwise db1's baseline would replay db1.c under db2)
        LogicalPlan baseTable = parse("SELECT * FROM my_cte");
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(baseTable, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(baseTable, "internal", "db2").toSpmDigest(),
                "a real table reference must stay keyed by the database");

        // a name is only exempt where an alias actually binds it: the first alias body
        // cannot see a later alias, so its reference is a base table. A blanket "collect
        // every CTE name of the tree" exemption would key this query namespace-free and
        // re-introduce the cross-database false match.
        LogicalPlan forwardReference = parse(
                "WITH a AS (SELECT * FROM my_cte), my_cte AS (SELECT 1 AS x) SELECT * FROM a");
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(forwardReference, "internal", "db1")
                        .toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(forwardReference, "internal", "db2")
                        .toSpmDigest(),
                "an alias body must not see a later alias: that reference binds as a base table");

        // an alias body sees the aliases defined before it; the main query sees all of them
        LogicalPlan chain = parse(
                "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(chain, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(chain, "internal", "db2").toSpmDigest(),
                "earlier aliases are visible inside a later alias body");

        // a self reference binds to the WITH clause only in a real recursive CTE; under a
        // plain WITH it is an ordinary base-table reference and must stay qualified
        LogicalPlan plainSelf = parse(
                "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5)"
                        + " SELECT * FROM r");
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(plainSelf, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(plainSelf, "internal", "db2").toSpmDigest(),
                "under a plain WITH a self reference binds as a base table");
        LogicalPlan recursive = parse(
                "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5)"
                        + " SELECT * FROM r");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(recursive, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(recursive, "internal", "db2").toSpmDigest(),
                "a WITH RECURSIVE self reference is the work table, not the database's r");

        // expression subqueries inherit the CTE scope of the point they appear in
        LogicalPlan subqueryCte = parse(
                "WITH my_cte AS (SELECT 1 AS x) SELECT (SELECT max(x) FROM my_cte)");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(subqueryCte, "internal", "db1").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(subqueryCte, "internal", "db2").toSpmDigest(),
                "a CTE reference inside an expression subquery is exempt as well");
    }

    // ==================== ROLLUP / GROUPING SETS digest rendering is rebuild-stable ====================

    /** The (first) LogicalRepeat of a grouping-sets / ROLLUP / CUBE query. */
    private static LogicalRepeat<?> findRepeat(LogicalPlan plan) {
        if (plan instanceof LogicalRepeat) {
            return (LogicalRepeat<?>) plan;
        }
        for (var child : plan.children()) {
            if (child instanceof LogicalPlan) {
                LogicalRepeat<?> found = findRepeat((LogicalPlan) child);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * Qualifying the child of a LogicalRepeat rebuilds the node; the rebuild must keep
     * the parse-time withInProjection flag, which switches toDigest() between
     * "SELECT outputExpressions FROM child" and a bare "child" rendering. The same
     * rebuild also produces the frozen planSql, so a flipped flag changed the digest /
     * planSql of every ROLLUP baseline on the way in.
     */
    @Test
    public void testRepeatDigestRenderingUnchangedByQualification() {
        for (String sql : List.of(
                "SELECT a, sum(b) FROM t GROUP BY GROUPING SETS ((a, b), (a))",
                "SELECT a, sum(b) FROM t GROUP BY ROLLUP(a, b)",
                "SELECT a, sum(b) FROM t GROUP BY CUBE(a, b)",
                "SELECT DISTINCT a FROM t GROUP BY a, b WITH ROLLUP")) {
            LogicalPlan plan = parse(sql);
            Assertions.assertNotNull(findRepeat(plan), "expected a LogicalRepeat in: " + sql);
            LogicalPlan qualified = SPMPlanTreeSupport.namespaceQualified(plan, "internal", "db1");
            Assertions.assertEquals(plan.toSpmDigest(),
                    qualified.toSpmDigest().replace("internal.db1.", ""),
                    "qualification must not change the digest rendering of: " + sql);
        }
    }

    /**
     * Pin the less common withInProjection=false rendering state as well: the rebuild
     * used to force the flag to true through the grouping-id-values constructor
     * overload and silently dropped the "SELECT ... FROM ..." prefix from the digest.
     */
    @Test
    public void testRepeatWithInProjectionFalseIsKeptByRebuild() {
        LogicalRepeat<?> repeat = findRepeat(parse("SELECT a, sum(b) FROM t GROUP BY ROLLUP(a, b)"));
        Assertions.assertNotNull(repeat);
        LogicalPlan plan = repeat.withInProjection(false);
        LogicalPlan qualified = SPMPlanTreeSupport.namespaceQualified(plan, "internal", "db1");
        Assertions.assertEquals(plan.toSpmDigest(),
                qualified.toSpmDigest().replace("internal.db1.", ""),
                "the LogicalRepeat rebuild must keep withInProjection=false");
    }

    // ==================== cross-block literals / set-operand limits / volatile deps ====================

    /**
     * The same literal at the same position of DIFFERENT query blocks must get DIFFERENT
     * placeholder ids: with one shared id a similar query (outer a=2 / inner a=3) was
     * rejected by the one-value-per-id check and the baseline could never hit.
     */
    @Test
    public void testIndependentLiteralsInSeparateQueryBlocks() {
        LogicalPlan bindPlan = parse(
                "SELECT * FROM t WHERE a = 1 AND EXISTS (SELECT 1 FROM u WHERE a = 1)");
        org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder builder =
                new org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));
        Assertions.assertEquals(3, builder.getPlaceholderExprs().size(),
                "outer 1, inner SELECT 1 and inner a=1 are three independent literals");
        Assertions.assertTrue(SPMPlanTreeSupport.check(parameterizedBind, parse(
                "SELECT * FROM t WHERE a = 1 AND EXISTS (SELECT 1 FROM u WHERE a = 1)"),
                new HashMap<>()), "same values must still match");
        Assertions.assertTrue(SPMPlanTreeSupport.check(parameterizedBind, parse(
                "SELECT * FROM t WHERE a = 2 AND EXISTS (SELECT 1 FROM u WHERE a = 3)"),
                new HashMap<>()),
                "cross-block literals must extract independently (outer a=2 / inner a=3)");
    }

    /**
     * A semantic LIMIT inside a SET OPERAND is a nested query block: the digest masks
     * the value and mergeLimits cannot reach it, so it must be compared exactly or the
     * frozen set silently keeps the captured slice (losing a t1 row for LIMIT 2).
     */
    @Test
    public void testSetOperandLimitIsPartOfMatch() {
        String bind = "(SELECT k FROM t1 LIMIT 1) UNION ALL SELECT k FROM t2";
        Assertions.assertTrue(matches(bind,
                "(SELECT k FROM t1 LIMIT 1) UNION ALL SELECT k FROM t2"),
                "an identical operand LIMIT must still match");
        Assertions.assertFalse(matches(bind,
                "(SELECT k FROM t1 LIMIT 2) UNION ALL SELECT k FROM t2"),
                "a different operand LIMIT must not match (the captured LIMIT 1"
                        + " would be replayed)");
    }

    /** key(...) folds a volatile secret into the plan: baselines using it are refused. */
    @Test
    public void testKeyFunctionBaselineRejected() {
        Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectVolatileFunctionDependencies(
                        parse("SELECT key('kdb', 'k') FROM t1")));
        // a plan without key(...) passes
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(parse("SELECT a FROM t1"));
    }

    /**
     * Internal SPM writes must parse under MODE_DEFAULT (escapeSQL doubles backslashes,
     * which only decode back in that mode) even when the global session mode is
     * NO_BACKSLASH_ESCAPES.
     */
    @Test
    public void testInternalIoRunsUnderDefaultMode() {
        org.apache.doris.qe.ConnectContext ctx = new org.apache.doris.qe.ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        try {
            ctx.getSessionVariable().setSqlMode(
                    org.apache.doris.qe.SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            Assertions.assertEquals(org.apache.doris.qe.SqlModeHelper.MODE_DEFAULT,
                    BaselineManager.internalIoModeForTest(),
                    "internal writes must not inherit the global NO_BACKSLASH_ESCAPES mode");
        } finally {
            org.apache.doris.qe.ConnectContext.remove();
        }
    }

    // ==================== capture regexes are validated at SET time ====================

    @Test
    public void testCaptureRegexValidatedAtSetTime() {
        SessionVariable variable = new SessionVariable();
        variable.setPlanCaptureIncludePattern("tbl_.*");
        Assertions.assertEquals("tbl_.*", variable.getPlanCaptureIncludePattern());
        variable.setPlanCaptureExcludePattern(""); // empty = no pattern, always allowed
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> variable.setPlanCaptureIncludePattern("[unclosed"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> variable.setPlanCaptureExcludePattern("("));
    }

    // ==================== SHOW BASELINE PLANS WHERE validation ====================

    @Test
    public void testShowBaselineWhereValidation() {
        // supported shapes still parse
        parse("SHOW BASELINE PLANS WHERE id = 1");
        parse("SHOW BASELINE PLANS WHERE status = 'ENABLED'");
        // the statement-level LIKE form filters by pattern
        parse("SHOW BASELINE PLANS LIKE '%lineitem%'");
        // reversed equality is supported by swapping
        Assertions.assertNotNull(parse("SHOW BASELINE PLANS WHERE 'CAPTURE' = source"));
        // AND / ranges / a column-level LIKE / unknown columns would leave every filter
        // unset and silently return ALL baselines - they must be rejected instead
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE bind_sql LIKE '%lineitem%'"));
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE bind_sql = 'a' AND status = 'ENABLED'"));
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE id > 1"));
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE unknown_column = 1"));
        // round-27: an equality whose LHS is not one of the supported columns (`1 = 1`,
        // `id + 0 = 1`) used to be read as a LIKE pattern ('1'): the true predicate
        // searched SQL / status / source text for 1 instead of being rejected with the
        // advertised analysis error
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE 1 = 1"));
        Assertions.assertThrows(AnalysisException.class,
                () -> parse("SHOW BASELINE PLANS WHERE id + 0 = 1"));
    }

    // ==================== MARK join state is part of the match ====================

    @Test
    public void testMarkJoinStateIsPartOfMatch() {
        LogicalJoin<LogicalPlan, LogicalPlan> markJoin = markJoin(List.of(
                new EqualTo(slot("SELECT a.k FROM a"), slot("SELECT b.k FROM b"))));
        LogicalJoin<LogicalPlan, LogicalPlan> sameMarkJoin = markJoin(List.of(
                new EqualTo(slot("SELECT a.k FROM a"), slot("SELECT b.k FROM b"))));
        LogicalJoin<LogicalPlan, LogicalPlan> otherMarkJoin = markJoin(List.of(
                new EqualTo(slot("SELECT a.k FROM a"), slot("SELECT b.j FROM b"))));
        LogicalJoin<LogicalPlan, LogicalPlan> plainJoin = new LogicalJoin<>(JoinType.CROSS_JOIN,
                relation("a"), relation("b"), new JoinReorderContext());

        Assertions.assertFalse(SPMPlanTreeSupport.check(plainJoin, markJoin,
                new HashMap<Long, Expression>()),
                "a plain CROSS JOIN must not match a CROSS MARK JOIN (row multiplicity differs)");
        Assertions.assertTrue(SPMPlanTreeSupport.check(markJoin, sameMarkJoin,
                new HashMap<Long, Expression>()),
                "two MARK joins with the same mark conjuncts must match");
        Assertions.assertFalse(SPMPlanTreeSupport.check(markJoin, otherMarkJoin,
                new HashMap<Long, Expression>()),
                "different MARK conjuncts must not match");
    }

    private static LogicalJoin<LogicalPlan, LogicalPlan> markJoin(List<Expression> markConjuncts) {
        return new LogicalJoin<>(JoinType.CROSS_JOIN, ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.EMPTY_CONDITION, markConjuncts,
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("mark")), relation("a"), relation("b"),
                new JoinReorderContext());
    }

    /** The (unbound) relation node of a "SELECT * FROM t"-shaped query. */
    private static LogicalPlan relation(String table) {
        LogicalPlan project = (LogicalPlan) parse("SELECT * FROM " + table).child(0);
        return (LogicalPlan) project.child(0);
    }

    /** The first SELECT item of the given query. */
    private static Expression slot(String sql) {
        LogicalProject<?> project = (LogicalProject<?>) parse(sql).child(0);
        return project.getProjects().get(0);
    }

    // ==================== group_concat uses its dedicated grammar ====================

    @Test
    public void testGroupConcatDedicatedRendering() {
        LogicalProject<?> project = (LogicalProject<?>) parse("SELECT v, k FROM t").child(0);
        Expression value = project.getProjects().get(0);
        Expression key = project.getProjects().get(1);
        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();
        SQLRelation relation = new SQLRelation();
        String valueSql = builder.print(value, relation);
        String keySql = builder.print(key, relation);
        OrderExpression order = new OrderExpression(new OrderKey(key, false, false));

        // separator + ORDER BY -> dedicated form (the old comma-joined form printed the
        // order expression as a third argument: group_concat(v, ',', k DESC))
        GroupConcat withSeparator = new GroupConcat(false, value, new StringLiteral(","), order);
        String sql = builder.renderGroupConcat(withSeparator, relation);
        Assertions.assertEquals("GROUP_CONCAT(" + valueSql
                + " ORDER BY " + keySql + " DESC NULLS LAST SEPARATOR ',')", sql);
        // the rendered text must be accepted by the parser (immediate validation)...
        new NereidsParser().parseSingle("SELECT " + sql + " FROM t");
        // ...and survive the post-reload rebuild, which re-parses the persisted planSql
        // when a baseline is loaded again after an FE restart
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                "SELECT " + sql + " FROM t", "SELECT " + sql + " FROM t");
        Assertions.assertNotNull(trees.first);

        // ORDER BY without a separator
        GroupConcat orderedOnly = new GroupConcat(false, value, order);
        Assertions.assertEquals(
                "GROUP_CONCAT(" + valueSql + " ORDER BY " + keySql + " DESC NULLS LAST)",
                builder.renderGroupConcat(orderedOnly, relation));

        // two NON-constant value arguments cannot be written down in SQL at all (the
        // analyzer requires the second non-order argument to be a constant separator):
        // such a programmatically-built shape keeps failing the decompile
        MultiDistinctGroupConcat multiDistinct = new MultiDistinctGroupConcat(value, key, order);
        Assertions.assertNull(builder.renderGroupConcat(multiDistinct, relation));
    }

    @Test
    public void testGroupConcatDistinctOrderRendering() {
        LogicalProject<?> project = (LogicalProject<?>) parse("SELECT v, k FROM t").child(0);
        Expression value = project.getProjects().get(0);
        Expression key = project.getProjects().get(1);
        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();
        SQLRelation relation = new SQLRelation();
        String valueSql = builder.print(value, relation);
        String keySql = builder.print(key, relation);
        OrderExpression order = new OrderExpression(new OrderKey(key, false, false));

        // the execution shape of GROUP_CONCAT(DISTINCT v ORDER BY k)
        // (GroupConcat.mustUseMultiDistinctAgg converts it to MultiDistinctGroupConcat):
        // it must be RENDERED - the old implementation returned null here and forced the
        // whole decompile to fall back to the user-supplied plan text
        String distinctOrder = builder.renderGroupConcat(
                new MultiDistinctGroupConcat(value, order), relation);
        Assertions.assertEquals("GROUP_CONCAT(DISTINCT " + valueSql
                + " ORDER BY " + keySql + " DESC NULLS LAST)", distinctOrder);
        // the frozen text must parse and survive the post-reload rebuild
        new NereidsParser().parseSingle("SELECT " + distinctOrder + " FROM t");
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                "SELECT " + distinctOrder + " FROM t", "SELECT " + distinctOrder + " FROM t");
        Assertions.assertNotNull(trees.first);

        // without ORDER BY the dedup contract must still be emitted: the class name is
        // the distinct contract while isDistinct() is false, and GROUP_CONCAT(v) would
        // concatenate the duplicates at replay ("1,1,2" instead of "1,2")
        Assertions.assertEquals("GROUP_CONCAT(DISTINCT " + valueSql + ")",
                builder.renderGroupConcat(new MultiDistinctGroupConcat(value), relation));

        // constant SEPARATOR + ORDER BY
        Assertions.assertEquals("GROUP_CONCAT(DISTINCT " + valueSql
                        + " ORDER BY " + keySql + " DESC NULLS LAST SEPARATOR ',')",
                builder.renderGroupConcat(
                        new MultiDistinctGroupConcat(value, new StringLiteral(","), order), relation));

        // ... while a NON-distinct group_concat keeps its duplicates: no DISTINCT is
        // invented for it
        Assertions.assertEquals("GROUP_CONCAT(" + valueSql + ")",
                builder.renderGroupConcat(new GroupConcat(value), relation));

        // two NON-constant value arguments cannot be written down in SQL at all (the
        // analyzer requires a constant separator): keep rejecting them
        Assertions.assertNull(builder.renderGroupConcat(
                new MultiDistinctGroupConcat(value, key, order), relation));
    }

    // ==================== audit scan SQL: OR-threshold pushdown + internal filter ====================

    @Test
    public void testAuditScanSqlShape() {
        String sql = AuditLogScanner.buildScanSql("2026-01-01 00:00:00", "2026-01-01 01:00:00",
                500, 1000, 10000);
        Assertions.assertTrue(sql.contains("(`query_time` >= 1000 OR `scan_rows` >= 10000)"), sql);
        Assertions.assertTrue(sql.contains("`is_internal` = false"), sql);
        Assertions.assertTrue(sql.contains("`is_query` = true"), sql);
        Assertions.assertTrue(sql.contains("`is_nereids` = true"), sql);
        Assertions.assertTrue(sql.endsWith("LIMIT 500"), sql);
    }

    // ==================== derived-table (query block) LIMIT is part of the match ====================

    @Test
    public void testDerivedTableLimitIsPartOfMatch() {
        // a derived-table LIMIT/OFFSET has no stable identity outside its query block, so
        // it must be compared EXACTLY: replaying the captured LIMIT 10 OFFSET 1 for a
        // user LIMIT 20 OFFSET 2 would change the result slice
        String bind = "SELECT * FROM (SELECT a FROM t2 WHERE k = 1 LIMIT 10 OFFSET 1) x"
                + " JOIN t1 ON x.a = t1.a";
        Assertions.assertTrue(matches(bind, bind), "identical derived limits must match");
        Assertions.assertFalse(matches(bind,
                "SELECT * FROM (SELECT a FROM t2 WHERE k = 1 LIMIT 20 OFFSET 2) x"
                        + " JOIN t1 ON x.a = t1.a"),
                "a different derived-table LIMIT/OFFSET must not match");
        Assertions.assertFalse(matches(bind,
                "SELECT * FROM (SELECT a FROM t2 WHERE k = 1) x JOIN t1 ON x.a = t1.a"),
                "dropping the derived-table LIMIT must not match");

        // the TOP-LEVEL LIMIT is merged from the user query (mergeLimits): the rewritten
        // plan runs the user's slice, so differing top-level limits stay matchable
        String topLevel = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 10";
        Assertions.assertTrue(matches(topLevel,
                "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 20"),
                "a top-level LIMIT is adopted from the user query");
    }

    // ==================== scan modifiers are part of the match ====================

    @Test
    public void testPartitionSelectionIsPartOfMatch() {
        String bind = "SELECT * FROM t1 PARTITION(p1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1";
        Assertions.assertTrue(matches(bind, bind));
        Assertions.assertFalse(matches(bind,
                "SELECT * FROM t1 PARTITION(p2) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "a different partition selection must not match (replay would read p1)");
        Assertions.assertFalse(matches(bind,
                "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "a query without the partition selection must not match a partitioned bind");

        // partition lists are sets: the decompiler emits the selected partitions in
        // partition-id order regardless of how the user ordered them
        Assertions.assertTrue(matches(
                "SELECT * FROM t1 PARTITION(p1, p2) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1",
                "SELECT * FROM t1 PARTITION(p2, p1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "the same partition selection in another order reads the same data");
        Assertions.assertFalse(matches(
                "SELECT * FROM t1 PARTITION(p1, p1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1",
                "SELECT * FROM t1 PARTITION(p1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "a duplicated partition name must not collapse into a smaller selection");

        // tablet pins are sets too (the decompiler emits them in id order)
        Assertions.assertTrue(matches(
                "SELECT * FROM t1 TABLET(1, 2) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1",
                "SELECT * FROM t1 TABLET(2, 1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "the same tablet selection in another order reads the same data");
        Assertions.assertFalse(matches(
                "SELECT * FROM t1 TABLET(1, 2) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1",
                "SELECT * FROM t1 TABLET(1) JOIN t2 ON t1.a = t2.a WHERE t1.k = 1"),
                "a smaller tablet selection must not match");
    }

    // ==================== user-visible alias identifiers are part of the match ====================

    @Test
    public void testExplicitAliasNameIsPartOfMatch() {
        Assertions.assertTrue(matches("SELECT k AS x FROM t1 WHERE a = 1",
                "SELECT k AS x FROM t1 WHERE a = 1"));
        Assertions.assertFalse(matches("SELECT k AS x FROM t1 WHERE a = 1",
                "SELECT k AS y FROM t1 WHERE a = 1"),
                "a different explicit alias is a different result header and must not match");
    }

    // ==================== two-part relations are keyed by the effective catalog ====================

    @Test
    public void testTwoPartRelationIsKeyedByCatalog() {
        LogicalPlan twoPart = parse("SELECT * FROM db1.t WHERE k = 1");
        Assertions.assertNotEquals(
                SPMPlanTreeSupport.namespaceQualified(twoPart, "cat1", "cur").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(twoPart, "cat2", "cur").toSpmDigest(),
                "db.table resolves relative to the CURRENT catalog: the same text in two"
                        + " catalogs names two different tables");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(twoPart, "cat1", "cur").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(twoPart, "cat1", "other").toSpmDigest(),
                "a two-part name pins its database: the session database is irrelevant");

        // a three-part name is already complete and stays verbatim
        LogicalPlan threePart = parse("SELECT * FROM cat1.db1.t WHERE k = 1");
        Assertions.assertEquals(
                SPMPlanTreeSupport.namespaceQualified(threePart, "catX", "cur").toSpmDigest(),
                SPMPlanTreeSupport.namespaceQualified(threePart, "catY", "cur").toSpmDigest(),
                "a three-part name carries its own catalog");
    }

    // ==================== capture regex is validated through SQL SET (VariableMgr) ====================

    @Test
    public void testCaptureRegexValidatedThroughVariableMgr() throws Exception {
        SessionVariable variable = new SessionVariable();
        VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                org.apache.doris.analysis.SetType.SESSION,
                SessionVariable.PLAN_CAPTURE_INCLUDE_PATTERN,
                new org.apache.doris.analysis.StringLiteral("tbl_.*")));
        Assertions.assertEquals("tbl_.*", variable.getPlanCaptureIncludePattern());

        // without the setter wiring this SET wrote the field directly and PERSISTED the
        // broken pattern; every enabled capture cycle then failed filter construction
        org.apache.doris.common.DdlException error = Assertions.assertThrows(
                org.apache.doris.common.DdlException.class,
                () -> VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                        org.apache.doris.analysis.SetType.SESSION,
                        SessionVariable.PLAN_CAPTURE_INCLUDE_PATTERN,
                        new org.apache.doris.analysis.StringLiteral("[unclosed"))),
                "SET with an invalid regex must fail instead of persisting a broken pattern");
        Assertions.assertTrue(error.getMessage().contains("Invalid plan capture table regex"),
                "the error must name the invalid pattern: " + error.getMessage());
        Assertions.assertEquals("tbl_.*", variable.getPlanCaptureIncludePattern(),
                "the invalid value must never be written");
    }

    // ==================== capture interval / batch size ranges ====================

    @Test
    public void testCaptureRangeValidatedThroughVariableMgr() throws Exception {
        SessionVariable variable = new SessionVariable();
        VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                org.apache.doris.analysis.SetType.SESSION,
                SessionVariable.PLAN_CAPTURE_INTERVAL_SECONDS,
                new org.apache.doris.analysis.IntLiteral(300)));
        Assertions.assertEquals(300, variable.getPlanCaptureIntervalSeconds());

        // a non-positive interval makes every cycle compute an empty window (scanStart >=
        // currentTime) and return without scanning a single row
        org.apache.doris.common.DdlException intervalError = Assertions.assertThrows(
                org.apache.doris.common.DdlException.class,
                () -> VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                        org.apache.doris.analysis.SetType.SESSION,
                        SessionVariable.PLAN_CAPTURE_INTERVAL_SECONDS,
                        new org.apache.doris.analysis.IntLiteral(0))),
                "SET with a zero interval must fail instead of disabling the daemon silently");
        Assertions.assertTrue(intervalError.getMessage().contains("must be a positive"),
                "the error must name the invalid interval: " + intervalError.getMessage());
        Assertions.assertEquals(300, variable.getPlanCaptureIntervalSeconds(),
                "the invalid value must never be written");

        // ... the negative form fails as well, and the Java setter validates direct
        // callers (the daemon reads the values every cycle)
        Assertions.assertThrows(org.apache.doris.common.DdlException.class,
                () -> VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                        org.apache.doris.analysis.SetType.SESSION,
                        SessionVariable.PLAN_CAPTURE_INTERVAL_SECONDS,
                        new org.apache.doris.analysis.IntLiteral(-5))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> variable.setPlanCaptureIntervalSeconds(0));

        // batch size: zero would produce LIMIT 0, mark the window exhausted and advance
        // the watermark over every eligible row
        VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                org.apache.doris.analysis.SetType.SESSION,
                SessionVariable.PLAN_CAPTURE_MAX_BATCH_SIZE,
                new org.apache.doris.analysis.IntLiteral(10)));
        Assertions.assertEquals(10, variable.getPlanCaptureMaxBatchSize());
        org.apache.doris.common.DdlException batchError = Assertions.assertThrows(
                org.apache.doris.common.DdlException.class,
                () -> VariableMgr.setVar(variable, new org.apache.doris.analysis.SetVar(
                        org.apache.doris.analysis.SetType.SESSION,
                        SessionVariable.PLAN_CAPTURE_MAX_BATCH_SIZE,
                        new org.apache.doris.analysis.IntLiteral(0))),
                "SET with a zero batch size must fail");
        Assertions.assertTrue(batchError.getMessage().contains("must be positive"),
                "the error must name the invalid batch size: " + batchError.getMessage());
        Assertions.assertEquals(10, variable.getPlanCaptureMaxBatchSize(),
                "the invalid value must never be written");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> variable.setPlanCaptureMaxBatchSize(-1));
    }

    // ==================== ALTER to the same status is a no-op ====================

    @Test
    public void testAlterToSameStatusIsNoop() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select 1");
        plan.setBindSqlDigest("d");
        plan.setBindSqlHash(1);
        plan.setPlanSql("select 1");
        plan.setStatus(BaselineStatus.ENABLED);
        long id = manager.createBaseline(plan);
        Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED));
        Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus());
        Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED));
        Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
    }

    // ==================== view guard (#10) ====================

    /**
     * A plan referencing a VIEW must be detected so that SPM neither freezes a planSql
     * from it nor replays a matched baseline for it: the replay is planned BEFORE the
     * normal authorization pass and expands the view into its base tables, so
     * authorization would check those base tables instead of the view (a view-only user
     * is denied on them, a base-table user passes the same view query unchecked).
     */
    @Test
    public void testViewRelationIsDetected() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(),
                Mockito.any())).thenReturn(Mockito.mock(View.class));
        LogicalPlan viewPlan = parse("SELECT v.a FROM cat.db.v AS v WHERE v.a > 1");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctx, viewPlan),
                "a view relation must be reported as a view reference");
    }

    @Test
    public void testTableRelationIsNotReportedAsView() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(),
                Mockito.any())).thenReturn(Mockito.mock(TableIf.class));
        LogicalPlan tablePlan = parse("SELECT t.a FROM cat.db.t AS t WHERE t.a > 1");
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctx, tablePlan),
                "a plain table must not be reported as a view");
        // a view hidden in a subquery is still a view reference
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(),
                Mockito.any())).thenReturn(Mockito.mock(TableIf.class),
                Mockito.mock(View.class));
        LogicalPlan subqueryPlan = parse(
                "SELECT x FROM (SELECT t.a AS x FROM cat.db.t AS t) s JOIN cat.db.v AS v ON s.x = v.a");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctx, subqueryPlan),
                "a view nested in a join / subquery must be reported as a view reference");
    }

    @Test
    public void testUnresolvableOrUnknownContextIsNotAView() {
        // no statement context: no catalog access, nothing can be resolved -> not a view
        ConnectContext bareCtx = Mockito.mock(ConnectContext.class);
        LogicalPlan plan = parse("SELECT t.a FROM cat.db.t AS t");
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(bareCtx, plan));
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(null, plan));
        // an unresolvable relation is left to the normal analysis pass (error reported there)
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(),
                Mockito.any())).thenThrow(new RuntimeException("catalog not ready"));
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctx, plan));
    }

    @Test
    public void testAnalyzedViewNodeIsDetected() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(Mockito.mock(StatementContext.class));
        LogicalView<?> view = Mockito.mock(LogicalView.class);
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctx, view),
                "an analyzed LogicalView node must be reported as a view reference");
    }

    // ==================== view guard: nested statements (#2) ====================

    /**
     * A view behind a CTE body must be reported: the CTE body lives in
     * LogicalCTE.getAliasQueries() / extraPlans(), NOT in children(), so a walk over
     * children() alone would miss it and SPM would freeze the expanded base-table scans -
     * replay would then authorize those base tables instead of the view.
     */
    @Test
    public void testViewBehindCteIsDetected() {
        LogicalPlan ctePlan = parse(
                "WITH c AS (SELECT v.a AS x FROM cat.db.v AS v) SELECT c.x FROM c");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("v")), ctePlan),
                "a view inside a CTE body must be reported as a view reference");
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of()), ctePlan),
                "a CTE over base tables is not a view reference");

        LogicalPlan nestedCte = parse("SELECT x FROM (WITH c AS (SELECT v.a AS x FROM cat.db.v AS v)"
                + " SELECT c.x FROM c) s");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of("v")), nestedCte),
                "a view in a CTE inside a derived table must be reported");
    }

    /**
     * IN / EXISTS / scalar subqueries hold their plan in SubqueryExpr.queryPlan (surfaced
     * through extraPlans() and through the node's expressions, coercions included) - a
     * view behind any of them must reach the view guard as well.
     */
    @Test
    public void testViewBehindSubqueriesIsDetected() {
        ConnectContext viewCtx = ctxResolving(Set.of("v"));
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(viewCtx,
                parse("SELECT t.a FROM cat.db.t AS t WHERE t.a IN (SELECT v.a FROM cat.db.v AS v)")),
                "a view behind an IN subquery must be reported");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(viewCtx,
                parse("SELECT t.a FROM cat.db.t AS t WHERE EXISTS (SELECT 1 FROM cat.db.v AS v"
                        + " WHERE v.a = t.a)")),
                "a view behind an EXISTS subquery must be reported");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(viewCtx,
                parse("SELECT (SELECT max(v.a) FROM cat.db.v AS v) AS m FROM cat.db.t AS t")),
                "a view behind a scalar select-list subquery must be reported");
        Assertions.assertTrue(SPMPlanTreeSupport.referencesView(viewCtx,
                parse("SELECT t.a FROM cat.db.t AS t WHERE t.a >"
                        + " (SELECT max(v.a) FROM cat.db.v AS v) + 1")),
                "a view behind a subquery nested in an expression (coercion) must be reported");

        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctxResolving(Set.of()),
                parse("SELECT t.a FROM cat.db.t AS t WHERE t.a IN (SELECT u.a FROM cat.db.u AS u)")),
                "a subquery over base tables is not a view reference");
    }

    /** A mocked ConnectContext whose relation resolution reports the given names as views. */
    private static ConnectContext ctxResolving(Set<String> viewNames) {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(),
                Mockito.any())).thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    String name = qualifier.get(qualifier.size() - 1);
                    return viewNames.contains(name)
                            ? Mockito.mock(View.class) : Mockito.mock(TableIf.class);
                });
        return ctx;
    }

    // ==================== review round: mixed IN / ASOF / conjunct order / sink ====================

    /** Parameterizes a parsed tree with the SPM placeholder builder. */
    private static LogicalPlan param(String sql) {
        LogicalPlan plan = parse(sql);
        return SPMPlanTreeSupport.transform(plan,
                expr -> expr.accept(new SPMPlaceholderBuilder(), null));
    }

    /**
     * The two filter conjunct SETS are unordered, and the bind side renders placeholders
     * while the user side carries the concrete literal text: a lexical (toSql) sort can
     * pair the wrong expressions when the literal ordering reverses the structural order,
     * rejecting a VALID baseline. The multiset match must accept the reversal.
     */
    @Test
    public void testConjunctLiteralOrderReversalMatches() {
        LogicalPlan bind = param("SELECT * FROM t1 WHERE a > 5 AND b < 10");
        LogicalPlan user = parse("SELECT * FROM t1 WHERE b < 20 AND a > 6");
        Map<Long, Expression> values = new HashMap<>();
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, user, values),
                "a literal-order reversal must not reject the baseline");
        Assertions.assertEquals(2, values.size(), "both literals are extracted: " + values);

        LogicalPlan differentSlot = parse("SELECT * FROM t1 WHERE b < 20 AND c > 6");
        Assertions.assertFalse(SPMPlanTreeSupport.check(bind, differentSlot, new HashMap<>()),
                "a conjunct on a different slot must not match");
    }

    /**
     * ASOF ... MATCH_CONDITION(...) USING(k) stores the temporal boundary in a field that
     * is outside children() and getExpressions(): the transform must parameterize it and
     * the match must compare it, otherwise a user variant keeps the captured boundary.
     */
    @Test
    public void testAsofMatchConditionTakesPartInTheMatch() {
        LogicalPlan bind = param(
                "SELECT * FROM t1 ASOF JOIN t2 MATCH_CONDITION(t1.ts >= 5) USING(k)");
        LogicalUsingJoin<?, ?> parameterized = findUsingJoin(bind);
        Assertions.assertNotNull(parameterized,
                "ASOF ... USING(k) must parse to a using join");
        Assertions.assertTrue(parameterized.getMatchCondition().isPresent(),
                "the ASOF boundary must be present");
        Assertions.assertTrue(SPMPlanTreeSupport.containsPlaceholder(
                        parameterized.getMatchCondition().get()),
                "the boundary literal must be parameterized: " + parameterized.getMatchCondition());

        Map<Long, Expression> values = new HashMap<>();
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind,
                        parse("SELECT * FROM t1 ASOF JOIN t2 MATCH_CONDITION(t1.ts >= 9) USING(k)"),
                        values),
                "a different boundary VALUE must match (the value is substituted)");
        Assertions.assertFalse(values.isEmpty(), "the boundary value must be extracted");

        Assertions.assertFalse(SPMPlanTreeSupport.check(bind,
                        parse("SELECT * FROM t1 ASOF JOIN t2 MATCH_CONDITION(t1.other >= 9) USING(k)"),
                        new HashMap<>()),
                "a boundary on a different slot must NOT match: replay would keep the captured one");
    }

    /** A SELECT ... INTO OUTFILE statement must be detected as sink-bearing. */
    @Test
    public void testFileSinkStatementsAreRejected() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsFileSink(parse(
                        "SELECT a FROM t1 INTO OUTFILE 'file:///tmp/spm_sink.out' FORMAT AS csv")),
                "an INTO OUTFILE statement must be detected: its destination is not comparable");
        Assertions.assertFalse(SPMPlanTreeSupport.containsFileSink(parse("SELECT a FROM t1")),
                "a plain SELECT is not sink-bearing");
    }

    /**
     * A derived (nameFromChild) alias must STAY derived when a child is rewritten, and the
     * match must require alias-kind parity: otherwise a derived side could pair with an
     * explicitly named side and replay the captured result header. The derived LABEL
     * itself is not part of the match any more - the replay hands the caller's label back
     * (alignRootOutputLabels), so a value variant keeps matching.
     */
    @Test
    public void testDerivedAliasProvenanceSurvivesAndIsEnforced() {
        LogicalPlan bind = param("SELECT k + 1 FROM t1");
        UnboundAlias alias = findUnboundAlias(bind);
        Assertions.assertNotNull(alias, "the select item must carry an alias node");
        Assertions.assertTrue(alias.isNameFromChild(),
                "the transform must preserve nameFromChild");

        LogicalPlan user = parse("SELECT k + 2 FROM t1");
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, user, new HashMap<>()),
                "a derived label carrying the varying literal must keep matching: the replay"
                        + " replaces the frozen alias with the caller's own text");
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, parse("SELECT k + 1 FROM t1"),
                        new HashMap<>()),
                "the identical derived label still matches");

        LogicalPlan explicitBind = SPMPlanTreeSupport.transform(bind, expr ->
                expr instanceof UnboundAlias
                        ? new UnboundAlias(((UnboundAlias) expr).child(), "total") : expr);
        Assertions.assertFalse(SPMPlanTreeSupport.check(explicitBind, user, new HashMap<>()),
                "explicit vs derived alias must not match");
        Assertions.assertTrue(SPMPlanTreeSupport.check(explicitBind,
                        parse("SELECT k + 2 AS total FROM t1"), new HashMap<>()),
                "the same explicit alias still matches");
    }

    /**
     * TableScanParams has no value-based equals / toString and every parse builds a fresh
     * instance: a baseline using @branch / @options syntax could never hit unless the type
     * and payloads are compared explicitly.
     */
    @Test
    public void testTableScanParamsCompareByValue() {
        Assertions.assertTrue(SPMPlanTreeSupport.check(
                        parse("SELECT * FROM t1 @incr(branch = 'main')"),
                        parse("SELECT * FROM t1 @incr(branch = 'main')"), new HashMap<>()),
                "identical scan parameters must hit");
        Assertions.assertFalse(SPMPlanTreeSupport.check(
                        parse("SELECT * FROM t1 @incr(branch = 'main')"),
                        parse("SELECT * FROM t1 @incr(branch = 'dev')"), new HashMap<>()),
                "differing scan parameters must not match");
        Assertions.assertTrue(SPMPlanTreeSupport.check(
                        parse("SELECT * FROM t1 @options('k' = 'v')"),
                        parse("SELECT * FROM t1 @options('k' = 'v')"), new HashMap<>()));
        Assertions.assertFalse(SPMPlanTreeSupport.check(
                        parse("SELECT * FROM t1 @options('k' = 'v')"),
                        parse("SELECT * FROM t1 @options('k' = 'w')"), new HashMap<>()));
        Assertions.assertTrue(SPMPlanTreeSupport.check(
                        parse("SELECT * FROM t1 @snapshot(a, b)"),
                        parse("SELECT * FROM t1 @snapshot(a, b)"), new HashMap<>()),
                "the identifier-list form compares by value too");
    }

    /**
     * The MATCH analyzer decides how the pattern is tokenized and lives outside children():
     * different analyzers must neither share a digest nor match each other.
     */
    @Test
    public void testMatchAnalyzerIsPartOfIdentity() {
        MatchPhrase standard = new MatchPhrase(
                new SlotReference("k", VarcharType.SYSTEM_DEFAULT), new StringLiteral("abc"), "standard");
        MatchPhrase other = new MatchPhrase(
                new SlotReference("k", VarcharType.SYSTEM_DEFAULT), new StringLiteral("abc"), "other");
        Assertions.assertNotEquals(standard.toDigest(), other.toDigest(),
                "the analyzer must take part in the digest");

        LogicalPlan bind = parse(
                "SELECT * FROM t1 WHERE k MATCH_PHRASE 'abc' USING ANALYZER standard");
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, parse(
                        "SELECT * FROM t1 WHERE k MATCH_PHRASE 'abc' USING ANALYZER standard"),
                        new HashMap<>()),
                "identical analyzers must match");
        Assertions.assertFalse(SPMPlanTreeSupport.check(bind, parse(
                        "SELECT * FROM t1 WHERE k MATCH_PHRASE 'abc' USING ANALYZER english"),
                        new HashMap<>()),
                "a different analyzer must NOT match: replay would keep the captured one");
    }

    /** First LogicalUsingJoin reachable from the plan (null when there is none). */
    private static LogicalUsingJoin<?, ?> findUsingJoin(LogicalPlan plan) {
        final LogicalUsingJoin<?, ?>[] found = new LogicalUsingJoin[1];
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (found[0] == null && node instanceof LogicalUsingJoin) {
                found[0] = (LogicalUsingJoin<?, ?>) node;
            }
        });
        return found[0];
    }

    /** First UnboundAlias reachable through the plan's expressions (null when none). */
    private static UnboundAlias findUnboundAlias(LogicalPlan plan) {
        final UnboundAlias[] found = new UnboundAlias[1];
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expression : node.getExpressions()) {
                findUnboundAlias(expression, found);
            }
        });
        return found[0];
    }

    private static void findUnboundAlias(Expression expression, UnboundAlias[] found) {
        if (found[0] != null) {
            return;
        }
        if (expression instanceof UnboundAlias) {
            found[0] = (UnboundAlias) expression;
            return;
        }
        for (Expression child : expression.children()) {
            findUnboundAlias(child, found);
        }
    }

    // ==================== reload keeps bind / fallback block numbering aligned ====================

    /**
     * Reload of a DISTINCT bind / raw-fallback plan pair: both texts must start their own
     * query-block numbering (like CREATE), otherwise the bind tree's nested literal
     * advances the shared counter and the SAME nested literal of the fallback text gets
     * a different block id - it can then not reuse the bind placeholder, and the
     * placeholder-residue check rejects a baseline that worked before the reload.
     */
    @Test
    public void testReloadKeepsNestedLiteralIdsAlignedAcrossTexts() {
        String bindSql = "SELECT a FROM t1 WHERE a = 100"
                + " AND EXISTS (SELECT 1 FROM t2 WHERE b = 5)";
        String planSql = "select a from t1 where a = 100"
                + " and exists (select 1 from t2 where b = 5)";
        Assertions.assertNotEquals(bindSql, planSql, "the texts must stay distinct");
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                bindSql, planSql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertNotNull(trees.first);
        Assertions.assertNotNull(trees.second);
        Set<Long> bindIds = placeholderIds(trees.first);
        Set<Long> planIds = placeholderIds(trees.second);
        Assertions.assertEquals(3, bindIds.size(),
                "a=100, the EXISTS subquery's 1 and b=5 are three literals: " + bindIds);
        Assertions.assertEquals(bindIds, planIds,
                "the fallback plan text must REUSE the bind placeholder ids (block"
                        + " numbering restarts per text): bind=" + bindIds
                        + " plan=" + planIds);
    }

    /** All placeholder ids of a plan, descending into expression-owned subquery plans. */
    private static Set<Long> placeholderIds(LogicalPlan plan) {
        Set<Long> ids = new HashSet<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                collectPlaceholderIds(expr, ids);
            }
        });
        return ids;
    }

    private static void collectPlaceholderIds(Expression expr, Set<Long> ids) {
        if (expr instanceof SpmConstVar) {
            ids.add(((SpmConstVar) expr).getId());
        } else if (expr instanceof SpmConstList) {
            ids.add(((SpmConstList) expr).getId());
        }
        if (expr instanceof SubqueryExpr) {
            ids.addAll(placeholderIds(((SubqueryExpr) expr).getQueryPlan()));
        }
        for (Expression child : expr.children()) {
            collectPlaceholderIds(child, ids);
        }
    }

    // ==================== dotted identifier boundaries in slot matching ====================

    /**
     * `a.b` (ONE component containing a dot) and a.b (qualifier a + column b) name
     * different columns; the earlier dot-join kept neither the digest nor the AST check
     * boundary, so a user query selecting b could accept a baseline captured for `a.b`
     * and replay its captured value.
     */
    @Test
    public void testDottedSlotBoundariesArePreserved() {
        UnboundSlot dotted = new UnboundSlot(List.of("a.b"));
        UnboundSlot qualified = new UnboundSlot(List.of("a", "b"));
        Assertions.assertEquals("`a.b`", dotted.toDigest(),
                "a component containing a dot must stay delimited in the digest");
        Assertions.assertEquals("a.b", qualified.toDigest(),
                "qualifier + column keeps the plain dotted rendering");
        Assertions.assertNotEquals(dotted.toDigest(), qualified.toDigest());

        // the AST check must reject the pair even though both texts print a.b
        Expression bind = new EqualTo(new SpmConstVar(1L, new IntegerLiteral(1)), dotted);
        Expression user = new EqualTo(new IntegerLiteral(1), qualified);
        Assertions.assertFalse(new SPMAstCheckVisitor().checkExpression(bind, user,
                new HashMap<Long, Expression>()),
                "`a.b` must not match a.b: replay could return the captured column");
        // control: the same reference still matches and extracts
        Map<Long, Expression> values = new HashMap<>();
        Assertions.assertTrue(new SPMAstCheckVisitor().checkExpression(bind,
                new EqualTo(new IntegerLiteral(1), dotted), values));
        Assertions.assertEquals(1, values.size());
    }
}
