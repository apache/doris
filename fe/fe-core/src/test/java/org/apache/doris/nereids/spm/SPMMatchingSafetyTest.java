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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.capture.AuditLogScanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
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
        Mockito.when(statementContext.getAndCacheTable(Mockito.anyList(), Mockito.any(),
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
        Mockito.when(statementContext.getAndCacheTable(Mockito.anyList(), Mockito.any(),
                Mockito.any())).thenReturn(Mockito.mock(TableIf.class));
        LogicalPlan tablePlan = parse("SELECT t.a FROM cat.db.t AS t WHERE t.a > 1");
        Assertions.assertFalse(SPMPlanTreeSupport.referencesView(ctx, tablePlan),
                "a plain table must not be reported as a view");
        // a view hidden in a subquery is still a view reference
        Mockito.when(statementContext.getAndCacheTable(Mockito.anyList(), Mockito.any(),
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
        Mockito.when(statementContext.getAndCacheTable(Mockito.anyList(), Mockito.any(),
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
        Mockito.when(statementContext.getAndCacheTable(Mockito.anyList(), Mockito.any(),
                Mockito.any())).thenAnswer(invocation -> {
                    List<String> qualifier = invocation.getArgument(0);
                    String name = qualifier.get(qualifier.size() - 1);
                    return viewNames.contains(name)
                            ? Mockito.mock(View.class) : Mockito.mock(TableIf.class);
                });
        return ctx;
    }
}
