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
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

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

    /** The (unbound) relation node of "SELECT * FROM <table>". */
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

        // multi-distinct values combined with ORDER BY have no faithful rendering
        MultiDistinctGroupConcat multiDistinct = new MultiDistinctGroupConcat(value, key, order);
        Assertions.assertNull(builder.renderGroupConcat(multiDistinct, relation));
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
}
