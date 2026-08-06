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

package org.apache.doris.nereids.mv;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.rules.exploration.mv.Predicates;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.ExpressionInfo;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.PredicateCompensation;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Test the method in Predicates*/
public class PredicatesTest extends SqlTestBase {
    private static final String SELECT_ALL_TEST_COLUMNS =
            "select id, score, event_date, event_time, amount, tag from T1 where ";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("predicates_test");
        useDatabase("predicates_test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint,\n"
                        + "    event_date date,\n"
                        + "    event_time datetime,\n"
                        + "    amount decimal(10, 2),\n"
                        + "    tag varchar(20)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint,\n"
                        + "    event_date date,\n"
                        + "    event_time datetime,\n"
                        + "    amount decimal(10, 2),\n"
                        + "    tag varchar(20)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n"
        );

        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
    }

    @Test
    public void testCompensateCouldNotPullUpPredicatesFail() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 5;",
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 1;");

        Map<Expression, ExpressionInfo> expressionExpressionInfoMap = Predicates.compensateCouldNotPullUpPredicates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult);
        Assertions.assertNull(expressionExpressionInfoMap);
    }

    @Test
    public void testFinalizeCompensationByResidualKeepsRangeCandidate() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 5;",
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 15 and id < 5;");

        Map<Expression, ExpressionInfo> compensateCouldNotPullUpPredicates = Predicates.compensateCouldNotPullUpPredicates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult);
        Assertions.assertNotNull(compensateCouldNotPullUpPredicates);
        Assertions.assertTrue(compensateCouldNotPullUpPredicates.isEmpty());

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertTrue(compensationCandidates.getResiduals().isEmpty());
        Assertions.assertEquals(1, compensationCandidates.getRanges().size());
        assertPredicateSqlEquals(compensationCandidates.getRanges(),
                "(score > 15)");
        Assertions.assertThrows(UnsupportedOperationException.class,
                compensationCandidates.getRanges()::clear);

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertEquals(1, finalPredicateCompensation.getRanges().size());
        assertPredicateSqlEquals(finalPredicateCompensation.getRanges(),
                "(score > 15)");
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testResidualCompensateSupportsDnfBranchImplication() {
        assertResidualCompensationSucceeds(
                "id = 5 or id > 10",
                "id > 10 or (score = 1 and id = 5)",
                "OR[(id > 10),AND[(score = 1),(id = 5)]]");
    }

    @Test
    public void testResidualCompensateSupportsStrongerRangeInDnfBranchImplication() {
        assertResidualCompensationSucceeds(
                "id > 10 or (score = 1 and id = 5)",
                "id > 15 or (score = 1 and id = 5)",
                "OR[(id > 15),AND[(score = 1),(id = 5)]]");
    }

    @Test
    public void testResidualCompensateSupportsComparableLiteralTypesInDnfBranchImplication() {
        String[][] cases = {
                {
                        "event_date >= date '2024-01-01' or score = 1",
                        "event_date > date '2024-01-01' or score = 1"
                },
                {
                        "event_time <= timestamp '2024-01-02 03:04:05' or score = 2",
                        "event_time < timestamp '2024-01-02 03:04:05' or score = 2"
                },
                {
                        "amount >= 10.50 or score = 3",
                        "amount = 10.50 or score = 3"
                },
                {
                        "tag >= 'm' or score = 4",
                        "tag > 'm' or score = 4"
                }
        };
        for (String[] testCase : cases) {
            assertResidualCompensationSucceeds(testCase[0], testCase[1]);
        }
    }

    @Test
    public void testResidualCompensateRejectsNonImpliedComparableLiteralTypesInDnfBranchImplication() {
        String[][] cases = {
                {
                        "event_date > date '2024-01-01' or score = 1",
                        "event_date >= date '2024-01-01' or score = 1"
                },
                {
                        "event_time < timestamp '2024-01-02 03:04:05' or score = 2",
                        "event_time <= timestamp '2024-01-02 03:04:05' or score = 2"
                },
                {
                        "amount = 10.50 or score = 3",
                        "amount >= 10.50 or score = 3"
                },
                {
                        "tag > 'm' or score = 4",
                        "tag >= 'm' or score = 4"
                }
        };
        for (String[] testCase : cases) {
            assertResidualCompensationFails(testCase[0], testCase[1]);
        }
    }

    @Test
    public void testResidualCompensateSupportsBoundaryOperatorImplicationInDnfBranch() {
        String[][] cases = {
                {
                        "id <= 10 or score = 1",
                        "id < 10 or score = 1"
                },
                {
                        "id >= 10 or score = 1",
                        "id > 10 or score = 1"
                },
                {
                        "id >= 10 or score = 1",
                        "id = 10 or score = 1"
                },
                {
                        "id <= 10 or score = 1",
                        "id = 10 or score = 1"
                }
        };
        for (String[] testCase : cases) {
            assertResidualCompensationSucceeds(testCase[0], testCase[1]);
        }
    }

    @Test
    public void testResidualCompensateRejectsReverseBoundaryOperatorImplicationInDnfBranch() {
        String[][] cases = {
                {
                        "id < 10 or score = 1",
                        "id <= 10 or score = 1"
                },
                {
                        "id > 10 or score = 1",
                        "id >= 10 or score = 1"
                },
                {
                        "id = 10 or score = 1",
                        "id >= 10 or score = 1"
                },
                {
                        "id = 10 or score = 1",
                        "id <= 10 or score = 1"
                }
        };
        for (String[] testCase : cases) {
            assertResidualCompensationFails(testCase[0], testCase[1]);
        }
    }

    @Test
    public void testResidualCompensateHandlesNestedDnfBranches() {
        // This case verifies branch-by-branch coverage for nested DNF predicates.
        // The view predicate is logically:
        //   id >= 10
        //   OR (score = 1 AND id IN (5, 6))
        //   OR (score = 2 AND id <= 3)
        // Each expanded branch in the covered query predicate is contained by one view branch.
        String viewPredicate = "id >= 10 "
                + "or (score = 1 and id = 5) "
                + "or (score = 1 and id = 6) "
                + "or (score = 2 and id <= 3)";
        // The covered query predicate is logically:
        //   (id > 20 AND score IN (3, 4))
        //   OR (score = 1 AND id IN (5, 6) AND (amount = 8.00 OR tag = 'x'))
        //   OR (score = 2 AND id <= 3)
        String coveredQueryPredicate = "((id > 20 and (score = 3 or score = 4)) "
                + "or ((score = 1 and (id = 5 or id = 6)) and (amount = 8.00 or tag = 'x')) "
                + "or (score = 2 and (id < 3 or id = 3)))";
        assertResidualCompensationSucceeds(viewPredicate, coveredQueryPredicate);

        // The extra branch only satisfies score = 1. Its id = 7 is not in the view's id IN (5, 6),
        // and it does not satisfy id >= 10 or score = 2 AND id <= 3, so the residual is unsafe.
        assertResidualCompensationFails(
                viewPredicate,
                coveredQueryPredicate + " or (score = 1 and id = 7)");
    }

    @Test
    public void testFinalizeCompensationByResidualConsumesCoveredResidual() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select id, score from T1 where id = 5 or id > 10",
                "select id, score from T1 where id = 5 or id > 10");

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertEquals(1, compensationCandidates.getResiduals().size());
        assertPredicateSqlEquals(compensationCandidates.getResiduals(),
                "OR[(id = 5),(id > 10)]");

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertTrue(finalPredicateCompensation.getEquals().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getRanges().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testCoveredWindowResidualIsAllowedAfterFinalization() {
        String sql = "select id, score from T1 qualify sum(score) over (partition by id) > 10";
        PredicateRewriteContext rewriteContext = buildRewriteContext(sql, sql);

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertTrue(compensationCandidates.getResiduals().keySet().stream()
                .anyMatch(expression -> expression.anyMatch(WindowExpression.class::isInstance)));

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testQueryOnlyWindowResidualIsRejectedAsCompensation() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select id, score from T1",
                "select id, score from T1 qualify sum(score) over (partition by id) > 10");

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertTrue(compensationCandidates.getResiduals().keySet().stream()
                .anyMatch(expression -> expression.anyMatch(WindowExpression.class::isInstance)));

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNull(finalPredicateCompensation);
    }

    @Test
    public void testCompensateCandidatesByViewResidualKeepsExactResidualWhenDnfBranchesOverflow() {
        String overflowResidual = buildDnfOverflowResidual();
        String sql = "select id, score from T1 where " + overflowResidual;
        PredicateRewriteContext rewriteContext = buildRewriteContext(sql, sql);

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertEquals(11, compensationCandidates.getResiduals().size());
        assertPredicateSqlEquals(compensationCandidates.getResiduals(),
                "OR[(id = 1),(score = 101)]",
                "OR[(id = 2),(score = 102)]",
                "OR[(id = 3),(score = 103)]",
                "OR[(id = 4),(score = 104)]",
                "OR[(id = 5),(score = 105)]",
                "OR[(id = 6),(score = 106)]",
                "OR[(id = 7),(score = 107)]",
                "OR[(id = 8),(score = 108)]",
                "OR[(id = 9),(score = 109)]",
                "OR[(id = 10),(score = 110)]",
                "OR[(id = 11),(score = 111)]");

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertTrue(finalPredicateCompensation.getEquals().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getRanges().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testCompensateCandidatesByViewResidualReturnsNullWhenDnfBranchesOverflow() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select id, score from T1 where id = 999 or score = 999",
                "select id, score from T1 where " + buildDnfOverflowResidual());

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertEquals(11, compensationCandidates.getResiduals().size());

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        // Non-exact DNF branches exceed the guard threshold, so implication falls back conservatively.
        Assertions.assertNull(finalPredicateCompensation);
    }

    @Test
    public void testCompensateCandidatesByViewResidualSkipsDnfWhenViewResidualEmpty() {
        String overflowResidual = buildDnfOverflowResidual();
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select id, score from T1",
                "select id, score from T1 where " + overflowResidual);

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertEquals(11, compensationCandidates.getResiduals().size());

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertEquals(compensationCandidates.getEquals(), finalPredicateCompensation.getEquals());
        Assertions.assertEquals(compensationCandidates.getRanges(), finalPredicateCompensation.getRanges());
        Assertions.assertEquals(compensationCandidates.getResiduals(), finalPredicateCompensation.getResiduals());
    }

    private String buildDnfOverflowResidual() {
        return "(id = 1 or score = 101)"
                + " and (id = 2 or score = 102)"
                + " and (id = 3 or score = 103)"
                + " and (id = 4 or score = 104)"
                + " and (id = 5 or score = 105)"
                + " and (id = 6 or score = 106)"
                + " and (id = 7 or score = 107)"
                + " and (id = 8 or score = 108)"
                + " and (id = 9 or score = 109)"
                + " and (id = 10 or score = 110)"
                + " and (id = 11 or score = 111)";
    }

    private PredicateRewriteContext buildRewriteContext(String viewSql, String querySql) {
        CascadesContext viewContext = createCascadesContext(viewSql, connectContext);
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .getPlan().child(0);

        CascadesContext queryContext = createCascadesContext(querySql, connectContext);
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .getAllPlan().get(0).child(0);

        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        RelationMapping relationMapping = RelationMapping.generate(viewStructInfo.getRelations(),
                queryStructInfo.getRelations(), 16).get(0);
        SlotMapping viewToQuerySlotMapping = SlotMapping.generate(relationMapping);
        ComparisonResult comparisonResult = HyperGraphComparator.isLogicCompatible(
                queryStructInfo.getHyperGraph(),
                viewStructInfo.getHyperGraph(),
                constructContext(queryPlan, viewPlan, queryContext));
        return new PredicateRewriteContext(
                viewStructInfo,
                queryStructInfo,
                viewToQuerySlotMapping,
                comparisonResult,
                queryContext);
    }

    private PredicateCompensation compensatePredicates(PredicateRewriteContext rewriteContext) {
        return Predicates.compensatePredicates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
    }

    private void assertResidualCompensationSucceeds(String viewPredicate, String queryPredicate,
            String... expectedResidualPredicates) {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                SELECT_ALL_TEST_COLUMNS + viewPredicate,
                SELECT_ALL_TEST_COLUMNS + queryPredicate);
        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        String message = "view predicate: " + viewPredicate + ", query predicate: " + queryPredicate;
        Assertions.assertNotNull(compensationCandidates, message);
        if (expectedResidualPredicates.length != 0) {
            assertPredicateSqlEquals(compensationCandidates.getResiduals(), expectedResidualPredicates);
        }

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNotNull(finalPredicateCompensation, message);
        if (expectedResidualPredicates.length != 0) {
            assertPredicateSqlEquals(finalPredicateCompensation.getResiduals(), expectedResidualPredicates);
        }
    }

    private void assertResidualCompensationFails(String viewPredicate, String queryPredicate) {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                SELECT_ALL_TEST_COLUMNS + viewPredicate,
                SELECT_ALL_TEST_COLUMNS + queryPredicate);
        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        String message = "view predicate: " + viewPredicate + ", query predicate: " + queryPredicate;
        Assertions.assertNotNull(compensationCandidates, message);
        Assertions.assertFalse(compensationCandidates.getResiduals().isEmpty(), message);

        PredicateCompensation finalPredicateCompensation = compensatePredicates(rewriteContext);
        Assertions.assertNull(finalPredicateCompensation, message);
    }

    private static final class PredicateRewriteContext {
        private final StructInfo viewStructInfo;
        private final StructInfo queryStructInfo;
        private final SlotMapping viewToQuerySlotMapping;
        private final ComparisonResult comparisonResult;
        private final CascadesContext queryContext;

        private PredicateRewriteContext(StructInfo viewStructInfo, StructInfo queryStructInfo,
                SlotMapping viewToQuerySlotMapping, ComparisonResult comparisonResult,
                CascadesContext queryContext) {
            this.viewStructInfo = viewStructInfo;
            this.queryStructInfo = queryStructInfo;
            this.viewToQuerySlotMapping = viewToQuerySlotMapping;
            this.comparisonResult = comparisonResult;
            this.queryContext = queryContext;
        }
    }

    private static void assertPredicateSqlEquals(Map<Expression, ExpressionInfo> predicates,
            String... expectedPredicates) {
        String actual = predicates.keySet().stream()
                .map(Expression::toSql)
                .map(PredicatesTest::normalizeSql)
                .sorted()
                .collect(Collectors.joining(";"));
        String expected = Arrays.stream(expectedPredicates)
                .map(PredicatesTest::normalizeSql)
                .sorted()
                .collect(Collectors.joining(";"));
        Assertions.assertEquals(expected, actual, "predicate sql mismatch");
    }

    // Normalize SQL text to remove non-semantic noise before string comparison.
    private static String normalizeSql(String sql) {
        return sql.replace("`", "")
                .replaceAll("\\s+", " ")
                .trim()
                .toLowerCase(Locale.ROOT);
    }
}
