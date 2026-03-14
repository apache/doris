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

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("predicates_test");
        useDatabase("predicates_test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
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
                        + ",INFER_PREDICATES"
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

        PredicateCompensation finalPredicateCompensation = Predicates.compensateCandidatesByViewResidual(
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                compensationCandidates);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertEquals(1, finalPredicateCompensation.getRanges().size());
        assertPredicateSqlEquals(finalPredicateCompensation.getRanges(),
                "(score > 15)");
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testResidualCompensateSupportsDnfBranchImplication() {
        PredicateRewriteContext rewriteContext = buildRewriteContext(
                "select id, score from T1 where id = 5 or id > 10",
                "select id, score from T1 where id > 10 or (score = 1 and id = 5)");

        PredicateCompensation compensationCandidates = Predicates.collectCompensationCandidates(
                rewriteContext.queryStructInfo,
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                rewriteContext.comparisonResult,
                rewriteContext.queryContext);
        Assertions.assertNotNull(compensationCandidates);
        Assertions.assertTrue(compensationCandidates.getEquals().isEmpty());
        Assertions.assertTrue(compensationCandidates.getRanges().isEmpty());
        Assertions.assertEquals(1, compensationCandidates.getResiduals().size());
        assertPredicateSqlEquals(compensationCandidates.getResiduals(),
                "OR[(id > 10),AND[(score = 1),(id = 5)]]");

        PredicateCompensation finalPredicateCompensation = Predicates.compensateCandidatesByViewResidual(
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                compensationCandidates);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertTrue(finalPredicateCompensation.getEquals().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getRanges().isEmpty());
        Assertions.assertEquals(1, finalPredicateCompensation.getResiduals().size());
        assertPredicateSqlEquals(finalPredicateCompensation.getResiduals(),
                "OR[(id > 10),AND[(score = 1),(id = 5)]]");
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

        PredicateCompensation finalPredicateCompensation = Predicates.compensateCandidatesByViewResidual(
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                compensationCandidates);
        Assertions.assertNotNull(finalPredicateCompensation);
        Assertions.assertTrue(finalPredicateCompensation.getEquals().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getRanges().isEmpty());
        Assertions.assertTrue(finalPredicateCompensation.getResiduals().isEmpty());
    }

    @Test
    public void testCompensateCandidatesByViewResidualReturnsNullWhenDnfBranchesOverflow() {
        String overflowResidual = "(id = 1 or score = 101)"
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

        PredicateCompensation finalPredicateCompensation = Predicates.compensateCandidatesByViewResidual(
                rewriteContext.viewStructInfo,
                rewriteContext.viewToQuerySlotMapping,
                compensationCandidates);
        // DNF branches exceed the guard threshold, so implication falls back conservatively.
        Assertions.assertNull(finalPredicateCompensation);
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
