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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CheckScoreUsageTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("score_usage");
        connectContext.setDatabase("score_usage");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        for (String format : new String[] {"V2", "V3", "SNII"}) {
            createTable("CREATE TABLE score_usage_" + format + " ("
                    + "id INT, c1 TEXT, c2 TEXT, "
                    + "INDEX idx_c1 (c1) USING INVERTED PROPERTIES "
                    + "('parser' = 'english', 'lower_case' = 'true', 'support_phrase' = 'true')) "
                    + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "PROPERTIES ('replication_num' = '1', "
                    + "'inverted_index_storage_format' = '" + format + "')");
        }
        createTable("CREATE TABLE score_usage_ann ("
                + "id INT NOT NULL, c1 TEXT, embedding ARRAY<FLOAT> NOT NULL, "
                + "INDEX idx_c1 (c1) USING INVERTED PROPERTIES ('parser' = 'english'), "
                + "INDEX idx_ann (embedding) USING ANN PROPERTIES "
                + "('index_type' = 'hnsw', 'metric_type' = 'l2_distance', 'dim' = '2')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'inverted_index_storage_format' = 'V3')");
    }

    static Stream<Arguments> scoringPredicates() {
        return Stream.of("V2", "V3", "SNII").flatMap(format -> Stream.of(
                Arguments.of(format, "c1 MATCH 'apple'"),
                Arguments.of(format, "search('c1:apple')")));
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreOnlyInFilterIsMaterialized(String format, String predicate) {
        assertScoreRangeWithoutTopN("SELECT id FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001");
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testLimitWithoutScoreOrderIsMaterialized(String format, String predicate) {
        for (String suffix : new String[] {" LIMIT 10", " ORDER BY id LIMIT 10"}) {
            assertScoreRangeWithoutTopN("SELECT id FROM score_usage_" + format
                    + " WHERE " + predicate + " AND score() > 0.00001" + suffix);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreProjectionWithRangeIsMaterialized(String format, String predicate) {
        assertScoreRangeWithoutTopN("SELECT id, score() AS s FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001", 2);
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testRepeatedScoreAliasesUseTheRangeBinding(String format, String predicate) {
        assertScoreRangeWithoutTopN("SELECT id, score() AS first_score, score() AS second_score"
                + " FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001", 3);
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testProjectedScoreWithOrdinaryOrderUsesFullScoring(String format, String predicate) {
        assertScoreRangeWithoutTopN("SELECT id, score() AS s FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001 ORDER BY id LIMIT 1 OFFSET 1", 2);
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testOtherSortShapesUseFullScoring(String format, String predicate) {
        for (String suffix : new String[] {
                " ORDER BY s DESC, id LIMIT 1 OFFSET 1", " ORDER BY id DESC LIMIT 1", " ORDER BY s DESC"}) {
            assertScoreRangeWithoutTopN("SELECT id, c2, score() AS s FROM score_usage_" + format
                    + " WHERE " + predicate + " AND score() > 0.00001" + suffix, 3);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreWithoutRangeOrTopNScoresEveryRow(String format, String predicate) {
        for (String suffix : new String[] {"", " ORDER BY id LIMIT 10", " ORDER BY s, id LIMIT 10"}) {
            assertFullScoring("SELECT id, score() AS s FROM score_usage_" + format
                    + " WHERE " + predicate + suffix, 2);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testOrdinaryOrderOverflowIsRejected(String format, String predicate) {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(
                        "SELECT id, score() AS s FROM score_usage_" + format
                                + " WHERE " + predicate + " AND score() > 0.00001 ORDER BY id"
                                + " LIMIT 9223372036854775807 OFFSET 9223372036854775807"));
        Assertions.assertTrue(exception.getMessage().contains("limit + offset overflows"), exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testOtherScoreFiltersSeeEveryScore(String format, String predicate) {
        // Only score > x and score >= x go into the scan; any other filter on the score is an
        // ordinary filter, which also keeps a TopN on the score from limiting the scoring.
        for (String filter : new String[] {"CAST(score() AS INT) > 0", "CAST(score() AS DECIMAL(10, 6)) > 0",
                "score() < 1", "score() * score() > 0", "(score() + 1 > 1 OR id = 1)"}) {
            for (String suffix : new String[] {"", " ORDER BY score() DESC LIMIT 10"}) {
                assertFullScoring("SELECT id FROM score_usage_" + format
                        + " WHERE " + predicate + " AND " + filter + suffix, 1);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testMinimumScoreFormsUseFullScoring(String format, String predicate) {
        for (String range : new String[] {"score() > 0.00001", "0.00001 < score()",
                "CAST(score() AS DOUBLE) > 0.00001", "CAST(score() AS FLOAT) > 0.00001"}) {
            assertScoreRangeWithoutTopN("SELECT id FROM score_usage_" + format
                    + " WHERE " + predicate + " AND " + range);
        }
        for (String range : new String[] {"score() >= 0.00001", "0.00001 <= score()"}) {
            assertScoreRangeWithoutTopN("SELECT id FROM score_usage_" + format
                    + " WHERE " + predicate + " AND " + range, 1, TExprOpcode.GE);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreCanBeAggregatedAndGrouped(String format, String predicate) {
        for (String query : new String[] {"SELECT sum(score())", "SELECT count(distinct score())",
                "SELECT max(score()) AS m, id"}) {
            assertScoreRangeWithoutTopN(query + " FROM score_usage_" + format
                    + " WHERE " + predicate + " AND score() > 0.00001"
                    + (query.endsWith("id") ? " GROUP BY id" : ""), query.endsWith("id") ? 2 : 1);
        }
        assertScoreRangeWithoutTopN("SELECT score() FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001 GROUP BY score()");
        assertFullScoring("SELECT c2 FROM score_usage_" + format + " WHERE " + predicate
                + " GROUP BY c2 HAVING max(score()) > 0.00001", 1);
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreRangeWithTopNIsMaterialized(String format, String predicate) {
        for (String projection : new String[] {"id", "id, score() AS s"}) {
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze("SELECT " + projection + " FROM score_usage_" + format
                            + " WHERE " + predicate + " AND score() > 0.00001"
                            + " ORDER BY score() DESC LIMIT 10")
                    .rewrite();
            LogicalOlapScan scan = (LogicalOlapScan) checker.getCascadesContext().getRewritePlan()
                    .collectFirst(plan -> plan instanceof LogicalOlapScan).get();
            Assertions.assertFalse(scan.getVirtualColumns().isEmpty());
            Assertions.assertFalse(scan.getScoreOrderKeys().isEmpty());
            Assertions.assertTrue(scan.getScoreLimit().isPresent());
            Assertions.assertEquals(10L, scan.getScoreLimit().get().longValue());
            Assertions.assertTrue(scan.getScoreRangeInfo().isPresent());
            Assertions.assertEquals(TExprOpcode.GT, scan.getScoreRangeInfo().get().getOp());
            Assertions.assertEquals(0.00001, scan.getScoreRangeInfo().get().getThreshold(), 1e-10);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testFilterWithoutScoreRemainsValid(String format, String predicate) {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext)
                .analyze("SELECT id FROM score_usage_" + format + " WHERE " + predicate)
                .rewrite());
    }

    @ParameterizedTest
    @ValueSource(strings = {"c1 MATCH 'apple'", "search('c1:apple')"})
    void testScoreRangeKeepsAnnTopN(String predicate) {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id, l2_distance_approximate(embedding, [0.0, 0.0]) AS dist FROM score_usage_ann"
                        + " WHERE " + predicate + " AND score() > 0.5 ORDER BY dist LIMIT 1",
                planner -> {
                    Assertions.assertEquals(1, planner.getScanNodes().size());
                    TPlanNode thriftScan = planner.getScanNodes().get(0).treeToThrift().getNodes().get(0);
                    Assertions.assertNotNull(thriftScan.olap_scan_node.ann_sort_info);
                    Assertions.assertEquals(1, thriftScan.olap_scan_node.ann_sort_limit);
                    Assertions.assertEquals(0, thriftScan.olap_scan_node.score_sort_limit);
                    Assertions.assertEquals(TExprOpcode.GT, thriftScan.olap_scan_node.score_range_info.op);
                });
    }

    @Test
    void testScoreNextToWindowFunctions() {
        // score() is a column of the scan, so a window in the same SELECT does not change it.
        assertScoreRangeWithoutTopN("SELECT id, score() AS s, row_number() OVER (ORDER BY id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001", 3);
        assertScoreRangeWithoutTopN("SELECT id, row_number() OVER (ORDER BY score() DESC) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001", 2);
        assertScoreRangeWithoutTopN("SELECT id, s, row_number() OVER (ORDER BY id) AS rn FROM ("
                + "SELECT id, score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001) x", 3);
        // A filter above the window must see the rows the window numbered: it stays an ordinary filter.
        assertFullScoring("SELECT id, rn FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn,"
                + " score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple') w WHERE s > 0.00001", 2);
        assertFullScoring("SELECT id FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                + " QUALIFY row_number() OVER (ORDER BY score() DESC, id) = 1", 1);
    }

    @Test
    void testScoreBindsToTheTableOfItsMatch() {
        for (String join : new String[] {"JOIN", "LEFT JOIN", "CROSS JOIN"}) {
            String on = join.equals("CROSS JOIN") ? "" : " ON a.id = b.id";
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT a.id, score() AS s, b.c2 FROM score_usage_V2 a " + join + " score_usage_V3 b" + on
                            + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001",
                    planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
        }
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM score_usage_V3 b RIGHT JOIN score_usage_V2 a ON a.id = b.id"
                        + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001",
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
        assertScoreRangeWithoutTopN("SELECT id, e FROM score_usage_V2 LATERAL VIEW explode(array(1, 2)) t AS e"
                + " WHERE c1 MATCH 'apple' AND score() > 0.00001", 2);
        assertScoreRangeWithoutTopN("WITH c AS (SELECT id, c1 FROM score_usage_V2)"
                + " SELECT id FROM c WHERE c1 MATCH 'apple' AND score() > 0.00001");
    }

    @Test
    void testScoreWithoutMatchInItsSelectIsRejected() {
        String error = CheckScoreUsage.NO_SCORING_PREDICATE_ERROR;
        assertScoreUsageError("SELECT score() FROM score_usage_V2", error);
        assertScoreUsageError("SELECT score() FROM score_usage_V2 WHERE c2 = 'sweet'", error);
        // score() belongs to the SELECT that calls it, not to the MATCH of a subquery, CTE or view.
        assertScoreUsageError("SELECT * FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple') w WHERE score() > 0.00001", error);
        assertScoreUsageError("SELECT * FROM (SELECT id FROM score_usage_V2 WHERE c1 MATCH 'apple' LIMIT 1) l"
                + " WHERE score() > 0.00001", error);
        assertScoreUsageError("WITH c AS (SELECT id, c1 FROM score_usage_V2 WHERE c1 MATCH 'apple')"
                + " SELECT id FROM c WHERE score() > 0.00001", error);
        // Its recursive reference is bound by the planner only, so plan the whole query.
        String recursive = "WITH RECURSIVE r AS (SELECT id, c1 FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                + " UNION ALL SELECT CAST(id + 1 AS INT), c1 FROM r WHERE id < 3)"
                + " SELECT id FROM r WHERE score() > 0.00001";
        NereidsException exception = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(recursive));
        Assertions.assertTrue(exception.getMessage().contains(error), exception.getMessage());
    }

    @Test
    void testScoreOfSeveralTablesIsRejected() {
        String error = BindScoreToOlapScan.UNRESOLVED_SCAN_ERROR;
        assertScoreUsageError("SELECT a.id FROM score_usage_V2 a JOIN score_usage_V3 b ON a.id = b.id"
                + " WHERE a.c1 MATCH 'apple' AND b.c1 MATCH 'apple' AND score() > 0.00001", error);
        for (String setOperation : new String[] {"UNION ALL", "UNION", "INTERSECT"}) {
            String input = "(SELECT id, c1 FROM score_usage_V2 " + setOperation
                    + " SELECT id, c1 FROM score_usage_V3) u";
            assertScoreUsageError("SELECT id FROM " + input + " WHERE c1 MATCH 'apple' AND score() > 0.00001",
                    error);
            assertScoreUsageError("SELECT id FROM (SELECT id, score() AS s FROM " + input
                    + " WHERE c1 MATCH 'apple') x WHERE s > 0.00001", error);
        }
        assertScoreUsageError("SELECT a.id FROM score_usage_V2 a JOIN score_usage_V3 b"
                + " ON a.id = b.id AND score() > 0.00001 WHERE a.c1 MATCH 'apple'",
                "can not contains Score expression");
    }

    @Test
    void testOneTableScoredByTwoSelectsIsRejected() {
        assertScoreUsageError("SELECT id, score() AS s2 FROM (SELECT id, c1, score() AS s1 FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple') x WHERE c1 MATCH 'red'", BindScoreToOlapScan.SHARED_SCAN_ERROR);
    }

    @Test
    void testScoreAboveGroupByIsRejected() {
        assertScoreUsageError("SELECT id FROM score_usage_V2 WHERE c1 MATCH 'apple' GROUP BY id"
                + " HAVING score() > 0.00001", BindScoreToOlapScan.SCORE_NOT_AVAILABLE_ERROR);
    }

    @Test
    void testMatchOfAnotherSelectDoesNotScore() {
        for (String outer : new String[] {"c1 MATCH 'red'", "search('c1:red')"}) {
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT id, s FROM (SELECT id, c1, score() AS s FROM score_usage_V2"
                            + " WHERE c1 MATCH 'apple' AND score() > 0.00001) x WHERE " + outer,
                    planner -> {
                        TPlanNode thriftScan = planner.getScanNodes().get(0).treeToThrift().getNodes().get(0);
                        List<Boolean> collectScore = new ArrayList<>();
                        for (TExpr conjunct : thriftScan.conjuncts) {
                            for (TExprNode node : conjunct.nodes) {
                                if (node.node_type == TExprNodeType.MATCH_PRED) {
                                    collectScore.add(node.match_predicate.collect_score);
                                } else if (node.node_type == TExprNodeType.SEARCH_EXPR) {
                                    collectScore.add(node.search_param.collect_score);
                                }
                            }
                        }
                        // The MATCH of the scored SELECT adds to the score, the outer predicate does not.
                        Assertions.assertEquals(2, collectScore.size(), thriftScan.conjuncts.toString());
                        Assertions.assertTrue(collectScore.contains(true), thriftScan.conjuncts.toString());
                        Assertions.assertTrue(collectScore.contains(false), thriftScan.conjuncts.toString());
                    });
        }
    }

    @Test
    void testScoreAliasFilterBindsToItsScan() {
        assertScoreRangeWithoutTopN("SELECT id, s FROM (SELECT id, score() AS s FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple') x WHERE s > 0.00001", 2);
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM (SELECT id, score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple') a"
                        + " JOIN score_usage_V3 b ON a.id = b.id WHERE a.s > 0.00001",
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
    }

    @Test
    void testScoreColumnsOfUnionBranches() {
        // A filter on the score column of each branch is a filter on a value, which UNION DISTINCT keeps.
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT * FROM (SELECT id, score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                        + " UNION SELECT id, score() AS s FROM score_usage_V3 WHERE c1 MATCH 'apple') u"
                        + " WHERE s > 0.00001",
                planner -> {
                    Assertions.assertEquals(2, planner.getScanNodes().size());
                    for (ScanNode scanNode : planner.getScanNodes()) {
                        Assertions.assertTrue(scanNode.treeToThrift().getNodes().get(0)
                                .olap_scan_node.isSetScoreRangeInfo());
                    }
                });
    }

    @Test
    void testScoredSubqueryCanBeJoined() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM (SELECT id FROM score_usage_V2"
                        + " WHERE c1 MATCH 'apple' AND score() > 0.00001) a"
                        + " JOIN score_usage_V3 b ON a.id = b.id",
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "id IN (SELECT id FROM score_usage_V3)",
            "id NOT IN (SELECT id FROM score_usage_V3)",
            "EXISTS (SELECT 1 FROM score_usage_V3 b WHERE b.id = score_usage_V2.id)",
            "NOT EXISTS (SELECT 1 FROM score_usage_V3 b WHERE b.id = score_usage_V2.id)"})
    void testScoreFilterStaysOnOuterScanOfSubquery(String subqueryPredicate) {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001 AND "
                        + subqueryPredicate,
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
    }

    @Test
    void testCountAndSelectStarWithScoreRange() {
        assertScoreRangeWithoutTopN("SELECT count(*) FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() > 0.00001");
        // The hidden score slot must not become an output column.
        assertScoreRangeWithoutTopN("SELECT * FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() > 0.00001", 3);
    }

    @Test
    void testCseVirtualColumnsLeaveScoreToTheBinding() {
        boolean enableCse = connectContext.getSessionVariable().experimentalEnableVirtualSlotForCse;
        connectContext.getSessionVariable().experimentalEnableVirtualSlotForCse = true;
        try {
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze("SELECT id, sin(abs(score()) + 1) AS a, cos(abs(score()) + 1) AS b"
                            + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001")
                    .rewrite();
            LogicalOlapScan scan = (LogicalOlapScan) checker.getCascadesContext().getRewritePlan()
                    .collectFirst(plan -> plan instanceof LogicalOlapScan).get();
            // Only the score column may contain score(); a CSE column would evaluate it as zero.
            List<NamedExpression> scoreColumns = scan.getVirtualColumns().stream()
                    .filter(column -> column.containsType(Score.class))
                    .collect(Collectors.toList());
            Assertions.assertEquals(1, scoreColumns.size(), scan.getVirtualColumns().toString());
            Assertions.assertTrue(((Alias) scoreColumns.get(0)).child() instanceof Score);
        } finally {
            connectContext.getSessionVariable().experimentalEnableVirtualSlotForCse = enableCse;
        }
    }

    @Test
    void testFilterOnScoreRunsAboveTheScan() throws Exception {
        // The scan fills its score column only after it evaluated its own conjuncts, so a filter on
        // the score runs in a node above the scan, and COUNT_ON_INDEX cannot count in the scan.
        for (String query : new String[] {"SELECT id", "SELECT count(*)"}) {
            for (String filter : new String[] {"score() < 1", "CAST(score() AS INT) = 0", "(score() > 0 OR id = 1)"}) {
                String explain = getSQLPlanOrErrorMsg("EXPLAIN " + query + " FROM score_usage_V2"
                        + " WHERE c1 MATCH 'apple' AND " + filter);
                String scanPredicates = Arrays.stream(explain.split("\n"))
                        .filter(line -> line.contains("PREDICATES:")).findFirst().orElse("");
                Assertions.assertTrue(scanPredicates.contains("MATCH_ANY"), explain);
                Assertions.assertFalse(scanPredicates.contains("__score"), explain);
                Assertions.assertTrue(explain.contains("VSELECT"), explain);
                Assertions.assertFalse(explain.contains("COUNT_ON_INDEX"), explain);
            }
        }
        // The scan applies the minimum score itself, before COUNT_ON_INDEX counts.
        String explain = getSQLPlanOrErrorMsg("EXPLAIN SELECT count(*) FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() > 0.5");
        Assertions.assertTrue(explain.contains("COUNT_ON_INDEX"), explain);
    }

    @Test
    void testExplainShowsScoreRange() throws Exception {
        String explain = getSQLPlanOrErrorMsg("EXPLAIN SELECT id FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() >= 0.5");
        Assertions.assertTrue(explain.contains("SCORE SORT LIMIT: 0"), explain);
        Assertions.assertTrue(explain.contains("SCORE RANGE: score() >= 0.5"), explain);
    }

    private void assertOnlyScanWithScoreRange(NereidsPlanner planner, String tableName) {
        Assertions.assertEquals(2, planner.getScanNodes().size());
        for (ScanNode scanNode : planner.getScanNodes()) {
            String scanTable = ((OlapScanNode) scanNode).getOlapTable().getName();
            TPlanNode thriftScan = scanNode.treeToThrift().getNodes().get(0);
            Assertions.assertEquals(scanTable.equals(tableName),
                    thriftScan.olap_scan_node.isSetScoreRangeInfo(), scanTable);
        }
    }

    private void assertScoreRangeWithoutTopN(String sql) {
        assertScoreRangeWithoutTopN(sql, 1);
    }

    private void assertScoreRangeWithoutTopN(String sql, int outputColumns) {
        assertScoreRangeWithoutTopN(sql, outputColumns, TExprOpcode.GT);
    }

    private void assertScoreRangeWithoutTopN(String sql, int outputColumns, TExprOpcode op) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            TPlanNode thriftScan = assertScoresEveryRow(planner, outputColumns);
            Assertions.assertNotNull(thriftScan.olap_scan_node.score_range_info, sql);
            Assertions.assertEquals(op, thriftScan.olap_scan_node.score_range_info.op);
            Assertions.assertEquals(0.00001, thriftScan.olap_scan_node.score_range_info.threshold, 1e-10);
        });
    }

    // The scan scores every row and leaves every filter on the score to the plan above it.
    private void assertFullScoring(String sql, int outputColumns) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            TPlanNode thriftScan = assertScoresEveryRow(planner, outputColumns);
            Assertions.assertFalse(thriftScan.olap_scan_node.isSetScoreRangeInfo(), sql);
        });
    }

    private TPlanNode assertScoresEveryRow(NereidsPlanner planner, int outputColumns) {
        Assertions.assertEquals(1, planner.getScanNodes().size());
        OlapScanNode scan = (OlapScanNode) planner.getScanNodes().get(0);
        TPlanNode thriftScan = scan.treeToThrift().getNodes().get(0);
        Assertions.assertNotNull(thriftScan.olap_scan_node.score_sort_info);
        Assertions.assertTrue(thriftScan.olap_scan_node.isSetScoreSortLimit());
        Assertions.assertEquals(0, thriftScan.olap_scan_node.score_sort_limit);
        Assertions.assertTrue(thriftScan.olap_scan_node.score_sort_info.ordering_exprs.get(0)
                .nodes.get(0).slot_ref.is_virtual_slot, "Scoring must reference a virtual slot");
        int scoreSlotId = thriftScan.olap_scan_node.score_sort_info.ordering_exprs.get(0)
                .nodes.get(0).slot_ref.slot_id;
        Assertions.assertTrue(scan.getTupleDesc().getSlots().stream()
                .anyMatch(slot -> slot.getId().asInt() == scoreSlotId),
                "The score binding must reference a slot in the final scan tuple");
        Assertions.assertEquals(outputColumns, planner.getFragments().get(0).getOutputExprs().size());
        return thriftScan;
    }

    private void assertScoreUsageError(String sql, String error) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
        Assertions.assertTrue(exception.getMessage().contains(error), exception.getMessage());
    }
}
