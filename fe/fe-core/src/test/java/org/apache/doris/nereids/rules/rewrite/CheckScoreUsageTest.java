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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

class CheckScoreUsageTest extends TestWithFeService {
    private static final String SCORE_USAGE_ERROR = "score()";

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
    void testIneligibleTopNWithoutRangeKeepsItsDiagnostic(String format, String predicate) {
        assertScoreUsageError("SELECT id, score() AS s FROM score_usage_" + format
                + " WHERE " + predicate + " ORDER BY id LIMIT 10",
                "ORDER BY expression must reference a score() function");
        assertScoreUsageError("SELECT id, score() AS s FROM score_usage_" + format
                + " WHERE " + predicate + " ORDER BY s, id LIMIT 10",
                "TopN must have exactly one ordering expression");
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
    void testLossyScoreCastsAreRejected(String format, String predicate) {
        for (String type : new String[] {"INT", "DECIMAL(10, 6)"}) {
            for (String suffix : new String[] {"", " ORDER BY score() DESC LIMIT 10"}) {
                assertScoreUsageError("SELECT id FROM score_usage_" + format
                        + " WHERE " + predicate + " AND CAST(score() AS " + type + ") > 0" + suffix,
                        "score() predicate in WHERE clause must be in the form");
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
    void testAggregateScoreRemainsRejected(String format, String predicate) {
        for (String projection : new String[] {"sum(score())", "count(distinct score())"}) {
            assertScoreUsageError("SELECT " + projection + " FROM score_usage_" + format
                    + " WHERE " + predicate + " AND score() > 0.00001");
        }
        assertScoreUsageError("SELECT score() FROM score_usage_" + format
                + " WHERE " + predicate + " AND score() > 0.00001 GROUP BY score()");
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testNestedScoreInFilterIsRejected(String format, String predicate) {
        assertScoreUsageError("SELECT id FROM score_usage_" + format
                + " WHERE " + predicate + " AND (score() + 1 > 1 OR id = 1)");
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreProjectionWithoutTopNIsRejected(String format, String predicate) {
        assertScoreUsageError("SELECT id, score() FROM score_usage_" + format
                + " WHERE " + predicate);
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
    void testScoreFilterAboveJoinIsRejected() {
        String join = "SELECT a.id FROM score_usage_V2 a JOIN score_usage_V3 b ON a.id = b.id"
                + " WHERE a.c1 MATCH 'apple'";
        String error = "score() filter must be in the WHERE clause of a query on one table";
        assertScoreUsageError(join + " AND score() > 0.00001", error);
        // With MATCH on both sides, score() must not silently filter each side by its own score.
        assertScoreUsageError(join + " AND b.c1 MATCH 'apple' AND score() > 0.00001", error);
        assertScoreUsageError("SELECT a.id FROM score_usage_V2 a, score_usage_V3 b"
                + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001", error);
        assertScoreUsageError("SELECT a.id FROM score_usage_V2 a JOIN score_usage_V3 b"
                + " ON a.id = b.id AND score() > 0.00001 WHERE a.c1 MATCH 'apple'",
                "can not contains Score expression");
    }

    @Test
    void testScoredSubqueryCanBeJoined() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM (SELECT id FROM score_usage_V2"
                        + " WHERE c1 MATCH 'apple' AND score() > 0.00001) a"
                        + " JOIN score_usage_V3 b ON a.id = b.id",
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
    }

    @Test
    void testScoreFilterFollowsPreservedSideOfOuterJoin() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM score_usage_V2 a LEFT JOIN score_usage_V3 b ON a.id = b.id"
                        + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001",
                planner -> assertOnlyScanWithScoreRange(planner, "score_usage_V2"));
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id, b.c2 FROM score_usage_V3 b RIGHT JOIN score_usage_V2 a ON a.id = b.id"
                        + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001",
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
            Assertions.assertEquals(1, planner.getScanNodes().size());
            OlapScanNode scan = (OlapScanNode) planner.getScanNodes().get(0);
            TPlanNode thriftScan = scan.treeToThrift().getNodes().get(0);
            Assertions.assertNotNull(thriftScan.olap_scan_node.score_sort_info);
            Assertions.assertTrue(thriftScan.olap_scan_node.isSetScoreSortLimit());
            Assertions.assertEquals(0, thriftScan.olap_scan_node.score_sort_limit);
            Assertions.assertNotNull(thriftScan.olap_scan_node.score_range_info);
            Assertions.assertEquals(op, thriftScan.olap_scan_node.score_range_info.op);
            Assertions.assertEquals(0.00001, thriftScan.olap_scan_node.score_range_info.threshold, 1e-10);
            Assertions.assertTrue(thriftScan.olap_scan_node.score_sort_info.ordering_exprs.get(0)
                    .nodes.get(0).slot_ref.is_virtual_slot, "Scoring must reference a virtual slot");
            int scoreSlotId = thriftScan.olap_scan_node.score_sort_info.ordering_exprs.get(0)
                    .nodes.get(0).slot_ref.slot_id;
            Assertions.assertTrue(scan.getTupleDesc().getSlots().stream()
                    .anyMatch(slot -> slot.getId().asInt() == scoreSlotId),
                    "The score binding must reference a slot in the final scan tuple");
            Assertions.assertEquals(outputColumns, planner.getFragments().get(0).getOutputExprs().size());
        });
    }

    private void assertScoreUsageError(String sql) {
        assertScoreUsageError(sql, SCORE_USAGE_ERROR);
    }

    private void assertScoreUsageError(String sql, String error) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
        Assertions.assertTrue(exception.getMessage().contains(error), exception.getMessage());
    }
}
