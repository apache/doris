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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RowTtlIsVisible;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BindScoreToOlapScanTest extends TestWithFeService {
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
        createView("CREATE VIEW score_usage_top AS SELECT id, score() AS s FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' ORDER BY s DESC LIMIT 2");
        createTable("CREATE TABLE score_usage_mow (id INT, c1 TEXT, "
                + "INDEX idx_c1 (c1) USING INVERTED PROPERTIES ('parser' = 'english')) "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true')");
        for (String table : new String[] {"score_usage_ttl_dup", "score_usage_ttl_mow"}) {
            String keyType = table.endsWith("mow") ? "UNIQUE" : "DUPLICATE";
            createTable("CREATE TABLE " + table + " (id INT, c1 TEXT, "
                    + "INDEX idx_c1 (c1) USING INVERTED PROPERTIES ('parser' = 'english')) "
                    + keyType + " KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "PROPERTIES ('replication_num' = '1', 'function_column.enable_row_ttl' = 'true')");
        }
    }

    static Stream<Arguments> scoringPredicates() {
        return Stream.of("V2", "V3", "SNII").flatMap(format -> Stream.of(
                Arguments.of(format, "c1 MATCH 'apple'"),
                Arguments.of(format, "search('c1:apple')")));
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testMinimumScoreIsAFilterOfTheScan(String format, String predicate) {
        for (String range : new String[] {"score() > 0.00001", "0.00001 < score()",
                "CAST(score() AS DOUBLE) > 0.00001", "CAST(score() AS FLOAT) > 0.00001",
                "score() >= 0.00001", "0.00001 <= score()"}) {
            assertMinimumScore("SELECT id FROM score_usage_" + format + " WHERE " + predicate + " AND " + range, 1);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreInSelectListAndOrderBy(String format, String predicate) {
        String from = " FROM score_usage_" + format + " WHERE " + predicate + " AND score() > 0.00001";
        assertMinimumScore("SELECT id, score() AS s" + from, 2);
        assertMinimumScore("SELECT id, score() AS first_score, score() AS second_score" + from, 3);
        for (String suffix : new String[] {" LIMIT 10", " ORDER BY id LIMIT 1 OFFSET 1",
                " ORDER BY s DESC, id LIMIT 1 OFFSET 1", " ORDER BY s DESC"}) {
            assertMinimumScore("SELECT id, c2, score() AS s" + from + suffix, 3);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreWithoutMinimumScoresEveryRow(String format, String predicate) {
        for (String suffix : new String[] {"", " ORDER BY id LIMIT 10", " ORDER BY s, id LIMIT 10"}) {
            assertFullScoring("SELECT id, score() AS s FROM score_usage_" + format + " WHERE " + predicate + suffix,
                    2);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreTopNIsPushedIntoTheScan(String format, String predicate) {
        for (String projection : new String[] {"id", "id, score() AS s"}) {
            for (String range : new String[] {"", " AND score() > 0.00001"}) {
                LogicalOlapScan scan = rewrittenScan("SELECT " + projection + " FROM score_usage_" + format
                        + " WHERE " + predicate + range + " ORDER BY score() DESC LIMIT 10");
                Assertions.assertEquals(10L, scan.getScoreLimit().get().longValue());
                Assertions.assertEquals(!range.isEmpty(), scan.getScoreRangeInfo().isPresent());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"score_usage_ttl_dup", "score_usage_ttl_mow"})
    void testRowTtlKeepsAllScoreCandidates(String table) {
        for (String predicate : new String[] {"c1 MATCH 'apple'", "search('c1:apple')"}) {
            assertMinimumScore("SELECT id FROM " + table + " WHERE " + predicate + " AND score() > 0.00001", 1);
            for (String projection : new String[] {"id", "id, score() AS s"}) {
                for (String range : new String[] {"", " AND score() > 0.00001"}) {
                    String sql = "SELECT " + projection + " FROM " + table + " WHERE " + predicate + range
                            + " ORDER BY score() DESC LIMIT 10 OFFSET 2";
                    Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite()
                            .getCascadesContext().getRewritePlan();
                    LogicalOlapScan scan = (LogicalOlapScan) plan.collectFirst(
                            node -> node instanceof LogicalOlapScan).get();
                    Assertions.assertEquals(0L, scan.getScoreLimit().get().longValue(), plan.treeString());
                    Assertions.assertFalse(scan.getScoreRangeInfo().isPresent(), plan.treeString());
                    Assertions.assertTrue(plan.anyMatch(node -> ((Plan) node).getExpressions().stream()
                            .anyMatch(expression -> expression.containsType(RowTtlIsVisible.class))), plan.treeString());
                    PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
                        TPlanNode thrift = thriftScan(planner);
                        Assertions.assertNotNull(thrift.olap_scan_node.score_sort_info);
                        Assertions.assertEquals(0, thrift.olap_scan_node.score_sort_limit, sql);
                        Assertions.assertFalse(thrift.olap_scan_node.isSetScoreRangeInfo(), sql);
                        if (!range.isEmpty()) {
                            assertFiltersScore(thrift);
                        }
                    });
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testMinimumScoreOfAggregation(String format, String predicate) {
        String from = " FROM score_usage_" + format + " WHERE " + predicate + " AND score() > 0.00001";
        assertMinimumScore("SELECT count(*)" + from, 1);
        assertMinimumScore("SELECT c2, count(*)" + from + " GROUP BY c2 HAVING count(*) > 0", 2);
        assertMinimumScore("SELECT DISTINCT c2" + from, 1);
        for (String groupBy : new String[] {"GROUPING SETS ((c2), ())", "ROLLUP(c2)", "CUBE(c2)"}) {
            assertMinimumScore("SELECT c2, count(*)" + from + " GROUP BY " + groupBy, 2);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testOtherScoreFiltersAreRejected(String format, String predicate) {
        String from = "SELECT id FROM score_usage_" + format + " WHERE " + predicate + " AND ";
        for (String filter : new String[] {"CAST(score() AS INT) > 0", "CAST(score() AS DECIMAL(10, 6)) > 0",
                "score() < 1", "score() <= 1", "score() = 1", "score() * score() > 0", "score() > id",
                "score() + 1 > 1"}) {
            assertScoreUsageError(from + filter, BindScoreToOlapScan.RANGE_FORM_ERROR);
        }
        assertScoreUsageError(from + "score() > 0.1 AND score() < 0.9", BindScoreToOlapScan.ONE_RANGE_ERROR);
        assertScoreUsageError(from + "(score() > 0 OR id = 1)", BindScoreToOlapScan.TOP_LEVEL_RANGE_ERROR);
        assertScoreUsageError(from + "score() > 'abc'", "score() function can only be compared with numeric types");
        assertScoreUsageError("SELECT id, 'abc' < score() FROM score_usage_" + format + " WHERE " + predicate,
                "score() function can only be compared with numeric types");
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testScoreOfAggregationIsRejected(String format, String predicate) {
        String from = " FROM score_usage_" + format + " WHERE " + predicate;
        for (String query : new String[] {"SELECT sum(score())" + from, "SELECT count(distinct score())" + from,
                "SELECT score()" + from + " GROUP BY score()", "SELECT DISTINCT score()" + from,
                "SELECT c2, max(score())" + from + " GROUP BY c2",
                "SELECT c2, max(score())" + from + " GROUP BY ROLLUP(c2)"}) {
            assertScoreUsageError(query, BindScoreToOlapScan.AGGREGATE_ERROR);
        }
    }

    @ParameterizedTest
    @MethodSource("scoringPredicates")
    void testOverflowingTopNIsRejected(String format, String predicate) {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(
                        "SELECT id, score() AS s FROM score_usage_" + format
                                + " WHERE " + predicate + " AND score() > 0.00001 ORDER BY s DESC"
                                + " LIMIT 9223372036854775807 OFFSET 9223372036854775807"));
        Assertions.assertTrue(exception.getMessage().contains("limit + offset overflows"), exception.getMessage());
    }

    @Test
    void testScoreNextToWindowFunctions() {
        assertMinimumScore("SELECT id, score() AS s, row_number() OVER (ORDER BY id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001", 3);
        assertMinimumScore("SELECT id, row_number() OVER (ORDER BY score() DESC, id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001", 2);
    }

    // The score column passes the delete sign filter that analysis puts on a UNIQUE table.
    @ParameterizedTest
    @ValueSource(strings = {"score_usage_V2", "score_usage_mow"})
    void testTableAlias(String table) {
        assertMinimumScore("SELECT a.id, score() AS s FROM " + table + " a"
                + " WHERE a.c1 MATCH 'apple' AND score() > 0.00001", 2);
        // ORDER BY score() reads the score although the select list does not.
        LogicalOlapScan scan = rewrittenScan("SELECT a.id FROM " + table + " a WHERE a.c1 MATCH 'apple'"
                + " ORDER BY score() DESC LIMIT 10");
        Assertions.assertEquals(10L, scan.getScoreLimit().get().longValue());
    }

    // The score column is not a column of the table.
    @Test
    void testStarLeavesOutTheScoreColumn() {
        String where = " WHERE c1 MATCH 'apple' AND score() > 0.00001";
        assertMinimumScore("SELECT * FROM score_usage_V2" + where, 3);
        assertMinimumScore("SELECT a.* FROM score_usage_V2 a" + where, 3);
        assertMinimumScore("SELECT *, score() AS s FROM score_usage_V2" + where, 4);
        assertMinimumScore("SELECT * EXCEPT (c2) FROM score_usage_V2" + where, 2);
        // * REPLACE puts score() into the select list like any other item.
        assertMinimumScore("SELECT * REPLACE (score() AS c2) FROM score_usage_V2" + where, 3);
        assertFullScoring("SELECT * REPLACE (score() AS c2) FROM score_usage_V2 WHERE c1 MATCH 'apple'", 3);
    }

    // Analysis expands an alias function, so its score() is bound like one in the select list.
    @Test
    void testScoreInAliasFunction() throws Exception {
        createFunction("CREATE ALIAS FUNCTION score_usage_alias() WITH PARAMETER() AS score()");
        assertFullScoring("SELECT id, score_usage_alias() FROM score_usage_V2 WHERE c1 MATCH 'apple'", 2);
    }

    // With prefer_udf_over_builtin, a UDF named score is called instead of the builtin score().
    @Test
    void testUdfNamedScore() throws Exception {
        createFunction("CREATE ALIAS FUNCTION score() WITH PARAMETER() AS abs(1)");
        boolean preferUdf = connectContext.getSessionVariable().preferUdfOverBuiltin;
        connectContext.getSessionVariable().preferUdfOverBuiltin = true;
        try {
            LogicalOlapScan scan = rewrittenScan("SELECT id, score() FROM score_usage_V2 WHERE c1 MATCH 'apple'");
            Assertions.assertFalse(BindScoreToOlapScan.findScoreSlot(scan).isPresent());
        } finally {
            connectContext.getSessionVariable().preferUdfOverBuiltin = preferUdf;
            executeSql("DROP FUNCTION score()");
        }
    }

    // SEARCH keeps every document when another predicate follows, so the scan only takes over the
    // minimum score, as before score() could be used without a TopN.
    @Test
    void testMinimumScoreOfSearchWithAnotherPredicate() {
        LogicalOlapScan scan = rewrittenScan("SELECT id, score() AS s FROM score_usage_V2 WHERE search('c1:apple')"
                + " AND id % 2 = 0 AND score() > 0.00001 ORDER BY s DESC LIMIT 10");
        Assertions.assertEquals(0L, scan.getScoreLimit().get().longValue());
        Assertions.assertTrue(scan.getScoreRangeInfo().isPresent());
    }

    @Test
    void testEachSelectScoresItsOwnTable() {
        String scored = "SELECT id, score() AS s FROM %s WHERE c1 MATCH '%s' ORDER BY s DESC LIMIT 2";
        Plan plan = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM (" + String.format(scored, "score_usage_V2", "apple") + ") x UNION ALL "
                        + "SELECT * FROM (" + String.format(scored, "score_usage_V3", "pear") + ") y")
                .rewrite().getCascadesContext().getRewritePlan();
        Set<Slot> scoreSlots = plan.<LogicalOlapScan>collect(LogicalOlapScan.class::isInstance).stream()
                .map(scan -> BindScoreToOlapScan.findScoreSlot(scan).get())
                .collect(Collectors.toSet());
        Assertions.assertEquals(2, scoreSlots.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT * FROM (%s) x",
            "WITH c AS (%s) SELECT * FROM c",
            "SELECT * FROM (%s) x UNION ALL SELECT id, 0 FROM score_usage_V3",
            "SELECT id FROM score_usage_V3 WHERE id IN (SELECT id FROM (%s) x)",
            "SELECT id FROM score_usage_V3 WHERE EXISTS (%s)"})
    void testNestedScoreRequiresTopN(String query) {
        String scored = "SELECT id, score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001";
        assertScoreUsageError(String.format(query, scored), BindScoreToOlapScan.NESTED_SELECT_ERROR);
        assertScoreUsageError(String.format(query, scored + " ORDER BY 1 DESC LIMIT 2"),
                BindScoreToOlapScan.NESTED_SELECT_ERROR);
        for (String topN : new String[] {" ORDER BY s DESC LIMIT 2", " ORDER BY 2 DESC LIMIT 2"}) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).checkPlannerResult(
                    String.format(query, scored + topN)), topN);
        }
    }

    @Test
    void testNestedScoreTopNAboveHaving() {
        // HAVING reads c2, which the SELECT does not return, so analysis restores the select list between
        // LIMIT and ORDER BY.
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT * FROM (SELECT id, score() AS s FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                        + " HAVING c2 = 'sweet' ORDER BY s DESC LIMIT 2) x"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT id FROM score_usage_V3 b WHERE (b.c1 MATCH 'red' OR b.c1 MATCH 'green') AND EXISTS ("
                    + "SELECT 1 FROM score_usage_V2 a WHERE a.c1 = b.c1 AND a.c1 MATCH 'apple'"
                    + " AND score() > 0.6 ORDER BY score() DESC LIMIT 1)",
            "SELECT id FROM score_usage_V3 b WHERE EXISTS (SELECT 1 FROM (SELECT c1, score() AS s"
                    + " FROM score_usage_V2 WHERE c1 MATCH 'apple' ORDER BY s DESC LIMIT 1) a WHERE a.c1 = b.c1)",
            "SELECT id, (SELECT max(x.id) FROM (SELECT id, score() AS s FROM score_usage_V2"
                    + " WHERE c1 MATCH 'apple' ORDER BY s DESC LIMIT 2) x WHERE x.id = b.id) FROM score_usage_V3 b"})
    void testCorrelatedSubqueryIsRejected(String query) {
        assertScoreUsageError(query, BindScoreToOlapScan.CORRELATED_SUBQUERY_ERROR);
    }

    // The main query of a WITH clause is the outermost SELECT.
    @Test
    void testOutermostSelectUnderWith() {
        assertFullScoring("WITH c AS (SELECT 1 AS one) SELECT id, score() AS s FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple'", 2);
    }

    @Test
    void testViewWithScoreTopN() {
        PlanChecker.from(connectContext).checkPlannerResult("SELECT * FROM score_usage_top WHERE id > 0",
                planner -> Assertions.assertEquals(2, thriftScan(planner).olap_scan_node.score_sort_limit));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT score() FROM score_usage_V2",
            "SELECT score() FROM score_usage_V2 WHERE c2 = 'sweet'",
            // score() belongs to the MATCH of its own SELECT, not to the one of a subquery or CTE.
            "SELECT score() FROM (SELECT id, c1 FROM score_usage_V2 WHERE c1 MATCH 'apple') x",
            "WITH c AS (SELECT id, c1 FROM score_usage_V2) SELECT id, score() FROM c WHERE c1 MATCH 'apple'",
            "SELECT a.id, score() FROM score_usage_V2 a JOIN score_usage_V3 b ON a.id = b.id"
                    + " WHERE a.c1 MATCH 'apple'",
            "SELECT a.id FROM score_usage_V2 a JOIN score_usage_V3 b ON a.id = b.id AND score() > 0"
                    + " WHERE a.c1 MATCH 'apple'",
            "SELECT id, score() FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                    + " AND id IN (SELECT id FROM score_usage_V3)",
            "SELECT id, e, score() FROM score_usage_V2 LATERAL VIEW explode(array(1, 2)) t AS e"
                    + " WHERE c1 MATCH 'apple'"})
    void testUnsupportedSelectIsRejected(String query) {
        assertScoreUsageError(query, BindScoreToOlapScan.UNSUPPORTED_SELECT_ERROR);
    }

    @Test
    void testHavingAndQualifyAreRejected() {
        assertScoreUsageError("SELECT c2, count(*) FROM score_usage_V2 WHERE c1 MATCH 'apple'"
                + " AND score() > 0.00001 GROUP BY c2 HAVING c2 MATCH 'sweet'",
                BindScoreToOlapScan.HAVING_OR_QUALIFY_ERROR);
        assertScoreUsageError("SELECT id, score() AS s, row_number() OVER (ORDER BY id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' QUALIFY rn = 1 AND s > 0.5",
                BindScoreToOlapScan.HAVING_OR_QUALIFY_ERROR);
        // MATCH through an alias of the select list.
        assertScoreUsageError("SELECT id, score() AS s, c1 MATCH 'red' AS m,"
                + " row_number() OVER (PARTITION BY c1 ORDER BY id) AS rn"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' QUALIFY m AND rn = 1",
                BindScoreToOlapScan.HAVING_OR_QUALIFY_ERROR);
        assertScoreUsageError("SELECT id, score() AS s, c1 MATCH 'red' AS m"
                + " FROM score_usage_V2 WHERE c1 MATCH 'apple' HAVING m",
                BindScoreToOlapScan.HAVING_OR_QUALIFY_ERROR);
        assertScoreUsageError("SELECT c1 MATCH 'red' AS m, count(*) FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() > 0.00001 GROUP BY m HAVING m",
                BindScoreToOlapScan.HAVING_OR_QUALIFY_ERROR);
    }

    @ParameterizedTest
    @ValueSource(strings = {"c1 MATCH 'apple'", "search('c1:apple')"})
    void testMinimumScoreKeepsAnnTopN(String predicate) {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id, l2_distance_approximate(embedding, [0.0, 0.0]) AS dist FROM score_usage_ann"
                        + " WHERE " + predicate + " AND score() > 0.5 ORDER BY dist LIMIT 1",
                planner -> {
                    TPlanNode scan = thriftScan(planner);
                    Assertions.assertNotNull(scan.olap_scan_node.ann_sort_info);
                    Assertions.assertEquals(1, scan.olap_scan_node.ann_sort_limit);
                    Assertions.assertEquals(0, scan.olap_scan_node.score_sort_limit);
                    assertFiltersScore(scan);
                });
    }

    @Test
    void testCseVirtualColumnsLeaveScoreToTheBinding() {
        boolean enableCse = connectContext.getSessionVariable().experimentalEnableVirtualSlotForCse;
        connectContext.getSessionVariable().experimentalEnableVirtualSlotForCse = true;
        try {
            LogicalOlapScan scan = rewrittenScan("SELECT id, sin(abs(score()) + 1) AS a, cos(abs(score()) + 1) AS b"
                    + " FROM score_usage_V2 WHERE c1 MATCH 'apple' AND score() > 0.00001");
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
    void testExplainShowsTheMinimumScore() throws Exception {
        String explain = getSQLPlanOrErrorMsg("EXPLAIN SELECT id FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() >= 0.5");
        Assertions.assertTrue(explain.contains("SCORE SORT LIMIT: 0"), explain);
        Assertions.assertFalse(explain.contains("SCORE RANGE"), explain);
        // The scan keeps the best rows of each segment only when it also applies the minimum score.
        explain = getSQLPlanOrErrorMsg("EXPLAIN SELECT id FROM score_usage_V2"
                + " WHERE c1 MATCH 'apple' AND score() >= 0.5 ORDER BY score() DESC LIMIT 10");
        Assertions.assertTrue(explain.contains("SCORE SORT LIMIT: 10"), explain);
        Assertions.assertTrue(explain.contains("SCORE RANGE: score() >= 0.5"), explain);
    }

    private LogicalOlapScan rewrittenScan(String sql) {
        return (LogicalOlapScan) PlanChecker.from(connectContext).analyze(sql).rewrite()
                .getCascadesContext().getRewritePlan()
                .collectFirst(plan -> plan instanceof LogicalOlapScan).get();
    }

    // The minimum score is a conjunct of the scan, which evaluates it like any other.
    private void assertMinimumScore(String sql, int outputColumns) {
        PlanChecker.from(connectContext).checkPlannerResult(sql,
                planner -> assertFiltersScore(assertScoresEveryRow(planner, outputColumns)));
    }

    private void assertFiltersScore(TPlanNode scan) {
        Assertions.assertFalse(scan.olap_scan_node.isSetScoreRangeInfo());
        int scoreSlotId = scan.olap_scan_node.score_sort_info.ordering_exprs.get(0).nodes.get(0).slot_ref.slot_id;
        Assertions.assertTrue(scan.conjuncts.stream().anyMatch(conjunct -> conjunct.nodes.stream()
                .anyMatch(node -> node.isSetSlotRef() && node.slot_ref.slot_id == scoreSlotId)));
    }

    private void assertFullScoring(String sql, int outputColumns) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            TPlanNode scan = assertScoresEveryRow(planner, outputColumns);
            Assertions.assertFalse(scan.olap_scan_node.isSetScoreRangeInfo(), sql);
        });
    }

    // The scan scores every row it returns, with the score column in its output tuple.
    private TPlanNode assertScoresEveryRow(NereidsPlanner planner, int outputColumns) {
        TPlanNode scan = thriftScan(planner);
        Assertions.assertNotNull(scan.olap_scan_node.score_sort_info);
        Assertions.assertEquals(0, scan.olap_scan_node.score_sort_limit);
        Assertions.assertTrue(scan.olap_scan_node.score_sort_info.ordering_exprs.get(0)
                .nodes.get(0).slot_ref.is_virtual_slot, "Scoring must reference a virtual slot");
        int scoreSlotId = scan.olap_scan_node.score_sort_info.ordering_exprs.get(0).nodes.get(0).slot_ref.slot_id;
        OlapScanNode scanNode = (OlapScanNode) planner.getScanNodes().get(0);
        Assertions.assertTrue(scanNode.getTupleDesc().getSlots().stream()
                .anyMatch(slot -> slot.getId().asInt() == scoreSlotId),
                "The score column must be in the scan tuple");
        Assertions.assertEquals(outputColumns, planner.getFragments().get(0).getOutputExprs().size());
        return scan;
    }

    private TPlanNode thriftScan(NereidsPlanner planner) {
        Assertions.assertEquals(1, planner.getScanNodes().size());
        return planner.getScanNodes().get(0).treeToThrift().getNodes().get(0);
    }

    private void assertScoreUsageError(String sql, String error) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
        Assertions.assertTrue(exception.getMessage().contains(error), exception.getMessage());
    }
}
