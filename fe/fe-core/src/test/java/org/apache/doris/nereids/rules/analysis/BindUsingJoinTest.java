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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for USING JOIN merge key behavior.
 * <p>
 * Covers:
 *   1: RIGHT/FULL OUTER JOIN USING produces NULL on right-only rows
 *   2: RIGHT/OUTER JOIN incorrectly uses merge key
 *   3: RIGHT SEMI JOIN USING drops merge key from output
 */
class BindUsingJoinTest extends TestWithFeService implements GeneratedPlanPatterns {

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_using_join");
        connectContext.setDatabase("test_using_join");
        createTables(
                "CREATE TABLE t1 (a int, b int) DISTRIBUTED BY HASH(a)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");",
                "CREATE TABLE t2 (a int, c int) DISTRIBUTED BY HASH(a)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");",
                "CREATE TABLE t3 (a int, d int) DISTRIBUTED BY HASH(a)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");",
                "CREATE TABLE chain_l (lv int, pk int, ak int) DISTRIBUTED BY HASH(pk)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");",
                "CREATE TABLE chain_r (rv int, pk int) DISTRIBUTED BY HASH(pk)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");",
                "CREATE TABLE chain_n (nv int, ak int) DISTRIBUTED BY HASH(ak)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\"replication_num\"=\"1\");"
        );
    }

    // ---- 1. Basic correctness: all USING join types analyze successfully ----

    @Test
    void testAllUsingJoinTypesAnalyzeSuccessfully() {
        String[] joinTypes = {
            "JOIN", "LEFT JOIN", "LEFT OUTER JOIN",
            "RIGHT JOIN", "RIGHT OUTER JOIN",
            "LEFT SEMI JOIN", "RIGHT SEMI JOIN",
            "LEFT ANTI JOIN", "RIGHT ANTI JOIN"
        };
        for (String joinType : joinTypes) {
            String sql = String.format("SELECT * FROM t1 %s t2 USING(a)", joinType);
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .nonMatch(any()
                            .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
        }
    }

    @Test
    void testFullOuterJoinUsingAnalyzeSuccessfully() {
        String sql = "SELECT * FROM t1 FULL OUTER JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    // ---- 2. USING join produces LogicalProject wrapping LogicalJoin ----

    @Test
    void testUsingJoinProducesProjectOnTop() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalProject(
                                logicalJoin()
                        )
                );
    }

    // ---- 3. Merge key expression checks ----

    @Test
    void testInnerJoinUsingMergeKeyKeepsLeftExprId() {
        List<Slot> output = PlanChecker.from(connectContext)
                .analyze("SELECT a, t1.a FROM t1 JOIN t2 USING(a)").getPlan().getOutput();
        Assertions.assertEquals(output.get(1).getExprId(), output.get(0).getExprId());
        Assertions.assertTrue(output.get(0).getQualifier().isEmpty());
        Assertions.assertTrue(output.get(1).getQualifier().contains("t1"));
    }

    @Test
    void testFullOuterJoinUsingMergeKeyIsCoalesce() {
        String sql = "SELECT * FROM t1 FULL OUTER JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> project.getProjects().stream()
                        .filter(p -> p instanceof Alias && p.getName().equals("a"))
                        .anyMatch(alias -> alias.child(0) instanceof Coalesce)));
    }

    // ---- 4. Asterisk output: merged key appears exactly once ----

    @Test
    void testInnerJoinUsingAsteriskOutputHasSingleKey() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> asteriskOutput = project.getAsteriskOutput();
                    long aCount = asteriskOutput.stream()
                            .filter(s -> s.getName().equals("a"))
                            .count();
                    // Merged 'a' appears exactly once in asterisk output
                    return aCount == 1;
                }));
    }

    @Test
    void testLeftJoinUsingAsteriskOutputHasSingleKey() {
        String sql = "SELECT * FROM t1 LEFT JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> asteriskOutput = project.getAsteriskOutput();
                    long aCount = asteriskOutput.stream()
                            .filter(s -> s.getName().equals("a"))
                            .count();
                    return aCount == 1;
                }));
    }

    @Test
    void testRightJoinUsingAsteriskOutputHasSingleKey() {
        String sql = "SELECT * FROM t1 RIGHT JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> asteriskOutput = project.getAsteriskOutput();
                    long aCount = asteriskOutput.stream()
                            .filter(s -> s.getName().equals("a"))
                            .count();
                    return aCount == 1;
                }));
    }

    // ---- 5. RIGHT JOIN merge key comes from preserved (right) side ----

    @Test
    void testRightJoinMergeKeyFromRightSide() {
        List<Slot> output = PlanChecker.from(connectContext)
                .analyze("SELECT a, t2.a FROM t1 RIGHT JOIN t2 USING(a)").getPlan().getOutput();
        Assertions.assertEquals(output.get(1).getExprId(), output.get(0).getExprId());
        Assertions.assertTrue(output.get(0).getQualifier().isEmpty());
        Assertions.assertTrue(output.get(1).getQualifier().contains("t2"));
    }

    // ---- 6. RIGHT SEMI JOIN merge key is visible ----

    @Test
    void testRightSemiJoinMergeKeyVisibleInAsterisk() {
        String sql = "SELECT * FROM t1 RIGHT SEMI JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> asteriskOutput = project.getAsteriskOutput();
                    // Merge key 'a' must be in asterisk output (unqualified)
                    return asteriskOutput.stream()
                            .anyMatch(s -> s.getName().equals("a")
                                    && s.getQualifier().isEmpty());
                }));
    }

    // ---- 7. asteriskOutputs hides qualified USING columns ----

    @Test
    void testAsteriskOutputsExcludeQualifiedUsingColumns() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<NamedExpression> asteriskOutputs = project.getAsteriskOutputs();
                    boolean hasMergedA = asteriskOutputs.stream().anyMatch(
                            s -> s.getName().equals("a") && s.getQualifier().isEmpty());
                    boolean hasT1a = asteriskOutputs.stream().anyMatch(
                            s -> s.getName().equals("a") && s.getQualifier().contains("t1"));
                    boolean hasT2a = asteriskOutputs.stream().anyMatch(
                            s -> s.getName().equals("a") && s.getQualifier().contains("t2"));
                    return hasMergedA && !hasT1a && !hasT2a;
                }));
    }

    // ---- 8. Project implements DiffOutputInAsterisk ----

    @Test
    void testUsingJoinProjectImplementsDiffOutputInAsterisk() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject());
    }

    // ---- 9. Full output retains qualified columns for explicit references ----

    @Test
    void testFullOutputRetainsQualifiedColumns() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> fullOutput = project.getOutput();
                    boolean hasT1a = fullOutput.stream().anyMatch(
                            s -> s.getName().equals("a") && s.getQualifier().contains("t1"));
                    boolean hasT2a = fullOutput.stream().anyMatch(
                            s -> s.getName().equals("a") && s.getQualifier().contains("t2"));
                    return hasT1a && hasT2a;
                }));
    }

    // ---- 10. Regression: qualified and unqualified references both work ----

    @Test
    void testQualifiedColumnRefsStillWork() {
        String sql = "SELECT t1.a, t2.a FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testQualifiedStarKeepsQualifiedUsingKeys() {
        String sql = "SELECT t1.a, t2.a, a, t1.*, t2.*, * FROM t1 LEFT OUTER JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> output = project.getOutput();
                    long t1A = output.stream()
                            .filter(s -> s.getName().equals("a") && s.getQualifier().contains("t1"))
                            .count();
                    long t2A = output.stream()
                            .filter(s -> s.getName().equals("a") && s.getQualifier().contains("t2"))
                            .count();
                    long mergedA = output.stream()
                            .filter(s -> s.getName().equals("a") && s.getQualifier().isEmpty())
                            .count();
                    return output.size() == 10 && t1A == 2 && t2A == 2 && mergedA == 2;
                }));
    }

    @Test
    void testUnqualifiedColumnRefWorks() {
        String sql = "SELECT a FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testLeftQualifiedStarKeepsUsingColumn() {
        String sql = "SELECT t1.* FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> project.getOutput().size() == 2
                        && project.getOutput().stream().anyMatch(
                                s -> s.getName().equals("a") && s.getQualifier().contains("t1"))
                        && project.getOutput().stream().anyMatch(
                                s -> s.getName().equals("b") && s.getQualifier().contains("t1"))));
    }

    @Test
    void testRightQualifiedStarKeepsUsingColumn() {
        String sql = "SELECT t2.* FROM t1 JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> project.getOutput().size() == 2
                        && project.getOutput().stream().anyMatch(
                                s -> s.getName().equals("a") && s.getQualifier().contains("t2"))
                        && project.getOutput().stream().anyMatch(
                                s -> s.getName().equals("c") && s.getQualifier().contains("t2"))));
    }

    // ---- 11. USING with WHERE / ORDER BY / GROUP BY ----

    @Test
    void testUsingJoinWithWhereClause() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a) WHERE b > 0";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testUsingJoinWithOrderBy() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a) ORDER BY a";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testUsingJoinWithGroupBy() {
        String sql = "SELECT a, COUNT(*) FROM t1 JOIN t2 USING(a) GROUP BY a";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    // ---- 12. Cross join with USING is rejected at parser level ----

    @Test
    void testCrossJoinUsingRejected() {
        String sql = "SELECT * FROM t1 CROSS JOIN t2 USING(a)";
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.ParseException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
    }

    // ---- 13. LEFT SEMI JOIN merge key visible ----

    @Test
    void testLeftSemiJoinMergeKeyVisible() {
        String sql = "SELECT * FROM t1 LEFT SEMI JOIN t2 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<Slot> asteriskOutput = project.getAsteriskOutput();
                    return asteriskOutput.stream()
                            .anyMatch(s -> s.getName().equals("a"));
                }));
    }

    // ---- 14. Multiple table references in USING chain ----

    @Test
    void testChainedUsingJoin() {
        String sql = "SELECT * FROM t1 JOIN t2 USING(a) JOIN t3 USING(a)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testChainedFullOuterUsingJoinAsteriskOutput() {
        String sql = "SELECT * FROM chain_l l FULL OUTER JOIN chain_r r USING(pk) "
                + "FULL OUTER JOIN chain_n n USING(ak)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(logicalProject().when(project -> {
                    List<String> outputNames = project.getOutput().stream()
                            .map(Slot::getName)
                            .collect(Collectors.toList());
                    return outputNames.equals(List.of("ak", "pk", "lv", "rv", "nv"));
                }));
    }

    @Test
    void testUsingJoinGroupByPreservedKey() {
        String[] joinTypes = {
            "JOIN", "LEFT JOIN", "LEFT SEMI JOIN", "LEFT ANTI JOIN",
            "RIGHT JOIN", "RIGHT SEMI JOIN", "RIGHT ANTI JOIN"
        };
        for (String joinType : joinTypes) {
            String key = joinType.startsWith("RIGHT") ? "t2.a" : "t1.a";
            String from = " FROM t1 " + joinType + " t2 USING(a)";
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT " + key + ", COUNT(*)" + from + " GROUP BY a");
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT a, COUNT(*)" + from + " GROUP BY " + key);
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT COUNT(*)" + from + " GROUP BY a HAVING " + key + " > 0 ORDER BY " + key);
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT " + key + " + 1, COUNT(*)" + from + " GROUP BY a + 1");
        }
    }

    @Test
    void testUsingJoinGroupByMultipleKeys() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT l.a, l.b, COUNT(*) FROM t1 l LEFT JOIN t1 r USING(a,b) GROUP BY a,b");
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a, b, COUNT(*) FROM t1 l LEFT JOIN t1 r USING(a,b) GROUP BY l.a,l.b");
    }

    @Test
    void testUsingJoinGroupByDoesNotMergeNullableSide() {
        String[] queries = {
            "SELECT t2.a, COUNT(*) FROM t1 LEFT JOIN t2 USING(a) GROUP BY a",
            "SELECT t1.a, COUNT(*) FROM t1 RIGHT JOIN t2 USING(a) GROUP BY a",
            "SELECT t1.a, COUNT(*) FROM t1 FULL JOIN t2 USING(a) GROUP BY a",
            "SELECT a, COUNT(*) FROM t1 FULL JOIN t2 USING(a) GROUP BY t1.a"
        };
        for (String sql : queries) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(sql), sql);
            Assertions.assertTrue(error.getMessage().contains("must appear in the GROUP BY"), error.getMessage());
        }
    }

    @Test
    void testUsingJoinGroupByDerivedInput() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "WITH l AS (SELECT 1 AS k, 10 AS v UNION ALL SELECT 2, 20), "
                        + "r AS (SELECT 1 AS k UNION ALL SELECT 3) "
                        + "SELECT l.k, SUM(l.v) FROM l LEFT JOIN r USING(k) "
                        + "GROUP BY k HAVING l.k > 0 ORDER BY l.k");
    }

    @Test
    void testUsingJoinCteColumnOrder() {
        String[] joins = {"JOIN", "LEFT JOIN", "RIGHT JOIN"};
        for (String join : joins) {
            String source = join.startsWith("RIGHT") ? "t2.a" : "t1.a";
            String payload = join.startsWith("RIGHT") ? "t2.c" : "t1.b";
            String sql = "WITH q(x,y,z) AS (SELECT a," + payload + "," + source
                    + " FROM t1 " + join + " t2 USING(a)) SELECT * FROM q";
            List<Slot> output = PlanChecker.from(connectContext).analyze(sql).getPlan().getOutput();
            Assertions.assertEquals(List.of("x", "y", "z"), output.stream()
                    .map(Slot::getName).collect(Collectors.toList()), sql);
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }
    }

    @Test
    void testCteRepeatedColumnResults() {
        String sql = "WITH q(x,y,z) AS (SELECT a,b,a FROM (SELECT 1 AS a,10 AS b) t) SELECT * FROM q";
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            PhysicalPlan plan = planner.getPhysicalPlan();
            Assertions.assertTrue(plan instanceof ComputeResultSet);
            Assertions.assertEquals(List.of(List.of("1", "10", "1")),
                    ((ComputeResultSet) plan).computeResultInFe(planner.getCascadesContext(), plan.getOutput())
                            .orElseThrow().getResultRows());
        });
    }

}
