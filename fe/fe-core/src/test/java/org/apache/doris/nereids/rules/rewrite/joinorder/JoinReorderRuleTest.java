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

package org.apache.doris.nereids.rules.rewrite.joinorder;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class JoinReorderRuleTest {

    @Test
    void testReorderClusterAndRestoreOriginalOutput() {
        LogicalOlapScan a = scan(101, "a", 1_000_000, 1_000_000);
        LogicalOlapScan b = scan(102, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(103, "c", 100, 100);
        Plan original = innerJoin(
                innerJoin(a, b, equal(a, b)),
                c,
                equal(b, c));

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertEquals(original.getOutput(), rewritten.getOutput());
        Assertions.assertTrue(hasJoinWithExactTables(rewritten, "b", "c"), rewritten.treeString());
        Assertions.assertFalse(hasJoinWithExactTables(rewritten, "a", "b"), rewritten.treeString());
    }

    @Test
    void testTransparentProjectDoesNotSplitCluster() {
        LogicalOlapScan a = scan(111, "a", 1_000_000, 1_000_000);
        LogicalOlapScan b = scan(112, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(113, "c", 100, 100);
        Plan ab = innerJoin(a, b, equal(a, b));
        List<NamedExpression> projects = ab.getOutput().stream()
                .map(slot -> (NamedExpression) slot)
                .collect(Collectors.toList());
        LogicalProject<Plan> project = new LogicalProject<>(projects, ab);
        Plan original = innerJoin(project, c, equal(b, c));

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertEquals(original.getOutput(), rewritten.getOutput());
        Assertions.assertTrue(hasJoinWithExactTables(rewritten, "b", "c"), rewritten.treeString());
    }

    @Test
    void testNonTransparentProjectSplitsCluster() {
        LogicalOlapScan a = scan(114, "a", 1_000_000, 1_000_000);
        LogicalOlapScan b = scan(115, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(116, "c", 100, 100);
        Plan ab = innerJoin(a, b, equal(a, b));
        List<NamedExpression> projects = new ArrayList<>(ab.getOutput());
        projects.set(1, new Alias(projects.get(1), "a_name"));
        LogicalProject<Plan> project = new LogicalProject<>(projects, ab);
        Plan original = innerJoin(project, c, equal(b, c));

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertEquals(original.getOutput(), rewritten.getOutput());
        Assertions.assertFalse(hasJoinWithExactTables(rewritten, "b", "c"), rewritten.treeString());
    }

    @Test
    void testReorderIndependentClusterBelowBoundary() {
        LogicalOlapScan a = scan(121, "a", 1_000_000, 1_000_000);
        LogicalOlapScan b = scan(122, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(123, "c", 100, 100);
        LogicalOlapScan d = scan(124, "d", 10, 10);
        Plan innerCluster = innerJoin(
                innerJoin(a, b, equal(a, b)),
                c,
                equal(b, c));
        LogicalJoin<Plan, Plan> original = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(equal(a, d)), innerCluster, d, null);

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertInstanceOf(LogicalJoin.class, rewritten);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, ((LogicalJoin<?, ?>) rewritten).getJoinType());
        Assertions.assertTrue(hasJoinWithExactTables(rewritten.child(0), "b", "c"), rewritten.treeString());
    }

    @Test
    void testFallbackWhenClusterExceedsAtomLimit() {
        Plan original = chain(JoinReorderRule.MAX_ATOM_NUM_FOR_GREEDY + 1);

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertSame(original, rewritten);
    }

    @Test
    void testReorderAtAtomLimit() {
        Plan original = chain(JoinReorderRule.MAX_ATOM_NUM_FOR_GREEDY);

        Plan rewritten = JoinReorderRule.INSTANCE.rewrite(original, null);

        Assertions.assertNotSame(original, rewritten);
        Assertions.assertEquals(original.getOutput(), rewritten.getOutput());
    }

    private static Plan chain(int atomCount) {
        List<LogicalOlapScan> scans = new ArrayList<>();
        for (int i = 0; i < atomCount; i++) {
            scans.add(scan(1_000 + i, "t" + i, atomCount - i, atomCount - i));
        }
        Plan result = scans.get(0);
        for (int i = 1; i < scans.size(); i++) {
            result = innerJoin(result, scans.get(i), equal(scans.get(i - 1), scans.get(i)));
        }
        return result;
    }

    private static LogicalJoin<Plan, Plan> innerJoin(Plan left, Plan right, Expression predicate) {
        return new LogicalJoin<>(JoinType.INNER_JOIN, ImmutableList.of(predicate), left, right, null);
    }

    private static Expression equal(LogicalOlapScan left, LogicalOlapScan right) {
        return new EqualTo(left.getOutput().get(0), right.getOutput().get(0));
    }

    private static LogicalOlapScan scan(long tableId, String tableName, double rowCount, double ndv) {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(tableId, tableName, 0);
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(rowCount)
                .setNdv(ndv)
                .setNumNulls(0)
                .setAvgSizeByte(4)
                .setMinValue(1)
                .setMaxValue(ndv)
                .build();
        scan.setStatistics(new Statistics(rowCount,
                ImmutableMap.of(scan.getOutput().get(0), columnStatistic)));
        return scan;
    }

    private static boolean hasJoinWithExactTables(Plan plan, String... expectedTables) {
        List<String> expected = new ArrayList<>(ImmutableList.copyOf(expectedTables));
        expected.sort(String::compareTo);
        if (plan instanceof LogicalJoin && collectTableNames(plan).equals(expected)) {
            return true;
        }
        return plan.children().stream().anyMatch(child -> hasJoinWithExactTables(child, expectedTables));
    }

    private static List<String> collectTableNames(Plan plan) {
        return plan.<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance).stream()
                .map(scan -> scan.getTable().getName())
                .sorted()
                .collect(Collectors.toList());
    }
}
