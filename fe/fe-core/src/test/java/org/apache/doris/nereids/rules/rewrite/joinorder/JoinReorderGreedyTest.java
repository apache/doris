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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class JoinReorderGreedyTest {

    @Test
    void testChooseLowestCostJoinOrder() {
        LogicalOlapScan a = scan(1, "a", 1_000_000, 1_000_000);
        LogicalOlapScan b = scan(2, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(3, "c", 100, 100);
        Expression ab = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        Expression bc = new EqualTo(b.getOutput().get(0), c.getOutput().get(0));

        JoinReorderGreedy greedy = new JoinReorderGreedy();
        Assertions.assertTrue(greedy.reorder(ImmutableList.of(a, b, c), ImmutableList.of(ab, bc)));

        Plan result = greedy.getResult().get(0);
        Assertions.assertTrue(hasJoinWithExactTables(result, "b", "c"), result.treeString());
        Assertions.assertFalse(hasJoinWithExactTables(result, "a", "b"), result.treeString());
    }

    @Test
    void testEqualCostProducesDeterministicJoinOrder() {
        LogicalOlapScan a = scan(11, "a", 100, 100);
        LogicalOlapScan b = scan(12, "b", 100, 100);
        LogicalOlapScan c = scan(13, "c", 100, 100);
        LogicalOlapScan d = scan(14, "d", 100, 100);
        List<Expression> predicates = ImmutableList.of(
                new EqualTo(a.getOutput().get(0), b.getOutput().get(0)),
                new EqualTo(b.getOutput().get(0), c.getOutput().get(0)),
                new EqualTo(c.getOutput().get(0), d.getOutput().get(0)));

        String expectedSignature = null;
        for (int i = 0; i < 20; i++) {
            JoinReorderGreedy greedy = new JoinReorderGreedy();
            Assertions.assertTrue(greedy.reorder(ImmutableList.of(a, b, c, d), predicates));
            String signature = planSignature(greedy.getResult().get(0));
            if (expectedSignature == null) {
                expectedSignature = signature;
            } else {
                Assertions.assertEquals(expectedSignature, signature);
            }
        }
        Assertions.assertEquals("(((a,b),c),d)", expectedSignature);
    }

    @Test
    void testPreserveAllPredicatesExactlyOnce() {
        LogicalOlapScan a = scan(21, "a", 10_000, 10_000);
        LogicalOlapScan b = scan(22, "b", 1_000, 1_000);
        LogicalOlapScan c = scan(23, "c", 100, 100);
        Expression ab = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        Expression bc = new EqualTo(b.getOutput().get(0), c.getOutput().get(0));
        Expression abc = new EqualTo(
                new Add(a.getOutput().get(0), b.getOutput().get(0)),
                c.getOutput().get(1));
        List<Expression> predicates = ImmutableList.of(ab, bc, abc);

        JoinReorderGreedy greedy = new JoinReorderGreedy();
        Assertions.assertTrue(greedy.reorder(ImmutableList.of(a, b, c), predicates));

        Map<Expression, Long> expected = toMultiset(predicates);
        Map<Expression, Long> actual = toMultiset(collectJoinPredicates(greedy.getResult().get(0)));
        Assertions.assertEquals(expected, actual, greedy.getResult().get(0).treeString());
    }

    @Test
    void testClassifyHashAndOtherPredicatesAfterChildrenReversed() {
        LogicalOlapScan a = scan(24, "a", 10, 10);
        LogicalOlapScan b = scan(25, "b", 1_000, 1_000);
        Expression equal = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        Expression greaterThan = new GreaterThan(a.getOutput().get(0), b.getOutput().get(0));

        JoinReorderGreedy greedy = new JoinReorderGreedy();
        Assertions.assertTrue(greedy.reorder(
                ImmutableList.of(a, b), ImmutableList.of(equal, greaterThan)));

        LogicalJoin<?, ?> result = (LogicalJoin<?, ?>) greedy.getResult().get(0);
        Assertions.assertEquals(ImmutableList.of(equal), result.getHashJoinConjuncts());
        Assertions.assertEquals(ImmutableList.of(greaterThan), result.getOtherJoinConjuncts());
        Assertions.assertEquals("b", ((LogicalOlapScan) result.left()).getTable().getName());
        Assertions.assertEquals("a", ((LogicalOlapScan) result.right()).getTable().getName());
    }

    @Test
    void testRejectPredicateWithUnknownInputSlot() {
        LogicalOlapScan a = scan(31, "a", 100, 100);
        LogicalOlapScan b = scan(32, "b", 100, 100);
        SlotReference unknown = new SlotReference("unknown", IntegerType.INSTANCE);
        Expression predicate = new EqualTo(a.getOutput().get(0), unknown);

        JoinReorderGreedy greedy = new JoinReorderGreedy();
        Assertions.assertFalse(greedy.reorder(ImmutableList.of(a, b), ImmutableList.of(predicate)));
    }

    @Test
    void testRejectAtomLocalPredicate() {
        LogicalOlapScan a = scan(41, "a", 100, 100);
        LogicalOlapScan b = scan(42, "b", 100, 100);
        Expression predicate = new GreaterThan(a.getOutput().get(0), new IntegerLiteral(1));

        JoinReorderGreedy greedy = new JoinReorderGreedy();
        Assertions.assertFalse(greedy.reorder(ImmutableList.of(a, b), ImmutableList.of(predicate)));
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
        if (plan instanceof LogicalJoin) {
            List<String> actual = collectTableNames(plan);
            if (actual.equals(expected)) {
                return true;
            }
        }
        return plan.children().stream().anyMatch(child -> hasJoinWithExactTables(child, expectedTables));
    }

    private static List<String> collectTableNames(Plan plan) {
        List<String> names = plan.<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance).stream()
                .map(scan -> scan.getTable().getName())
                .sorted()
                .collect(Collectors.toList());
        return names;
    }

    private static String planSignature(Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return ((LogicalOlapScan) plan).getTable().getName();
        }
        if (plan instanceof LogicalJoin) {
            return "(" + planSignature(plan.child(0)) + "," + planSignature(plan.child(1)) + ")";
        }
        Assertions.fail("Unexpected plan in join signature: " + plan.treeString());
        return "";
    }

    private static List<Expression> collectJoinPredicates(Plan plan) {
        List<Expression> predicates = new ArrayList<>();
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            predicates.addAll(join.getHashJoinConjuncts());
            predicates.addAll(join.getOtherJoinConjuncts());
        }
        for (Plan child : plan.children()) {
            predicates.addAll(collectJoinPredicates(child));
        }
        return predicates;
    }

    private static Map<Expression, Long> toMultiset(List<Expression> expressions) {
        return expressions.stream().collect(Collectors.groupingBy(
                Function.identity(), HashMap::new, Collectors.counting()));
    }
}
