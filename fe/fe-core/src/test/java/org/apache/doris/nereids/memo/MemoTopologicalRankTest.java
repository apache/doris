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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.util.MemoValidator;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

class MemoTopologicalRankTest {
    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @Test
    void testEqualRanksMergeWithoutSearch() {
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan));
        Group source = memo.getRoot();
        Group leaf = source.getLogicalExpression().child(0);
        Group destination = memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE),
                new GroupPlan(leaf)), null, false).correspondingExpression.getOwnerGroup();
        Assertions.assertEquals(source.getTopologicalRank(), destination.getTopologicalRank());

        memo.mergeGroup(source, destination, null);

        Assertions.assertSame(destination, memo.getRoot());
        Assertions.assertEquals(0, memo.searches);
        assertRanks(memo);
    }

    @Test
    void testDifferentRanksStillMergeIndependentGroups() {
        LogicalProject<?> middle = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add(new Alias(new TinyIntLiteral((byte) 1))).addAll(scan.getOutput()).build(), scan);
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), middle));
        Group source = memo.getRoot();
        Group leaf = source.getLogicalExpression().child(0).getLogicalExpression().child(0);
        Group destination = memo.copyIn(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), new GroupPlan(leaf)),
                null, false).correspondingExpression.getOwnerGroup();

        memo.mergeGroup(source, destination, null);

        Assertions.assertSame(destination, memo.getRoot());
        Assertions.assertEquals(1, memo.searches);
        assertRanks(memo);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndirectDependencySearchesOnlyHigherRank(boolean reverse) {
        LogicalProject<?> bottom = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        LogicalProject<?> middle = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), bottom);
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), middle));
        Group root = memo.getRoot();
        Group descendant = root.getLogicalExpression().child(0).getLogicalExpression().child(0);

        memo.mergeGroup(reverse ? descendant : root, reverse ? root : descendant, null);

        Assertions.assertSame(root, memo.getRoot());
        Assertions.assertEquals(4, memo.getGroups().size());
        Assertions.assertEquals(1, memo.searches);
        assertRanks(memo);
    }

    @Test
    void testAddingAlternativeRaisesAncestorsAndIgnoresEnforcer() {
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()),
                new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan)));
        Group root = memo.getRoot();
        Group child = root.getLogicalExpression().child(0);
        Group leaf = child.getLogicalExpression().child(0);
        root.addEnforcer(new GroupExpression(new PhysicalDistribute<>(DistributionSpecGather.INSTANCE,
                new GroupPlan(root)), Lists.newArrayList(root)));
        Group deep = leaf;
        for (int i = 0; i < 5; i++) {
            deep = memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), new GroupPlan(deep)),
                    null, false).correspondingExpression.getOwnerGroup();
        }

        memo.copyIn(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), new GroupPlan(deep)), child, false);

        Assertions.assertTrue(child.getTopologicalRank() > deep.getTopologicalRank());
        Assertions.assertTrue(root.getTopologicalRank() > child.getTopologicalRank());
        assertRanks(memo);
    }

    @Test
    void testReplacingChildRaisesAncestors() {
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()),
                new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan)));
        Group root = memo.getRoot();
        GroupExpression childExpression = root.getLogicalExpression().child(0).getLogicalExpression();
        Group leaf = childExpression.child(0);
        Group deep = memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), new GroupPlan(leaf)),
                null, false).correspondingExpression.getOwnerGroup();
        memo.getGroupExpressions().remove(childExpression);
        childExpression.setChild(0, deep);
        memo.getGroupExpressions().put(childExpression, childExpression);

        Assertions.assertTrue(childExpression.getOwnerGroup().getTopologicalRank() > deep.getTopologicalRank());
        Assertions.assertTrue(root.getTopologicalRank() > childExpression.getOwnerGroup().getTopologicalRank());
        assertRanks(memo);
    }

    @Test
    void testRewriteInvalidatesRanksAndUsesConservativeSearch() {
        CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan));
        Group root = memo.getRoot();
        Group leaf = root.getLogicalExpression().child(0);
        memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), new GroupPlan(leaf)), root, true);
        Group destination = memo.copyIn(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), new GroupPlan(leaf)),
                null, false).correspondingExpression.getOwnerGroup();
        Assertions.assertTrue(memo.getGroups().stream().allMatch(group -> group.getTopologicalRank() < 0));

        memo.mergeGroup(root, destination, null);

        Assertions.assertSame(destination, memo.getRoot());
        Assertions.assertEquals(2, memo.searches);
        MemoValidator.validate(memo);
    }

    @Test
    void testRankedMergesMatchUnprunedDependencySearch() {
        for (int seed = 0; seed < 10; seed++) {
            Random random = new Random(seed);
            CountingMemo memo = new CountingMemo(new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan));
            for (int step = 0; step < 150; step++) {
                List<Group> groups = new ArrayList<>(memo.getGroups());
                Group child = groups.get(random.nextInt(groups.size()));
                if (step % 3 == 0) {
                    Plan alternative = random.nextBoolean()
                            ? new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), new GroupPlan(child))
                            : new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), new GroupPlan(child));
                    memo.copyIn(alternative, null, false);
                } else {
                    Group source = groups.get(random.nextInt(groups.size()));
                    boolean dependent = reachesWithoutRanks(source, child) || reachesWithoutRanks(child, source);
                    int groupCount = groups.size();
                    memo.mergeGroup(source, child, null);
                    if (dependent) {
                        Assertions.assertEquals(groupCount, memo.getGroups().size());
                    } else {
                        Assertions.assertNull(memo.getGroup(source.getGroupId()));
                    }
                }
                assertRanks(memo);
            }
        }
    }

    private static boolean reachesWithoutRanks(Group from, Group target) {
        Set<Group> visited = new HashSet<>();
        ArrayDeque<Group> pending = new ArrayDeque<>();
        pending.add(from);
        while (!pending.isEmpty()) {
            Group group = pending.removeFirst();
            if (group == target) {
                return true;
            }
            if (visited.add(group)) {
                group.getLogicalExpressions().forEach(expression -> pending.addAll(expression.children()));
                group.getPhysicalExpressions().forEach(expression -> pending.addAll(expression.children()));
            }
        }
        return false;
    }

    private static void assertRanks(Memo memo) {
        MemoValidator.validate(memo);
        for (Group group : memo.getGroups()) {
            for (GroupExpression expression : ImmutableList.<GroupExpression>builder()
                    .addAll(group.getLogicalExpressions()).addAll(group.getPhysicalExpressions()).build()) {
                for (Group child : expression.children()) {
                    Assertions.assertTrue(group.getTopologicalRank() > child.getTopologicalRank());
                }
            }
        }
    }

    private static class CountingMemo extends Memo {
        private int searches;

        CountingMemo(Plan plan) {
            super(null, plan, null);
        }

        @Override
        boolean isReachable(Group from, Group target) {
            searches++;
            Assertions.assertTrue(from.getTopologicalRank() < 0 || target.getTopologicalRank() < 0
                    || from.getTopologicalRank() > target.getTopologicalRank());
            return super.isReachable(from, target);
        }
    }
}
