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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class GroupExpressionMatchingTest {

    @Test
    public void testLeafNode() {
        Pattern pattern = new Pattern<>(PlanType.LOGICAL_UNBOUND_RELATION);

        Memo memo = new Memo();
        memo.initialize(new UnboundRelation(Lists.newArrayList("test")));

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION, actual.getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testDepth2() {
        Pattern pattern = new Pattern<>(PlanType.LOGICAL_PROJECT,
                new Pattern<>(PlanType.LOGICAL_UNBOUND_RELATION));

        Plan leaf = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject root = new LogicalProject(Lists.newArrayList(), leaf);
        Memo memo = new Memo();
        memo.initialize(root);

        Plan anotherLeaf = new UnboundRelation(Lists.newArrayList("test2"));
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual;
        actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, actual.getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION, actual.child(0).getType());
        Assertions.assertTrue(iterator.hasNext());
        actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, actual.getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION, actual.child(0).getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testDepth2WithGroup() {
        Pattern pattern = new Pattern<>(PlanType.LOGICAL_PROJECT, Pattern.GROUP);

        Plan leaf = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject root = new LogicalProject(Lists.newArrayList(), leaf);
        Memo memo = new Memo();
        memo.initialize(root);

        Plan anotherLeaf = new UnboundRelation(Lists.newArrayList("test2"));
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual;
        actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, actual.getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(PlanType.GROUP_PLAN, actual.child(0).getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testLeafAny() {
        Pattern pattern = Pattern.ANY;

        Memo memo = new Memo();
        memo.initialize(new UnboundRelation(Lists.newArrayList("test")));

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION, actual.getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testAnyWithChild() {
        Plan root = new LogicalProject(Lists.newArrayList(),
                new UnboundRelation(Lists.newArrayList("test")));
        Memo memo = new Memo();
        memo.initialize(root);

        Plan anotherLeaf = new UnboundRelation(ImmutableList.of("test2"));
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(Pattern.ANY, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, actual.getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(PlanType.GROUP_PLAN, actual.child(0).getType());

        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testInnerLogicalJoinMatch() {
        Plan root = new LogicalJoin(JoinType.INNER_JOIN,
                new UnboundRelation(ImmutableList.of("a")),
                new UnboundRelation(ImmutableList.of("b"))
        );

        Memo memo = new Memo();
        memo.initialize(root);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(patterns().innerLogicalJoin().pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(PlanType.LOGICAL_JOIN, actual.getType());
        Assertions.assertEquals(2, actual.arity());
        Assertions.assertEquals(PlanType.GROUP_PLAN, actual.child(0).getType());
        Assertions.assertEquals(PlanType.GROUP_PLAN, actual.child(1).getType());
    }

    @Test
    public void testInnerLogicalJoinMismatch() {
        Plan root = new LogicalJoin(JoinType.LEFT_OUTER_JOIN,
                new UnboundRelation(ImmutableList.of("a")),
                new UnboundRelation(ImmutableList.of("b"))
        );

        Memo memo = new Memo();
        memo.initialize(root);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(patterns().innerLogicalJoin().pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopMatchButChildrenNotMatch() {
        Plan root = new LogicalJoin(JoinType.LEFT_OUTER_JOIN,
                new UnboundRelation(ImmutableList.of("a")),
                new UnboundRelation(ImmutableList.of("b"))
        );


        Memo memo = new Memo();
        memo.initialize(root);

        Pattern pattern = patterns()
                .innerLogicalJoin(patterns().logicalFilter(), patterns().any()).pattern;
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertFalse(iterator.hasNext());
    }

    private org.apache.doris.nereids.pattern.GeneratedPatterns patterns() {
        return () -> RulePromise.REWRITE;
    }
}
