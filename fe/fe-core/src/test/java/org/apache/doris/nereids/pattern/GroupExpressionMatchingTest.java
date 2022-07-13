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
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class GroupExpressionMatchingTest implements Plans {

    @Test
    public void testLeafNode() {
        Pattern pattern = new Pattern<>(OperatorType.LOGICAL_UNBOUND_RELATION);

        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        Plan plan = plan(unboundRelation);
        Memo memo = new Memo();
        memo.initialize(plan);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_UNBOUND_RELATION, actual.getOperator().getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testDepth2() {
        Pattern pattern = new Pattern<>(OperatorType.LOGICAL_PROJECT,
                new Pattern<>(OperatorType.LOGICAL_UNBOUND_RELATION));

        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        Plan leaf = plan(unboundRelation);
        LogicalProject project = new LogicalProject(Lists.newArrayList());
        Plan root = plan(project, leaf);
        Memo memo = new Memo();
        memo.initialize(root);

        UnboundRelation anotherUnboundRelation = new UnboundRelation(Lists.newArrayList("test2"));
        Plan anotherLeaf = plan(anotherUnboundRelation);
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual;
        actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, actual.getOperator().getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(OperatorType.LOGICAL_UNBOUND_RELATION, actual.child(0).getOperator().getType());
        Assertions.assertTrue(iterator.hasNext());
        actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, actual.getOperator().getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(OperatorType.LOGICAL_UNBOUND_RELATION, actual.child(0).getOperator().getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testDepth2WithGroup() {
        Pattern pattern = new Pattern<>(OperatorType.LOGICAL_PROJECT, Pattern.GROUP);

        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        Plan leaf = plan(unboundRelation);
        LogicalProject project = new LogicalProject(Lists.newArrayList());
        Plan root = plan(project, leaf);
        Memo memo = new Memo();
        memo.initialize(root);

        UnboundRelation anotherUnboundRelation = new UnboundRelation(Lists.newArrayList("test2"));
        Plan anotherLeaf = plan(anotherUnboundRelation);
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual;
        actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, actual.getOperator().getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(OperatorType.GROUP_PLAN, actual.child(0).getOperator().getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testLeafAny() {
        Pattern pattern = Pattern.ANY;

        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        Plan plan = plan(unboundRelation);
        Memo memo = new Memo();
        memo.initialize(plan);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_UNBOUND_RELATION, actual.getOperator().getType());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testAnyWithChild() {
        Plan root = plan(
                new LogicalProject(Lists.newArrayList()),
                plan(new UnboundRelation(Lists.newArrayList("test")))
        );
        Memo memo = new Memo();
        memo.initialize(root);

        Plan anotherLeaf = plan(new UnboundRelation(ImmutableList.of("test2")));
        memo.copyIn(anotherLeaf, memo.getRoot().getLogicalExpression().child(0), false);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(Pattern.ANY, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, actual.getOperator().getType());
        Assertions.assertEquals(1, actual.arity());
        Assertions.assertEquals(OperatorType.GROUP_PLAN, actual.child(0).getOperator().getType());

        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testInnerLogicalJoinMatch() {
        Plan root = plan(new LogicalJoin(JoinType.INNER_JOIN),
                plan(new UnboundRelation(ImmutableList.of("a"))),
                plan(new UnboundRelation(ImmutableList.of("b")))
        );

        Memo memo = new Memo();
        memo.initialize(root);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(patterns().innerLogicalJoin().pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertTrue(iterator.hasNext());
        Plan actual = iterator.next();
        Assertions.assertEquals(OperatorType.LOGICAL_JOIN, actual.getOperator().getType());
        Assertions.assertEquals(2, actual.arity());
        Assertions.assertEquals(OperatorType.GROUP_PLAN, actual.child(0).getOperator().getType());
        Assertions.assertEquals(OperatorType.GROUP_PLAN, actual.child(1).getOperator().getType());
    }

    @Test
    public void testInnerLogicalJoinMismatch() {
        Plan root = plan(new LogicalJoin(JoinType.LEFT_OUTER_JOIN),
                plan(new UnboundRelation(ImmutableList.of("a"))),
                plan(new UnboundRelation(ImmutableList.of("b")))
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
        Plan root = plan(new LogicalJoin(JoinType.LEFT_OUTER_JOIN),
                plan(new UnboundRelation(ImmutableList.of("a"))),
                plan(new UnboundRelation(ImmutableList.of("b")))
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
