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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class GroupExpressionMatchingTest {

    @Test
    public void testLeafNode() {
        Pattern pattern = new Pattern<>(PlanType.LOGICAL_UNBOUND_RELATION);

        Memo memo = new Memo(null, new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test")));

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

        Plan leaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test"));
        LogicalProject root = new LogicalProject(ImmutableList
                .of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                leaf);
        Memo memo = new Memo(null, root);

        Plan anotherLeaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test2"));
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

        Plan leaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test"));
        LogicalProject root = new LogicalProject(ImmutableList
                .of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                leaf);
        Memo memo = new Memo(null, root);

        Plan anotherLeaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test2"));
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

        Memo memo = new Memo(null, new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test")));

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
        Plan root = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true,
                        ImmutableList.of("test"))),
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test")));
        Memo memo = new Memo(null, root);

        Plan anotherLeaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("test2"));
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
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("a")),
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("b"))
        );

        Memo memo = new Memo(null, root);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(patterns().innerLogicalJoin().pattern,
                memo.getRoot().getLogicalExpression());
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
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("a")),
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("b"))
        );

        Memo memo = new Memo(null, root);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(patterns().innerLogicalJoin().pattern,
                memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopMatchButChildrenNotMatch() {
        Plan root = new LogicalJoin(JoinType.LEFT_OUTER_JOIN,
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("a")),
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("b"))
        );

        Memo memo = new Memo(null, root);

        Pattern pattern = patterns()
                .innerLogicalJoin(patterns().logicalFilter(), patterns().any()).pattern;
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        Iterator<Plan> iterator = groupExpressionMatching.iterator();

        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void testSubTreeMatch() {
        Plan root =
                new LogicalFilter(ImmutableSet.of(new EqualTo(new UnboundSlot(Lists.newArrayList("a", "id")),
                        new UnboundSlot(Lists.newArrayList("b", "id")))),
                        new LogicalJoin(JoinType.INNER_JOIN,
                                new LogicalJoin(JoinType.LEFT_OUTER_JOIN,
                                        new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("a")),
                                        new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("b"))),
                                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("c")))
                );
        Pattern p1 = patterns().logicalFilter(patterns().subTree(LogicalFilter.class, LogicalJoin.class)).pattern;
        Iterator<Plan> matchResult1 = match(root, p1);
        assertSubTreeMatch(matchResult1);

        Pattern p2 = patterns().subTree(LogicalFilter.class, LogicalJoin.class).pattern;
        Iterator<Plan> matchResult2 = match(root, p2);
        assertSubTreeMatch(matchResult2);

        Pattern p3 = patterns().subTree(LogicalProject.class).pattern;
        Iterator<Plan> matchResult3 = match(root, p3);
        Assertions.assertFalse(matchResult3.hasNext());
    }

    private void assertSubTreeMatch(Iterator<Plan> matchResult) {
        Assertions.assertTrue(matchResult.hasNext());
        Plan plan = matchResult.next();
        System.out.println(plan.treeString());
        new SubTreeMatchChecker().check(plan);
    }

    // TODO: add an effective approach to compare actual and expected plan tree. Maybe a DSL to generate expected plan
    // and leverage a comparing framework to check result. We could reuse pattern match to check shape and properties.
    private class SubTreeMatchChecker extends PlanVisitor<Void, Context> {
        public void check(Plan plan) {
            plan.accept(this, new Context(null));
        }

        @Override
        public Void visit(Plan plan, Context context) {
            notExpectedPlan(plan, context);
            return null;
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, Context context) {
            Assertions.assertTrue(context.parent == null);
            filter.child().accept(this, new Context(filter));
            return null;
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Context context) {
            switch (join.getJoinType()) {
                case INNER_JOIN:
                    Assertions.assertTrue(context.parent instanceof LogicalFilter);
                    break;
                case LEFT_OUTER_JOIN:
                    Assertions.assertTrue(context.parent instanceof LogicalJoin);
                    LogicalJoin parent = (LogicalJoin) context.parent;
                    Assertions.assertEquals(JoinType.INNER_JOIN, parent.getJoinType());
                    break;
                default:
                    notExpectedPlan(join, context);
            }

            join.left().accept(this, new Context(join));
            join.right().accept(this, new Context(join));
            return null;
        }

        @Override
        public Void visitGroupPlan(GroupPlan groupPlan, Context context) {
            Plan plan = groupPlan.getGroup().logicalExpressionsAt(0).getPlan();
            Assertions.assertTrue(plan instanceof UnboundRelation);
            UnboundRelation relation = (UnboundRelation) plan;
            String relationName = relation.getNameParts().get(0);
            switch (relationName) {
                case "a":
                case "b": {
                    Assertions.assertTrue(context.parent instanceof LogicalJoin);
                    LogicalJoin parent = (LogicalJoin) context.parent;
                    Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, parent.getJoinType());
                    break;
                }
                case "c": {
                    Assertions.assertTrue(context.parent instanceof LogicalJoin);
                    LogicalJoin parent = (LogicalJoin) context.parent;
                    Assertions.assertEquals(JoinType.INNER_JOIN, parent.getJoinType());
                    break;
                }
                default:
                    notExpectedPlan(groupPlan, context);
            }
            return null;
        }

        private void notExpectedPlan(Plan plan, Context context) {
            throw new RuntimeException("Not expected plan node in match result:\n"
                    + "PlanNode:\n" + plan.toString()
                    + "\nparent:\n" + context.parent);
        }
    }

    private class Context {
        final Plan parent;

        public Context(Plan parent) {
            this.parent = parent;
        }
    }

    private org.apache.doris.nereids.pattern.GeneratedMemoPatterns patterns() {
        return () -> RulePromise.REWRITE;
    }

    private Iterator<Plan> match(Plan root, Pattern pattern) {
        Memo memo = new Memo(null, root);
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(pattern, memo.getRoot().getLogicalExpression());
        return groupExpressionMatching.iterator();
    }
}
