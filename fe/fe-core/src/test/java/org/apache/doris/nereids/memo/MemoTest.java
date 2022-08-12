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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.expressions.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoTest {
    @Test
    public void testInsert() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject rootProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);

        Group rootGroup = memo.getRoot();

        Assertions.assertEquals(3, memo.getGroups().size());
        Assertions.assertEquals(3, memo.getGroupExpressions().size());

        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, rootGroup.logicalExpressionsAt(0).getPlan().getType());
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).getPlan().getType());
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).child(0).logicalExpressionsAt(0)
                        .getPlan().getType());
    }

    /**
     * Original:
     * Project(name)
     * |---Project(name)
     *     |---UnboundRelation
     *
     * After rewrite:
     * Project(name)
     * |---Project(rewrite)
     *     |---Project(rewrite_inside)
     *         |---UnboundRelation
     */
    @Test
    public void testRewrite() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 1).findFirst().get();
        LogicalProject rewriteInsideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite_inside", StringType.INSTANCE,
                        false, ImmutableList.of("test"))),
                new GroupPlan(leafGroup)
        );
        LogicalProject rewriteProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite", StringType.INSTANCE,
                        true, ImmutableList.of("test"))),
                rewriteInsideProject
        );
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(4, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("name", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite_inside", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());
    }

    /**
     * Test rewrite current Plan with its child.
     *
     * Original(Group 2 is root):
     * Group2: Project(outside)
     * Group1: |---Project(inside)
     * Group0:     |---UnboundRelation
     *
     * and we want to rewrite group 2 by Project(inside, GroupPlan(group 0))
     *
     * After rewriting we should get(Group 2 is root):
     * Group2: Project(inside)
     * Group0: |---UnboundRelation
     */
    @Test
    public void testRewriteByChild() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject<UnboundRelation> insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("inside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject<LogicalProject<UnboundRelation>> rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("outside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 2).findFirst().get();
        LogicalPlan rewriteProject = insideProject.withChildren(Lists.newArrayList(new GroupPlan(leafGroup)));
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(3, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals(insideProject.getProjects().get(0), ((LogicalProject<?>) node).getProjects().get(0));
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());

        // check Group 1's GroupExpression is not in GroupExpressionMaps anymore
        GroupExpression groupExpression = new GroupExpression(rewriteProject, Lists.newArrayList(leafGroup));
        Assertions.assertEquals(2,
                memo.getGroupExpressions().get(groupExpression).getOwnerGroup().getGroupId().asInt());
    }

    /**
     * Original:
     * Group 0: UnboundRelation C
     * Group 1: UnboundRelation B
     * Group 2: UnboundRelation A
     * Group 3: Join(Group 1, Group 2)
     * Group 4: Join(Group 0, Group 3)
     * Group 5: Filter(Group 4)
     * Group 6: Join(Group 2, Group 1)
     * Group 7: Join(Group 0, Group 6)
     *
     * Then:
     * Copy In Join(Group 2, Group 1) into Group 3
     *
     * Expected:
     * Group 0: UnboundRelation C
     * Group 1: UnboundRelation B
     * Group 2: UnboundRelation A
     * Group 5: Filter(Group 7)
     * Group 6: Join(Group 2, Group 1), Join(Group 1, Group 2)
     * Group 7: Join(Group 0, Group 6)
     */
    @Test
    public void testMergeGroup() {
        UnboundRelation unboundRelationA = new UnboundRelation(Lists.newArrayList("A"));
        UnboundRelation unboundRelationB = new UnboundRelation(Lists.newArrayList("B"));
        UnboundRelation unboundRelationC = new UnboundRelation(Lists.newArrayList("C"));
        LogicalJoin logicalJoinAB = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationA, unboundRelationB);
        LogicalJoin logicalJoinBA = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationB, unboundRelationA);
        LogicalJoin logicalJoinCAB = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationC, logicalJoinAB);
        LogicalJoin logicalJoinCBA = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationC, logicalJoinBA);
        LogicalFilter logicalFilter = new LogicalFilter<>(new BooleanLiteral(true), logicalJoinCBA);

        Memo memo = new Memo(logicalFilter);
        memo.copyIn(logicalJoinCAB, null, false);
        Assertions.assertEquals(8, memo.getGroups().size());
        Assertions.assertEquals(8, memo.getGroupExpressions().size());

        Group target = memo.getRoot().getLogicalExpression().child(0).getLogicalExpression().child(1);
        LogicalJoin repeat = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationA, unboundRelationB);
        memo.copyIn(repeat, target, false);
        Assertions.assertEquals(6, memo.getGroups().size());
        Assertions.assertEquals(7, memo.getGroupExpressions().size());
        Group root = memo.getRoot();
        Assertions.assertEquals(1, root.getLogicalExpressions().size());
        GroupExpression filter = root.getLogicalExpression();
        GroupExpression joinCBA = filter.child(0).getLogicalExpression();
        Assertions.assertEquals(1, joinCBA.child(0).getLogicalExpressions().size());
        Assertions.assertEquals(2, joinCBA.child(1).getLogicalExpressions().size());
        GroupExpression joinBA = joinCBA.child(1).getLogicalExpressions().get(0);
        GroupExpression joinAB = joinCBA.child(1).getLogicalExpressions().get(1);
        Assertions.assertTrue(joinAB.getPlan() instanceof LogicalJoin);
        Assertions.assertTrue(joinBA.getPlan() instanceof LogicalJoin);
    }
}
