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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoTest {
    @Test
    public void testCopyIn() {
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
     * initial Memo status:
     *      group#1(project1)-->group#0(relationA)
     *      group#3(project2)-->group#2(relationB)
     *      group#4(project3)-->group#2(relationB)
     * copy relationA into group#2
     * after merge:
     *      group#1(project1)-->group#0(relationA, relationB)
     *      group#4(project3)-->group#0
     * merging group#2 and group#0 recursively invoke merging group#3 and group#1 and merging group#4 and group#1
     */
    @Test
    public void testMergeGroup() {
        UnboundRelation relation1 = new UnboundRelation(Lists.newArrayList("A"));
        LogicalProject project1 = new LogicalProject(
                ImmutableList.of(new SlotReference(new ExprId(1), "name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                relation1
        );

        Memo memo = new Memo(project1);

        UnboundRelation relation2 = new UnboundRelation(Lists.newArrayList("B"));
        LogicalProject project2 = new LogicalProject(
                ImmutableList.of(new SlotReference(new ExprId(1), "name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                relation2
        );
        memo.copyIn(project2, null, false);
        Assertions.assertEquals(4, memo.getGroups().size());

        LogicalProject project3 = new LogicalProject(
                ImmutableList.of(new SlotReference(new ExprId(1), "other", StringType.INSTANCE, true, ImmutableList.of("other"))),
                relation2
        );

        memo.copyIn(project3, null, false);

        //after copyIn, group#2 contains relationA and relationB
        memo.copyIn(relation1, memo.getGroups().get(2), true);


        Assertions.assertEquals(3, memo.getGroups().size());
        Group root = memo.getRoot();
        Assertions.assertEquals(1, root.getLogicalExpressions().size());
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, root.logicalExpressionsAt(0).getPlan().getType());
        GroupExpression rootExpression = root.logicalExpressionsAt(0);
        Assertions.assertEquals(1, rootExpression.children().size());
        //two expressions: relationA and relationB
        Assertions.assertEquals(2, rootExpression.child(0).getLogicalExpressions().size());
        GroupExpression childExpression = rootExpression.child(0).logicalExpressionsAt(0);
        Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION, childExpression.getPlan().getType());

        Group groupProjct3 = memo.getGroups().get(2); //group for project3
        Group groupRelation = memo.getGroups().get(0); //group for relation
        //group0 is child of group4
        Assertions.assertEquals(groupRelation, groupProjct3.logicalExpressionsAt(0).child(0));
        Assertions.assertEquals(1, groupProjct3.getLogicalExpressions().size());
        Assertions.assertEquals(1, groupProjct3.logicalExpressionsAt(0).children().size());
    }
}
