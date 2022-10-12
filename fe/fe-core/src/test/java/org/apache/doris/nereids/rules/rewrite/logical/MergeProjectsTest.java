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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * MergeConsecutiveProjects ut
 */
public class MergeProjectsTest {
    @Test
    public void testMergeConsecutiveProjects() {
        UnboundRelation relation = new UnboundRelation(Lists.newArrayList("db", "table"));
        NamedExpression colA = new SlotReference("a", IntegerType.INSTANCE, true, Lists.newArrayList("a"));
        NamedExpression colB = new SlotReference("b", IntegerType.INSTANCE, true, Lists.newArrayList("b"));
        NamedExpression colC = new SlotReference("c", IntegerType.INSTANCE, true, Lists.newArrayList("c"));
        LogicalProject project1 = new LogicalProject<>(Lists.newArrayList(colA, colB, colC), relation);
        LogicalProject project2 = new LogicalProject<>(Lists.newArrayList(colA, colB), project1);
        LogicalProject project3 = new LogicalProject<>(Lists.newArrayList(colA), project2);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(project3);
        List<Rule> rules = Lists.newArrayList(new MergeProjects().build());
        cascadesContext.bottomUpRewrite(rules);
        Plan plan = cascadesContext.getMemo().copyOut();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan instanceof LogicalProject);
        Assertions.assertEquals(((LogicalProject<?>) plan).getProjects(), Lists.newArrayList(colA));
        Assertions.assertTrue(plan.child(0) instanceof UnboundRelation);
    }

    /**
     *       project2(X + 2)
     *             |
     *       project1(B, C, A+1 as X)
     *             |
     *       relation
     * transform to :
     *       project2((A + 1) + 2)
     *             |
     *       relation
     */
    @Test
    public void testMergeConsecutiveProjectsWithAlias() {
        UnboundRelation relation = new UnboundRelation(Lists.newArrayList("db", "table"));
        NamedExpression colA = new SlotReference("a", IntegerType.INSTANCE, true, Lists.newArrayList("a"));
        NamedExpression colB = new SlotReference("b", IntegerType.INSTANCE, true, Lists.newArrayList("b"));
        NamedExpression colC = new SlotReference("c", IntegerType.INSTANCE, true, Lists.newArrayList("c"));
        Alias alias = new Alias(new Add(colA, new IntegerLiteral(1)), "X");
        Slot aliasRef = alias.toSlot();

        LogicalProject project1 = new LogicalProject<>(
                Lists.newArrayList(
                        colB,
                        colC,
                        alias),
                relation);
        LogicalProject project2 = new LogicalProject<>(
                Lists.newArrayList(
                        new Alias(new Add(aliasRef, new IntegerLiteral(2)), "Y"),
                        aliasRef
                ),
                project1);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(project2);
        List<Rule> rules = Lists.newArrayList(new MergeProjects().build());
        cascadesContext.bottomUpRewrite(rules);
        Plan plan = cascadesContext.getMemo().copyOut();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan instanceof LogicalProject);
        LogicalProject finalProject = (LogicalProject) plan;
        Add aPlus1Plus2 = new Add(
                new Add(colA, new IntegerLiteral(1)),
                new IntegerLiteral(2)
        );
        Assertions.assertEquals(2, finalProject.getProjects().size());
        Assertions.assertEquals(aPlus1Plus2, ((Alias) finalProject.getProjects().get(0)).child());
        Assertions.assertEquals(alias, finalProject.getProjects().get(1));
    }
}
