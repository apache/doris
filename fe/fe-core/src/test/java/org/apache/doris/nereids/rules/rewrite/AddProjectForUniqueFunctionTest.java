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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class AddProjectForUniqueFunctionTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan studentOlapScan
            = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @Test
    void testGenUniqueFunctionAlias() {
        Random random1 = new Random();
        Random random2 = new Random();
        Random random3 = new Random();
        List<Expression> expressions = ImmutableList.of(
                new Add(random1, new Add(random1, new DoubleLiteral(1.0))),
                new Add(random2, random3),
                random3);
        List<NamedExpression> namedExpressions = new AddProjectForUniqueFunction().tryGenUniqueFunctionAlias(expressions);
        Assertions.assertEquals(2, namedExpressions.size());
        Assertions.assertInstanceOf(Alias.class, namedExpressions.get(0));
        Assertions.assertEquals(((Alias) namedExpressions.get(0)).child(), random1);
        Assertions.assertInstanceOf(Alias.class, namedExpressions.get(1));
        Assertions.assertEquals(((Alias) namedExpressions.get(1)).child(), random3);
    }

    @Test
    void testRewriteExpressionNoChange() {
        Random random1 = new Random();
        Random random2 = new Random();
        Random random3 = new Random();
        List<NamedExpression> projections = ImmutableList.of(
                new Alias(new Add(random1, new Add(new DoubleLiteral(1.0), new DoubleLiteral(1.0)))),
                new Alias(new Add(random2, new DoubleLiteral(1.0))),
                new Alias(random3));
        LogicalProject<?> project = new LogicalProject<Plan>(projections, studentOlapScan);
        Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>> result = new AddProjectForUniqueFunction()
                .rewriteExpressions(project, project.getProjects());
        Assertions.assertEquals(Optional.empty(), result);
    }

    @Test
    void testRewriteExpressionProjectSucc() {
        Random random1 = new Random();
        Random random2 = new Random();
        List<NamedExpression> projections = ImmutableList.of(
                new Alias(new Add(random1, new Add(new DoubleLiteral(1.0), new DoubleLiteral(1.0)))),
                new Alias(new Add(random2, new DoubleLiteral(1.0))),
                new Alias(random2));
        LogicalProject<?> project = new LogicalProject<Plan>(projections, studentOlapScan);
        Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>> result = new AddProjectForUniqueFunction()
                .rewriteExpressions(project, project.getProjects());
        Assertions.assertTrue(result.isPresent());
        Assertions.assertInstanceOf(LogicalProject.class, result.get().second);
        LogicalProject<?> bottomProject = (LogicalProject<?>) result.get().second;
        List<NamedExpression> bottomProjections = bottomProject.getProjects();
        Assertions.assertEquals(studentOlapScan.getOutput().size() + 1, bottomProjections.size());
        Assertions.assertEquals(studentOlapScan.getOutput(), bottomProjections.subList(0, studentOlapScan.getOutput().size()));
        Alias alis = (Alias) bottomProjections.get(bottomProjections.size() - 1);
        Assertions.assertEquals(alis.child(), random2);
        List<NamedExpression> expectedTopProjections = ImmutableList.of(
                projections.get(0),
                new Alias(projections.get(1).getExprId(), new Add(alis.toSlot(), new DoubleLiteral(1.0))),
                new Alias(projections.get(2).getExprId(), alis.toSlot())
        );
        Assertions.assertEquals(expectedTopProjections, result.get().first);
    }

    @Test
    void testRewriteJoin() {
        LogicalOlapScan scoreOlapScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference sid = (SlotReference) scoreOlapScan.getOutput().get(0);
        Random random = new Random();
        LogicalJoin<?, ?> join = new LogicalJoin<Plan, Plan>(JoinType.CROSS_JOIN,
                ImmutableList.of(),
                ImmutableList.of(new EqualTo(random, sid)),
                ImmutableList.of(new EqualTo(random, new DoubleLiteral(1.0))),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                ImmutableList.of(studentOlapScan, scoreOlapScan),
                null);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new AddProjectForUniqueFunction())
                .getPlan();
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> newJoin = (LogicalJoin<?, ?>) root;
        Assertions.assertInstanceOf(LogicalProject.class, newJoin.left());
        LogicalProject<?> leftProject = (LogicalProject<?>) newJoin.left();
        Assertions.assertEquals(studentOlapScan, leftProject.child());
        Assertions.assertEquals(scoreOlapScan, newJoin.right());
        Alias alias = (Alias) leftProject.getProjects().get(leftProject.getProjects().size() - 1);
        Assertions.assertEquals(alias.child(), random);
        Assertions.assertEquals(ImmutableList.of(new EqualTo(alias.toSlot(), sid)), newJoin.getHashJoinConjuncts());
        Assertions.assertEquals(ImmutableList.of(), newJoin.getOtherJoinConjuncts());
        Assertions.assertEquals(ImmutableList.of(new EqualTo(alias.toSlot(), new DoubleLiteral(1.0))), newJoin.getMarkJoinConjuncts());
        Assertions.assertEquals(JoinType.INNER_JOIN, newJoin.getJoinType());
    }
}
