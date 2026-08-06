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

import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class AddProjectForVolatileExpressionTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan studentOlapScan
            = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @Test
    void testGenVolatileExpressionAlias() {
        Random random1 = new Random();
        Random random2 = new Random();
        Random random3 = new Random();
        List<Expression> expressions = ImmutableList.of(
                new Add(random1, new Add(random1, new DoubleLiteral(1.0))),
                new Add(random2, random3),
                random3);
        List<NamedExpression> namedExpressions = new AddProjectForVolatileExpression()
                .tryGenVolatileExpressionAlias(expressions);
        Assertions.assertEquals(2, namedExpressions.size());
        Assertions.assertInstanceOf(Alias.class, namedExpressions.get(0));
        Assertions.assertEquals(((Alias) namedExpressions.get(0)).child(), random1);
        Assertions.assertInstanceOf(Alias.class, namedExpressions.get(1));
        Assertions.assertEquals(((Alias) namedExpressions.get(1)).child(), random3);
    }

    @Test
    void testGenVolatileUdfAlias() {
        JavaUdf volatileUdf = javaUdf(FunctionVolatility.VOLATILE, VolatileIdentity.newVolatileIdentity());
        JavaUdf stableUdf = javaUdf(FunctionVolatility.STABLE, VolatileIdentity.NON_VOLATILE);
        List<Expression> expressions = ImmutableList.of(
                new Add(volatileUdf, new IntegerLiteral(1)),
                volatileUdf,
                new Add(stableUdf, stableUdf));

        List<NamedExpression> namedExpressions = new AddProjectForVolatileExpression()
                .tryGenVolatileExpressionAlias(expressions);
        Assertions.assertEquals(1, namedExpressions.size());
        Assertions.assertInstanceOf(Alias.class, namedExpressions.get(0));
        Assertions.assertEquals(((Alias) namedExpressions.get(0)).child(), volatileUdf);
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
        Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>> result = new AddProjectForVolatileExpression()
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
        Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>> result = new AddProjectForVolatileExpression()
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
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> newJoin = (LogicalJoin<?, ?>) root;
        Assertions.assertEquals(studentOlapScan, newJoin.left());
        Assertions.assertInstanceOf(LogicalProject.class, newJoin.right());
        LogicalProject<?> rightProject = (LogicalProject<?>) newJoin.right();
        Assertions.assertEquals(scoreOlapScan, rightProject.child());
        Alias alias = (Alias) rightProject.getProjects().get(rightProject.getProjects().size() - 1);
        Assertions.assertEquals(alias.child(), random);
        Assertions.assertEquals(ImmutableList.of(), newJoin.getHashJoinConjuncts());
        Assertions.assertEquals(ImmutableList.of(new EqualTo(alias.toSlot(), sid)), newJoin.getOtherJoinConjuncts());
        Assertions.assertEquals(ImmutableList.of(new EqualTo(alias.toSlot(), new DoubleLiteral(1.0))), newJoin.getMarkJoinConjuncts());
        Assertions.assertEquals(JoinType.CROSS_JOIN, newJoin.getJoinType());
    }

    @Test
    void testRewriteJoinProjectRepeatedVolatileToRightSide() {
        LogicalOlapScan scoreOlapScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference score = (SlotReference) scoreOlapScan.getOutput().get(2);
        Random random = new Random();
        Add repeated = new Add(score, random);
        LogicalJoin<?, ?> join = new LogicalJoin<Plan, Plan>(JoinType.INNER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(
                        new GreaterThanEqual(repeated, new DoubleLiteral(0.1)),
                        new LessThanEqual(repeated, new DoubleLiteral(0.5))),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                studentOlapScan,
                scoreOlapScan,
                null);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> newJoin = (LogicalJoin<?, ?>) root;
        Assertions.assertEquals(studentOlapScan, newJoin.left());
        Assertions.assertInstanceOf(LogicalProject.class, newJoin.right());
        LogicalProject<?> rightProject = (LogicalProject<?>) newJoin.right();
        Assertions.assertEquals(scoreOlapScan, rightProject.child());
        Alias alias = (Alias) rightProject.getProjects().get(rightProject.getProjects().size() - 1);
        Assertions.assertEquals(random, alias.child());
        Assertions.assertTrue(newJoin.getOtherJoinConjuncts().stream()
                .allMatch(conjunct -> conjunct.anyMatch(alias.toSlot()::equals)));
    }

    @Test
    void testRewriteJoinProjectRepeatedVolatileToLeftSideByDefault() {
        LogicalOlapScan scoreOlapScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        Random random = new Random();
        LogicalJoin<?, ?> join = new LogicalJoin<Plan, Plan>(JoinType.INNER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(
                        new GreaterThanEqual(random, new DoubleLiteral(0.1)),
                        new LessThanEqual(random, new DoubleLiteral(0.5))),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                studentOlapScan,
                scoreOlapScan,
                null);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> newJoin = (LogicalJoin<?, ?>) root;
        Assertions.assertInstanceOf(LogicalProject.class, newJoin.left());
        Assertions.assertEquals(scoreOlapScan, newJoin.right());
        LogicalProject<?> leftProject = (LogicalProject<?>) newJoin.left();
        Assertions.assertEquals(studentOlapScan, leftProject.child());
        Alias alias = (Alias) leftProject.getProjects().get(leftProject.getProjects().size() - 1);
        Assertions.assertEquals(random, alias.child());
        Assertions.assertTrue(newJoin.getOtherJoinConjuncts().stream()
                .allMatch(conjunct -> conjunct.anyMatch(alias.toSlot()::equals)));
    }

    @Test
    void testRewriteJoinProjectRepeatedVolatileFunctionWithRightInputToRightSide() {
        LogicalOlapScan scoreOlapScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference studentId = (SlotReference) studentOlapScan.getOutput().get(0);
        SlotReference scoreId = (SlotReference) scoreOlapScan.getOutput().get(0);
        JavaUdf volatileUdf = javaUdf(FunctionVolatility.VOLATILE,
                VolatileIdentity.newVolatileIdentity(), scoreId);
        Add repeated = new Add(studentId, volatileUdf);
        LogicalJoin<?, ?> join = new LogicalJoin<Plan, Plan>(JoinType.INNER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(
                        new GreaterThanEqual(repeated, new DoubleLiteral(0.1)),
                        new LessThanEqual(repeated, new DoubleLiteral(0.5))),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                studentOlapScan,
                scoreOlapScan,
                null);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> newJoin = (LogicalJoin<?, ?>) root;
        Assertions.assertEquals(studentOlapScan, newJoin.left());
        Assertions.assertInstanceOf(LogicalProject.class, newJoin.right());
        LogicalProject<?> rightProject = (LogicalProject<?>) newJoin.right();
        Assertions.assertEquals(scoreOlapScan, rightProject.child());
        Alias alias = (Alias) rightProject.getProjects().get(rightProject.getProjects().size() - 1);
        Assertions.assertEquals(volatileUdf, alias.child());
        Assertions.assertTrue(newJoin.getOtherJoinConjuncts().stream()
                .allMatch(conjunct -> conjunct.anyMatch(alias.toSlot()::equals)));
    }

    @Test
    void testRewriteJoinSkipRepeatedVolatileFunctionWithBothSideInputs() {
        LogicalOlapScan scoreOlapScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference studentId = (SlotReference) studentOlapScan.getOutput().get(0);
        SlotReference scoreId = (SlotReference) scoreOlapScan.getOutput().get(0);
        JavaUdf volatileUdf = javaUdf(FunctionVolatility.VOLATILE,
                VolatileIdentity.newVolatileIdentity(), studentId, scoreId);
        LogicalJoin<?, ?> join = new LogicalJoin<Plan, Plan>(JoinType.INNER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(
                        new GreaterThanEqual(volatileUdf, new DoubleLiteral(0.1)),
                        new LessThanEqual(volatileUdf, new DoubleLiteral(0.5))),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                studentOlapScan,
                scoreOlapScan,
                null);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();
        Assertions.assertEquals(join, root);
    }

    private JavaUdf javaUdf(FunctionVolatility volatility, VolatileIdentity volatileIdentity) {
        return javaUdf(volatility, volatileIdentity, new IntegerLiteral(1));
    }

    private JavaUdf javaUdf(FunctionVolatility volatility, VolatileIdentity volatileIdentity,
            Expression... arguments) {
        return new JavaUdf("java_fn", 1, "db1", org.apache.doris.catalog.Function.BinaryType.JAVA_UDF,
                FunctionSignature.ret(IntegerType.INSTANCE).args(
                        Collections.nCopies(arguments.length, IntegerType.INSTANCE).toArray(new IntegerType[0])),
                NullableMode.ALWAYS_NULLABLE, volatility, volatileIdentity,
                null, "evaluate", null, null, "", false, 360, arguments);
    }
}
