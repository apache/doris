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

import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearCeil;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DateArithmeticFilterPushDownTest {
    private final Alias datetime = new Alias(new DateTimeV2Literal("9999-12-31 23:59:59.000001"), "dt");
    private final Alias id = new Alias(new IntegerLiteral(1), "id");
    private final LogicalOneRowRelation relation = new LogicalOneRowRelation(
            StatementScopeIdGenerator.newRelationId(), ImmutableList.of(datetime, id));

    private List<Expression> dateExpressions() {
        return ImmutableList.of(
                new WeeksAdd(datetime.toSlot(), new IntegerLiteral(5)),
                new HoursAdd(datetime.toSlot(), new IntegerLiteral(5)),
                new SecondCeil(datetime.toSlot(), new IntegerLiteral(5)),
                new YearCeil(datetime.toSlot(), new IntegerLiteral(5)),
                new DaysAdd(datetime.toSlot(), new IntegerLiteral(5)));
    }

    @Test
    void protectRejectingSetOperations() {
        Alias rightDatetime = new Alias(new DateTimeV2Literal("2020-01-01 00:00:00"), "right_dt");
        Alias rightId = new Alias(new IntegerLiteral(1), "right_id");
        LogicalOneRowRelation right = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(rightDatetime, rightId));
        List<Plan> children = ImmutableList.of(relation, right);
        List<NamedExpression> setOutputs = ImmutableList.of(datetime.toSlot(), id.toSlot());
        List<List<SlotReference>> outputs = ImmutableList.of(
                ImmutableList.of((SlotReference) datetime.toSlot(), (SlotReference) id.toSlot()),
                ImmutableList.of((SlotReference) rightDatetime.toSlot(), (SlotReference) rightId.toSlot()));
        LogicalExcept except = new LogicalExcept(Qualifier.DISTINCT, setOutputs, outputs, children);
        LogicalIntersect intersect = new LogicalIntersect(Qualifier.DISTINCT,
                setOutputs, outputs, children);
        PhysicalOneRowRelation source = new PhysicalOneRowRelation(relation.getRelationId(),
                relation.getProjects(), relation.getLogicalProperties());
        PhysicalOneRowRelation physicalRight = new PhysicalOneRowRelation(right.getRelationId(),
                right.getProjects(), right.getLogicalProperties());
        List<Plan> physicalChildren = ImmutableList.of(source, physicalRight);
        assertRejectingBoundary(except, new PhysicalExcept(Qualifier.DISTINCT,
                setOutputs, outputs, except.getLogicalProperties(), physicalChildren));
        assertRejectingBoundary(intersect, new PhysicalIntersect(Qualifier.DISTINCT,
                setOutputs, outputs, intersect.getLogicalProperties(), physicalChildren));
    }

    @Test
    void protectGenerate() {
        Explode explode = new Explode(new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1))));
        SlotReference item = new SlotReference("item", IntegerType.INSTANCE);
        LogicalGenerate<?> generate = new LogicalGenerate<>(ImmutableList.of(explode),
                ImmutableList.of(item), relation);
        PhysicalOneRowRelation source = new PhysicalOneRowRelation(relation.getRelationId(),
                relation.getProjects(), relation.getLogicalProperties());
        assertRejectingBoundary(generate, new PhysicalGenerate<>(ImmutableList.of(explode),
                ImmutableList.of(item), ImmutableList.of(), generate.getLogicalProperties(), source));
    }

    private void assertRejectingBoundary(Plan logicalInput, Plan physicalInput) {
        for (int offset : new int[] {0, 5}) {
            Alias result = new Alias(new Cast(new WeeksAdd(datetime.toSlot(), new IntegerLiteral(offset)),
                    DateTimeV2Type.of(0)), "result");
            Expression predicate = new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-02-05 00:00:00"));
            LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result), logicalInput);
            LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
            Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                    .applyTopDown(new PushDownFilterThroughProject()).getPlan();
            Assertions.assertEquals(offset == 0 ? LogicalProject.class : LogicalFilter.class, rewritten.getClass());
            PhysicalProject<?> physicalProject = new PhysicalProject<>(ImmutableList.of(result),
                    project.getLogicalProperties(), physicalInput);
            PhysicalFilter<?> physicalFilter = new PhysicalFilter<>(ImmutableSet.of(predicate),
                    physicalProject.getLogicalProperties(), physicalProject);
            Plan physicalRewritten = physicalFilter.accept(
                    new org.apache.doris.nereids.processor.post.PushDownFilterThroughProject(), null);
            Assertions.assertEquals(offset == 0 ? PhysicalProject.class : PhysicalFilter.class,
                    physicalRewritten.getClass());
        }
    }

    @Test
    void keepLogicalDatePredicateAboveProject() {
        for (Expression expression : dateExpressions()) {
            Alias result = new Alias(new Cast(expression, DateTimeV2Type.of(0)), "result");
            LogicalFilter<LogicalOneRowRelation> input = new LogicalFilter<>(
                    ImmutableSet.of(new EqualTo(id.toSlot(), new IntegerLiteral(0))), relation);
            LogicalProject<LogicalFilter<LogicalOneRowRelation>> project = new LogicalProject<>(
                    ImmutableList.of(result, id.toSlot()), input);
            Expression predicate = new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"));
            LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
            Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                    .applyTopDown(new PushDownFilterThroughProject()).getPlan();
            Assertions.assertInstanceOf(LogicalFilter.class, rewritten, expression.toSql());
            Assertions.assertInstanceOf(LogicalProject.class, rewritten.child(0));
        }
    }

    @Test
    void pushZeroOffsetDatePredicateThroughLowerFilter() {
        for (Expression zero : ImmutableList.of(new IntegerLiteral(0), new BigIntLiteral(0))) {
            Alias result = new Alias(new DaysAdd(datetime.toSlot(), zero), "result");
            LogicalFilter<?> input = new LogicalFilter<>(
                    ImmutableSet.of(new EqualTo(id.toSlot(), new IntegerLiteral(0))), relation);
            LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result, id.toSlot()), input);
            Expression predicate = new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"));
            LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
            Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                    .applyTopDown(new PushDownFilterThroughProject()).getPlan();
            Assertions.assertInstanceOf(LogicalProject.class, rewritten);
            Assertions.assertInstanceOf(LogicalFilter.class, rewritten.child(0));

            PhysicalOneRowRelation source = new PhysicalOneRowRelation(relation.getRelationId(),
                    relation.getProjects(), relation.getLogicalProperties());
            PhysicalFilter<?> physicalInput = new PhysicalFilter<>(input.getConjuncts(),
                    source.getLogicalProperties(), source);
            PhysicalProject<?> physicalProject = new PhysicalProject<>(project.getProjects(),
                    project.getLogicalProperties(), physicalInput);
            PhysicalFilter<?> physicalFilter = new PhysicalFilter<>(ImmutableSet.of(predicate),
                    physicalProject.getLogicalProperties(), physicalProject);
            Plan physicalRewritten = physicalFilter.accept(
                    new org.apache.doris.nereids.processor.post.PushDownFilterThroughProject(), null);
            Assertions.assertInstanceOf(PhysicalProject.class, physicalRewritten);
            Assertions.assertInstanceOf(PhysicalFilter.class, physicalRewritten.child(0));
        }
    }

    @Test
    void pushLogicalDatePredicateWithoutLowerFilter() {
        for (Expression expression : ImmutableList.<Expression>builder().addAll(dateExpressions())
                .add(new DaysAdd(datetime.toSlot(), new IntegerLiteral(0))).build()) {
            Alias result = new Alias(new Cast(expression, DateTimeV2Type.of(0)), "result");
            LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result, id.toSlot()), relation);
            Expression predicate = new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"));
            LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
            Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                    .applyTopDown(new PushDownFilterThroughProject()).getPlan();
            Assertions.assertInstanceOf(LogicalProject.class, rewritten, expression.toSql());
            Assertions.assertInstanceOf(LogicalFilter.class, rewritten.child(0));
        }
    }

    @Test
    void pushPhysicalDatePredicateWithoutLowerFilter() {
        for (Expression expression : ImmutableList.<Expression>builder().addAll(dateExpressions())
                .add(new DaysAdd(datetime.toSlot(), new IntegerLiteral(0))).build()) {
            Alias result = new Alias(new Cast(expression, DateTimeV2Type.of(0)), "result");
            PhysicalOneRowRelation source = new PhysicalOneRowRelation(relation.getRelationId(),
                    relation.getProjects(), relation.getLogicalProperties());
            LogicalProject<?> logicalProject = new LogicalProject<>(ImmutableList.of(result, id.toSlot()), relation);
            PhysicalProject<?> project = new PhysicalProject<>(ImmutableList.of(result, id.toSlot()),
                    logicalProject.getLogicalProperties(), source);
            PhysicalFilter<?> filter = new PhysicalFilter<>(
                    ImmutableSet.of(new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"))),
                    project.getLogicalProperties(), project);
            Plan rewritten = filter.accept(
                    new org.apache.doris.nereids.processor.post.PushDownFilterThroughProject(), null);
            Assertions.assertInstanceOf(PhysicalProject.class, rewritten, expression.toSql());
            Assertions.assertInstanceOf(PhysicalFilter.class, rewritten.child(0));
        }
    }

    @Test
    void keepLogicalDatePredicateAboveJoinOrLimit() {
        LogicalOneRowRelation right = new LogicalOneRowRelation(
                StatementScopeIdGenerator.newRelationId(), ImmutableList.of(new Alias(new IntegerLiteral(1), "right")));
        for (Plan input : ImmutableList.of(
                new LogicalJoin<>(JoinType.INNER_JOIN, relation, right, new JoinReorderContext()),
                new LogicalLimit<>(0, 0, LimitPhase.ORIGIN, relation))) {
            Alias result = new Alias(new WeeksAdd(datetime.toSlot(), new IntegerLiteral(5)), "result");
            LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result), input);
            LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(
                    new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"))), project);
            Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                    .applyTopDown(new PushDownFilterThroughProject()).getPlan();
            Assertions.assertInstanceOf(LogicalFilter.class, rewritten);
            Assertions.assertInstanceOf(LogicalProject.class, rewritten.child(0));
        }
    }

    @Test
    void keepLogicalDatePredicateAboveLimitProject() {
        Alias result = new Alias(new WeeksAdd(datetime.toSlot(), new IntegerLiteral(5)), "result");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result), relation);
        LogicalLimit<?> limit = new LogicalLimit<>(0, 0, LimitPhase.ORIGIN, project);
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"))), limit);
        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughProject()).getPlan();
        Assertions.assertInstanceOf(LogicalFilter.class, rewritten);
        Assertions.assertInstanceOf(LogicalLimit.class, rewritten.child(0));
    }

    @Test
    void keepDatePredicateAboveCteConsumer() {
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(StatementScopeIdGenerator.newRelationId(),
                StatementScopeIdGenerator.newCTEId(), "c", relation);
        Alias result = new Alias(new WeeksAdd(consumer.getOutput().get(0), new IntegerLiteral(5)), "result");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result), consumer);
        Expression predicate = new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"));
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughProject()).getPlan();
        Assertions.assertInstanceOf(LogicalFilter.class, rewritten);
        Assertions.assertInstanceOf(LogicalProject.class, rewritten.child(0));

        PhysicalCTEConsumer physicalConsumer = new PhysicalCTEConsumer(consumer.getRelationId(), consumer.getCteId(),
                consumer.getConsumerToProducerOutputMap(), consumer.getProducerToConsumerOutputMap(),
                consumer.getLogicalProperties());
        PhysicalProject<?> physicalProject = new PhysicalProject<>(ImmutableList.of(result),
                project.getLogicalProperties(), physicalConsumer);
        PhysicalFilter<?> physicalFilter = new PhysicalFilter<>(ImmutableSet.of(predicate),
                physicalProject.getLogicalProperties(), physicalProject);
        Plan physicalRewritten = physicalFilter.accept(
                new org.apache.doris.nereids.processor.post.PushDownFilterThroughProject(), null);
        Assertions.assertInstanceOf(PhysicalFilter.class, physicalRewritten);
        Assertions.assertInstanceOf(PhysicalProject.class, physicalRewritten.child(0));
    }

    @Test
    void stillPushLogicalPassthroughPredicate() {
        Alias result = new Alias(new WeeksAdd(datetime.toSlot(), new IntegerLiteral(5)), "result");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(result, id.toSlot()), relation);
        Expression predicate = new EqualTo(id.toSlot(), new IntegerLiteral(0));
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(predicate), project);
        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughProject()).getPlan();
        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        Assertions.assertInstanceOf(LogicalFilter.class, rewritten.child(0));
    }

    @Test
    void keepPhysicalDatePredicateAboveProject() {
        for (Expression expression : dateExpressions()) {
            Alias result = new Alias(new Cast(expression, DateTimeV2Type.of(0)), "result");
            PhysicalOneRowRelation source = new PhysicalOneRowRelation(relation.getRelationId(),
                    relation.getProjects(), relation.getLogicalProperties());
            PhysicalFilter<?> input = new PhysicalFilter<>(
                    ImmutableSet.of(new EqualTo(id.toSlot(), new IntegerLiteral(0))),
                    source.getLogicalProperties(), source);
            LogicalProject<?> logicalProject = new LogicalProject<>(ImmutableList.of(result, id.toSlot()), relation);
            PhysicalProject<?> project = new PhysicalProject<>(ImmutableList.of(result, id.toSlot()),
                    logicalProject.getLogicalProperties(), input);
            PhysicalFilter<?> filter = new PhysicalFilter<>(
                    ImmutableSet.of(new EqualTo(result.toSlot(), new DateTimeV2Literal("2020-01-01 00:00:00"))),
                    project.getLogicalProperties(), project);
            Plan rewritten = filter.accept(
                    new org.apache.doris.nereids.processor.post.PushDownFilterThroughProject(), null);
            Assertions.assertInstanceOf(PhysicalFilter.class, rewritten, expression.toSql());
            Assertions.assertInstanceOf(PhysicalProject.class, rewritten.child(0));
        }
    }
}
