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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class ConstantPropagationTest {

    private final ConstantPropagation executor = new ConstantPropagation();
    private final NereidsParser parser = new NereidsParser();
    private final ExpressionRewriteContext exprRewriteContext;

    private final JobContext jobContext;

    private final LogicalOlapScan student;
    private final SlotReference studentId;
    private final SlotReference studentGender;
    private final SlotReference studentAge;
    private final LogicalOlapScan score;
    private final SlotReference scoreSid;
    private final SlotReference scoreCid;
    private final SlotReference scoreGrade;

    ConstantPropagationTest() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        exprRewriteContext = new ExpressionRewriteContext(cascadesContext);
        jobContext = new JobContext(cascadesContext, null, Double.MAX_VALUE);

        student = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of(""));
        studentId = (SlotReference) student.getOutput().get(0);
        studentGender = (SlotReference) student.getOutput().get(1);
        studentAge = (SlotReference) student.getOutput().get(3);

        score = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score, ImmutableList.of(""));
        scoreSid = (SlotReference) score.getOutput().get(0);
        scoreCid = (SlotReference) score.getOutput().get(1);
        scoreGrade = (SlotReference) score.getOutput().get(2);
    }

    @Test
    public void testExpressionReplace() {
        assertRewrite("a = 1 and a = b", "a = 1 and b = 1");
        assertRewrite("a = 1 and a + b = 2 and b + c = 2 and c + d = 2 and d + e = 2 and e + f = 2",
                "a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1");
        assertRewrite("(a = 1 and a + 1 = b or b = 2 and b + c = 5) and d = 4",
                "b = 2 and (a = 1 or c = 3) and d = 4");

        // Multiple equalities chained together
        assertRewrite("a = 10 and a = b and b = c", "a = 10 and b = 10 and c = 10");

        // Conflicting constants
        assertRewrite("a = 10 and a = 20", "false");
        assertRewrite("a = 10 and b = a and b = 20", "false");

        // Different data types
        assertRewrite("a = 1.5 and a = b", "cast(a as decimal(21, 1)) = cast(1.5 as decimal(21, 1)) and a = b");
        assertRewrite("SA = 'test' and SB = SA", "SA = 'test' and SB = 'test'");
        assertRewrite("BA = true and BB = BA", "BA = true and BB = true");

        // Complex arithmetic expressions
        assertRewrite("a = 10 and b = a + 5 and c = b * 2",
                "a = 10 and b = 15 and c = 30");
        assertRewrite("x = 5 and y = x * 2 and z = y + x",
                "x = 5 and y = 10 and z = 15");

        // OR conditions
        assertRewrite("(a = 1 and b = a) or (a = 2 and b = a)",
                "(a = 1 and b = 1) or (a = 2 and b = 2)");
        assertRewrite("a = 5 or (b = a and c = b)",
                "a = 5 or (b = a and c = b)");

        // Mixed AND/OR conditions
        assertRewrite("(a = 1 and b = a) or (c = 2 and d = c and e = d)",
                "(a = 1 and b = 1) or (c = 2 and d = 2 and e = 2)");

        // Multiple arithmetic operations
        assertRewrite("x = 10 and y = x + 5 and z = y * 2 and w = z - x",
                "x = 10 and y = 15 and z = 30 and w = 20");

        // Complex expressions with multiple variables
        assertRewrite("a = 5 and b = a and c = a + b and d = b + c",
                "a = 5 and c = 10 and d = 15 and b = 5");

        // Transitive equality relationships
        assertRewrite("x = y and y = z and z = 100",
                "z = 100 and x = 100 and y = 100");

        // Redundant conditions
        assertRewrite("a = 10 and a = 10 and b = a",
                "a = 10 and b = 10");

        // Mixed constant types in expressions
        assertRewrite("x = 10 and y = x/2 and z = y + 5",
                "x = 10 and y = 5 and z = 10");
    }

    @Test
    void testExpressionNotReplace() {
        // for `a = b`, if a and b's qualifier diff, will not rewrite it.
        assertRewrite("t.a = 1 and t.a = t.b", "a = 1 and b = 1");
        assertRewrite("t1.a = 1 and t1.a = t2.b", "a = 1 and a = b and b = 1");

        // for `a is not null`, if this Not isGeneratedIsNotNull, then will not rewrite it
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true);
        Expression expr1 = ExpressionUtils.and(new EqualTo(a, new IntegerLiteral(1)), new Not(new IsNull(a), false));
        Expression rewrittenExpr1 = executor.replaceConstantsAndRewriteExpr(student, expr1, true, exprRewriteContext);
        Expression expectExpr1 = new EqualTo(a, new IntegerLiteral(1));
        Assertions.assertEquals(expectExpr1, rewrittenExpr1);
        Expression expr2 = ExpressionUtils.and(new EqualTo(a, new IntegerLiteral(1)), new Not(new IsNull(a), true));
        Expression rewrittenExpr2 = executor.replaceConstantsAndRewriteExpr(student, expr2, true, exprRewriteContext);
        Assertions.assertEquals(expr2, rewrittenExpr2);

        // for `a match_any xx`, don't replace it, because the match require left child is column, not literal
        SlotReference b = new SlotReference("b", StringType.INSTANCE, true);
        Expression expr3 = ExpressionUtils.and(new EqualTo(b, new StringLiteral("hello")), new MatchAny(b, new StringLiteral("%ll%")));
        Expression rewrittenExpr3 = executor.replaceConstantsAndRewriteExpr(student, expr3, true, exprRewriteContext);
        Assertions.assertEquals(expr3, rewrittenExpr3);
    }

    @Test
    void testUseInnerInferConstants() {
        // `a = 1` will propagate to `a * 3 = 10` => `3 * 3 = 10` => `FALSE`
        assertRewrite("a = 1 and a * 3 = 10", "FALSE", true);
        // `a = 1` will not propagate.
        assertRewrite("a = 1 and a * 3 = 10", "a = 1 and a * 3 = 10", false);
    }

    @Test
    void testLogicalFilter() {
        Set<Expression> conjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1)),
                new EqualTo(studentId, studentAge)
        );
        Set<Expression> expectConjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1)),
                new EqualTo(studentAge, new IntegerLiteral(1))
        );
        LogicalFilter filter1 = new LogicalFilter<>(conjunctions1, student);
        LogicalFilter rewrittenFilter1 = (LogicalFilter) executor.rewriteRoot(filter1, jobContext);
        Assertions.assertEquals(expectConjunctions1, rewrittenFilter1.getConjuncts());

        Set<Expression> conjunctions2 = ImmutableSet.of(
                new EqualTo(new Add(studentAge, new IntegerLiteral(10)),
                        new Cast(studentGender, BigIntType.INSTANCE))
        );
        Set<Expression> expectConjunctions2 = ImmutableSet.of(
                new EqualTo(studentGender, new IntegerLiteral(11))
        );
        LogicalFilter filter2 = new LogicalFilter<>(conjunctions2, filter1);
        LogicalFilter rewrittenFilter2 = (LogicalFilter) executor.rewriteRoot(filter2, jobContext);
        Assertions.assertEquals(expectConjunctions2, rewrittenFilter2.getConjuncts());
    }

    @Test
    void testLogicalHaving() {
        Set<Expression> conjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1)),
                new EqualTo(studentId, studentAge)
        );
        Set<Expression> expectConjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1)),
                new EqualTo(studentAge, new IntegerLiteral(1))
        );
        LogicalHaving having1 = new LogicalHaving<>(conjunctions1, student);
        LogicalHaving rewrittenHaving1 = (LogicalHaving) executor.rewriteRoot(having1, jobContext);
        Assertions.assertEquals(expectConjunctions1, rewrittenHaving1.getConjuncts());

        Set<Expression> conjunctions2 = ImmutableSet.of(
                new EqualTo(new Add(studentAge, new IntegerLiteral(10)),
                        new Cast(studentGender, BigIntType.INSTANCE))
        );
        Set<Expression> expectConjunctions2 = ImmutableSet.of(
                new EqualTo(studentGender, new IntegerLiteral(11))
        );
        LogicalHaving having2 = new LogicalHaving<>(conjunctions2, having1);
        LogicalHaving rewrittenHaving2 = (LogicalHaving) executor.rewriteRoot(having2, jobContext);
        Assertions.assertEquals(expectConjunctions2, rewrittenHaving2.getConjuncts());
    }

    @Test
    void testLogicalProject() {
        Set<Expression> conjunctions = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1))
        );
        List<NamedExpression> projection = ImmutableList.of(
                // don't replace
                studentId,
                new Alias(new Cast(studentId, StringType.INSTANCE)),

                // replace
                new Alias(new Add(studentId, new IntegerLiteral(10))),
                new Alias(new Add(studentId, new IntegerLiteral(20)), "b")
        );
        List<NamedExpression> expectProjection = ImmutableList.of(
                studentId,
                projection.get(1),
                (NamedExpression) projection.get(2).withChildren(new IntegerLiteral(11)),
                (NamedExpression) projection.get(3).withChildren(new IntegerLiteral(21))
        );
        LogicalFilter filter = new LogicalFilter<>(conjunctions, student);
        LogicalProject project = new LogicalProject<>(projection, filter);
        LogicalProject rewrittenProject = (LogicalProject) executor.rewriteRoot(project, jobContext);
        Assertions.assertEquals(expectProjection, rewrittenProject.getProjects());
    }

    @Test
    void testLogicalSort() {
        Set<Expression> conjunctions = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1))
        );
        List<OrderKey> keys1 = ImmutableList.of(
                new OrderKey(studentId, true, false),
                new OrderKey(studentAge, true, false),
                new OrderKey(new Add(studentId, new IntegerLiteral(10)), true, false),
                new OrderKey(new Add(studentId, studentAge), true, false)
        );
        List<OrderKey> expectKeys1 = ImmutableList.of(
                new OrderKey(studentAge, true, false),
                new OrderKey(new Add(new IntegerLiteral(1), studentAge), true, false)
        );
        LogicalFilter filter = new LogicalFilter<>(conjunctions, student);
        LogicalSort sort1 = new LogicalSort<>(keys1, filter);
        LogicalSort rewrittenSort1 = (LogicalSort) executor.rewriteRoot(sort1, jobContext);
        Assertions.assertEquals(expectKeys1, rewrittenSort1.getOrderKeys());

        List<OrderKey> keys2 = ImmutableList.of(
                new OrderKey(studentId, true, false),
                new OrderKey(new Add(studentId, new IntegerLiteral(10)), true, false)
        );
        LogicalSort sort2 = new LogicalSort<>(keys2, filter);
        Assertions.assertEquals(filter, executor.rewriteRoot(sort2, jobContext));
    }

    @Test
    void testLogicalAggregate() {
        Set<Expression> conjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1))
        );
        List<NamedExpression> projection = Lists.newArrayList(
                new Alias(new Add(studentId, new IntegerLiteral(10))),
                new Alias(new Add(studentAge, new IntegerLiteral(10))),
                new Alias(new Add(studentId, studentAge))
        );
        projection.addAll(student.getOutput());
        LogicalFilter filter1 = new LogicalFilter<>(conjunctions1, student);
        List<Expression> groupby = ImmutableList.of(
                studentId,
                studentAge,
                projection.get(0).toSlot(),
                projection.get(1).toSlot(),
                projection.get(2).toSlot()
        );
        List<Expression> expectGroupby1 = ImmutableList.of(
                groupby.get(1),
                groupby.get(3),
                (new Alias(((SlotReference) groupby.get(4)).getExprId(),
                        new Add(new IntegerLiteral(1), studentAge))).toSlot()
        );
        List<NamedExpression> aggOutput = Lists.newArrayList(
                new Alias(new Sum(studentId)),
                new Alias(new Sum(studentAge)),
                new Alias(new Sum(new Add(studentId, new IntegerLiteral(1)))),
                new Alias(new Sum(new Add(studentAge, new IntegerLiteral(1)))),
                new Alias(new Sum(new Add(studentId, studentAge)))
        );
        aggOutput.addAll((List) groupby);
        List<NamedExpression> expectAggOutput1 = ImmutableList.of(
                (NamedExpression) aggOutput.get(0).withChildren(new Sum(new IntegerLiteral(1))),
                aggOutput.get(1),
                (NamedExpression) aggOutput.get(2).withChildren(new Sum(new IntegerLiteral(2))),
                aggOutput.get(3),
                (NamedExpression) aggOutput.get(4).withChildren(new Sum(new Add(new IntegerLiteral(1), studentAge))),

                // for output also in group by, if is not constant, then just keep it
                aggOutput.get(6),
                aggOutput.get(8),
                aggOutput.get(9)
        );
        List<NamedExpression> expectProjection1 = ImmutableList.of(
                expectAggOutput1.get(0).toSlot(),
                expectAggOutput1.get(1).toSlot(),
                expectAggOutput1.get(2).toSlot(),
                expectAggOutput1.get(3).toSlot(),
                expectAggOutput1.get(4).toSlot(),
                new Alias(aggOutput.get(5).getExprId(), new IntegerLiteral(1), aggOutput.get(5).getName()),
                // for output also in group by, if is not constant, then just keep it
                expectAggOutput1.get(5).toSlot(),
                new Alias(aggOutput.get(7).getExprId(), new IntegerLiteral(11), aggOutput.get(7).getName()),
                expectAggOutput1.get(6).toSlot(),
                expectAggOutput1.get(7).toSlot()
        );
        LogicalAggregate agg1 = new LogicalAggregate<>(groupby, aggOutput, new LogicalProject<>(projection, filter1));
        LogicalProject rewrittenProject1 = (LogicalProject) executor.rewriteRoot(agg1, jobContext);
        LogicalAggregate rewrittenAgg1 = (LogicalAggregate) rewrittenProject1.child();
        Assertions.assertEquals(expectGroupby1, rewrittenAgg1.getGroupByExpressions());
        Assertions.assertEquals(expectAggOutput1, rewrittenAgg1.getOutputExpressions());
        Assertions.assertEquals(expectProjection1, rewrittenProject1.getProjects());

        Set<Expression> conjunctions2 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(3)),
                new EqualTo(studentAge, new IntegerLiteral(3))
        );
        List<Expression> expectGroupby2 = ImmutableList.of(
                groupby.get(0)
        );
        List<NamedExpression> expectAggOutput2 = ImmutableList.of(
                (NamedExpression) aggOutput.get(0).withChildren(new Sum(new IntegerLiteral(3))),
                (NamedExpression) aggOutput.get(1).withChildren(new Sum(new IntegerLiteral(3))),
                (NamedExpression) aggOutput.get(2).withChildren(new Sum(new IntegerLiteral(4))),
                (NamedExpression) aggOutput.get(3).withChildren(new Sum(new IntegerLiteral(4))),
                (NamedExpression) aggOutput.get(4).withChildren(new Sum(new IntegerLiteral(6))),
                aggOutput.get(5)
        );
        List<NamedExpression> expectProjection2 = ImmutableList.of(
                expectAggOutput2.get(0).toSlot(),
                expectAggOutput2.get(1).toSlot(),
                expectAggOutput2.get(2).toSlot(),
                expectAggOutput2.get(3).toSlot(),
                expectAggOutput2.get(4).toSlot(),
                new Alias(expectAggOutput2.get(5).getExprId(), new IntegerLiteral(3)),
                new Alias(aggOutput.get(6).getExprId(), new IntegerLiteral(3), aggOutput.get(6).getName()),
                new Alias(aggOutput.get(7).getExprId(), new IntegerLiteral(13), aggOutput.get(7).getName()),
                new Alias(aggOutput.get(8).getExprId(), new IntegerLiteral(13), aggOutput.get(8).getName()),
                new Alias(aggOutput.get(9).getExprId(), new IntegerLiteral(6), aggOutput.get(9).getName())
        );
        LogicalFilter filter2 = new LogicalFilter<>(conjunctions2, student);
        LogicalAggregate agg2 = new LogicalAggregate<>(groupby, aggOutput, new LogicalProject<>(projection, filter2));
        LogicalProject rewrittenProject2 = (LogicalProject) executor.rewriteRoot(agg2, jobContext);
        LogicalAggregate rewrittenAgg2 = (LogicalAggregate) rewrittenProject2.child();
        Assertions.assertEquals(expectGroupby2, rewrittenAgg2.getGroupByExpressions());
        Assertions.assertEquals(expectAggOutput2, rewrittenAgg2.getOutputs());
        Assertions.assertEquals(expectProjection2, rewrittenProject2.getProjects());
    }

    @Test
    void testLogicalJoin() {
        Set<Expression> conjunctions1 = ImmutableSet.of(
                new EqualTo(studentId, new IntegerLiteral(1))
        );
        Set<Expression> conjunctions2 = ImmutableSet.of(
                new EqualTo(scoreCid, new IntegerLiteral(2))
        );
        LogicalFilter left = new LogicalFilter<>(conjunctions1, student);
        LogicalFilter right = new LogicalFilter<>(conjunctions2, score);

        List<Expression> hashConjuncts1 = ImmutableList.of(
                new EqualTo(studentId, scoreSid),
                TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(
                                TypeCoercionUtils.processBinaryArithmetic(new Add(studentId, new IntegerLiteral(10))),
                                scoreGrade))
        );
        List<Expression> expectHashConjuncts1 = ImmutableList.of(
                new EqualTo(studentId, scoreSid)
        );
        List<Expression> otherConjuncts1 = ImmutableList.of(
                new EqualTo(TypeCoercionUtils.processBinaryArithmetic(new Add(studentId, studentAge)), new BigIntLiteral(50L))
        );
        List<Expression> expectOtherConjuncts1 = ImmutableList.of(
                new EqualTo(scoreGrade, new DoubleLiteral(11)),
                new EqualTo(studentAge, new IntegerLiteral(49)),
                new EqualTo(scoreSid, new IntegerLiteral(1))
        );
        LogicalJoin join1 = new LogicalJoin<>(JoinType.CROSS_JOIN, hashConjuncts1, otherConjuncts1, left, right, null);
        LogicalJoin rewrittenJoin1 = (LogicalJoin) executor.rewriteRoot(join1, jobContext);
        Assertions.assertEquals(JoinType.INNER_JOIN, rewrittenJoin1.getJoinType());
        Assertions.assertEquals(expectHashConjuncts1, rewrittenJoin1.getHashJoinConjuncts());
        Assertions.assertEquals(expectOtherConjuncts1, rewrittenJoin1.getOtherJoinConjuncts());
    }

    private void assertRewrite(String expression, String expected) {
        assertRewrite(expression, expected, true);
    }

    private void assertRewrite(String expression, String expected, boolean useInnerInferConstants) {
        Expression rewriteExpression = parser.parseExpression(expression);
        rewriteExpression = ExpressionRewriteTestHelper.typeCoercion(
                ExpressionRewriteTestHelper.replaceUnboundSlot(rewriteExpression, Maps.newHashMap()));
        rewriteExpression = executor.replaceConstantsAndRewriteExpr(student, rewriteExpression,
                useInnerInferConstants, exprRewriteContext);
        Expression expectedExpression = parser.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewriteExpression.toSql());
    }

}
