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

import org.apache.doris.nereids.rules.rewrite.UnequalPredicateInfer.InferenceGraph;
import org.apache.doris.nereids.rules.rewrite.UnequalPredicateInfer.InferenceGraph.Relation;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PredicateInferUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class UnequalPredicateInferTest {
    @Test
    public void testInferWithTransitiveEqualitySameTable() {
        // t1.a = t1.b, t1.b = t1.c  only output 2 predicates
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        EqualTo equalTo1 = new EqualTo(a, b);
        EqualTo equalTo2 = new EqualTo(b, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Set<? extends Expression> result = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        EqualTo expected1 = new EqualTo(a, b);
        EqualTo expected2 = new EqualTo(a, c);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(expected1) && result.contains(expected2));
    }

    @Test
    public void testTopoSort() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        // a b c has index 0 1 2 (sort by toSql())
        // a>b b>c
        ComparisonPredicate gt1 = new GreaterThan(a, b);
        ComparisonPredicate gt2 = new GreaterThan(b, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(gt1);
        inputs.add(gt2);
        UnequalPredicateInfer.InferenceGraph inferenceGraph = new UnequalPredicateInfer.InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        List<Integer> res = inferenceGraph.topoSort();
        // list(2,1,0) means order c b a
        List<Integer> expected = Arrays.asList(2, 1, 0);
        Assertions.assertEquals(expected, res);
        // a>=b b>=c
        ComparisonPredicate gte1 = new GreaterThanEqual(a, b);
        ComparisonPredicate gte2 = new GreaterThanEqual(b, c);
        Set<Expression> inputs2 = new LinkedHashSet<>();
        inputs2.add(gte1);
        inputs2.add(gte2);
        UnequalPredicateInfer.InferenceGraph inferenceGraph2 = new UnequalPredicateInfer.InferenceGraph(inputs2);
        inferenceGraph2.deduce(inferenceGraph2.getGraph());
        List<Integer> res2 = inferenceGraph2.topoSort();
        List<Integer> expected2 = Arrays.asList(2, 1, 0);
        Assertions.assertEquals(expected2, res2);
        // a<=b b<=c
        ComparisonPredicate lte1 = new LessThanEqual(a, b);
        ComparisonPredicate lte2 = new LessThanEqual(b, c);
        Set<Expression> inputs3 = new LinkedHashSet<>();
        inputs3.add(lte1);
        inputs3.add(lte2);
        UnequalPredicateInfer.InferenceGraph inferenceGraph3 = new UnequalPredicateInfer.InferenceGraph(inputs3);
        inferenceGraph3.deduce(inferenceGraph3.getGraph());
        List<Integer> res3 = inferenceGraph3.topoSort();
        List<Integer> expected3 = Arrays.asList(0, 1, 2);
        Assertions.assertEquals(expected3, res3);
        // a<=b b<c
        ComparisonPredicate lte3 = new LessThanEqual(a, b);
        ComparisonPredicate gt3 = new GreaterThan(c, b);
        Set<Expression> inputs4 = new LinkedHashSet<>();
        inputs4.add(lte3);
        inputs4.add(gt3);
        UnequalPredicateInfer.InferenceGraph inferenceGraph4 = new UnequalPredicateInfer.InferenceGraph(inputs4);
        inferenceGraph4.deduce(inferenceGraph4.getGraph());
        List<Integer> res4 = inferenceGraph4.topoSort();
        List<Integer> expected4 = Arrays.asList(0, 1, 2);
        Assertions.assertEquals(expected4, res4);
    }

    @Test
    public void testTopoSortWithEqual() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        // a=b b>c
        ComparisonPredicate gt1 = new EqualTo(a, b);
        ComparisonPredicate gt2 = new GreaterThan(b, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(gt1);
        inputs.add(gt2);
        UnequalPredicateInfer.InferenceGraph inferenceGraph = new UnequalPredicateInfer.InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        List<Integer> res = inferenceGraph.topoSort();
        // order is c a b
        List<Integer> expected = Arrays.asList(2, 0, 1);
        Assertions.assertEquals(expected, res);
    }

    @Test
    public void testTopoSortWithEqualMulti() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal d = new IntegerLiteral(1);
        // a=b b>c 1<c
        ComparisonPredicate eq = new EqualTo(a, b);
        ComparisonPredicate gt = new GreaterThan(b, c);
        ComparisonPredicate lte = new LessThanEqual(d, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq);
        inputs.add(gt);
        inputs.add(lte);
        UnequalPredicateInfer.InferenceGraph inferenceGraph = new UnequalPredicateInfer.InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        List<Integer> res = inferenceGraph.topoSort();
        // order is 1 c a b
        List<Integer> expected = Arrays.asList(0, 3, 1, 2);
        Assertions.assertEquals(expected, res);
    }

    public void initGraph(Relation[][] g, int size) {
        for (int i = 0; i < size; ++i) {
            for (int j = 0; j < size; ++j) {
                g[i][j] = Relation.UNDEFINED;
            }
        }
    }

    public static void assert2DArrayEquals(Relation[][] expected, Relation[][] actual) {
        for (int i = 0; i < expected.length; i++) {
            Assertions.assertArrayEquals(expected[i], actual[i], "Row " + i + " is not equal");
        }
    }

    // t1.a = 1, t1.b = 1 -> t1.a = 1, t1.b = 1
    @Test
    public void testChooseEqualPredicatesSameTable1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, d);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2));
    }

    // t1.a = 1, t1.b = 1, t1.c = 1 -> t1.a = 1, t1.b = 1, t1.c = 1
    @Test
    public void testChooseEqualPredicatesSameTable2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, d);
        ComparisonPredicate eq3 = new EqualTo(c, d);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[4][4];
        initGraph(expected, 4);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[0][3] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[3][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2)
                && equalWithLiteral.contains(3));
    }

    // t1.a = 1, t1.b = t1.a -> t1.a = 1, t1.b = 1
    @Test
    public void testChooseEqualPredicatesSameTable3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2));
    }

    // t1.a = 1, t1.b = t1.a, t1.a = t1.c -> t1.a = 1, t1.b = 1, t1.c = 1
    @Test
    public void testChooseEqualPredicatesSameTable4() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, a);
        ComparisonPredicate eq3 = new EqualTo(c, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[4][4];
        initGraph(expected, 4);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[0][3] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[3][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2)
                && equalWithLiteral.contains(3));
    }

    // t1.a = 1, t1.b = t1.a, t1.d = t1.c -> t1.a = 1, t1.b = 1, t1.c = t1.d
    @Test
    public void testChooseEqualPredicatesSameTable5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, literal);
        ComparisonPredicate eq2 = new EqualTo(b, a);
        ComparisonPredicate eq3 = new EqualTo(d, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[5][5];
        initGraph(expected, 5);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[3][4] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[4][3] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2));
    }

    @Test
    // t1.a = 1, t2.b = 1 -> t1.a = 1, t2.b = 1
    public void testChooseEqualPredicatesDiffTable1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, d);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2));
    }

    // t1.a = 1, t2.b = 1, t3.c = 1 -> t1.a = 1, t2.b = 1, t2.c = 1
    @Test
    public void testChooseEqualPredicatesDiffTable2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, d);
        ComparisonPredicate eq3 = new EqualTo(c, d);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[4][4];
        initGraph(expected, 4);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[0][3] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[3][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2)
                && equalWithLiteral.contains(3));
    }

    // t1.a = 1, t2.b = t1.a, t1.a = t3.c -> t1.a = 1, t2.b = 1, t3.c = 1
    @Test
    public void testChooseEqualPredicatesDiffTable3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        Literal d = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, d);
        ComparisonPredicate eq2 = new EqualTo(b, a);
        ComparisonPredicate eq3 = new EqualTo(c, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[4][4];
        initGraph(expected, 4);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[0][3] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[3][0] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2)
                && equalWithLiteral.contains(3));
    }

    // t1.a = 1, t2.b = t1.a, t4.d = t3.c -> t1.a = 1, t2.b = 1, t4.d = t3.c
    @Test
    public void testChooseEqualPredicatesDiffTable5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE, true, ImmutableList.of("t4"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate eq1 = new EqualTo(a, literal);
        ComparisonPredicate eq2 = new EqualTo(b, a);
        ComparisonPredicate eq3 = new EqualTo(d, c);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(eq1);
        inputs.add(eq2);
        inputs.add(eq3);
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithLiteral = new HashSet<>();
        Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithLiteral);
        Relation[][] expected = new Relation[5][5];
        initGraph(expected, 5);
        expected[0][1] = Relation.EQ;
        expected[0][2] = Relation.EQ;
        expected[1][0] = Relation.EQ;
        expected[2][0] = Relation.EQ;
        expected[3][4] = Relation.EQ;
        expected[4][3] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
        Assertions.assertTrue(equalWithLiteral.contains(1) && equalWithLiteral.contains(2));
        Set<Expression> chosenInputs = inferenceGraph.chooseInputPredicates(chosen);
        // expected[3][4] (t1.d=t1.c) choose in chooseInputPredicates
        Assertions.assertTrue(chosenInputs.contains(eq3));
    }

    // a>1 b>a -> a>1 b>a
    @Test
    public void testChooseUnequalPredicatesSameTable1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate cmp1 = new GreaterThan(a, literal);
        ComparisonPredicate cmp2 = new GreaterThan(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(2, sets.size());
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithConstant = new HashSet<>();
        InferenceGraph.Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithConstant);
        inferenceGraph.chooseUnequalPredicates(chosen, equalWithConstant);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[1][0] = Relation.GT;
        expected[2][1] = Relation.GT;
        assert2DArrayEquals(expected, chosen);
    }

    // a<1 b=a -> b<1 b=a
    @Test
    public void testChooseUnequalPredicatesSameTable2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate cmp1 = new LessThan(a, literal);
        ComparisonPredicate cmp2 = new EqualTo(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(2, sets.size());
        Assertions.assertTrue(sets.contains(new LessThan(b, literal)) && sets.contains(cmp2));
        for (Expression e : sets) {
            if (e.equals(cmp2)) {
                Assertions.assertFalse(e.isInferred());
            } else {
                Assertions.assertTrue(e.isInferred());
            }
        }
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithConstant = new HashSet<>();
        InferenceGraph.Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithConstant);
        inferenceGraph.chooseUnequalPredicates(chosen, equalWithConstant);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[1][2] = Relation.EQ;
        expected[2][1] = Relation.EQ;
        expected[0][2] = Relation.GT;
        assert2DArrayEquals(expected, chosen);
    }

    // t1.a>1 t1.b>t1.a -> t1.a>1,t1.b>1,t1.b>t1.a
    @Test
    public void testChooseUnequalPredicatesDiffTable1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate cmp1 = new GreaterThan(a, literal);
        ComparisonPredicate cmp2 = new GreaterThan(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(3, sets.size());
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithConstant = new HashSet<>();
        InferenceGraph.Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithConstant);
        inferenceGraph.chooseUnequalPredicates(chosen, equalWithConstant);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        // t1.a>1,t1.b>1 is chosen in chooseUnequalPredicates
        expected[1][0] = Relation.GT;
        expected[2][0] = Relation.GT;
        assert2DArrayEquals(expected, chosen);
    }

    // t1.a<1 t2.b=t1.a -> t2.b<1 t2.a<1 t2.b=t1.a
    @Test
    public void testChooseUnequalPredicatesDiffTable2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate cmp1 = new LessThan(b, literal);
        ComparisonPredicate cmp2 = new EqualTo(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(3, sets.size());
        Assertions.assertTrue(sets.contains(new LessThan(b, literal)) && sets.contains(cmp2) && sets.contains(cmp1));
        for (Expression e : sets) {
            if (e.equals(cmp1) || e.equals(cmp2)) {
                Assertions.assertFalse(e.isInferred());
            } else {
                Assertions.assertTrue(e.isInferred());
            }
        }
        InferenceGraph inferenceGraph = new InferenceGraph(inputs);
        inferenceGraph.deduce(inferenceGraph.getGraph());
        Set<Integer> equalWithConstant = new HashSet<>();
        InferenceGraph.Relation[][] chosen = inferenceGraph.chooseEqualPredicates(equalWithConstant);
        inferenceGraph.chooseUnequalPredicates(chosen, equalWithConstant);
        Relation[][] expected = new Relation[3][3];
        initGraph(expected, 3);
        expected[0][2] = Relation.GT;
        expected[0][1] = Relation.GT;
        expected[1][2] = Relation.EQ;
        expected[2][1] = Relation.EQ;
        assert2DArrayEquals(expected, chosen);
    }

    // t1.a=t2.b t1.a=t3.c t2.b=t3.c -> t1.a=t2.b t1.a=t3.c t2.b=t3.c
    @Test
    public void testInferWithTransitiveEqualityDifferentTableThreeConjuncts1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        ComparisonPredicate cmp1 = new EqualTo(a, b);
        ComparisonPredicate cmp2 = new EqualTo(a, c);
        ComparisonPredicate cmp3 = new EqualTo(b, c);

        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        inputs.add(cmp3);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(3, sets.size());
        Assertions.assertTrue(sets.contains(cmp1) && sets.contains(cmp2) && sets.contains(cmp3));
    }

    // t1.a=t3.c t1.a=t2.b t1.b=t3.c -> t1.a=t2.b t1.a=t3.c t2.b=t3.c
    @Test
    public void testInferWithTransitiveEqualityDifferentTableTwoConjuncts() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        ComparisonPredicate cmp1 = new EqualTo(a, c);
        ComparisonPredicate cmp2 = new EqualTo(a, b);
        ComparisonPredicate cmp3 = new EqualTo(b, c);

        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        Assertions.assertEquals(3, sets.size());
        Assertions.assertTrue(sets.contains(cmp1) && sets.contains(cmp2) && sets.contains(cmp3));
    }

    // t1.a=t3.c t1.a=t2.b t1.b=t3.c -> t1.a=t2.b t1.a=t3.c t2.b=t3.c
    @Test
    public void testUtilChooseMultiEquals() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        ComparisonPredicate cmp1 = new EqualTo(a, c);
        ComparisonPredicate cmp2 = new EqualTo(a, b);
        ComparisonPredicate cmp3 = new EqualTo(b, c);

        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        inputs.add(cmp3);
        Set<? extends Expression> sets = PredicateInferUtils.inferPredicate(inputs);
        Assertions.assertEquals(3, sets.size());
        Assertions.assertTrue(sets.contains(cmp1) && sets.contains(cmp2) && sets.contains(cmp3));
    }

    // t1.a=t3.c t1.a=t2.b -> t1.a=t2.b t1.a=t3.c t2.b=t3.c
    @Test
    public void testUtilChooseMultiEquals2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t3"));
        ComparisonPredicate cmp1 = new EqualTo(a, c);
        ComparisonPredicate cmp2 = new EqualTo(a, b);
        ComparisonPredicate cmp3 = new EqualTo(b, c);

        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = PredicateInferUtils.inferPredicate(inputs);
        Assertions.assertEquals(3, sets.size());
        Assertions.assertTrue(sets.contains(cmp1) && sets.contains(cmp2) && sets.contains(cmp3));
    }

    @Test
    public void testPredicateUtils() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        Literal literal = new IntegerLiteral(1);
        ComparisonPredicate cmp1 = new LessThan(a, literal);
        ComparisonPredicate cmp2 = new EqualTo(b, a);
        Set<Expression> inputs = new LinkedHashSet<>();
        inputs.add(cmp1);
        inputs.add(cmp2);
        Set<? extends Expression> sets = PredicateInferUtils.inferPredicate(inputs);
        Assertions.assertEquals(2, sets.size());
        Assertions.assertTrue(sets.contains(new LessThan(b, literal)) && sets.contains(cmp2));
        for (Expression e : sets) {
            if (e.equals(cmp2)) {
                Assertions.assertFalse(e.isInferred());
            } else {
                Assertions.assertTrue(e.isInferred());
            }
        }
    }

    @Test
    public void testInferWithTransitiveEqualityWithCastDateToDateTime() {
        // cast(d_datev2 as datetime) = cast(d_datev2 as datetime)
        SlotReference a = new SlotReference("a", DateV2Type.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", DateV2Type.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", DateTimeType.INSTANCE, true, ImmutableList.of("t3"));
        EqualTo equalTo1 = new EqualTo(new Cast(a, DateTimeType.INSTANCE), c);
        EqualTo equalTo2 = new EqualTo(new Cast(b, DateTimeType.INSTANCE), c);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Set<? extends Expression> result = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        EqualTo expected = new EqualTo(a, b);
        Assertions.assertTrue(result.contains(expected) || result.contains(expected.commute()), "Expected to find a = b in the result.");
    }

    @Test
    public void testInferWithTransitiveEqualityWithCastDatev2andDate() {
        // cast(d_datev2 as date) = cast(d_date as d_datev2)
        SlotReference a = new SlotReference("a", DateV2Type.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", DateV2Type.INSTANCE, true, ImmutableList.of("t2"));
        SlotReference c = new SlotReference("c", DateType.INSTANCE, true, ImmutableList.of("t3"));
        EqualTo equalTo1 = new EqualTo(new Cast(a, DateType.INSTANCE), c);
        EqualTo equalTo2 = new EqualTo(b, new Cast(c, DateV2Type.INSTANCE));

        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Set<? extends Expression> result = UnequalPredicateInfer.inferUnequalPredicates(inputs);
        EqualTo expected = new EqualTo(a, b);
        Assertions.assertTrue(result.contains(expected) || result.contains(expected.commute()), "Expected to find a = b in the result.");
    }
}
