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

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SignBit;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class InferPredicateByReplaceTest {
    @Test
    public void testDoNotInferNoneMovablePredicateInsideOr() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression predicate = new Or(
                new AssertTrue(new GreaterThan(a, new IntegerLiteral(0)), new StringLiteral("bad")),
                new GreaterThan(a, new IntegerLiteral(10)));
        Set<Expression> inputs = new LinkedHashSet<>(ImmutableList.of(new EqualTo(a, b), predicate));

        Assertions.assertEquals(inputs, InferPredicateByReplace.infer(inputs));
        Assertions.assertEquals(inputs, PredicateInferUtils.inferAllPredicate(inputs));
    }

    @Test
    public void testInferWithEqualTo() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(1, result.size(), "Expected no additional predicates.");
    }

    @Test
    public void testInferWithInPredicate() {
        // abs(a) IN (1, 2, 3)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(new Abs(a),
                ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(inPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testInferWithInPredicateNotSupport() {
        // a IN (1, b)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(a,
                ImmutableList.of(new IntegerLiteral(1), b));
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(inPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testInferWithNotPredicate() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(a, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)));
        Not notPredicate = new Not(inPredicate);
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(notPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Not expected = new Not(new InPredicate(b, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))));
        Assertions.assertTrue(result.contains(expected));
    }

    @Test
    public void testInferWithLikePredicate() {
        // a LIKE 'test%'
        SlotReference a = new SlotReference("a", StringType.INSTANCE);
        SlotReference b = new SlotReference("b", StringType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Like like = new Like(a, new StringLiteral("test%"));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(like);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Like expected = new Like(b, new StringLiteral("test%"));
        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.contains(expected), "Expected to find b like 'test%' in the result");
    }

    @Test
    public void testInferWithLikePredicateNotSupport() {
        // a LIKE b
        SlotReference a = new SlotReference("a", StringType.INSTANCE);
        SlotReference b = new SlotReference("b", StringType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Like like = new Like(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(like);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testInferWithOrPredicate() {
        SlotReference a = new SlotReference("a", DateTimeV2Type.SYSTEM_DEFAULT);
        SlotReference b = new SlotReference("b", DateTimeV2Type.SYSTEM_DEFAULT);
        EqualTo equalTo = new EqualTo(a, b);
        Or or = new Or(new GreaterThan(a, new DateTimeV2Literal("2022-02-01 10:00:00")),
                new LessThan(a, new DateTimeV2Literal("2022-01-01 10:00:00")));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(or);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testInferWithPredicateDateTrunc() {
        SlotReference a = new SlotReference("a", DateTimeV2Type.SYSTEM_DEFAULT);
        SlotReference b = new SlotReference("b", DateTimeV2Type.SYSTEM_DEFAULT);
        EqualTo equalTo = new EqualTo(a, b);
        GreaterThan greaterThan = new GreaterThan(new DateTrunc(a, new VarcharLiteral("year")), new DateTimeV2Literal("2022-02-01 10:00:00"));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(greaterThan);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testInferWithDateTimeV2CastPrecision() {
        for (int sourceScale : new int[] {0, 3, 6}) {
            SlotReference a = new SlotReference("a", DateTimeV2Type.of(sourceScale));
            SlotReference b = new SlotReference("b", DateTimeV2Type.of(sourceScale));
            InPredicate predicate = new InPredicate(a, ImmutableList.of(
                    new DateTimeV2Literal(DateTimeV2Type.of(sourceScale), "2025-01-01 00:00:00"),
                    new DateTimeV2Literal(DateTimeV2Type.of(sourceScale), "2025-01-01 00:00:01")));
            InPredicate expected = new InPredicate(b, predicate.getOptions());
            for (int targetScale : new int[] {0, 3, 6}) {
                EqualTo equality = new EqualTo(new Cast(a, DateTimeV2Type.of(targetScale)),
                        new Cast(b, DateTimeV2Type.of(targetScale)));
                Set<Expression> inputs = new HashSet<>(ImmutableList.of(equality, predicate));
                Assertions.assertEquals(sourceScale <= targetScale,
                        InferPredicateByReplace.infer(inputs).contains(expected),
                        "Unexpected inference for scale " + sourceScale + " -> " + targetScale);
            }
            EqualTo nestedEquality = new EqualTo(
                    new Cast(new Cast(a, DateTimeV2Type.of(0)), DateTimeV2Type.of(6)),
                    new Cast(new Cast(b, DateTimeV2Type.of(0)), DateTimeV2Type.of(6)));
            Set<Expression> inputs = new HashSet<>(ImmutableList.of(nestedEquality, predicate));
            Assertions.assertEquals(sourceScale == 0, InferPredicateByReplace.infer(inputs).contains(expected),
                    "Nested casts must not hide precision loss");
        }
    }

    @Test
    public void testValidForInfer() {
        SlotReference a = new SlotReference("a", TinyIntType.INSTANCE);
        Cast castExprA = new Cast(a, IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", BigIntType.INSTANCE);
        Cast castExprB = new Cast(b, IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", DateType.INSTANCE);
        Cast castExprC = new Cast(c, IntegerType.INSTANCE);

        EqualTo equalTo1 = new EqualTo(castExprA, castExprB);
        EqualTo equalTo2 = new EqualTo(castExprA, castExprC);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Assertions.assertEquals(2, InferPredicateByReplace.infer(inputs).size());
    }

    @Test
    public void testTimestampNsCastIsNotRemovedForPredicateInference() {
        SlotReference timestampNs = new SlotReference("timestamp_ns", TimeStampNsType.INSTANCE);
        Cast roundedToMicroseconds = new Cast(timestampNs, DateTimeV2Type.of(6));
        EqualTo equalTo = new EqualTo(
                roundedToMicroseconds, new DateTimeV2Literal("2024-01-01 00:00:00.123457"));

        Assertions.assertFalse(PredicateInferUtils.getPairFromCast(equalTo).isPresent());

        Cast roundedToSeconds = new Cast(timestampNs, DateTimeType.INSTANCE);
        EqualTo legacyEqualTo = new EqualTo(
                roundedToSeconds, new DateTimeLiteral("2024-01-01 00:00:00"));
        Assertions.assertFalse(PredicateInferUtils.getPairFromCast(legacyEqualTo).isPresent());
    }

    @Test
    public void testTimestampTzCastIsNotRemovedForPredicateInference() {
        for (int sourceScale : new int[] {0, 3, 6}) {
            SlotReference timestampTz = new SlotReference("tz", TimeStampTzType.of(sourceScale));
            for (DataType target : ImmutableList.of(DateTimeType.INSTANCE,
                    DateTimeV2Type.of(0), DateTimeV2Type.of(3), DateTimeV2Type.of(6))) {
                SlotReference localTime = new SlotReference("dt", target);
                Cast cast = new Cast(timestampTz, target);
                Assertions.assertFalse(PredicateInferUtils.getPairFromCast(new EqualTo(cast, localTime)).isPresent());
                Assertions.assertFalse(PredicateInferUtils.getPairFromCast(new GreaterThan(cast, localTime)).isPresent());
                Assertions.assertFalse(PredicateInferUtils.getPairFromCast(
                        new EqualTo(new Cast(cast, DateTimeV2Type.of(6)),
                                new Cast(localTime, DateTimeV2Type.of(6)))).isPresent());
            }
        }
    }

    @Test
    public void testNotInferWithTransitiveEqualitySameTable() {
        // a = b, b = c
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        EqualTo equalTo1 = new EqualTo(a, b);
        EqualTo equalTo2 = new EqualTo(b, c);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    static Stream<Arguments> replacementTypes() {
        List<Arguments> cases = new ArrayList<>();
        for (DataType type : ImmutableList.of(BooleanType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE,
                StringType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(6),
                DecimalV3Type.createDecimalV3Type(9, 2), TimeStampNsType.INSTANCE,
                CharType.createCharType(10), VarcharType.createVarcharType(10),
                IPv4Type.INSTANCE, IPv6Type.INSTANCE)) {
            cases.add(Arguments.of(type, type, true));
        }
        for (List<DataType> pair : ImmutableList.<List<DataType>>of(
                ImmutableList.of(DateV2Type.INSTANCE, DateTimeV2Type.of(0)),
                ImmutableList.of(DateTimeV2Type.of(0), DateTimeV2Type.of(6)),
                ImmutableList.of(IntegerType.INSTANCE, BigIntType.INSTANCE),
                ImmutableList.of(DecimalV3Type.createDecimalV3Type(9, 2),
                        DecimalV3Type.createDecimalV3Type(9, 3)))) {
            cases.add(Arguments.of(pair.get(0), pair.get(1), false));
            cases.add(Arguments.of(pair.get(1), pair.get(0), false));
        }
        cases.add(Arguments.of(FloatType.INSTANCE, FloatType.INSTANCE, false));
        cases.add(Arguments.of(DoubleType.INSTANCE, DoubleType.INSTANCE, false));
        cases.add(Arguments.of(ArrayType.of(DoubleType.INSTANCE), ArrayType.of(DoubleType.INSTANCE), false));
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} -> {1}, replace={2}")
    @MethodSource("replacementTypes")
    public void testTypeSensitiveReplacement(DataType sourceType, DataType targetType, boolean canReplace) {
        SlotReference a = new SlotReference("a", sourceType);
        SlotReference b = new SlotReference("b", targetType);
        Expression equality = TypeCoercionUtils.processComparisonPredicate(new EqualTo(a, b));
        Expression predicate = new EqualTo(new Length(new Cast(a, StringType.INSTANCE)), new IntegerLiteral(10));
        Set<Expression> inputs = new LinkedHashSet<>(ImmutableList.of(equality, predicate));
        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        if (canReplace) {
            Expression expected = new EqualTo(new Length(new Cast(b, StringType.INSTANCE)), new IntegerLiteral(10));
            Assertions.assertTrue(result.contains(expected), () -> "Missing " + expected + " in " + result);
        } else {
            Assertions.assertEquals(inputs, result);
        }
    }

    @Test
    public void testSignedZeroInOr() {
        SlotReference x = new SlotReference("x", DoubleType.INSTANCE);
        SlotReference y = new SlotReference("y", DoubleType.INSTANCE);
        Expression predicate = new Or(new SignBit(x), new GreaterThan(x, new DoubleLiteral(1.0)));
        Set<Expression> inputs = new LinkedHashSet<>(ImmutableList.of(new EqualTo(x, y), predicate));
        // x = -0.0 and y = +0.0 satisfy the input, but not the predicate with x replaced by y.
        Assertions.assertEquals(inputs, InferPredicateByReplace.infer(inputs));
        Assertions.assertEquals(inputs, PredicateInferUtils.inferPredicate(inputs));
    }

    @Test
    public void testSafeComparisonPropagation() {
        SlotReference x = new SlotReference("x", DoubleType.INSTANCE, true, ImmutableList.of("left"));
        SlotReference y = new SlotReference("y", DoubleType.INSTANCE, true, ImmutableList.of("right"));
        Set<Expression> floating = new LinkedHashSet<>(ImmutableList.of(new EqualTo(x, y),
                new GreaterThan(x, new DoubleLiteral(1.0))));
        Assertions.assertTrue(PredicateInferUtils.inferPredicate(floating)
                .contains(new GreaterThan(y, new DoubleLiteral(1.0))));

        SlotReference small = new SlotReference("small", IntegerType.INSTANCE, true, ImmutableList.of("left"));
        SlotReference wide = new SlotReference("wide", BigIntType.INSTANCE, true, ImmutableList.of("right"));
        Set<Expression> integers = new LinkedHashSet<>(ImmutableList.of(
                new EqualTo(new Cast(small, BigIntType.INSTANCE), wide),
                new GreaterThan(small, new IntegerLiteral(1))));
        Assertions.assertTrue(PredicateInferUtils.inferPredicate(integers).stream()
                .anyMatch(p -> p instanceof GreaterThan && p.child(0).equals(wide)
                        && p.child(1).equals(new BigIntLiteral(1))));
    }

    @Test
    public void testDirectComparisonReplacement() {
        SlotReference small = new SlotReference("small", IntegerType.INSTANCE);
        SlotReference wide = new SlotReference("wide", BigIntType.INSTANCE);
        Expression equality = new EqualTo(new Cast(small, BigIntType.INSTANCE), wide);
        List<Expression> predicates = ImmutableList.of(
                new Not(new EqualTo(small, new IntegerLiteral(10))),
                new InPredicate(small, ImmutableList.of(new IntegerLiteral(10), new IntegerLiteral(20))),
                new Not(new InPredicate(small,
                        ImmutableList.of(new IntegerLiteral(10), new IntegerLiteral(20)))));
        List<Expression> expected = ImmutableList.of(
                new Not(new EqualTo(wide, new BigIntLiteral(10))),
                new InPredicate(wide, ImmutableList.of(new BigIntLiteral(10), new BigIntLiteral(20))),
                new Not(new InPredicate(wide, ImmutableList.of(new BigIntLiteral(10), new BigIntLiteral(20)))));
        for (int i = 0; i < predicates.size(); i++) {
            Set<Expression> inputs = new LinkedHashSet<>(ImmutableList.of(equality, predicates.get(i)));
            Assertions.assertTrue(InferPredicateByReplace.infer(inputs).contains(expected.get(i)));
        }
    }

    @Test
    public void testSameTypeBehindWideningCasts() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression equality = new EqualTo(new Cast(a, BigIntType.INSTANCE), new Cast(b, BigIntType.INSTANCE));
        Expression predicate = new EqualTo(new Length(new Cast(a, StringType.INSTANCE)), new IntegerLiteral(1));
        Set<Expression> inputs = new LinkedHashSet<>(ImmutableList.of(equality, predicate));
        Expression expected = new EqualTo(new Length(new Cast(b, StringType.INSTANCE)), new IntegerLiteral(1));
        Assertions.assertTrue(InferPredicateByReplace.infer(inputs).contains(expected));
    }
}
