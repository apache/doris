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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.IdenticalSignature;
import org.apache.doris.nereids.trees.expressions.functions.ImplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.NullOrIdenticalSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class TemporalConversionTest {
    @Test
    void precisionPromotionPreservesFixedScaleAndStructMetadata() {
        DataType original = new StructType(List.of(
                new StructField("ns", TimeStampNsType.INSTANCE, false, "fixed"),
                new StructField("tz", ArrayType.of(TimeStampTzType.of(2)), true, "zoned"),
                new StructField("time", MapType.of(TimeV2Type.of(1), DateTimeV2Type.of(3)), false, "pair")));
        DataType expected = new StructType(List.of(
                new StructField("ns", TimeStampNsType.INSTANCE, false, "fixed"),
                new StructField("tz", ArrayType.of(TimeStampTzType.of(5)), true, "zoned"),
                new StructField("time", MapType.of(TimeV2Type.of(5), DateTimeV2Type.of(5)), false, "pair")));
        Assertions.assertEquals(expected, TypeCoercionUtils.replaceTimesWithTargetPrecision(original, 5));
        Assertions.assertTrue(TypeCoercionUtils.hasAdjustableTimeScale(original));
        Assertions.assertFalse(TypeCoercionUtils.hasAdjustableTimeScale(ArrayType.of(TimeStampNsType.INSTANCE)));
        Assertions.assertSame(TimeStampNsType.INSTANCE,
                TypeCoercionUtils.replaceTimesWithTargetPrecision(TimeStampNsType.INSTANCE, 3));
    }

    @Test
    void commonTypeCastsKeepRangeChecksInsideContainers() {
        DataType source = ArrayType.of(MapType.of(StringType.INSTANCE, DateV2Type.INSTANCE));
        DataType target = ArrayType.of(MapType.of(StringType.INSTANCE, TimeStampNsType.INSTANCE));
        Expression slot = new SlotReference("dates", source);
        Cast cast = (Cast) TypeCoercionUtils.castForCommonType(slot, target);
        Assertions.assertTrue(cast.isStrict());
        Assertions.assertEquals(target, cast.getDataType());
        Assertions.assertTrue(((Cast) TypeCoercionUtils.castForCommonTypeWithExactTarget(slot, target)).isStrict());
        Assertions.assertSame(slot, TypeCoercionUtils.castForCommonType(slot, source));
        Assertions.assertFalse(TemporalConversionProperties.narrowsCalendarRangeToTimeStampNs(target, source));
    }

    @Test
    void commonTypeCastsPreserveMatchingAndLiteralPromotion() {
        Expression text = new SlotReference("text", VarcharType.createVarcharType(10));
        DataType widerText = VarcharType.createVarcharType(20);
        Assertions.assertSame(text, TypeCoercionUtils.castForCommonType(text, widerText));
        Expression exact = TypeCoercionUtils.castForCommonTypeWithExactTarget(text, widerText);
        Assertions.assertInstanceOf(Cast.class, exact);
        Assertions.assertEquals(widerText, exact.getDataType());
        Assertions.assertSame(text, exact.child(0));
        Assertions.assertFalse(((Cast) exact).isStrict());
        Assertions.assertSame(text, TypeCoercionUtils.implicitCastInputTypes(
                new Nullable(text), List.of(widerText)).child(0));

        Expression datetime = new SlotReference("datetime", DateTimeV2Type.of(3));
        Assertions.assertSame(datetime, TypeCoercionUtils.implicitCastInputTypes(
                new Nullable(datetime), List.of(DateTimeV2Type.WILDCARD)).child(0));
        Assertions.assertEquals(new BigIntLiteral(7),
                TypeCoercionUtils.castForCommonType(new IntegerLiteral(7), BigIntType.INSTANCE));
    }

    @Test
    void calendarRangeCheckPreservesTypedAndUntypedNullCasts() {
        Expression typedNull = new NullLiteral(DateV2Type.INSTANCE);
        Expression narrowed = TypeCoercionUtils.castForCommonType(typedNull, TimeStampNsType.INSTANCE);
        Assertions.assertInstanceOf(Cast.class, narrowed);
        Assertions.assertTrue(((Cast) narrowed).isStrict());
        Assertions.assertSame(typedNull, narrowed.child(0));
        Assertions.assertEquals(new NullLiteral(TimeStampNsType.INSTANCE),
                FoldConstantRuleOnFE.evaluateWithoutContext(narrowed));
        Assertions.assertEquals(new NullLiteral(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.castForCommonTypeWithExactTarget(NullLiteral.INSTANCE, TimeStampNsType.INSTANCE));
    }

    @Test
    void argumentRangeChecksAreForcedOnlyForMixedTemporalInputs() {
        Expression date = new SlotReference("date", DateV2Type.INSTANCE);
        Expression ns = new SlotReference("ns", TimeStampNsType.INSTANCE);
        Expression text = new SlotReference("text", StringType.INSTANCE);
        try (MockedStatic<SessionVariable> session = Mockito.mockStatic(SessionVariable.class)) {
            session.when(SessionVariable::enableStrictCast).thenReturn(false);
            Expression single = TypeCoercionUtils.implicitCastInputTypes(
                    new Nullable(date), List.of(TimeStampNsType.INSTANCE)).child(0);
            Assertions.assertInstanceOf(Cast.class, single);
            Assertions.assertSame(date, single.child(0));
            Assertions.assertFalse(((Cast) single).isStrict());

            Expression mixed = TypeCoercionUtils.implicitCastInputTypes(new CreateStruct(date, ns, text),
                    List.of(TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE));
            Assertions.assertInstanceOf(Cast.class, mixed.child(0));
            Assertions.assertTrue(((Cast) mixed.child(0)).isStrict());
            Assertions.assertSame(date, mixed.child(0).child(0));
            Assertions.assertSame(ns, mixed.child(1));
            Assertions.assertInstanceOf(Cast.class, mixed.child(2));
            Assertions.assertFalse(((Cast) mixed.child(2)).isStrict());
            Assertions.assertSame(text, mixed.child(2).child(0));
        }
    }

    @Test
    void signatureAdmissionAndArgumentCoercionAgreeOnLiteralExactness() {
        Expression exact = new TimeStampNsLiteral("2024-01-02 03:04:05.123000000");
        Expression inexact = new TimeStampNsLiteral("2024-01-02 03:04:05.123000001");
        DataType target = DateTimeV2Type.of(3);
        assertTemporalCoercion(exact, target, true);
        assertTemporalCoercion(inexact, target, false);
        assertTemporalCoercion(new SlotReference("ns", TimeStampNsType.INSTANCE), target, false);
    }

    @Test
    void exactNsLiteralStillPrefersNativeNsSignature() {
        Expression input = new TimeStampNsLiteral("2024-01-02 03:04:05.123000000");
        FunctionSignature dateSignature = FunctionSignature.ret(DateTimeV2Type.of(3)).args(DateTimeV2Type.of(3));
        FunctionSignature nsSignature = FunctionSignature.ret(TimeStampNsType.INSTANCE).args(TimeStampNsType.INSTANCE);
        Assertions.assertEquals(Optional.of(nsSignature),
                searchSignature(List.of(dateSignature, nsSignature), List.of(input)));
        Assertions.assertEquals(Optional.of(nsSignature),
                searchSignature(List.of(nsSignature, dateSignature), List.of(input)));
    }

    @Test
    void exactNsLiteralDoesNotBypassImplicitOnlyMatchingRounds() {
        Expression input = new TimeStampNsLiteral("2024-01-02 03:04:05.123000000");
        DataType target = DateTimeV2Type.of(3);
        FunctionSignature signature = FunctionSignature.ret(target).args(target);
        Optional<FunctionSignature> result = SearchSignature.from(Mockito.mock(ComputeSignature.class),
                List.of(signature), List.of(input))
                .orElseSearch(IdenticalSignature::isIdentical)
                .orElseSearch(NullOrIdenticalSignature::isNullOrIdentical)
                .orElseSearch(ImplicitlyCastableSignature::isImplicitlyCastable)
                .result();
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(Optional.of(signature), searchSignature(List.of(signature), List.of(input)));
    }

    @Test
    void exactNsCastTypeResolvesWildcardPrecisionWithoutRounding() {
        Expression exact = new TimeStampNsLiteral("2024-01-02 03:04:05.123456000");
        Expression inexact = new TimeStampNsLiteral("2024-01-02 03:04:05.123456001");
        List<DataType> targets = List.of(DateTimeV2Type.WILDCARD, TimeStampTzType.WILDCARD);
        List<DataType> concreteTypes = List.of(DateTimeV2Type.MAX, TimeStampTzType.MAX);
        for (int i = 0; i < targets.size(); i++) {
            DataType target = targets.get(i);
            Assertions.assertEquals(Optional.of(concreteTypes.get(i)),
                    TypeCoercionUtils.getExactTimeStampNsLiteralCastType(exact, target));
            Assertions.assertTrue(TypeCoercionUtils.getExactTimeStampNsLiteralCastType(inexact, target).isEmpty());
            FunctionSignature signature = FunctionSignature.ret(target).args(target);
            Assertions.assertEquals(Optional.of(signature), searchSignature(List.of(signature), List.of(exact)));
            Assertions.assertTrue(searchSignature(List.of(signature), List.of(inexact)).isEmpty());
        }
        Assertions.assertEquals("2024-01-02 03:04:05.123456000", ((Literal) exact).getStringValue());
        Assertions.assertEquals("2024-01-02 03:04:05.123456001", ((Literal) inexact).getStringValue());
    }

    @Test
    void exactNsCastTypeRejectsOtherSourcesAndTargets() {
        for (Expression input : List.of(new StringLiteral("2024-01-02 00:00:00"),
                new DateV2Literal("2024-01-02"), new IntegerLiteral(7),
                new Cast(new StringLiteral("invalid"), DateTimeV2Type.MAX, false, true))) {
            Assertions.assertTrue(TypeCoercionUtils.getExactTimeStampNsLiteralCastType(
                    input, DateTimeV2Type.MAX).isEmpty());
        }
        Expression ns = new TimeStampNsLiteral("2024-01-02 00:00:00");
        for (DataType target : List.of(TimeStampNsType.INSTANCE, TimeV2Type.MAX,
                StringType.INSTANCE, BigIntType.INSTANCE)) {
            Assertions.assertTrue(TypeCoercionUtils.getExactTimeStampNsLiteralCastType(ns, target).isEmpty());
        }
    }

    @Test
    void literalExactnessChecksTheValueWithoutRounding() {
        TimeStampNsLiteral value = new TimeStampNsLiteral("2024-01-02 03:04:05.123000000");
        Assertions.assertTrue(TemporalConversionProperties.isTimeStampNsLiteralExactlyRepresentable(
                value, DateTimeV2Type.of(3)));
        Assertions.assertTrue(TemporalConversionProperties.isTimeStampNsLiteralExactlyRepresentable(
                value, TimeStampTzType.of(3)));
        Assertions.assertFalse(TemporalConversionProperties.isTimeStampNsLiteralExactlyRepresentable(
                value, DateTimeV2Type.of(2)));
        Assertions.assertFalse(TemporalConversionProperties.isTimeStampNsLiteralExactlyRepresentable(
                value, DateV2Type.INSTANCE));
        Assertions.assertEquals("2024-01-02 03:04:05.123000000", value.getStringValue());
        assertTemporalCoercion(new Nullable(value), DateTimeV2Type.of(3), true);
    }

    @Test
    void castAdmissionChecksTheCastResultRatherThanItsChild() {
        DataType target = DateTimeV2Type.of(3);
        Expression exact = new Cast(new StringLiteral("2024-01-02 03:04:05.123000000"),
                TimeStampNsType.INSTANCE);
        Expression inexact = new Cast(new StringLiteral("2024-01-02 03:04:05.123000001"),
                TimeStampNsType.INSTANCE);
        Assertions.assertEquals(new TimeStampNsLiteral("2024-01-02 03:04:05.123000000"),
                FoldConstantRuleOnFE.evaluateWithoutContext(exact));
        Assertions.assertEquals(new TimeStampNsLiteral("2024-01-02 03:04:05.123000001"),
                FoldConstantRuleOnFE.evaluateWithoutContext(inexact));
        assertTemporalCoercion(exact, target, true);
        assertTemporalCoercion(inexact, target, false);

        // The explicitly requested intermediate rounding crosses midnight. Only its result,
        // not the original nanosecond literal, can be represented exactly as DATEV2.
        TimeStampNsLiteral original = new TimeStampNsLiteral("2024-01-02 23:59:59.999999999");
        Cast rounded = new Cast(original, DateTimeV2Type.of(0));
        Cast expression = new Cast(rounded, TimeStampNsType.INSTANCE);
        Assertions.assertEquals(new TimeStampNsLiteral("2024-01-03 00:00:00"),
                FoldConstantRuleOnFE.evaluateWithoutContext(expression));
        assertTemporalCoercion(original, DateV2Type.INSTANCE, false);
        assertTemporalCoercion(expression, DateV2Type.INSTANCE, true);
        Assertions.assertSame(rounded, expression.child());
        Assertions.assertSame(original, rounded.child());
    }

    @Test
    void castAdmissionPreservesStrictCastFailures() {
        DateV2Literal outsideRange = new DateV2Literal("1600-01-01");
        try (MockedStatic<SessionVariable> session = Mockito.mockStatic(SessionVariable.class)) {
            session.when(SessionVariable::enableStrictCast).thenReturn(false);
            assertTemporalCastFailure(new Cast(outsideRange, TimeStampNsType.INSTANCE, false, true));
            session.when(SessionVariable::enableStrictCast).thenReturn(true);
            assertTemporalCastFailure(new Cast(outsideRange, TimeStampNsType.INSTANCE));
        }
    }

    @Test
    void nullCastResultsDoNotProveLiteralExactness() {
        DateV2Literal outsideRange = new DateV2Literal("1600-01-01");
        DataType target = DateTimeV2Type.MAX;
        assertTemporalCoercion(new NullLiteral(TimeStampNsType.INSTANCE), target, false);
        try (MockedStatic<SessionVariable> session = Mockito.mockStatic(SessionVariable.class)) {
            session.when(SessionVariable::enableStrictCast).thenReturn(false);
            Cast cast = new Cast(outsideRange, TimeStampNsType.INSTANCE);
            Assertions.assertEquals(new NullLiteral(TimeStampNsType.INSTANCE),
                    FoldConstantRuleOnFE.evaluateWithoutContext(cast));
            assertTemporalCoercion(cast, target, false);

            session.when(SessionVariable::enableStrictCast).thenReturn(true);
            TryCast tryCast = new TryCast(outsideRange, TimeStampNsType.INSTANCE);
            Assertions.assertEquals(new NullLiteral(TimeStampNsType.INSTANCE),
                    FoldConstantRuleOnFE.evaluateWithoutContext(tryCast));
            assertTemporalCoercion(tryCast, target, false);
            assertTemporalCoercion(new Cast(new NullLiteral(StringType.INSTANCE), TimeStampNsType.INSTANCE),
                    target, false);
        }
    }

    @Test
    void castsWithoutEvaluatedNsValuesDoNotProveLiteralExactness() {
        Cast unsupported = new Cast(BooleanLiteral.TRUE, TimeStampNsType.INSTANCE);
        Assertions.assertFalse(CheckCast.check(BooleanLiteral.TRUE.getDataType(), TimeStampNsType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanLiteral.TRUE.getDataType(), TimeStampNsType.INSTANCE, true));
        Cast nonConstant = new Cast(new SlotReference("date", DateV2Type.INSTANCE), TimeStampNsType.INSTANCE);
        Cast backendOnly = new Cast(new Cast(new TimeStampNsLiteral("2024-01-02 00:00:00"),
                VariantType.INSTANCE), TimeStampNsType.INSTANCE);
        for (Cast expression : List.of(unsupported, nonConstant, backendOnly)) {
            Assertions.assertSame(expression, FoldConstantRuleOnFE.evaluateWithoutContext(expression));
            assertTemporalCoercion(expression, DateTimeV2Type.MAX, false);
        }
    }

    private Optional<FunctionSignature> searchSignature(List<FunctionSignature> signatures, List<Expression> inputs) {
        return SearchSignature.from(Mockito.mock(ComputeSignature.class), signatures, inputs)
                .orElseSearch(IdenticalSignature::isIdentical)
                .orElseSearch(NullOrIdenticalSignature::isNullOrIdentical)
                .orElseSearch(ImplicitlyCastableSignature::isImplicitlyCastable)
                .orElseSearch(ExplicitlyCastableSignature::isExplicitlyCastable)
                .result();
    }

    private void assertTemporalCoercion(Expression input, DataType target, boolean accepted) {
        FunctionSignature signature = FunctionSignature.ret(target).args(target);
        Assertions.assertEquals(accepted ? Optional.of(signature) : Optional.empty(),
                searchSignature(List.of(signature), List.of(input)));
        Assertions.assertEquals(accepted ? Optional.of(target) : Optional.empty(),
                TypeCoercionUtils.getExactTimeStampNsLiteralCastType(input, target));
        // Exercise parameter insertion with the same expected type. The proof must not replace
        // the original argument or strip an explicitly requested CAST from its expression tree.
        Expression argument = TypeCoercionUtils.implicitCastInputTypes(
                new Nullable(input), List.of(target)).child(0);
        if (accepted) {
            Assertions.assertInstanceOf(Cast.class, argument);
            Assertions.assertEquals(target, argument.getDataType());
            Assertions.assertSame(input, argument.child(0));
        } else {
            Assertions.assertSame(input, argument);
        }
    }

    private void assertTemporalCastFailure(Cast input) {
        DataType target = DateTimeV2Type.MAX;
        CastException expected = Assertions.assertThrows(CastException.class,
                () -> FoldConstantRuleOnFE.evaluateWithoutContext(input));
        List<FunctionSignature> signatures = List.of(FunctionSignature.ret(target).args(target));
        Assertions.assertEquals(expected.getMessage(), Assertions.assertThrows(CastException.class,
                () -> searchSignature(signatures, List.of(input))).getMessage());
        Assertions.assertEquals(expected.getMessage(), Assertions.assertThrows(CastException.class,
                () -> TypeCoercionUtils.getExactTimeStampNsLiteralCastType(input, target)).getMessage());
        Assertions.assertEquals(expected.getMessage(), Assertions.assertThrows(CastException.class,
                () -> TypeCoercionUtils.implicitCastInputTypes(new Nullable(input), List.of(target))).getMessage());
    }

    @Test
    void mixedTemporalOverloadsDoNotFallBackToStrings() {
        Expression ns = new SlotReference("ns", TimeStampNsType.INSTANCE);
        Expression date = new SlotReference("date", DateV2Type.INSTANCE);
        FunctionSignature nsSignature = FunctionSignature.ret(TimeStampNsType.INSTANCE)
                .args(TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
        FunctionSignature dateSignature = FunctionSignature.ret(DateTimeV2Type.MAX)
                .args(DateTimeV2Type.MAX, DateTimeV2Type.MAX);
        FunctionSignature stringSignature = FunctionSignature.ret(StringType.INSTANCE)
                .args(StringType.INSTANCE, StringType.INSTANCE);
        Assertions.assertEquals(Optional.of(nsSignature), searchSignature(
                List.of(stringSignature, dateSignature, nsSignature), List.of(ns, date)));
        Assertions.assertTrue(searchSignature(List.of(dateSignature, stringSignature), List.of(ns, date)).isEmpty());
        Assertions.assertTrue(searchSignature(List.of(nsSignature), List.of(date, date)).isEmpty());
    }

    @Test
    void nestedTemporalArgumentsKeepStrictRangeChecks() {
        DataType nsArray = ArrayType.of(TimeStampNsType.INSTANCE);
        DataType dates = MapType.of(StringType.INSTANCE, DateTimeV2Type.MAX);
        DataType nsDates = MapType.of(StringType.INSTANCE, TimeStampNsType.INSTANCE);
        DataType both = new StructType(List.of(
                new StructField("ns", nsArray, true, ""), new StructField("dates", dates, true, "")));
        DataType promotedBoth = new StructType(List.of(
                new StructField("ns", nsArray, true, ""), new StructField("dates", nsDates, true, "")));
        try (MockedStatic<SessionVariable> session = Mockito.mockStatic(SessionVariable.class)) {
            session.when(SessionVariable::enableStrictCast).thenReturn(false);
            assertNestedArgumentRangeChecks(List.of(both), List.of(promotedBoth), true);
            assertNestedArgumentRangeChecks(List.of(nsArray, dates), List.of(nsArray, nsDates), true);
            assertNestedArgumentRangeChecks(List.of(dates, nsArray), List.of(nsDates, nsArray), true);
            assertNestedArgumentRangeChecks(List.of(MapType.of(TimeStampNsType.INSTANCE, TimeStampTzType.MAX)),
                    List.of(MapType.of(TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE)), true);
            assertNestedArgumentRangeChecks(List.of(nsArray), List.of(nsArray), false);
            assertNestedArgumentRangeChecks(List.of(dates), List.of(nsDates), false);
            Assertions.assertTrue(TypeCoercionUtils.implicitCastInputTypes(new Array(), List.of()).children().isEmpty());
        }
    }

    private void assertNestedArgumentRangeChecks(List<DataType> sourceTypes, List<DataType> targetTypes,
            boolean strict) {
        List<Expression> arguments = new ArrayList<>();
        for (int i = 0; i < sourceTypes.size(); i++) {
            arguments.add(new SlotReference("argument" + i, sourceTypes.get(i)));
        }
        Expression converted = TypeCoercionUtils.implicitCastInputTypes(
                new CreateStruct(arguments.toArray(new Expression[0])), targetTypes);
        for (int i = 0; i < arguments.size(); i++) {
            Expression argument = converted.child(i);
            Assertions.assertEquals(targetTypes.get(i), argument.getDataType());
            if (sourceTypes.get(i).equals(targetTypes.get(i))) {
                Assertions.assertSame(arguments.get(i), argument);
            } else {
                Assertions.assertInstanceOf(Cast.class, argument);
                Assertions.assertEquals(strict, ((Cast) argument).isStrict());
                Assertions.assertSame(arguments.get(i), argument.child(0));
            }
        }
    }

    @Test
    void timestampNsComparisonsPreserveRangeWithoutChangingOtherPairs() {
        DataType ns = TimeStampNsType.INSTANCE;
        try (MockedStatic<SessionVariable> session = Mockito.mockStatic(SessionVariable.class)) {
            session.when(SessionVariable::enableStrictCast).thenReturn(false);
            for (DataType other : List.of(DateV2Type.INSTANCE, DateTimeV2Type.MAX, TimeStampTzType.MAX)) {
                assertComparisonCoercion(ns, other, ns, true);
            }
            assertComparisonCoercion(ns, ns, ns, false);
            assertComparisonCoercion(ns, StringType.INSTANCE, ns, false);
            assertComparisonCoercion(DateV2Type.INSTANCE, DateTimeV2Type.MAX, DateTimeV2Type.MAX, false);
        }
    }

    private void assertComparisonCoercion(DataType leftType, DataType rightType, DataType targetType, boolean strict) {
        Expression left = new SlotReference("left", leftType);
        Expression right = new SlotReference("right", rightType);
        for (List<Expression> arguments : List.of(List.of(left, right), List.of(right, left))) {
            Assertions.assertEquals(Optional.of(targetType), TypeCoercionUtils.findWiderTypeForTwo(
                    arguments.get(0).getDataType(), arguments.get(1).getDataType(), false, false));
            Expression comparison = TypeCoercionUtils.processComparisonPredicate(
                    new EqualTo(arguments.get(0), arguments.get(1)));
            for (int i = 0; i < arguments.size(); i++) {
                Expression argument = comparison.child(i);
                Assertions.assertEquals(targetType, argument.getDataType());
                if (arguments.get(i).getDataType().equals(targetType)) {
                    Assertions.assertSame(arguments.get(i), argument);
                } else {
                    Assertions.assertInstanceOf(Cast.class, argument);
                    Assertions.assertEquals(strict, ((Cast) argument).isStrict());
                    Assertions.assertSame(arguments.get(i), argument.child(0));
                }
            }
        }
    }

    @Test
    void timezoneCastsRetainDstFractionAndRoundingOrder() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            DateTimeV2Literal local = new DateTimeV2Literal("2024-11-03 01:30:00.123456");
            Assertions.assertEquals("2024-11-03 05:30:00.123456+00:00",
                    ((Literal) local.checkedCastTo(TimeStampTzType.of(6))).getStringValue());
            TimeStampNsLiteral gap = new TimeStampNsLiteral("2024-03-10 02:30:00.123456789");
            Assertions.assertEquals("2024-03-10 07:00:00.123457+00:00",
                    ((Literal) gap.checkedCastTo(TimeStampTzType.of(6))).getStringValue());
            // Rounding into 02:00 selects the post-transition offset, not the overlap's earlier offset.
            TimeStampNsLiteral carry = new TimeStampNsLiteral("2024-11-03 01:59:59.999999999");
            Assertions.assertEquals("2024-11-03 07:00:00.000000+00:00",
                    ((Literal) carry.checkedCastTo(TimeStampTzType.of(6))).getStringValue());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void timezoneConversionAcceptsSessionOffsets() {
        DateTimeV2Literal local = new DateTimeV2Literal("2024-01-02 12:00:00.123456");
        TimeStampNsLiteral nanos = new TimeStampNsLiteral("2024-01-02 12:00:00.123456789");
        String[][] cases = {
                {"+08:17", "2024-01-02 03:43:00"},
                {"+08:01", "2024-01-02 03:59:00"},
                {"+14:00", "2024-01-01 22:00:00"},
                {"-12:00", "2024-01-03 00:00:00"}
        };
        for (String prefix : new String[] {"", "UTC", "GMT", "utc", "gmt"}) {
            for (String[] testCase : cases) {
                String zone = prefix + testCase[0];
                DateTimeV2Literal expected = new DateTimeV2Literal(testCase[1] + ".123456");
                TimeStampNsLiteral expectedNanos = new TimeStampNsLiteral(testCase[1] + ".123456789");
                Assertions.assertEquals(expected, local.inTimeZone(zone, "UTC"), zone);
                Assertions.assertEquals(local, expected.inTimeZone("UTC", zone), zone);
                Assertions.assertEquals(expected, DateTimeExtractAndTransform.convertTz(
                        local, new StringLiteral(zone), new StringLiteral("UTC")), zone);
                Assertions.assertEquals(expectedNanos, DateTimeExtractAndTransform.convertTz(
                        nanos, new StringLiteral(zone), new StringLiteral("UTC")), zone);
                Assertions.assertEquals(nanos, DateTimeExtractAndTransform.convertTz(
                        expectedNanos, new StringLiteral("UTC"), new StringLiteral(zone)), zone);
            }
        }
        Assertions.assertEquals(new DateTimeV2Literal("2024-01-02 04:00:00.123456"),
                local.inTimeZone("CST", "UTC"));
    }

    @Test
    void timezoneConversionRejectsInvalidSessionOffsets() {
        DateTimeV2Literal local = new DateTimeV2Literal("2024-01-02 12:00:00.123456");
        TimeStampNsLiteral nanos = new TimeStampNsLiteral("2024-01-02 12:00:00.123456789");
        for (String prefix : new String[] {"", "UTC", "GMT", "utc", "gmt"}) {
            for (String offset : new String[] {"+14:01", "+14:30", "-12:01", "-12:30", "+15:00", "-13:00",
                    "+08:60", "+08:00:01"}) {
                String zone = prefix + offset;
                Assertions.assertThrows(DateTimeException.class, () -> local.inTimeZone(zone, "UTC"), zone);
                Assertions.assertThrows(DateTimeException.class, () -> local.inTimeZone("UTC", zone), zone);
                Assertions.assertThrows(DateTimeException.class, () -> DateTimeExtractAndTransform.convertTz(
                        local, new StringLiteral(zone), new StringLiteral("UTC")), zone);
                Assertions.assertThrows(DateTimeException.class, () -> DateTimeExtractAndTransform.convertTz(
                        nanos, new StringLiteral(zone), new StringLiteral("UTC")), zone);
                Assertions.assertThrows(DateTimeException.class, () -> DateTimeExtractAndTransform.convertTz(
                        nanos, new StringLiteral("UTC"), new StringLiteral(zone)), zone);
            }
        }
    }
}
