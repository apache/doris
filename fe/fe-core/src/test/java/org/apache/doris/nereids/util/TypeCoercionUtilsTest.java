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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

public class TypeCoercionUtilsTest {
    @Test
    public void testReplaceDateTimeV2WithMaxKeepsTimestampNsIndependent() {
        Assertions.assertEquals(DateTimeV2Type.MAX,
                TypeCoercionUtils.replaceDateTimeV2WithMax(DateTimeV2Type.of(3)));
        Assertions.assertSame(TimeStampNsType.INSTANCE,
                TypeCoercionUtils.replaceDateTimeV2WithMax(TimeStampNsType.INSTANCE));
        Assertions.assertEquals(ArrayType.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.replaceDateTimeV2WithMax(ArrayType.of(TimeStampNsType.INSTANCE)));
    }

    @Test
    public void testVariantWiderTypeKeepsTimestampNsPrecision() {
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.findWiderTypeForTwo(
                        VariantType.INSTANCE, TimeStampNsType.INSTANCE, false, false));
        Assertions.assertEquals(Optional.of(ArrayType.of(TimeStampNsType.INSTANCE)),
                TypeCoercionUtils.findWiderTypeForTwo(
                        VariantType.INSTANCE, ArrayType.of(TimeStampNsType.INSTANCE), false, false));

        StructType mixedDateLikeType = new StructType(ImmutableList.of(
                new StructField("ts", TimeStampNsType.INSTANCE, false, ""),
                new StructField("date", DateV2Type.INSTANCE, true, "")));
        StructType normalizedMixedDateLikeType = new StructType(ImmutableList.of(
                new StructField("ts", TimeStampNsType.INSTANCE, false, ""),
                new StructField("date", DateTimeV2Type.MAX, true, "")));
        Assertions.assertEquals(Optional.of(normalizedMixedDateLikeType),
                TypeCoercionUtils.findWiderTypeForTwo(
                        VariantType.INSTANCE, mixedDateLikeType, false, false));
    }

    @Test
    public void testTimestampNsIsNotInDateTimeV2PrecisionFamily() {
        Assertions.assertFalse(TypeCoercionUtils.hasDateTimeV2Type(TimeStampNsType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasDateTimeV2Type(
                ArrayType.of(TimeStampNsType.INSTANCE)));
        Assertions.assertTrue(TypeCoercionUtils.hasDateTimeV2Type(DateTimeV2Type.MAX));
    }

    @Test
    public void testTimestampNsCommonType() {
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.findWiderTypeForTwo(
                        TimeStampNsType.INSTANCE, StringType.INSTANCE, false, false));
        Assertions.assertEquals(Optional.of(StringType.INSTANCE),
                TypeCoercionUtils.findWiderTypeForTwo(
                        TimeStampNsType.INSTANCE, StringType.INSTANCE, false, true));
        Assertions.assertEquals(Optional.empty(), TypeCoercionUtils.findWiderTypeForTwo(
                TimeStampNsType.INSTANCE, DateTimeV2Type.MAX, false, false));
        for (DataType widerDateLikeType : ImmutableList.of(
                DateType.INSTANCE, DateV2Type.INSTANCE, DateTimeType.INSTANCE, TimeStampTzType.MAX)) {
            Assertions.assertEquals(Optional.empty(), TypeCoercionUtils.findWiderTypeForTwo(
                    widerDateLikeType, TimeStampNsType.INSTANCE, false, false));
        }
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE), TypeCoercionUtils.findWiderTypeForTwo(
                TimeStampNsType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false, false));

        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.implicitCast(StringType.INSTANCE, TimeStampNsType.INSTANCE));
        Assertions.assertEquals(Optional.of(StringType.INSTANCE),
                TypeCoercionUtils.implicitCast(TimeStampNsType.INSTANCE, StringType.INSTANCE));
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.implicitCast(NullType.INSTANCE, TimeStampNsType.INSTANCE));
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.implicitCast(
                        TimeStampNsType.INSTANCE, AnyDataType.INSTANCE_WITHOUT_INDEX));
        Assertions.assertEquals(Optional.empty(),
                TypeCoercionUtils.implicitCast(TimeStampNsType.INSTANCE, DateTimeV2Type.MAX));
        Assertions.assertEquals(Optional.of(FloatType.INSTANCE),
                TypeCoercionUtils.implicitCast(TimeStampNsType.INSTANCE, FloatType.INSTANCE));
        Assertions.assertEquals(Optional.of(DoubleType.INSTANCE),
                TypeCoercionUtils.implicitCast(TimeStampNsType.INSTANCE, DoubleType.INSTANCE));
        Assertions.assertEquals(Optional.empty(),
                TypeCoercionUtils.implicitCast(TimeStampNsType.INSTANCE, IntegerType.INSTANCE));
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.implicitCast(DateTimeV2Type.MAX, TimeStampNsType.INSTANCE));
    }

    @Test
    public void testTemporalCommonTypePrecedenceWithoutTimestampNs() {
        Assertions.assertEquals(Optional.of(DateTimeV2Type.MAX), TypeCoercionUtils.findWiderTypeForTwo(
                DateTimeV2Type.of(3), TimeStampTzType.MAX, false, false));
        Assertions.assertEquals(Optional.of(DateTimeV2Type.MAX), TypeCoercionUtils.findWiderTypeForTwo(
                DateTimeV2Type.of(3), TimeV2Type.MAX, false, false));
        Assertions.assertEquals(Optional.of(DateTimeV2Type.MAX), TypeCoercionUtils.findWiderTypeForTwo(
                TimeStampTzType.of(3), TimeV2Type.MAX, false, false));
        Assertions.assertEquals(Optional.of(TimeStampTzType.MAX), TypeCoercionUtils.findWiderTypeForTwo(
                TimeStampTzType.of(3), TimeStampTzType.MAX, false, false));
        Assertions.assertEquals(Optional.of(TimeV2Type.MAX), TypeCoercionUtils.findWiderTypeForTwo(
                TimeV2Type.of(3), TimeV2Type.MAX, false, false));
    }

    @Test
    public void testTimestampNsComparisonCoercionInBothModes() {
        boolean oldBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            for (boolean newBehavior : ImmutableList.of(false, true)) {
                GlobalVariable.enableNewTypeCoercionBehavior = newBehavior;

                EqualTo stringComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(new SlotReference("ts", TimeStampNsType.INSTANCE),
                                new SlotReference("s", StringType.INSTANCE)));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        stringComparison.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        stringComparison.right().getDataType());

                EqualTo timestampNsAndDateTime = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(new SlotReference("ts", TimeStampNsType.INSTANCE),
                                new SlotReference("dt", DateTimeV2Type.MAX)));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        timestampNsAndDateTime.left().getDataType());
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        timestampNsAndDateTime.right().getDataType());
                EqualTo dateTimeAndTimestampNs = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(new SlotReference("dt", DateTimeV2Type.MAX),
                                new SlotReference("ts", TimeStampNsType.INSTANCE)));
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        dateTimeAndTimestampNs.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        dateTimeAndTimestampNs.right().getDataType());

                EqualTo timestampNsAndTime = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(new SlotReference("ts", TimeStampNsType.INSTANCE),
                                new SlotReference("time", TimeV2Type.MAX)));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        timestampNsAndTime.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        timestampNsAndTime.right().getDataType());

                for (DataType widerDateLikeType : ImmutableList.of(
                        DateType.INSTANCE, DateV2Type.INSTANCE, DateTimeType.INSTANCE, TimeStampTzType.MAX)) {
                    Assertions.assertThrows(AnalysisException.class,
                            () -> TypeCoercionUtils.processComparisonPredicate(
                                    new EqualTo(new SlotReference("ts", TimeStampNsType.INSTANCE),
                                            new SlotReference("wider", widerDateLikeType))));
                    Assertions.assertThrows(AnalysisException.class,
                            () -> org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation
                                    .getAssignmentCompatibleType(TimeStampNsType.INSTANCE, widerDateLikeType));
                }

                Assertions.assertEquals(Optional.empty(),
                        TypeCoercionUtils.findWiderCommonTypeByVariable(
                                ImmutableList.of(TimeStampTzType.MAX, TimeV2Type.MAX,
                                        TimeStampNsType.INSTANCE), false, false));
                Assertions.assertEquals(Optional.empty(),
                        TypeCoercionUtils.findWiderCommonTypeByVariable(
                                ImmutableList.of(DateTimeV2Type.MAX, TimeStampNsType.INSTANCE),
                                false, false));

                InPredicate mixedIn = (InPredicate) TypeCoercionUtils.processInPredicate(new InPredicate(
                        new SlotReference("ts", TimeStampNsType.INSTANCE), ImmutableList.of(
                                new SlotReference("time", TimeV2Type.MAX))));
                for (Expression child : mixedIn.children()) {
                    Assertions.assertEquals(TimeStampNsType.INSTANCE, child.getDataType());
                }
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(new InPredicate(
                                new SlotReference("ts", TimeStampNsType.INSTANCE), ImmutableList.of(
                                        new SlotReference("tz", TimeStampTzType.MAX)))));

                SlotReference timestampNs = new SlotReference("ts", TimeStampNsType.INSTANCE);
                SlotReference datetime = new SlotReference("dt", DateTimeV2Type.MAX);
                SlotReference timestampTz = new SlotReference("tz", TimeStampTzType.MAX);
                SlotReference time = new SlotReference("time", TimeV2Type.MAX);
                Assertions.assertThrows(AnalysisException.class,
                        () -> new If(BooleanLiteral.TRUE, timestampNs, timestampTz).getSignature());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new NullIf(timestampNs, time).getSignature().returnType);
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(new InPredicate(timestampNs,
                                ImmutableList.of(datetime))));
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Nvl(timestampNs, datetime).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(timestampNs, datetime, timestampTz, time).getSignature());
            }
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = oldBehavior;
        }
    }

    @Test
    public void testTimestampNsDateTimeV2RangeAwareCoercionInBothModes() {
        boolean oldBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            for (boolean newBehavior : ImmutableList.of(false, true)) {
                GlobalVariable.enableNewTypeCoercionBehavior = newBehavior;

                SlotReference timestampNs = new SlotReference("ts", TimeStampNsType.INSTANCE);
                SlotReference dateTimeV2 = new SlotReference("dt", DateTimeV2Type.MAX);
                Expression lowerOutsideDateTime = new Cast(
                        new StringLiteral("1677-09-21 00:12:43.145224"), DateTimeV2Type.MAX, true);
                Expression upperOutsideDateTime = new Cast(
                        new StringLiteral("2262-04-11 23:47:16.854776"), DateTimeV2Type.MAX, true);
                Expression lowerInsideDateTime = new Cast(
                        new StringLiteral("1677-09-21 00:12:43.145225"), DateTimeV2Type.MAX, true);
                Expression exactTimestampNs = new Cast(
                        new StringLiteral("1677-09-21 00:12:43.145225000"),
                        TimeStampNsType.INSTANCE, true);
                Expression inexactTimestampNs = new Cast(
                        new StringLiteral("1677-09-21 00:12:43.145225001"),
                        TimeStampNsType.INSTANCE, true);
                Expression exactDateTimestampNs = new Cast(
                        new StringLiteral("2024-01-02 00:00:00.000000000"),
                        TimeStampNsType.INSTANCE, true);
                Expression inexactDateTimestampNs = new Cast(
                        new StringLiteral("2024-01-02 00:00:00.000000001"),
                        TimeStampNsType.INSTANCE, true);
                DateV2Literal insideDate = new DateV2Literal("2024-01-02");
                DateV2Literal outsideDate = new DateV2Literal("2500-01-02");
                SlotReference dateV2 = new SlotReference("date", DateV2Type.INSTANCE);

                EqualTo safeDateTimeLiteralComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(timestampNs, lowerInsideDateTime));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        safeDateTimeLiteralComparison.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        safeDateTimeLiteralComparison.right().getDataType());
                EqualTo safeDateLiteralComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(timestampNs, insideDate));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        safeDateLiteralComparison.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        safeDateLiteralComparison.right().getDataType());
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processComparisonPredicate(
                                new EqualTo(timestampNs, outsideDate)));

                EqualTo exactTimestampLiteralComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(dateTimeV2, exactTimestampNs));
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        exactTimestampLiteralComparison.left().getDataType());
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        exactTimestampLiteralComparison.right().getDataType());
                EqualTo exactDateTimestampLiteralComparison = (EqualTo)
                        TypeCoercionUtils.processComparisonPredicate(
                                new EqualTo(dateV2, exactDateTimestampNs));
                Assertions.assertEquals(DateV2Type.INSTANCE,
                        exactDateTimestampLiteralComparison.left().getDataType());
                Assertions.assertEquals(DateV2Type.INSTANCE,
                        exactDateTimestampLiteralComparison.right().getDataType());
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processComparisonPredicate(
                                new EqualTo(dateV2, inexactDateTimestampNs)));

                EqualTo lowerBoundaryComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(timestampNs, lowerOutsideDateTime));
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        lowerBoundaryComparison.left().getDataType());
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        lowerBoundaryComparison.right().getDataType());
                EqualTo upperBoundaryComparison = (EqualTo) TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(upperOutsideDateTime, timestampNs));
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        upperBoundaryComparison.left().getDataType());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        upperBoundaryComparison.right().getDataType());

                InPredicate safeIn = (InPredicate) TypeCoercionUtils.processInPredicate(
                        new InPredicate(timestampNs, ImmutableList.of(lowerInsideDateTime)));
                safeIn.children().forEach(child -> Assertions.assertEquals(
                        TimeStampNsType.INSTANCE, child.getDataType()));
                InPredicate safeDateIn = (InPredicate) TypeCoercionUtils.processInPredicate(
                        new InPredicate(timestampNs, ImmutableList.of(insideDate)));
                safeDateIn.children().forEach(child -> Assertions.assertEquals(
                        TimeStampNsType.INSTANCE, child.getDataType()));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(
                                new InPredicate(timestampNs, ImmutableList.of(outsideDate))));
                InPredicate exactTimestampIn = (InPredicate) TypeCoercionUtils.processInPredicate(
                        new InPredicate(dateTimeV2, ImmutableList.of(exactTimestampNs)));
                exactTimestampIn.children().forEach(child -> Assertions.assertEquals(
                        DateTimeV2Type.MAX, child.getDataType()));
                InPredicate exactDateTimestampIn = (InPredicate) TypeCoercionUtils.processInPredicate(
                        new InPredicate(dateV2, ImmutableList.of(exactDateTimestampNs)));
                exactDateTimestampIn.children().forEach(child -> Assertions.assertEquals(
                        DateV2Type.INSTANCE, child.getDataType()));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(
                                new InPredicate(dateV2, ImmutableList.of(inexactDateTimestampNs))));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(
                                new InPredicate(timestampNs, ImmutableList.of(lowerOutsideDateTime))));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(
                                new InPredicate(timestampNs, ImmutableList.of(upperOutsideDateTime))));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processInPredicate(
                                new InPredicate(dateTimeV2, ImmutableList.of(inexactTimestampNs))));

                CaseWhen safeCase = (CaseWhen) TypeCoercionUtils.processCaseWhen(new CaseWhen(
                        ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, timestampNs)),
                        lowerInsideDateTime));
                Assertions.assertEquals(TimeStampNsType.INSTANCE, safeCase.getDataType());
                CaseWhen safeDateCase = (CaseWhen) TypeCoercionUtils.processCaseWhen(new CaseWhen(
                        ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, timestampNs)),
                        insideDate));
                Assertions.assertEquals(TimeStampNsType.INSTANCE, safeDateCase.getDataType());
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processCaseWhen(new CaseWhen(
                                ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, timestampNs)),
                                outsideDate)));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processCaseWhen(new CaseWhen(
                                ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, timestampNs)),
                                lowerOutsideDateTime)));
                Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processCaseWhen(new CaseWhen(
                                ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, timestampNs)),
                                upperOutsideDateTime)));

                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new Coalesce(timestampNs, lowerInsideDateTime).getSignature().returnType);
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new Coalesce(timestampNs, insideDate).getSignature().returnType);
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(timestampNs, outsideDate).getSignature());
                Assertions.assertEquals(DateTimeV2Type.MAX,
                        new Coalesce(dateTimeV2, exactTimestampNs).getSignature().returnType);
                Assertions.assertEquals(DateV2Type.INSTANCE,
                        new Coalesce(dateV2, exactDateTimestampNs).getSignature().returnType);
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(dateV2, inexactDateTimestampNs).getSignature());
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new If(BooleanLiteral.TRUE, timestampNs, lowerInsideDateTime)
                                .getSignature().returnType);
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new Nvl(timestampNs, lowerInsideDateTime).getSignature().returnType);
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new NullIf(timestampNs, lowerInsideDateTime).getSignature().returnType);
                Assertions.assertEquals(TimeStampNsType.INSTANCE,
                        new Greatest(timestampNs, lowerInsideDateTime).getSignature().returnType);
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(timestampNs, lowerOutsideDateTime).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(timestampNs, upperOutsideDateTime).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Coalesce(dateTimeV2, inexactTimestampNs).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new If(BooleanLiteral.TRUE, timestampNs, lowerOutsideDateTime)
                                .getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Nvl(timestampNs, lowerOutsideDateTime).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new NullIf(timestampNs, lowerOutsideDateTime).getSignature());
                Assertions.assertThrows(AnalysisException.class,
                        () -> new Greatest(timestampNs, lowerOutsideDateTime).getSignature());

                Assertions.assertEquals(Optional.empty(),
                        TypeCoercionUtils.findWiderCommonTypeByVariable(
                                ImmutableList.of(TimeStampNsType.INSTANCE, DateTimeV2Type.MAX),
                                false, false));
            }
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = oldBehavior;
        }
    }

    @Test
    public void testTimestampNsLiteralAndCommonTypeCoercion() {
        String timestamp = "2024-01-02 03:04:05.123456789";
        Optional<Expression> literal = TypeCoercionUtils.characterLiteralTypeCoercion(
                timestamp, TimeStampNsType.INSTANCE);
        Assertions.assertTrue(literal.isPresent());
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, literal.get());
        Assertions.assertEquals(TimeStampNsType.INSTANCE, literal.get().getDataType());
        Assertions.assertTrue(TypeCoercionUtils.characterLiteralTypeCoercion(
                "2024-02-30 03:04:05.123456789", TimeStampNsType.INSTANCE).isEmpty());

        SlotReference timestampSlot = new SlotReference("ts", TimeStampNsType.INSTANCE);
        EqualTo rightString = TypeCoercionUtils.processCharacterLiteralInBinaryOperator(
                new EqualTo(timestampSlot, new StringLiteral(timestamp)));
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, rightString.right());
        EqualTo leftString = TypeCoercionUtils.processCharacterLiteralInBinaryOperator(
                new EqualTo(new StringLiteral(timestamp), timestampSlot));
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, leftString.left());

        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.findWiderCommonTypeForComparison(
                        ImmutableList.of(StringType.INSTANCE, TimeStampNsType.INSTANCE)));
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.findWiderCommonTypeForComparison(
                        ImmutableList.of(TimeStampNsType.INSTANCE, StringType.INSTANCE)));
        Assertions.assertEquals(Optional.of(TimeStampNsType.INSTANCE),
                TypeCoercionUtils.findCommonPrimitiveTypeForCaseWhen(
                        TimeStampNsType.INSTANCE, IntegerType.INSTANCE));

        Assertions.assertSame(TimeStampNsType.INSTANCE,
                TypeCoercionUtils.replaceTimesWithTargetPrecision(TimeStampNsType.INSTANCE, 6));
        MapType nestedTimestamp = MapType.of(IntegerType.INSTANCE,
                ArrayType.of(TimeStampNsType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasDateTimeV2Type(nestedTimestamp));
        Assertions.assertEquals(nestedTimestamp,
                TypeCoercionUtils.replaceTimesWithTargetPrecision(nestedTimestamp, 3));

        MapType normalizedNestedTimestamp = MapType.of(DecimalV3Type.SYSTEM_DEFAULT,
                ArrayType.of(TimeStampNsType.INSTANCE));
        Assertions.assertEquals(Optional.of(normalizedNestedTimestamp),
                TypeCoercionUtils.findWiderTypeForTwo(
                        nestedTimestamp, VariantType.INSTANCE, false, false));
        Optional<Expression> dateTargetWithTime = TypeCoercionUtils.characterLiteralTypeCoercion(
                "2024-01-02 03:04:05.123456", DateV2Type.INSTANCE);
        Assertions.assertTrue(dateTargetWithTime.isPresent());
        Assertions.assertEquals(DateTimeV2Type.of(6), dateTargetWithTime.get().getDataType());
    }

    @Test
    public void testImplicitCastAccept() {
        IntegerType integerType = IntegerType.INSTANCE;
        IntegralType integralType = IntegralType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(integerType, integralType).get());
    }

    @Test
    public void testImplicitCastNullType() {
        NullType nullType = NullType.INSTANCE;
        IntegralType integralType = IntegralType.INSTANCE;
        Assertions.assertEquals(integralType.defaultConcreteType(),
                TypeCoercionUtils.implicitCast(nullType, integralType).get());
    }

    @Test
    public void testImplicitCastNumericWithExpectDecimal() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        DecimalV2Type decimalV2Type = DecimalV2Type.createDecimalV2Type(27, 9);
        Assertions.assertEquals(DecimalV2Type.forType(bigIntType),
                TypeCoercionUtils.implicitCast(bigIntType, decimalV2Type).get());
    }

    @Test
    public void testImplicitCastNumericWithExpectNumeric() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        IntegerType integerType = IntegerType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(bigIntType, integerType).get());
    }

    @Test
    public void testImplicitCastStringToDecimal() {
        StringType stringType = StringType.INSTANCE;
        DecimalV2Type decimalV2Type = DecimalV2Type.SYSTEM_DEFAULT;
        Assertions.assertEquals(decimalV2Type, TypeCoercionUtils.implicitCast(stringType, decimalV2Type).get());
    }

    @Test
    public void testImplicitCastStringToNumeric() {
        VarcharType varcharType = VarcharType.createVarcharType(10);
        IntegerType integerType = IntegerType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(varcharType, integerType).get());
    }

    @Test
    public void testImplicitCastFromPrimitiveToString() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        StringType stringType = StringType.INSTANCE;
        Assertions.assertEquals(stringType, TypeCoercionUtils.implicitCast(bigIntType, stringType).get());
    }

    @Test
    public void testVariantToJsonImplicitCastRequiresExplicitCast() {
        Assertions.assertTrue(TypeCoercionUtils.implicitCast(
                VariantType.INSTANCE, JsonType.INSTANCE).isEmpty());
    }

    @Test
    public void testVariantToJsonFunctionSignatureRequiresExplicitCast() {
        Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(
                JsonType.INSTANCE, VariantType.INSTANCE));
    }

    @Test
    public void testVariantExistingImplicitCastsArePreserved() {
        Assertions.assertEquals(IntegerType.INSTANCE,
                TypeCoercionUtils.implicitCast(VariantType.INSTANCE, IntegerType.INSTANCE).get());
        Assertions.assertEquals(StringType.INSTANCE,
                TypeCoercionUtils.implicitCast(VariantType.INSTANCE, StringType.INSTANCE).get());
        Assertions.assertEquals(JsonType.INSTANCE,
                TypeCoercionUtils.implicitCast(JsonType.INSTANCE, JsonType.INSTANCE).get());
        Assertions.assertEquals(JsonType.INSTANCE,
                TypeCoercionUtils.implicitCast(StringType.INSTANCE, JsonType.INSTANCE).get());
    }

    @Test
    public void testVariantCommonTypeRequiresSameStorageProperties() {
        VariantType v1 = new VariantType(100);
        VariantType anotherV1 = new VariantType(200);

        Assertions.assertTrue(
                TypeCoercionUtils.findWiderTypeForTwo(v1, anotherV1, false, true).isEmpty());
        Assertions.assertTrue(TypeCoercionUtils.findWiderTypeForTwo(
                ArrayType.of(v1), ArrayType.of(anotherV1), false, true).isEmpty());

        Assertions.assertTrue(
                TypeCoercionUtils.findCommonPrimitiveTypeForCaseWhen(v1, anotherV1).isEmpty());
        Assertions.assertTrue(TypeCoercionUtils.findWiderCommonTypeForCaseWhen(
                ImmutableList.of(ArrayType.of(v1), ArrayType.of(anotherV1))).isEmpty());

        Assertions.assertEquals(v1,
                TypeCoercionUtils.findWiderTypeForTwo(v1, new VariantType(100), false, true).get());
    }

    @Test
    public void testCoalesceCastsRequiredStructFieldsToNullableLayout() {
        StructType nullableStruct = new StructType(ImmutableList.of(
                new StructField("name", VarcharType.createVarcharType(10), true, ""),
                new StructField("age", IntegerType.INSTANCE, true, "")));
        SlotReference nullableStructSlot = new SlotReference("value", nullableStruct, true);
        StructLiteral requiredStruct = new StructLiteral(ImmutableList.of(
                new StringLiteral("Charlie"), new IntegerLiteral(18)));

        Expression coerced = TypeCoercionUtils.processBoundFunction(
                new Coalesce(nullableStructSlot, requiredStruct));

        Assertions.assertInstanceOf(Cast.class, coerced.child(1));
        StructType castType = (StructType) coerced.child(1).getDataType();
        Assertions.assertTrue(castType.getFields().get(0).isNullable());
        Assertions.assertTrue(castType.getFields().get(1).isNullable());
    }

    @Test
    public void testLegacyCaseWhenUnionsStructFieldNullabilityInBothOrders() {
        StructType required = new StructType(ImmutableList.of(
                new StructField("event_time", DateTimeV2Type.of(3), false, "")));
        StructType nullable = new StructType(ImmutableList.of(
                new StructField("event_time", DateTimeV2Type.of(6), true, "")));

        StructType leftRequired = (StructType) TypeCoercionUtils.findWiderCommonTypeForCaseWhen(
                ImmutableList.of(required, nullable)).orElseThrow();
        StructType leftNullable = (StructType) TypeCoercionUtils.findWiderCommonTypeForCaseWhen(
                ImmutableList.of(nullable, required)).orElseThrow();

        Assertions.assertTrue(leftRequired.getFields().get(0).isNullable());
        Assertions.assertTrue(leftNullable.getFields().get(0).isNullable());
    }

    @Test
    public void testLegacySetOperationIncludesCastNullability() {
        boolean oldBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        GlobalVariable.enableNewTypeCoercionBehavior = false;
        try {
            StructType millis = new StructType(ImmutableList.of(
                    new StructField("event_time", DateTimeV2Type.of(3), false, "")));
            StructType micros = new StructType(ImmutableList.of(
                    new StructField("event_time", DateTimeV2Type.of(6), false, "")));

            StructType common = (StructType) org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation
                    .getAssignmentCompatibleType(millis, micros);

            Assertions.assertTrue(common.getFields().get(0).isNullable());
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = oldBehavior;
        }
    }

    @Test
    public void testStrictStructCommonTypesKeepRequiredFields() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().enableStrictCast = true;
        connectContext.setThreadLocalInfo();
        boolean oldBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            StructType millis = new StructType(ImmutableList.of(
                    new StructField("event_time", DateTimeV2Type.of(3), false, "")));
            StructType micros = new StructType(ImmutableList.of(
                    new StructField("event_time", DateTimeV2Type.of(6), false, "")));

            StructType wider = (StructType) TypeCoercionUtils.findWiderCommonType(
                    ImmutableList.of(millis, micros), false, false).orElseThrow();
            StructType caseWhen = (StructType) TypeCoercionUtils.findWiderCommonTypeForCaseWhen(
                    ImmutableList.of(millis, micros)).orElseThrow();
            StructType implicit = (StructType) TypeCoercionUtils.implicitCast(millis, micros).orElseThrow();
            Assertions.assertFalse(wider.getFields().get(0).isNullable());
            Assertions.assertFalse(caseWhen.getFields().get(0).isNullable());
            Assertions.assertFalse(implicit.getFields().get(0).isNullable());
            Assertions.assertTrue(CheckCast.check(wider, millis, true),
                    "a strict CASE/common result must remain consumable by a required target");
            Assertions.assertTrue(CheckCast.check(caseWhen, millis, true),
                    "a strict CASE result must remain consumable by a required target");

            for (boolean newBehavior : new boolean[] {false, true}) {
                GlobalVariable.enableNewTypeCoercionBehavior = newBehavior;
                StructType setOperation = (StructType) org.apache.doris.nereids.trees.plans.logical
                        .LogicalSetOperation.getAssignmentCompatibleType(millis, micros);
                Assertions.assertFalse(setOperation.getFields().get(0).isNullable(),
                        "strict set-operation casts abort instead of producing a nullable child");
                Assertions.assertTrue(CheckCast.check(setOperation, millis, true),
                        "a strict UNION result must remain consumable by a required target");
            }
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = oldBehavior;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testInPredicateStructCastKeepsNullableCastResult() {
        StructLiteral stringStruct = new StructLiteral(ImmutableList.of(
                new IntegerLiteral(1), new StringLiteral("2")));
        StructLiteral integerStruct = new StructLiteral(ImmutableList.of(
                new IntegerLiteral(1), new IntegerLiteral(3)));
        InPredicate predicate = new InPredicate(stringStruct,
                ImmutableList.of(integerStruct, NullLiteral.INSTANCE));

        InPredicate coerced = (InPredicate) TypeCoercionUtils.processInPredicate(predicate);

        StructType commonType = (StructType) coerced.getCompareExpr().getDataType();
        Assertions.assertTrue(commonType.getFields().get(1).isNullable(),
                "String-to-decimal coercion can produce NULL inside the struct field");
    }

    @Test
    public void testHasCharacterType() {
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(NullType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(BooleanType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(TinyIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(SmallIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(IntegerType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(BigIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(LargeIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(FloatType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DoubleType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DecimalV2Type.SYSTEM_DEFAULT));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(CharType.createCharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(VarcharType.createVarcharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(StringType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateTimeType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateType.INSTANCE));
    }

    @Test
    public void testCastIfNotSameType() {
        Assertions.assertEquals(new DoubleLiteral(5L),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), DoubleType.INSTANCE));
        Assertions.assertEquals(new Cast(new DoubleLiteral(5L), BooleanType.INSTANCE),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), BooleanType.INSTANCE));
        Assertions.assertEquals(new StringLiteral("varchar"),
                TypeCoercionUtils.castIfNotSameType(new VarcharLiteral("varchar"), StringType.INSTANCE));
        Assertions.assertEquals(new StringLiteral("char"),
                TypeCoercionUtils.castIfNotSameType(new CharLiteral("char", 4), StringType.INSTANCE));
        Assertions.assertEquals(new CharLiteral("char", 4),
                TypeCoercionUtils.castIfNotSameType(new CharLiteral("char", 4), VarcharType.createVarcharType(100)));
        Assertions.assertEquals(new StringLiteral("string"),
                TypeCoercionUtils.castIfNotSameType(new StringLiteral("string"), VarcharType.createVarcharType(100)));
    }

    @Test
    public void testDecimalArithmetic() {
        Multiply multiply = new Multiply(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        Expression expression = TypeCoercionUtils.processBinaryArithmetic(multiply);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(9, 3)));

        Divide divide = new Divide(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(divide);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(9, 3)));

        Add add = new Add(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(add);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(10, 3)));

        Subtract sub = new Subtract(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(sub);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(10, 3)));
    }

    @Test
    public void testProcessInDowngrade() {
        // DecimalV2 slot vs DecimalV3 literal
        InPredicate decimalDowngrade = new InPredicate(
                new SlotReference("c1", DecimalV2Type.createDecimalV2Type(15, 6)),
                ImmutableList.of(
                        new DecimalV3Literal(BigDecimal.valueOf(12345.1234567)),
                        new DecimalLiteral(BigDecimal.valueOf(12345.1234))));
        decimalDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(decimalDowngrade);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(16, 7), decimalDowngrade.getCompareExpr().getDataType());

        // DateV1 slot vs DateV2 literal
        InPredicate dateDowngrade = new InPredicate(
                new SlotReference("c1", DateType.INSTANCE),
                ImmutableList.of(
                        new DateLiteral(2024, 4, 12),
                        new DateV2Literal(2024, 4, 12)));
        dateDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(dateDowngrade);
        Assertions.assertEquals(DateType.INSTANCE, dateDowngrade.getCompareExpr().getDataType());

        // DatetimeV1 slot vs DateLike literal
        InPredicate datetimeDowngrade = new InPredicate(
                new SlotReference("c1", DateTimeType.INSTANCE),
                ImmutableList.of(
                        new DateLiteral(2024, 4, 12),
                        new DateV2Literal(2024, 4, 12),
                        new DateTimeLiteral(2024, 4, 12, 18, 25, 30),
                        new DateTimeV2Literal(2024, 4, 12, 18, 25, 30, 0)));
        datetimeDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(datetimeDowngrade);
        Assertions.assertEquals(DateTimeType.INSTANCE, datetimeDowngrade.getCompareExpr().getDataType());
    }

    @Test
    public void testProcessComparisonPredicateDowngrade() {
        // DecimalV2 slot vs DecimalV3 literal
        EqualTo decimalDowngrade = new EqualTo(
                new SlotReference("c1", DecimalV2Type.createDecimalV2Type(15, 6)),
                new DecimalV3Literal(BigDecimal.valueOf(12345.1234567))
        );
        decimalDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(decimalDowngrade);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(16, 7), decimalDowngrade.left().getDataType());

        // DateV1 slot vs DateV2 literal (this case cover right slot vs left literal)
        EqualTo dateDowngrade = new EqualTo(
                new DateV2Literal(2024, 4, 12),
                new SlotReference("c1", DateType.INSTANCE)
        );
        dateDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(dateDowngrade);
        Assertions.assertEquals(DateType.INSTANCE, dateDowngrade.left().getDataType());

        // DatetimeV1 slot vs DateLike literal
        EqualTo datetimeDowngrade = new EqualTo(
                new SlotReference("c1", DateTimeType.INSTANCE),
                new DateTimeV2Literal(2024, 4, 12, 18, 25, 30, 0)
        );
        datetimeDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(datetimeDowngrade);
        Assertions.assertEquals(DateTimeType.INSTANCE, datetimeDowngrade.left().getDataType());
    }

    @Test
    public void testVariantComparisonRequiresExplicitCast() {
        SlotReference variant = new SlotReference("v", VariantType.INSTANCE);
        SlotReference anotherVariant = new SlotReference("v2", VariantType.INSTANCE);
        SlotReference integer = new SlotReference("i", IntegerType.INSTANCE);
        ElementAt variantSubpath = new ElementAt(variant, new StringLiteral("c"));
        ElementAt anotherVariantSubpath = new ElementAt(anotherVariant, new StringLiteral("c"));

        AnalysisException equality = Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(variant, anotherVariant)));
        Assertions.assertTrue(equality.getMessage().contains("CAST to a concrete type first"));

        AnalysisException nullSafeEquality = Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new NullSafeEqual(variant, anotherVariant)));
        Assertions.assertTrue(nullSafeEquality.getMessage().contains("CAST to a concrete type first"));

        AnalysisException mixedType = Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new GreaterThan(variant, integer)));
        Assertions.assertTrue(mixedType.getMessage().contains("could not used in ComparisonPredicate"));
        Assertions.assertTrue(mixedType.getMessage().contains("CAST to a concrete type first"));

        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(
                        new GreaterThan(variant, anotherVariant)));
        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(variant, integer)));

        Expression subpathComparison = TypeCoercionUtils.processComparisonPredicate(
                new EqualTo(variantSubpath, integer));
        Assertions.assertTrue(subpathComparison instanceof EqualTo);
        Assertions.assertTrue(subpathComparison.child(0) instanceof Cast);
        Assertions.assertFalse(subpathComparison.child(0).getDataType().isVariantType());
        Assertions.assertEquals(subpathComparison.child(0).getDataType(),
                subpathComparison.child(1).getDataType());

        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(
                        new EqualTo(variantSubpath, anotherVariantSubpath)));

        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processComparisonPredicate(
                new GreaterThan(new Cast(variant, IntegerType.INSTANCE), integer)));
    }

    @Test
    public void testProcessInStringCoercion() {
        // BigInt slot vs String literal
        InPredicate bigintString = new InPredicate(
                new SlotReference("c1", BigIntType.INSTANCE),
                ImmutableList.of(
                        new VarcharLiteral("200"),
                        new VarcharLiteral("922337203685477001")));
        bigintString = (InPredicate) TypeCoercionUtils.processInPredicate(bigintString);
        Assertions.assertEquals(BigIntType.INSTANCE, bigintString.getCompareExpr().getDataType());
        Assertions.assertEquals(BigIntType.INSTANCE, bigintString.getOptions().get(0).getDataType());

        // SmallInt slot vs String literal
        InPredicate smallIntString = new InPredicate(
                new SlotReference("c1", SmallIntType.INSTANCE),
                ImmutableList.of(
                        new DecimalLiteral(new BigDecimal("987654.321")),
                        new VarcharLiteral("922337203685477001")));
        smallIntString = (InPredicate) TypeCoercionUtils.processInPredicate(smallIntString);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(23, 3), smallIntString.getCompareExpr().getDataType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(23, 3), smallIntString.getOptions().get(0).getDataType());
    }

    @Test
    public void testDateStringSubMicrosecondComparisonCoercion() {
        Expression date = new SlotReference("date", DateV2Type.INSTANCE, true);
        Expression nanoString = new StringLiteral("2024-01-01 00:00:00.000000001");

        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(date, nanoString)));
        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processComparisonPredicate(new EqualTo(nanoString, date)));
    }

    @Test
    public void testDateStringSubMicrosecondInCoercion() {
        Expression date = new SlotReference("date", DateV2Type.INSTANCE, true);
        Expression nanoString = new StringLiteral("2024-01-01 00:00:00.000000001");
        Expression dateString = new StringLiteral("2024-01-02");

        Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processInPredicate(
                        new InPredicate(date, ImmutableList.of(nanoString, dateString))));
        Assertions.assertThrows(AnalysisException.class,
                () -> ExpressionAnalyzer.FUNCTION_ANALYZER_RULE.rewrite(
                        new Not(new InPredicate(date, ImmutableList.of(dateString, nanoString))), null));
    }

    @Test
    public void testCharacterLiteralTypeCoercion() {
        // datev2
        Assertions.assertEquals(DateV2Type.INSTANCE,
                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateV2Type.INSTANCE).get().getDataType());
        // datetimev2
        Assertions.assertEquals(DateTimeV2Type.of(0),
                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateTimeV2Type.of(0)).get().getDataType());
        // date
        Assertions.assertEquals(DateV2Type.INSTANCE,
                        TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateType.INSTANCE).get()
                                        .getDataType());
        // datetime
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT,
                                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateTimeType.INSTANCE).get().getDataType());
        // timestamptz wildcard
        Assertions.assertEquals(TimeStampTzType.SYSTEM_DEFAULT,
                TypeCoercionUtils.characterLiteralTypeCoercion("2023-08-17T01:41:18Z", TimeStampTzType.WILDCARD)
                        .get().getDataType());
        // No-zone TIMESTAMPTZ coercion uses the session timezone to define the local civil time.
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
        connectContext.setThreadLocalInfo();
        try {
            // timestamptz without explicit timezone keeps the literal scale during signature search
            Assertions.assertEquals(TimeStampTzType.SYSTEM_DEFAULT,
                    TypeCoercionUtils.characterLiteralTypeCoercion("2004-12-31", TimeStampTzType.MAX)
                            .get().getDataType());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testProcessBoundFunction() {
        SlotReference jsonCol = new SlotReference("c_json", JsonType.INSTANCE);
        BoundFunction sum = new Sum(jsonCol);
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(sum));

        Expression coerced = TypeCoercionUtils.processBoundFunction(sum);
        Expression coercedArg = ((BoundFunction) coerced).child(0);
        Assertions.assertTrue(
                coercedArg.getDataType().equals(DoubleType.INSTANCE) || coercedArg.getDataType().isNumericType(),
                "The argument of SUM should be of a numeric type after type coercion."
        );

        BoundFunction avg = new Avg(jsonCol);
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(avg));

        coerced = TypeCoercionUtils.processBoundFunction(sum);
        coercedArg = ((BoundFunction) coerced).child(0);
        Assertions.assertTrue(
                coercedArg.getDataType().equals(DoubleType.INSTANCE) || coercedArg.getDataType().isNumericType(),
                "The argument of AVG should be of a numeric type after type coercion."
        );
    }

    @Test
    public void testGetNumResultType() {
        // Numeric type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(TinyIntType.INSTANCE));
        Assertions.assertEquals(SmallIntType.INSTANCE, TypeCoercionUtils.getNumResultType(SmallIntType.INSTANCE));
        Assertions.assertEquals(IntegerType.INSTANCE, TypeCoercionUtils.getNumResultType(IntegerType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(BigIntType.INSTANCE));
        Assertions.assertEquals(LargeIntType.INSTANCE, TypeCoercionUtils.getNumResultType(LargeIntType.INSTANCE));
        Assertions.assertEquals(FloatType.INSTANCE, TypeCoercionUtils.getNumResultType(FloatType.INSTANCE));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(DoubleType.INSTANCE));
        Assertions.assertEquals(DecimalV3Type.INSTANCE, TypeCoercionUtils.getNumResultType(DecimalV3Type.INSTANCE));
        // Null type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(NullType.INSTANCE));
        // Boolean type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(BooleanType.INSTANCE));
        // Date like type
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateV2Type.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateTimeType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateTimeV2Type.SYSTEM_DEFAULT));
        // String like type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(StringType.INSTANCE));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(VarcharType.SYSTEM_DEFAULT));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(CharType.SYSTEM_DEFAULT));
        // Hll type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(HllType.INSTANCE));
        // Time type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(TimeV2Type.SYSTEM_DEFAULT));
        // Json
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(JsonType.INSTANCE));
        // Other
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(BitmapType.INSTANCE));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(ArrayType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(MapType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(StructType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(QuantileStateType.INSTANCE));
    }
}
