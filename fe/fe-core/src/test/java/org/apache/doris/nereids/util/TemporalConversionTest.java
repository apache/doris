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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.ArrayType;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
        Assertions.assertFalse(TemporalCastSupport.requiresStrictTemporalCast(target, source));
    }

    @Test
    void signatureAdmissionAndArgumentCoercionAgreeOnLiteralExactness() {
        Expression exact = new TimeStampNsLiteral("2024-01-02 03:04:05.123000000");
        Expression inexact = new TimeStampNsLiteral("2024-01-02 03:04:05.123000001");
        DataType target = DateTimeV2Type.of(3);
        List<FunctionSignature> signatures = List.of(FunctionSignature.ret(target).args(target));
        TemporalTypeCoercion.SignaturePolicy exactPolicy =
                new TemporalTypeCoercion.SignaturePolicy(signatures, List.of(exact));
        Assertions.assertTrue(exactPolicy.accepts(exact, target));
        Assertions.assertEquals(Optional.of(target), TemporalTypeCoercion.implicitCastArgument(exact, target));
        TemporalTypeCoercion.SignaturePolicy inexactPolicy =
                new TemporalTypeCoercion.SignaturePolicy(signatures, List.of(inexact));
        Assertions.assertFalse(inexactPolicy.accepts(inexact, target));
        Assertions.assertTrue(TemporalTypeCoercion.implicitCastArgument(inexact, target).isEmpty());
        Assertions.assertTrue(TemporalTypeCoercion.implicitCastArgument(
                new SlotReference("ns", TimeStampNsType.INSTANCE), target).isEmpty());
    }

    @Test
    void mixedTemporalOverloadsDoNotFallBackToStrings() {
        Expression ns = new SlotReference("ns", TimeStampNsType.INSTANCE);
        Expression date = new SlotReference("date", DateV2Type.INSTANCE);
        List<FunctionSignature> signatures = List.of(
                FunctionSignature.ret(TimeStampNsType.INSTANCE)
                        .args(TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE),
                FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE));
        TemporalTypeCoercion.SignaturePolicy policy =
                new TemporalTypeCoercion.SignaturePolicy(signatures, List.of(ns, date));
        Assertions.assertFalse(policy.accepts(ns, StringType.INSTANCE));
        Assertions.assertFalse(policy.accepts(date, DateTimeV2Type.MAX));
        Assertions.assertTrue(policy.accepts(date, TimeStampNsType.INSTANCE));
        TemporalTypeCoercion.SignaturePolicy untyped =
                new TemporalTypeCoercion.SignaturePolicy(signatures, List.of(date, date));
        Assertions.assertFalse(untyped.accepts(date, TimeStampNsType.INSTANCE));
    }

    @Test
    void timezoneConversionRetainsDstAndFractionSemantics() {
        DateTimeV2Literal local = new DateTimeV2Literal("2024-11-03 01:30:00.123456");
        Assertions.assertEquals("2024-11-03 05:30:00.123456",
                local.inTimeZone("America/New_York", "UTC").getStringValue());
        LocalDateTime gap = LocalDateTime.of(2024, 3, 10, 2, 30, 0, 123456789);
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 10, 7, 0, 0, 123456789),
                TemporalValueConversion.convertZonePreservingFraction(
                        gap, ZoneId.of("America/New_York"), ZoneOffset.UTC));
        Assertions.assertThrows(AnalysisException.class,
                () -> local.inTimeZone("+15:00", "UTC"));
        Assertions.assertThrows(AnalysisException.class,
                () -> local.inTimeZone("+08:17", "UTC"));
    }
}
