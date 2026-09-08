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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lance.source.LancePredicateConverter;
import org.apache.doris.thrift.TFunctionBinaryType;

import io.substrait.proto.ExtendedExpression;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

public class LancePredicateConverterTest {
    private final LancePredicateConverter converter = new LancePredicateConverter(new Schema(Arrays.asList(
            new Field("row_id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            Field.nullable("label", ArrowType.Utf8.INSTANCE),
            Field.nullable("event-type", ArrowType.Utf8.INSTANCE),
            Field.nullable("decimal_9_2", new ArrowType.Decimal(9, 2, 128)),
            Field.nullable("date_value", new ArrowType.Date(DateUnit.DAY)),
            Field.nullable("date_millis", new ArrowType.Date(DateUnit.MILLISECOND)),
            Field.nullable("decimal256_small", new ArrowType.Decimal(38, 4, 256)),
            Field.nullable("decimal256", new ArrowType.Decimal(39, 4, 256)),
            Field.nullable("uint8_value", new ArrowType.Int(8, false)),
            Field.nullable("uint16_value", new ArrowType.Int(16, false)),
            Field.nullable("uint32_value", new ArrowType.Int(32, false)),
            Field.nullable("uint64_value", new ArrowType.Int(64, false)),
            Field.nullable("large_label", ArrowType.LargeUtf8.INSTANCE),
            Field.nullable("timestamp_s", new ArrowType.Timestamp(TimeUnit.SECOND, null)),
            Field.nullable("timestamp_ms", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            Field.nullable("timestamp_us", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
            Field.nullable("timestamp_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
            Field.nullable("timestamp_us_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")))));

    @Test
    public void testComparisonAndStringLiteralEncoding() throws Exception {
        Expr rowId = new SlotRef(null, "row_id");
        Expr comparison = new BinaryPredicate(BinaryPredicate.Operator.GE, rowId, new IntLiteral(2));
        Expr label = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "label"), new StringLiteral("O'Reilly\0tail"));

        LancePredicateConverter.ConversionResult result = converter.convert(Arrays.asList(comparison, label));

        Assertions.assertTrue(result.getSubstraitFilter().length > 0);
        ExtendedExpression envelope = ExtendedExpression.parseFrom(result.getSubstraitFilter());
        Assertions.assertEquals("doris-lance", envelope.getVersion().getProducer());
        Assertions.assertEquals(18, envelope.getBaseSchema().getStruct().getTypesCount());
        Assertions.assertEquals(Arrays.asList("row_id", "label", "event-type", "decimal_9_2",
                        "date_value", "__unlikely_name_placeholder_doris_5",
                        "__unlikely_name_placeholder_doris_6",
                        "__unlikely_name_placeholder_doris_7",
                        "uint8_value", "uint16_value", "uint32_value", "uint64_value",
                        "large_label", "timestamp_s", "timestamp_ms", "timestamp_us",
                        "__unlikely_name_placeholder_doris_16",
                        "__unlikely_name_placeholder_doris_17"),
                envelope.getBaseSchema().getNamesList());
        Assertions.assertTrue(envelope.getBaseSchema().getStruct().getTypes(5).hasUserDefined());
        Assertions.assertTrue(envelope.getBaseSchema().getStruct().getTypes(6).hasUserDefined());
        Assertions.assertTrue(envelope.getBaseSchema().getStruct().getTypes(7).hasUserDefined());
        Assertions.assertEquals(1, envelope.getReferredExprCount());
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue()
                .getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertEquals("O'Reilly\0tail", literal.getString());
        Assertions.assertEquals(0, envelope.getBaseSchema().getStruct()
                .getTypes(1).getString().getTypeVariationReference());
        Assertions.assertEquals(0, literal.getTypeVariationReference());
        Assertions.assertTrue(result.getDebugPredicate().contains("row_id"));
        Assertions.assertTrue(result.getDebugPredicate().contains("label"));
        Assertions.assertEquals(2, result.getPushedConjuncts().size());
    }

    @Test
    public void testLargeUtf8LiteralEncoding() {
        Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "large_label"), new StringLiteral("large string"));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        int ordinal = envelope.getBaseSchema().getNamesList().indexOf("large_label");
        Assertions.assertEquals(12, ordinal);
        Assertions.assertEquals(1, envelope.getBaseSchema().getStruct()
                .getTypes(ordinal).getString().getTypeVariationReference());
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertEquals("large string", literal.getString());
        Assertions.assertEquals(1, literal.getTypeVariationReference());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testLargeUtf8InLiteralEncoding() {
        Expr predicate = new InPredicate(
                new SlotRef(null, "large_label"),
                Arrays.asList(new StringLiteral("alpha"), new StringLiteral("omega")),
                false);

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        io.substrait.proto.Expression.SingularOrList in = envelope.getReferredExpr(0)
                .getExpression().getSingularOrList();
        Assertions.assertEquals(2, in.getOptionsCount());
        Assertions.assertEquals(1, in.getOptions(0).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(1, in.getOptions(1).getLiteral().getTypeVariationReference());
        Assertions.assertEquals("alpha", in.getOptions(0).getLiteral().getString());
        Assertions.assertEquals("omega", in.getOptions(1).getLiteral().getString());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testTimestampLiteralEncoding() {
        assertTimestampLiteral(
                "timestamp_s",
                new DateLiteral(1970, 1, 1, 0, 0, 1, 0,
                        ScalarType.createDatetimeV2Type(0)),
                0,
                1);
        assertTimestampLiteral(
                "timestamp_ms",
                new DateLiteral(1969, 12, 31, 23, 59, 59, 123000,
                        ScalarType.createDatetimeV2Type(3)),
                3,
                -877);
        assertTimestampLiteral(
                "timestamp_us",
                new DateLiteral(1970, 1, 1, 0, 0, 0, 123456,
                        ScalarType.createDatetimeV2Type(6)),
                6,
                123456);
    }

    @Test
    public void testUnsupportedTimestampPrecisionAndTimezoneRemainResidual() {
        LancePredicateConverter.ConversionResult result = converter.convert(Arrays.asList(
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "timestamp_s"),
                        new DateLiteral(1970, 1, 1, 0, 0, 0, 1,
                                ScalarType.createDatetimeV2Type(6))),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "timestamp_ms"),
                        new DateLiteral(1970, 1, 1, 0, 0, 0, 123001,
                                ScalarType.createDatetimeV2Type(6))),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "timestamp_ns"),
                        new DateLiteral(1970, 1, 1, 0, 0, 0, 123456,
                                ScalarType.createDatetimeV2Type(6))),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "timestamp_us_utc"),
                        new DateLiteral(1970, 1, 1, 0, 0, 0, 123456,
                                ScalarType.createDatetimeV2Type(6))),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "timestamp_us"),
                        new DateLiteral(1970, 1, 1, 0, 0, 0, Type.DATETIME))));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testComparisonInAndIsNull() {
        Expr comparison = new BinaryPredicate(BinaryPredicate.Operator.GT,
                new SlotRef(null, "row_id"), new IntLiteral(2));
        Expr in = new InPredicate(new SlotRef(null, "row_id"),
                Arrays.asList(new IntLiteral(3), new IntLiteral(4)), false);
        Expr isNotNull = new IsNullPredicate(new SlotRef(null, "label"), true);

        LancePredicateConverter.ConversionResult result =
                converter.convert(Arrays.asList(comparison, in, isNotNull));

        Assertions.assertTrue(result.getSubstraitFilter().length > 0);
        Assertions.assertDoesNotThrow(() -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        Assertions.assertEquals(3, result.getPushedConjuncts().size());
    }

    @Test
    public void testStringPredicates() {
        Expr legacyLike = new LikePredicate(LikePredicate.Operator.LIKE,
                new SlotRef(null, "label"), new StringLiteral("ready%"));
        Expr like = stringFunction(
                "like", new SlotRef(null, "label"), new StringLiteral("%ead_"));
        Expr startsWith = stringFunction(
                "starts_with", new SlotRef(null, "label"), new StringLiteral("ready"));
        Expr endsWith = stringFunction(
                "ends_with", new SlotRef(null, "large_label"), new StringLiteral("done"));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Arrays.asList(legacyLike, like, startsWith, endsWith));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        String serialized = envelope.toString();
        Assertions.assertTrue(serialized.contains("like:str_str"));
        Assertions.assertTrue(serialized.contains("starts_with:str_str"));
        Assertions.assertTrue(serialized.contains("ends_with:str_str"));
        Assertions.assertEquals(4, result.getPushedConjuncts().size());
    }

    @Test
    public void testUnsupportedStringPredicatesRemainResidual() {
        Expr regexp = new LikePredicate(LikePredicate.Operator.REGEXP,
                new SlotRef(null, "label"), new StringLiteral("ready.*"));
        Expr escapedLike = new LikePredicate(LikePredicate.Operator.LIKE,
                new SlotRef(null, "label"), new StringLiteral("ready\\%"));
        Expr explicitEscape = stringFunction("like",
                new SlotRef(null, "label"), new StringLiteral("ready!%"), new StringLiteral("!"));
        Expr nonLiteralPattern = stringFunction(
                "starts_with", new SlotRef(null, "label"), new SlotRef(null, "event-type"));
        Expr nonStringInput = stringFunction(
                "ends_with", new SlotRef(null, "row_id"), new StringLiteral("1"));

        LancePredicateConverter.ConversionResult result = converter.convert(
                Arrays.asList(regexp, escapedLike, explicitEscape, nonLiteralPattern, nonStringInput));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testResolvedUdfAndNulStringPredicatesRemainResidual() {
        FunctionCallExpr udf = stringFunction(
                "starts_with", new SlotRef(null, "label"), new StringLiteral("ready"));
        udf.getFn().setBinaryType(TFunctionBinaryType.JAVA_UDF);
        Expr legacyNulLike = new LikePredicate(LikePredicate.Operator.LIKE,
                new SlotRef(null, "label"), new StringLiteral("m\0_"));
        Expr functionNulLike = stringFunction(
                "like", new SlotRef(null, "label"), new StringLiteral("m\0_"));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Arrays.asList(udf, legacyNulLike, functionNulLike));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testDirectBooleanPredicates() {
        LancePredicateConverter boolConverter = new LancePredicateConverter(new Schema(
                Collections.singletonList(Field.nullable("active", ArrowType.Bool.INSTANCE))));
        Expr active = new SlotRef(null, "active");
        Expr notActive = new CompoundPredicate(
                CompoundPredicate.Operator.NOT, new SlotRef(null, "active"), null);

        LancePredicateConverter.ConversionResult result =
                boolConverter.convert(Arrays.asList(active, notActive));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        String serialized = envelope.toString();
        Assertions.assertTrue(serialized.contains("equal:any_any"));
        Assertions.assertTrue(serialized.contains("not:bool"));
        Assertions.assertEquals(2, result.getPushedConjuncts().size());
    }

    @Test
    public void testNullableNullSafeEqualityPreservesTwoValuedSemantics() {
        Expr nullableNullSafeEqual = new BinaryPredicate(BinaryPredicate.Operator.EQ_FOR_NULL,
                new SlotRef(null, "label"), new StringLiteral("ready"));

        LancePredicateConverter.ConversionResult nullableResult =
                converter.convert(Collections.singletonList(nullableNullSafeEqual));

        ExtendedExpression nullableEnvelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(nullableResult.getSubstraitFilter()));
        String nullableExpression = nullableEnvelope.toString();
        Assertions.assertTrue(nullableExpression.contains("and:bool"));
        Assertions.assertTrue(nullableExpression.contains("is_not_null:any"));
        Assertions.assertTrue(nullableExpression.contains("equal:any_any"));
        io.substrait.proto.Expression.ScalarFunction nullableRoot = nullableEnvelope
                .getReferredExpr(0).getExpression().getScalarFunction();
        Assertions.assertEquals(2, nullableRoot.getArgumentsCount());
        Assertions.assertEquals(1, nullableRoot.getArguments(0).getValue()
                .getScalarFunction().getArgumentsCount());
        Assertions.assertEquals(2, nullableRoot.getArguments(1).getValue()
                .getScalarFunction().getArgumentsCount());

        Expr requiredNullSafeEqual = new BinaryPredicate(BinaryPredicate.Operator.EQ_FOR_NULL,
                new SlotRef(null, "row_id"), new IntLiteral(1));
        LancePredicateConverter.ConversionResult requiredResult =
                converter.convert(Collections.singletonList(requiredNullSafeEqual));
        ExtendedExpression requiredEnvelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(requiredResult.getSubstraitFilter()));
        Assertions.assertFalse(requiredEnvelope.toString().contains("is_not_null:any"));
        Assertions.assertEquals(2, requiredEnvelope.getReferredExpr(0).getExpression()
                .getScalarFunction().getArgumentsCount());
    }

    @Test
    public void testNullableNullSafeEqualityInBooleanCompositions() {
        Expr nullSafeEqual = new BinaryPredicate(BinaryPredicate.Operator.EQ_FOR_NULL,
                new SlotRef(null, "label"), new StringLiteral("ready"));
        Expr comparison = new BinaryPredicate(BinaryPredicate.Operator.GT,
                new SlotRef(null, "row_id"), new IntLiteral(1));

        assertNullSafeEqualityComposition(new CompoundPredicate(
                CompoundPredicate.Operator.NOT, nullSafeEqual, null));
        assertNullSafeEqualityComposition(new CompoundPredicate(
                CompoundPredicate.Operator.AND, nullSafeEqual, comparison));
        assertNullSafeEqualityComposition(new CompoundPredicate(
                CompoundPredicate.Operator.OR, nullSafeEqual, comparison));
    }

    @Test
    public void testSpecialIdentifierUsesSchemaOrdinal() {
        Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "event-type"), new StringLiteral("ready"));

        LancePredicateConverter.ConversionResult result = converter.convert(Collections.singletonList(predicate));

        Assertions.assertTrue(result.getSubstraitFilter().length > 0);
        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        Assertions.assertEquals("event-type", envelope.getBaseSchema().getNames(2));
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testPartiallyConvertibleOrRemainsResidual() {
        Expr supported = new BinaryPredicate(BinaryPredicate.Operator.GT,
                new SlotRef(null, "row_id"), new IntLiteral(2));
        Expr unsupported = new FunctionCallExpr("abs", Collections.singletonList(new SlotRef(null, "row_id")));
        Expr or = new CompoundPredicate(CompoundPredicate.Operator.OR, supported, unsupported);

        LancePredicateConverter.ConversionResult result = converter.convert(Collections.singletonList(or));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertEquals("", result.getDebugPredicate());
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testOutOfRangeIntegerRemainsResidual() {
        Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "row_id"), new LargeIntLiteral(BigInteger.ONE.shiftLeft(64)));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testUnsignedIntegerLiteralEncoding() {
        assertUnsignedIntegerLiteral(
                "uint8_value", new IntLiteral(255), 8, 255);
        assertUnsignedIntegerLiteral(
                "uint16_value", new IntLiteral(65535), 16, 65535);
        assertUnsignedIntegerLiteral(
                "uint32_value", new IntLiteral(4294967295L), 32, -1);
        assertUnsignedIntegerLiteral(
                "uint64_value",
                new LargeIntLiteral(new BigInteger("18446744073709551615")),
                64,
                -1);
    }

    @Test
    public void testUnsignedIntegerInLiteralEncoding() {
        Expr predicate = new InPredicate(
                new SlotRef(null, "uint32_value"),
                Arrays.asList(new IntLiteral(0), new IntLiteral(4294967295L)),
                false);

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        io.substrait.proto.Expression.SingularOrList in = envelope.getReferredExpr(0)
                .getExpression().getSingularOrList();
        Assertions.assertEquals(2, in.getOptionsCount());
        Assertions.assertEquals(1, in.getOptions(0).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(1, in.getOptions(1).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(0, in.getOptions(0).getLiteral().getI32());
        Assertions.assertEquals(-1, in.getOptions(1).getLiteral().getI32());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testUnsignedIntegerOutOfRangeLiteralsRemainResidual() {
        LancePredicateConverter.ConversionResult result = converter.convert(Arrays.asList(
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "uint8_value"), new IntLiteral(-1)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "uint8_value"), new IntLiteral(256)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "uint16_value"), new IntLiteral(65536)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "uint32_value"), new IntLiteral(4294967296L)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "uint64_value"),
                        new LargeIntLiteral(BigInteger.ONE.shiftLeft(64)))));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    public void testUnsignedVariationDoesNotLeakIntoSignedLiteral() {
        Expr signed = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "row_id"), new IntLiteral(1));
        Expr unsigned = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "uint8_value"), new IntLiteral(1));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Arrays.asList(signed, unsigned));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        io.substrait.proto.Expression.ScalarFunction and =
                envelope.getReferredExpr(0).getExpression().getScalarFunction();
        io.substrait.proto.Expression.Literal signedLiteral = and.getArguments(0).getValue()
                .getScalarFunction().getArguments(1).getValue().getLiteral();
        io.substrait.proto.Expression.Literal unsignedLiteral = and.getArguments(1).getValue()
                .getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertEquals(0, signedLiteral.getTypeVariationReference());
        Assertions.assertEquals(1, unsignedLiteral.getTypeVariationReference());
    }

    @Test
    public void testDecimalLiteralEncoding() {
        Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.GE,
                new SlotRef(null, "decimal_9_2"), new DecimalLiteral(new BigDecimal("0.00")));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertTrue(literal.hasDecimal());
        Assertions.assertEquals(9, literal.getDecimal().getPrecision());
        Assertions.assertEquals(2, literal.getDecimal().getScale());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testDateDayLiteralEncoding() {
        Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.GE,
                new SlotRef(null, "date_value"), new DateLiteral(1970, 1, 1, Type.DATEV2));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertTrue(literal.hasDate());
        Assertions.assertEquals(0, literal.getDate());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    @Test
    public void testUnsupportedDateUnitAndDecimal256RemainResidual() {
        Expr date = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "date_millis"), new DateLiteral(1970, 1, 1, Type.DATEV2));
        Expr decimal = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "decimal256"), new DecimalLiteral(new BigDecimal("1.0000")));
        Expr decimal256Small = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "decimal256_small"), new DecimalLiteral(new BigDecimal("1.0000")));
        Expr legacyDate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "date_value"), new DateLiteral(1970, 1, 1, Type.DATE));

        LancePredicateConverter.ConversionResult result =
                converter.convert(Arrays.asList(date, decimal, decimal256Small, legacyDate));

        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    private void assertUnsignedIntegerLiteral(
            String columnName, Expr literalExpression, int bitWidth, long encodedValue) {
        Expr predicate = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, new SlotRef(null, columnName), literalExpression);

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        int ordinal = envelope.getBaseSchema().getNamesList().indexOf(columnName);
        Assertions.assertTrue(ordinal >= 0);
        io.substrait.proto.Type type = envelope.getBaseSchema().getStruct().getTypes(ordinal);
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertEquals(1, literal.getTypeVariationReference());
        switch (bitWidth) {
            case 8:
                Assertions.assertEquals(1, type.getI8().getTypeVariationReference());
                Assertions.assertEquals(encodedValue, literal.getI8());
                break;
            case 16:
                Assertions.assertEquals(1, type.getI16().getTypeVariationReference());
                Assertions.assertEquals(encodedValue, literal.getI16());
                break;
            case 32:
                Assertions.assertEquals(1, type.getI32().getTypeVariationReference());
                Assertions.assertEquals(encodedValue, literal.getI32());
                break;
            case 64:
                Assertions.assertEquals(1, type.getI64().getTypeVariationReference());
                Assertions.assertEquals(encodedValue, literal.getI64());
                break;
            default:
                Assertions.fail("Unexpected integer bit width: " + bitWidth);
        }
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    private FunctionCallExpr stringFunction(String name, Expr... arguments) {
        FunctionCallExpr function = new FunctionCallExpr(name, Arrays.asList(arguments));
        function.setFn(new ScalarFunction(new FunctionName(name),
                Collections.nCopies(arguments.length, Type.VARCHAR), Type.BOOLEAN, false, true));
        return function;
    }

    private void assertNullSafeEqualityComposition(Expr predicate) {
        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        String expression = envelope.toString();
        Assertions.assertTrue(expression.contains("is_not_null:any"));
        Assertions.assertTrue(expression.contains("equal:any_any"));
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }

    private void assertTimestampLiteral(
            String columnName, DateLiteral literalExpression, int precision, long encodedValue) {
        Expr predicate = new BinaryPredicate(
                BinaryPredicate.Operator.EQ,
                new SlotRef(null, columnName),
                literalExpression);

        LancePredicateConverter.ConversionResult result =
                converter.convert(Collections.singletonList(predicate));

        ExtendedExpression envelope = Assertions.assertDoesNotThrow(
                () -> ExtendedExpression.parseFrom(result.getSubstraitFilter()));
        int ordinal = envelope.getBaseSchema().getNamesList().indexOf(columnName);
        Assertions.assertTrue(ordinal >= 0);
        io.substrait.proto.Type.PrecisionTimestamp type = envelope.getBaseSchema()
                .getStruct().getTypes(ordinal).getPrecisionTimestamp();
        io.substrait.proto.Expression.Literal.PrecisionTimestamp literal =
                envelope.getReferredExpr(0).getExpression().getScalarFunction()
                        .getArguments(1).getValue().getLiteral().getPrecisionTimestamp();
        Assertions.assertEquals(precision, type.getPrecision());
        Assertions.assertEquals(precision, literal.getPrecision());
        Assertions.assertEquals(encodedValue, literal.getValue());
        Assertions.assertEquals(1, result.getPushedConjuncts().size());
    }
}
