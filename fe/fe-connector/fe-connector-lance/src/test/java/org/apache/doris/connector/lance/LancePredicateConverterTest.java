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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Which conjuncts become Substrait and how they are encoded. The input is what the engine hands a connector:
 * {@code ConnectorExpression}s whose literals carry the Doris literal type and the SPI's value shapes.
 */
class LancePredicateConverterTest {
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            new Field("row_id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            Field.nullable("label", ArrowType.Utf8.INSTANCE),
            Field.nullable("nested", ArrowType.Struct.INSTANCE),
            Field.nullable("decimal_9_2", new ArrowType.Decimal(9, 2, 128)),
            Field.nullable("date_value", new ArrowType.Date(DateUnit.DAY)),
            Field.nullable("date_millis", new ArrowType.Date(DateUnit.MILLISECOND)),
            Field.nullable("decimal256", new ArrowType.Decimal(39, 4, 256)),
            Field.nullable("uint8_value", new ArrowType.Int(8, false)),
            Field.nullable("uint64_value", new ArrowType.Int(64, false)),
            Field.nullable("large_label", ArrowType.LargeUtf8.INSTANCE),
            Field.nullable("timestamp_ms", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            Field.nullable("timestamp_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
            Field.nullable("timestamp_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
            Field.nullable("flag", ArrowType.Bool.INSTANCE),
            Field.nullable("float_value", new ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)),
            new Field("labels", FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE)))));

    private final LancePredicateConverter converter = new LancePredicateConverter(SCHEMA);

    private static ConnectorColumnRef column(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("STRING"));
    }

    private static ConnectorLiteral literal(String type, Object value) {
        return new ConnectorLiteral(ConnectorType.of(type), value);
    }

    private static ConnectorComparison compare(ConnectorComparison.Operator operator, ConnectorExpression left,
            ConnectorExpression right) {
        return new ConnectorComparison(operator, left, right);
    }

    private static ConnectorFunctionCall call(String name, ConnectorExpression... arguments) {
        return new ConnectorFunctionCall(name, ConnectorType.of("BOOLEAN"), Arrays.asList(arguments));
    }

    /** A call that resolved to a Java or Python UDF, which may carry a builtin's name. */
    private static ConnectorFunctionCall udfCall(String name, ConnectorExpression... arguments) {
        return new ConnectorFunctionCall(name, ConnectorType.of("BOOLEAN"), Arrays.asList(arguments), false);
    }

    private LancePredicateConverter.ConversionResult convert(ConnectorExpression... conjuncts) {
        return converter.convert(Arrays.asList(conjuncts));
    }

    private void assertPushed(ConnectorExpression conjunct) {
        Assertions.assertTrue(convert(conjunct).allPushed(), "expected pushed: " + conjunct);
    }

    private void assertResidual(ConnectorExpression conjunct) {
        LancePredicateConverter.ConversionResult result = convert(conjunct);
        Assertions.assertEquals(Collections.singleton(0), result.getNotPushedIndices(), "expected residual: " + conjunct);
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
    }

    private static ExtendedExpression envelope(LancePredicateConverter.ConversionResult result) throws Exception {
        return ExtendedExpression.parseFrom(result.getSubstraitFilter());
    }

    /** Unsupported fields keep their place in the envelope, so field references keep their ordinals. */
    @Test
    void envelopeKeepsEveryFieldAndEncodesTheLiteral() throws Exception {
        LancePredicateConverter.ConversionResult result = convert(
                compare(ConnectorComparison.Operator.GE, column("row_id"), literal("BIGINT", 2L)),
                compare(ConnectorComparison.Operator.EQ, column("LABEL"), literal("VARCHAR", "O'Reilly")));
        Assertions.assertTrue(result.allPushed());
        ExtendedExpression envelope = envelope(result);
        Assertions.assertEquals("doris-lance", envelope.getVersion().getProducer());
        Assertions.assertEquals(SCHEMA.getFields().size(), envelope.getBaseSchema().getStruct().getTypesCount());
        Assertions.assertEquals("__unlikely_name_placeholder_doris_2", envelope.getBaseSchema().getNames(2));
        Assertions.assertTrue(envelope.getBaseSchema().getStruct().getTypes(2).hasUserDefined());
        io.substrait.proto.Expression.ScalarFunction and =
                envelope.getReferredExpr(0).getExpression().getScalarFunction();
        io.substrait.proto.Expression.ScalarFunction label = and.getArguments(1).getValue().getScalarFunction();
        Assertions.assertEquals(1, label.getArguments(0).getValue().getSelection().getDirectReference()
                .getStructField().getField());
        Assertions.assertEquals("O'Reilly", label.getArguments(1).getValue().getLiteral().getString());
        Assertions.assertEquals("(row_id >= 2) AND (LABEL = 'O''Reilly')", result.getDebugPredicate());
    }

    /** Each top-level conjunct is pushed or kept on its own; the engine drops the pushed ones by position. */
    @Test
    void notPushedIndicesNameTheConjunctsDorisStillEvaluates() {
        ConnectorExpression pushed = compare(ConnectorComparison.Operator.EQ, column("row_id"), literal("BIGINT", 1L));
        ConnectorExpression unsupportedType = compare(ConnectorComparison.Operator.EQ, column("decimal256"),
                literal("DECIMAL256", new BigDecimal("1.0000")));
        ConnectorExpression unknownColumn = compare(ConnectorComparison.Operator.EQ, column("missing"),
                literal("BIGINT", 1L));
        LancePredicateConverter.ConversionResult result = convert(unsupportedType, pushed, unknownColumn);
        Assertions.assertEquals(Arrays.asList(0, 2), Arrays.asList(result.getNotPushedIndices().toArray()));
        Assertions.assertEquals(Collections.singletonList(pushed), result.getPushedConjuncts());
        Assertions.assertFalse(result.allPushed());

        ConnectorAnd filter = new ConnectorAnd(Arrays.asList(unsupportedType, pushed));
        Assertions.assertEquals(Arrays.asList(unsupportedType, pushed),
                LancePredicateConverter.conjuncts(Optional.of(filter)));
        Assertions.assertEquals(Collections.singletonList(pushed),
                LancePredicateConverter.conjuncts(Optional.of(pushed)));
        Assertions.assertTrue(LancePredicateConverter.conjuncts(Optional.empty()).isEmpty());
    }

    @Test
    void comparisonsInListsAndNullChecks() {
        for (ConnectorComparison.Operator operator : Arrays.asList(ConnectorComparison.Operator.EQ,
                ConnectorComparison.Operator.NE, ConnectorComparison.Operator.LT, ConnectorComparison.Operator.LE,
                ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE)) {
            assertPushed(compare(operator, column("row_id"), literal("INT", 3L)));
        }
        assertPushed(new ConnectorIn(column("row_id"), Arrays.asList(literal("INT", 1L), literal("INT", 2L)), false));
        assertPushed(new ConnectorIn(column("row_id"), Arrays.asList(literal("INT", 1L), literal("INT", 2L)), true));
        assertPushed(new ConnectorIsNull(column("label"), false));
        assertPushed(new ConnectorIsNull(column("label"), true));
        // A column must be on the left and a literal on the right, as the 4.x converter required.
        assertResidual(compare(ConnectorComparison.Operator.EQ, literal("INT", 3L), column("row_id")));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("row_id"), column("uint8_value")));
        // NULL inside IN, an empty IN and comparisons with NULL keep Doris three-valued logic.
        assertResidual(new ConnectorIn(column("row_id"),
                Arrays.asList(literal("INT", 1L), ConnectorLiteral.ofNull(ConnectorType.of("INT"))), false));
        assertResidual(new ConnectorIn(column("row_id"), Collections.emptyList(), false));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("row_id"),
                ConnectorLiteral.ofNull(ConnectorType.of("BIGINT"))));
        assertResidual(new ConnectorIsNull(column("nested"), false));
    }

    @Test
    void nullSafeEqualityKeepsItsTwoValuedResult() throws Exception {
        ExtendedExpression isNull = envelope(convert(compare(ConnectorComparison.Operator.EQ_FOR_NULL,
                column("label"), ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR")))));
        Assertions.assertTrue(isNull.toString().contains("is_null:any"));
        // A nullable column: column <=> v becomes column IS NOT NULL AND column = v.
        ExtendedExpression nullable = envelope(convert(compare(ConnectorComparison.Operator.EQ_FOR_NULL,
                column("label"), literal("VARCHAR", "a"))));
        Assertions.assertTrue(nullable.toString().contains("is_not_null:any"));
        Assertions.assertTrue(nullable.toString().contains("and:bool"));
        ExtendedExpression required = envelope(convert(compare(ConnectorComparison.Operator.EQ_FOR_NULL,
                column("row_id"), literal("BIGINT", 1L))));
        Assertions.assertFalse(required.toString().contains("is_not_null:any"));
    }

    /** OR, AND and NOT are pushed only when every operand converts. */
    @Test
    void booleanCompositionsPushOnlyWhenEveryOperandConverts() {
        ConnectorExpression a = compare(ConnectorComparison.Operator.EQ, column("row_id"), literal("BIGINT", 1L));
        ConnectorExpression b = compare(ConnectorComparison.Operator.EQ, column("label"), literal("VARCHAR", "x"));
        ConnectorExpression residual = compare(ConnectorComparison.Operator.EQ, column("nested"),
                literal("VARCHAR", "x"));
        assertPushed(new ConnectorOr(Arrays.asList(a, b)));
        assertPushed(new ConnectorNot(new ConnectorOr(Arrays.asList(a, b))));
        assertPushed(new ConnectorOr(Arrays.asList(new ConnectorAnd(Arrays.asList(a, b)), b)));
        assertResidual(new ConnectorOr(Arrays.asList(a, residual)));
        assertResidual(new ConnectorNot(residual));
        assertResidual(new ConnectorOr(Arrays.asList(new ConnectorAnd(Arrays.asList(a, residual)), b)));
        assertPushed(column("flag"));
        assertResidual(column("label"));
    }

    @Test
    void stringPredicates() throws Exception {
        assertPushed(new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "a%")));
        assertPushed(call("starts_with", column("label"), literal("VARCHAR", "a%_")));
        assertPushed(call("ends_with", column("large_label"), literal("STRING", "z")));
        // Doris escapes LIKE with a backslash; the Substrait function has no escape argument.
        assertResidual(new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "a\\%")));
        assertResidual(new ConnectorLike(ConnectorLike.Operator.REGEXP, column("label"), literal("VARCHAR", "a.*")));
        assertResidual(call("starts_with", column("label"), literal("VARCHAR", "a\0b")));
        assertResidual(call("starts_with", column("row_id"), literal("VARCHAR", "1")));
        // A three-argument LIKE ... ESCAPE, and an expression the engine could not model, never match.
        assertResidual(call("like", column("label"), literal("VARCHAR", "a!%"), literal("VARCHAR", "!")));
        assertResidual(new ConnectorFunctionCall("starts_with(label, 'a') = true", ConnectorType.of("BOOLEAN"),
                Collections.emptyList()));
        ExtendedExpression large = envelope(convert(
                compare(ConnectorComparison.Operator.EQ, column("large_label"), literal("STRING", "x"))));
        io.substrait.proto.Expression.ScalarFunction equal =
                large.getReferredExpr(0).getExpression().getScalarFunction();
        Assertions.assertEquals(1, equal.getArguments(1).getValue().getLiteral().getTypeVariationReference());
    }

    @Test
    void integerLiteralsFollowTheFieldWidthAndSignedness() throws Exception {
        assertPushed(compare(ConnectorComparison.Operator.EQ, column("uint8_value"), literal("SMALLINT", 255L)));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("uint8_value"), literal("SMALLINT", 256L)));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("uint8_value"), literal("SMALLINT", -1L)));
        ExtendedExpression unsigned = envelope(convert(compare(ConnectorComparison.Operator.EQ,
                column("uint64_value"), literal("LARGEINT", "18446744073709551615"))));
        io.substrait.proto.Expression.Literal value = unsigned.getReferredExpr(0).getExpression()
                .getScalarFunction().getArguments(1).getValue().getLiteral();
        Assertions.assertEquals(-1L, value.getI64());
        Assertions.assertEquals(1, value.getTypeVariationReference());
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("uint64_value"),
                literal("LARGEINT", "18446744073709551616")));
        // A signed field's literal carries no unsigned variation.
        ExtendedExpression signed = envelope(convert(compare(ConnectorComparison.Operator.EQ,
                column("row_id"), literal("BIGINT", -1L))));
        Assertions.assertEquals(0, signed.getReferredExpr(0).getExpression().getScalarFunction()
                .getArguments(1).getValue().getLiteral().getTypeVariationReference());
    }

    @Test
    void temporalDecimalAndFloatLiterals() throws Exception {
        assertPushed(compare(ConnectorComparison.Operator.EQ, column("timestamp_ms"),
                literal("DATETIMEV2", LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_000_000))));
        // A value finer than the field's unit, a legacy DATETIME, nanoseconds and time zones stay in Doris.
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("timestamp_ms"),
                literal("DATETIMEV2", LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_001_000))));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("timestamp_ms"),
                literal("DATETIME", LocalDateTime.of(2024, 1, 2, 3, 4, 5))));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("timestamp_ns"),
                literal("DATETIMEV2", LocalDateTime.of(2024, 1, 2, 3, 4, 5))));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("timestamp_utc"),
                literal("DATETIMEV2", LocalDateTime.of(2024, 1, 2, 3, 4, 5))));
        ExtendedExpression date = envelope(convert(compare(ConnectorComparison.Operator.EQ, column("date_value"),
                literal("DATEV2", LocalDate.of(1969, 12, 31)))));
        Assertions.assertEquals(-1, date.getReferredExpr(0).getExpression().getScalarFunction()
                .getArguments(1).getValue().getLiteral().getDate());
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("date_value"),
                literal("DATE", LocalDate.of(2024, 1, 1))));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("date_millis"),
                literal("DATEV2", LocalDate.of(2024, 1, 1))));
        assertPushed(compare(ConnectorComparison.Operator.EQ, column("decimal_9_2"),
                literal("DECIMAL64", new BigDecimal("12.50"))));
        assertPushed(compare(ConnectorComparison.Operator.EQ, column("decimal_9_2"), literal("INT", 12L)));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("decimal_9_2"),
                literal("DECIMAL64", new BigDecimal("12.505"))));
        assertResidual(compare(ConnectorComparison.Operator.EQ, column("decimal_9_2"),
                literal("DECIMAL128", new BigDecimal("12345678.00"))));
        assertPushed(compare(ConnectorComparison.Operator.EQ, column("decimal_9_2"),
                literal("DECIMAL128", new BigDecimal("1234567.000"))));
        assertPushed(compare(ConnectorComparison.Operator.GT, column("float_value"), literal("DOUBLE", 1.5d)));
        assertPushed(compare(ConnectorComparison.Operator.GT, column("float_value"), literal("INT", 1L)));
        assertResidual(compare(ConnectorComparison.Operator.GT, column("float_value"),
                literal("DOUBLE", Double.NaN)));
        assertResidual(compare(ConnectorComparison.Operator.GT, column("float_value"), literal("DOUBLE", 1e300)));
    }

    @Test
    void arrayMembership() throws Exception {
        ConnectorExpression red = call("array_contains", column("labels"), literal("VARCHAR", "red"));
        ExtendedExpression contains = envelope(convert(red));
        Assertions.assertTrue(contains.getExtensionsList().stream()
                .anyMatch(extension -> extension.hasExtensionFunction()
                        && extension.getExtensionFunction().getName().startsWith("array_has:")));
        Assertions.assertTrue(contains.getExtensionUrisList().stream()
                .anyMatch(uri -> uri.getUri().equals(LancePredicateConverter.LANCE_FUNCTIONS)));
        // Doris matches a NULL needle against NULL elements; DataFusion does not.
        assertResidual(call("array_contains", column("labels"), ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR"))));
        assertResidual(call("array_contains_all", column("labels"), literal("VARCHAR", "red")));
        assertResidual(call("array_contains", column("label"), literal("VARCHAR", "red")));

        List<Object> needles = Arrays.asList("red", "blue", "green");
        ConnectorLiteral values = new ConnectorLiteral(ConnectorType.arrayOf(ConnectorType.of("VARCHAR")), needles);
        for (ConnectorExpression overlap : Arrays.asList(call("arrays_overlap", column("labels"), values),
                call("arrays_overlap", values, column("labels")))) {
            String encoded = envelope(convert(overlap)).toString();
            Assertions.assertTrue(encoded.contains("or:bool"));
            Assertions.assertTrue(encoded.contains("green"));
        }
        assertResidual(call("arrays_overlap", column("labels"), new ConnectorLiteral(
                ConnectorType.arrayOf(ConnectorType.of("VARCHAR")), Arrays.asList("red", null))));
        assertResidual(call("arrays_overlap", column("labels"), new ConnectorLiteral(
                ConnectorType.arrayOf(ConnectorType.of("VARCHAR")), Collections.emptyList())));
        // LARGEINT elements arrive as Strings, but they do not compare with labels as text.
        assertResidual(call("arrays_overlap", column("labels"), new ConnectorLiteral(
                ConnectorType.arrayOf(ConnectorType.of("LARGEINT")), Arrays.asList("1", "2"))));
        // Without element values (a literal rendered to text) the needles cannot be read back exactly.
        assertResidual(call("arrays_overlap", column("labels"), new ConnectorLiteral(
                ConnectorType.arrayOf(ConnectorType.of("VARCHAR")), "[red, blue]")));
    }

    // ---------------------------------------------------------------------------------------------------------
    // The cases of branch-4.2's LancePredicateConverterTest, on its eighteen-field schema. Its inputs were legacy
    // Exprs; here they are what the engine hands a connector. A Java UDF that 4.2 marked with
    // TFunctionBinaryType.JAVA_UDF is a ConnectorFunctionCall whose isBuiltin() is false.
    // ---------------------------------------------------------------------------------------------------------

    private static final Schema SCHEMA_42 = new Schema(Arrays.asList(
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
            Field.nullable("timestamp_us_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    private final LancePredicateConverter converter42 = new LancePredicateConverter(SCHEMA_42);

    private LancePredicateConverter.ConversionResult convert42(ConnectorExpression... conjuncts) {
        return converter42.convert(Arrays.asList(conjuncts));
    }

    private static ConnectorComparison equal(String columnName, ConnectorLiteral value) {
        return compare(ConnectorComparison.Operator.EQ, column(columnName), value);
    }

    private static LancePredicateConverter arrayConverter(ArrowType container, ArrowType element) {
        return new LancePredicateConverter(new Schema(Collections.singletonList(new Field("labels",
                FieldType.nullable(container), Collections.singletonList(Field.nullable("item", element))))));
    }

    private static ConnectorExpression contains(String needle) {
        return call("array_contains", column("labels"), literal("VARCHAR", needle));
    }

    private static io.substrait.proto.Expression.Literal rightLiteral(ExtendedExpression envelope) {
        return envelope.getReferredExpr(0).getExpression().getScalarFunction().getArguments(1).getValue()
                .getLiteral();
    }

    @Test
    void stringArrayMembershipSchemaAndFunction() throws Exception {
        // An unsupported preceding field must not shift the array's field reference.
        LancePredicateConverter.ConversionResult result = new LancePredicateConverter(new Schema(Arrays.asList(
                Field.nullable("nested", ArrowType.Struct.INSTANCE),
                new Field("labels", FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE))))))
                .convert(Collections.singletonList(contains("red")));
        Assertions.assertTrue(result.allPushed());
        ExtendedExpression envelope = envelope(result);
        Assertions.assertEquals(Arrays.asList("__unlikely_name_placeholder_doris_0", "labels"),
                envelope.getBaseSchema().getNamesList());
        io.substrait.proto.Type.List list = envelope.getBaseSchema().getStruct().getTypes(1).getList();
        Assertions.assertEquals(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE, list.getNullability());
        Assertions.assertTrue(list.getType().hasString());
        Assertions.assertEquals(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE,
                list.getType().getString().getNullability());
        io.substrait.proto.Expression.ScalarFunction function =
                envelope.getReferredExpr(0).getExpression().getScalarFunction();
        Assertions.assertEquals(1, function.getArguments(0).getValue().getSelection()
                .getDirectReference().getStructField().getField());
        Assertions.assertEquals("red", function.getArguments(1).getValue().getLiteral().getString());
        Assertions.assertEquals(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE,
                function.getOutputType().getBool().getNullability());
        Assertions.assertTrue(envelope.getExtensionsList().stream()
                .anyMatch(extension -> extension.hasExtensionFunction()
                        && extension.getExtensionFunction().getName().startsWith("array_has:")));
    }

    @Test
    void arrayMembershipBooleanCombinations() {
        LancePredicateConverter arrays = arrayConverter(ArrowType.List.INSTANCE, ArrowType.Utf8.INSTANCE);
        ConnectorExpression red = contains("red");
        ConnectorExpression blue = contains("blue");
        for (ConnectorExpression predicate : Arrays.asList(red, new ConnectorAnd(Arrays.asList(red, blue)),
                new ConnectorOr(Arrays.asList(red, blue)), new ConnectorNot(red))) {
            Assertions.assertTrue(arrays.convert(Collections.singletonList(predicate)).allPushed(),
                    predicate.toString());
        }
    }

    @Test
    void rewrittenArrayOverlapPushdown() throws Exception {
        LancePredicateConverter arrays = arrayConverter(ArrowType.List.INSTANCE, ArrowType.Utf8.INSTANCE);
        ConnectorType stringArray = ConnectorType.arrayOf(ConnectorType.of("STRING"));
        ConnectorLiteral values = new ConnectorLiteral(stringArray, Arrays.asList("red", "blue", "green"));
        for (ConnectorExpression predicate : Arrays.asList(call("arrays_overlap", column("labels"), values),
                call("arrays_overlap", values, column("labels")),
                new ConnectorNot(call("arrays_overlap", column("labels"), values)))) {
            LancePredicateConverter.ConversionResult result = arrays.convert(Collections.singletonList(predicate));
            Assertions.assertTrue(result.allPushed(), predicate.toString());
            String encoded = envelope(result).toString();
            Assertions.assertTrue(encoded.contains("array_has:list_str"));
            Assertions.assertTrue(encoded.contains("or:bool"));
            Assertions.assertTrue(encoded.contains("green"));
        }
        // A NULL needle can match a NULL element in Doris and must not become array_has.
        for (ConnectorExpression valuesWithDifferentSemantics : Arrays.asList(
                new ConnectorLiteral(stringArray, Arrays.asList("red", null)),
                new ConnectorLiteral(stringArray, Collections.emptyList()), column("labels"))) {
            ConnectorExpression predicate = call("arrays_overlap", column("labels"), valuesWithDifferentSemantics);
            Assertions.assertEquals(Collections.singleton(0),
                    arrays.convert(Collections.singletonList(predicate)).getNotPushedIndices(), predicate.toString());
        }
    }

    @Test
    void unsafeArrayMembershipRemainsResidual() {
        LancePredicateConverter arrays = arrayConverter(ArrowType.List.INSTANCE, ArrowType.Utf8.INSTANCE);
        ConnectorExpression red = contains("red");
        ConnectorExpression nullNeedle = call("array_contains", column("labels"),
                ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR")));
        ConnectorExpression udf = udfCall("array_contains", column("labels"), literal("VARCHAR", "red"));
        for (ConnectorExpression predicate : Arrays.asList(nullNeedle, udf,
                call("array_contains_all", column("labels"), literal("VARCHAR", "red")),
                call("array_contains", column("labels"), column("needle")),
                new ConnectorOr(Arrays.asList(red, nullNeedle)), new ConnectorNot(nullNeedle),
                new ConnectorIsNull(column("labels"), false))) {
            Assertions.assertEquals(Collections.singleton(0),
                    arrays.convert(Collections.singletonList(predicate)).getNotPushedIndices(), predicate.toString());
        }
        Assertions.assertEquals(1, arrays.convert(Arrays.asList(red, nullNeedle)).getPushedConjuncts().size());
        for (LancePredicateConverter unsupported : Arrays.asList(
                arrayConverter(ArrowType.List.INSTANCE, new ArrowType.Int(64, true)),
                arrayConverter(ArrowType.List.INSTANCE, ArrowType.LargeUtf8.INSTANCE),
                arrayConverter(ArrowType.LargeList.INSTANCE, ArrowType.Utf8.INSTANCE),
                arrayConverter(new ArrowType.FixedSizeList(2), ArrowType.Utf8.INSTANCE))) {
            Assertions.assertFalse(unsupported.convert(Collections.singletonList(red)).allPushed());
        }
    }

    @Test
    void comparisonAndStringLiteralEncoding() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(
                compare(ConnectorComparison.Operator.GE, column("row_id"), literal("TINYINT", 2L)),
                equal("label", literal("VARCHAR", "O'Reilly\0tail")));
        Assertions.assertEquals(2, result.getPushedConjuncts().size());
        ExtendedExpression envelope = envelope(result);
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
        for (int ordinal : new int[] {5, 6, 7}) {
            Assertions.assertTrue(envelope.getBaseSchema().getStruct().getTypes(ordinal).hasUserDefined());
        }
        Assertions.assertEquals(1, envelope.getReferredExprCount());
        io.substrait.proto.Expression.Literal literal = envelope.getReferredExpr(0)
                .getExpression().getScalarFunction().getArguments(1).getValue()
                .getScalarFunction().getArguments(1).getValue().getLiteral();
        // A NUL inside an equality literal is just a character; only LIKE patterns refuse it.
        Assertions.assertEquals("O'Reilly\0tail", literal.getString());
        Assertions.assertEquals(0, envelope.getBaseSchema().getStruct()
                .getTypes(1).getString().getTypeVariationReference());
        Assertions.assertEquals(0, literal.getTypeVariationReference());
        Assertions.assertTrue(result.getDebugPredicate().contains("row_id"));
        Assertions.assertTrue(result.getDebugPredicate().contains("label"));
    }

    @Test
    void largeUtf8LiteralEncoding() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(equal("large_label", literal("VARCHAR", "large string")));
        Assertions.assertTrue(result.allPushed());
        ExtendedExpression envelope = envelope(result);
        int ordinal = envelope.getBaseSchema().getNamesList().indexOf("large_label");
        Assertions.assertEquals(12, ordinal);
        Assertions.assertEquals(1, envelope.getBaseSchema().getStruct()
                .getTypes(ordinal).getString().getTypeVariationReference());
        Assertions.assertEquals("large string", rightLiteral(envelope).getString());
        Assertions.assertEquals(1, rightLiteral(envelope).getTypeVariationReference());
    }

    @Test
    void largeUtf8InLiteralEncoding() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(new ConnectorIn(column("large_label"),
                Arrays.asList(literal("VARCHAR", "alpha"), literal("VARCHAR", "omega")), false));
        Assertions.assertTrue(result.allPushed());
        io.substrait.proto.Expression.SingularOrList in = envelope(result).getReferredExpr(0)
                .getExpression().getSingularOrList();
        Assertions.assertEquals(2, in.getOptionsCount());
        Assertions.assertEquals(1, in.getOptions(0).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(1, in.getOptions(1).getLiteral().getTypeVariationReference());
        Assertions.assertEquals("alpha", in.getOptions(0).getLiteral().getString());
        Assertions.assertEquals("omega", in.getOptions(1).getLiteral().getString());
    }

    @Test
    void timestampLiteralEncoding() throws Exception {
        assertTimestampLiteral("timestamp_s", LocalDateTime.of(1970, 1, 1, 0, 0, 1), 0, 1);
        assertTimestampLiteral("timestamp_ms", LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_000_000), 3, -877);
        assertTimestampLiteral("timestamp_us", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000), 6, 123456);
    }

    private void assertTimestampLiteral(String columnName, LocalDateTime value, int precision, long encoded)
            throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(equal(columnName, literal("DATETIMEV2", value)));
        Assertions.assertTrue(result.allPushed(), columnName);
        ExtendedExpression envelope = envelope(result);
        int ordinal = envelope.getBaseSchema().getNamesList().indexOf(columnName);
        Assertions.assertEquals(precision, envelope.getBaseSchema().getStruct().getTypes(ordinal)
                .getPrecisionTimestamp().getPrecision());
        io.substrait.proto.Expression.Literal.PrecisionTimestamp literal =
                rightLiteral(envelope).getPrecisionTimestamp();
        Assertions.assertEquals(precision, literal.getPrecision());
        Assertions.assertEquals(encoded, literal.getValue());
    }

    @Test
    void unsupportedTimestampPrecisionAndTimezoneRemainResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                equal("timestamp_s", literal("DATETIMEV2", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1_000))),
                equal("timestamp_ms", literal("DATETIMEV2", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_001_000))),
                equal("timestamp_ns", literal("DATETIMEV2", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000))),
                equal("timestamp_us_utc", literal("DATETIMEV2",
                        LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000))),
                equal("timestamp_us", literal("DATETIME", LocalDateTime.of(1970, 1, 1, 0, 0, 0))));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    void stringPredicatesUseTheLanceFunctions() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(
                new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "ready%")),
                new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "%ead_")),
                call("starts_with", column("label"), literal("VARCHAR", "ready")),
                call("ends_with", column("large_label"), literal("VARCHAR", "done")));
        Assertions.assertEquals(4, result.getPushedConjuncts().size());
        String serialized = envelope(result).toString();
        Assertions.assertTrue(serialized.contains("like:str_str"));
        Assertions.assertTrue(serialized.contains("starts_with:str_str"));
        Assertions.assertTrue(serialized.contains("ends_with:str_str"));
    }

    @Test
    void unsupportedStringPredicatesRemainResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                new ConnectorLike(ConnectorLike.Operator.REGEXP, column("label"), literal("VARCHAR", "ready.*")),
                new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "ready\\%")),
                call("like", column("label"), literal("VARCHAR", "ready!%"), literal("VARCHAR", "!")),
                call("starts_with", column("label"), column("event-type")),
                call("ends_with", column("row_id"), literal("VARCHAR", "1")),
                new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "m\0_")));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    void resolvedUdfAndNulStringPredicatesRemainResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                udfCall("starts_with", column("label"), literal("VARCHAR", "ready")),
                new ConnectorLike(ConnectorLike.Operator.LIKE, column("label"), literal("VARCHAR", "m\0_")),
                call("like", column("label"), literal("VARCHAR", "m\0_")));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    /** Every name the converter translates is pushed as a builtin and stays in Doris as a UDF. */
    @Test
    void udfCallsWithBuiltinNamesRemainResidual() {
        ConnectorLiteral values = new ConnectorLiteral(ConnectorType.arrayOf(ConnectorType.of("VARCHAR")),
                Arrays.asList("red", "blue"));
        List<List<ConnectorExpression>> calls = Arrays.asList(
                Arrays.asList(column("label"), literal("VARCHAR", "ready")),
                Arrays.asList(column("label"), literal("VARCHAR", "done")),
                Arrays.asList(column("labels"), literal("VARCHAR", "red")),
                Arrays.asList(column("labels"), values));
        List<String> names = Arrays.asList("starts_with", "ends_with", "array_contains", "arrays_overlap");
        for (int i = 0; i < names.size(); i++) {
            ConnectorExpression[] arguments = calls.get(i).toArray(new ConnectorExpression[0]);
            assertPushed(call(names.get(i), arguments));
            assertResidual(udfCall(names.get(i), arguments));
        }
    }

    @Test
    void directBooleanPredicates() throws Exception {
        LancePredicateConverter.ConversionResult result = new LancePredicateConverter(new Schema(
                Collections.singletonList(Field.nullable("active", ArrowType.Bool.INSTANCE))))
                .convert(Arrays.asList(column("active"), new ConnectorNot(column("active"))));
        Assertions.assertEquals(2, result.getPushedConjuncts().size());
        String serialized = envelope(result).toString();
        Assertions.assertTrue(serialized.contains("equal:any_any"));
        Assertions.assertTrue(serialized.contains("not:bool"));
    }

    @Test
    void nullableNullSafeEqualityPreservesTwoValuedSemantics() throws Exception {
        io.substrait.proto.Expression.ScalarFunction nullable = envelope(convert42(compare(
                ConnectorComparison.Operator.EQ_FOR_NULL, column("label"), literal("VARCHAR", "ready"))))
                .getReferredExpr(0).getExpression().getScalarFunction();
        Assertions.assertEquals(2, nullable.getArgumentsCount());
        Assertions.assertEquals(1, nullable.getArguments(0).getValue().getScalarFunction().getArgumentsCount());
        Assertions.assertEquals(2, nullable.getArguments(1).getValue().getScalarFunction().getArgumentsCount());

        ExtendedExpression required = envelope(convert42(compare(ConnectorComparison.Operator.EQ_FOR_NULL,
                column("row_id"), literal("TINYINT", 1L))));
        Assertions.assertFalse(required.toString().contains("is_not_null:any"));
        Assertions.assertEquals(2, required.getReferredExpr(0).getExpression().getScalarFunction()
                .getArgumentsCount());
    }

    @Test
    void nullableNullSafeEqualityInBooleanCompositions() throws Exception {
        ConnectorExpression nullSafeEqual = compare(ConnectorComparison.Operator.EQ_FOR_NULL, column("label"),
                literal("VARCHAR", "ready"));
        ConnectorExpression comparison = compare(ConnectorComparison.Operator.GT, column("row_id"),
                literal("TINYINT", 1L));
        for (ConnectorExpression predicate : Arrays.asList(new ConnectorNot(nullSafeEqual),
                new ConnectorAnd(Arrays.asList(nullSafeEqual, comparison)),
                new ConnectorOr(Arrays.asList(nullSafeEqual, comparison)))) {
            LancePredicateConverter.ConversionResult result = convert42(predicate);
            Assertions.assertEquals(1, result.getPushedConjuncts().size());
            String expression = envelope(result).toString();
            Assertions.assertTrue(expression.contains("is_not_null:any"), predicate.toString());
            Assertions.assertTrue(expression.contains("equal:any_any"), predicate.toString());
        }
    }

    @Test
    void specialIdentifierUsesSchemaOrdinal() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(equal("event-type", literal("VARCHAR", "ready")));
        Assertions.assertTrue(result.allPushed());
        Assertions.assertEquals("event-type", envelope(result).getBaseSchema().getNames(2));
    }

    @Test
    void partiallyConvertibleOrRemainsResidual() {
        ConnectorExpression supported = compare(ConnectorComparison.Operator.GT, column("row_id"),
                literal("TINYINT", 2L));
        ConnectorExpression unsupported = new ConnectorFunctionCall("abs", ConnectorType.of("BIGINT"),
                Collections.singletonList(column("row_id")));
        LancePredicateConverter.ConversionResult result = convert42(
                new ConnectorOr(Arrays.asList(supported, unsupported)));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertEquals("", result.getDebugPredicate());
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    void outOfRangeIntegerRemainsResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                equal("row_id", literal("LARGEINT", "18446744073709551616")));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    void unsignedIntegerLiteralEncoding() throws Exception {
        assertUnsignedIntegerLiteral("uint8_value", literal("SMALLINT", 255L), 8, 255);
        assertUnsignedIntegerLiteral("uint16_value", literal("INT", 65535L), 16, 65535);
        assertUnsignedIntegerLiteral("uint32_value", literal("BIGINT", 4294967295L), 32, -1);
        assertUnsignedIntegerLiteral("uint64_value", literal("LARGEINT", "18446744073709551615"), 64, -1);
    }

    private void assertUnsignedIntegerLiteral(String columnName, ConnectorLiteral value, int bitWidth,
            long encoded) throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(equal(columnName, value));
        Assertions.assertTrue(result.allPushed(), columnName);
        ExtendedExpression envelope = envelope(result);
        io.substrait.proto.Type type = envelope.getBaseSchema().getStruct()
                .getTypes(envelope.getBaseSchema().getNamesList().indexOf(columnName));
        io.substrait.proto.Expression.Literal literal = rightLiteral(envelope);
        Assertions.assertEquals(1, literal.getTypeVariationReference());
        switch (bitWidth) {
            case 8:
                Assertions.assertEquals(1, type.getI8().getTypeVariationReference());
                Assertions.assertEquals(encoded, literal.getI8());
                break;
            case 16:
                Assertions.assertEquals(1, type.getI16().getTypeVariationReference());
                Assertions.assertEquals(encoded, literal.getI16());
                break;
            case 32:
                Assertions.assertEquals(1, type.getI32().getTypeVariationReference());
                Assertions.assertEquals(encoded, literal.getI32());
                break;
            default:
                Assertions.assertEquals(1, type.getI64().getTypeVariationReference());
                Assertions.assertEquals(encoded, literal.getI64());
                break;
        }
    }

    @Test
    void unsignedIntegerInLiteralEncoding() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(new ConnectorIn(column("uint32_value"),
                Arrays.asList(literal("TINYINT", 0L), literal("BIGINT", 4294967295L)), false));
        Assertions.assertTrue(result.allPushed());
        io.substrait.proto.Expression.SingularOrList in = envelope(result).getReferredExpr(0)
                .getExpression().getSingularOrList();
        Assertions.assertEquals(2, in.getOptionsCount());
        Assertions.assertEquals(1, in.getOptions(0).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(1, in.getOptions(1).getLiteral().getTypeVariationReference());
        Assertions.assertEquals(0, in.getOptions(0).getLiteral().getI32());
        Assertions.assertEquals(-1, in.getOptions(1).getLiteral().getI32());
    }

    @Test
    void unsignedIntegerOutOfRangeLiteralsRemainResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                equal("uint8_value", literal("TINYINT", -1L)),
                equal("uint8_value", literal("SMALLINT", 256L)),
                equal("uint16_value", literal("INT", 65536L)),
                equal("uint32_value", literal("BIGINT", 4294967296L)),
                equal("uint64_value", literal("LARGEINT", "18446744073709551616")));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }

    @Test
    void unsignedVariationDoesNotLeakIntoSignedLiteral() throws Exception {
        io.substrait.proto.Expression.ScalarFunction and = envelope(convert42(
                equal("row_id", literal("TINYINT", 1L)), equal("uint8_value", literal("TINYINT", 1L))))
                .getReferredExpr(0).getExpression().getScalarFunction();
        Assertions.assertEquals(0, and.getArguments(0).getValue().getScalarFunction().getArguments(1).getValue()
                .getLiteral().getTypeVariationReference());
        Assertions.assertEquals(1, and.getArguments(1).getValue().getScalarFunction().getArguments(1).getValue()
                .getLiteral().getTypeVariationReference());
    }

    @Test
    void decimalLiteralEncoding() throws Exception {
        LancePredicateConverter.ConversionResult result = convert42(compare(ConnectorComparison.Operator.GE,
                column("decimal_9_2"), literal("DECIMAL32", new BigDecimal("0.00"))));
        Assertions.assertTrue(result.allPushed());
        io.substrait.proto.Expression.Literal literal = rightLiteral(envelope(result));
        Assertions.assertTrue(literal.hasDecimal());
        Assertions.assertEquals(9, literal.getDecimal().getPrecision());
        Assertions.assertEquals(2, literal.getDecimal().getScale());
    }

    @Test
    void unsupportedDateUnitAndDecimal256RemainResidual() {
        LancePredicateConverter.ConversionResult result = convert42(
                equal("date_millis", literal("DATEV2", LocalDate.of(1970, 1, 1))),
                equal("decimal256", literal("DECIMAL64", new BigDecimal("1.0000"))),
                equal("decimal256_small", literal("DECIMAL64", new BigDecimal("1.0000"))),
                equal("date_value", literal("DATE", LocalDate.of(1970, 1, 1))));
        Assertions.assertEquals(0, result.getSubstraitFilter().length);
        Assertions.assertTrue(result.getPushedConjuncts().isEmpty());
    }
}
