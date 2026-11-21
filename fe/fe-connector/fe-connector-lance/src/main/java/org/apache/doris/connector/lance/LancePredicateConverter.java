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

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Converts the conjuncts whose Doris and Lance semantics are identical to one Substrait ExtendedExpression.
 *
 * <p>A conjunct is either converted whole or not at all: the engine removes a converted conjunct from the
 * Doris residuals ({@code ScanNodePropertiesResult.withPushdownTracking}), so a translation that is not exact
 * would return wrong rows. Conjuncts holding a CAST never arrive here, because the Lance connector does not
 * support CAST pushdown and the engine keeps those in Doris.
 */
final class LancePredicateConverter {
    private static final TypeCreator REQUIRED = TypeCreator.of(false);
    /** Names the YAML that declares {@code array_has}; lance-c resolves Substrait functions by name. */
    static final String LANCE_FUNCTIONS = "https://github.com/apache/doris/blob/master/"
            + "fe/fe-connector/fe-connector-lance/src/main/resources/substrait/lance_functions.yaml";
    private static final SimpleExtension.ExtensionCollection EXTENSIONS = loadExtensions();

    private final Schema schema;
    private final Map<String, ResolvedField> fieldsByLowerCaseName = new HashMap<>();

    LancePredicateConverter(Schema schema) {
        this.schema = schema;
        List<Field> fields = schema.getFields();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            Field field = fields.get(ordinal);
            fieldsByLowerCaseName.putIfAbsent(
                    field.getName().toLowerCase(Locale.ROOT), new ResolvedField(field, ordinal));
        }
    }

    /**
     * The conjuncts of the engine's filter, in the order the residual protocol numbers them: the children of a
     * top-level AND, or the filter itself.
     */
    static List<ConnectorExpression> conjuncts(Optional<ConnectorExpression> filter) {
        if (!filter.isPresent()) {
            return Collections.emptyList();
        }
        return filter.get() instanceof ConnectorAnd
                ? ((ConnectorAnd) filter.get()).getConjuncts() : Collections.singletonList(filter.get());
    }

    ConversionResult convert(List<ConnectorExpression> conjuncts) {
        List<Expression> filters = new ArrayList<>();
        List<ConnectorExpression> pushedConjuncts = new ArrayList<>();
        Set<Integer> notPushedIndices = new TreeSet<>();
        for (int i = 0; i < conjuncts.size(); i++) {
            Optional<Expression> filter = convert(conjuncts.get(i));
            if (filter.isPresent()) {
                filters.add(filter.get());
                pushedConjuncts.add(conjuncts.get(i));
            } else {
                notPushedIndices.add(i);
            }
        }
        if (filters.isEmpty()) {
            return new ConversionResult(new byte[0], "", pushedConjuncts, notPushedIndices);
        }
        Expression combined = filters.size() == 1 ? filters.get(0) : booleanFunction("and:bool", filters);
        String debugPredicate = pushedConjuncts.stream()
                .map(LancePredicateConverter::render)
                .map(sql -> "(" + sql + ")")
                .collect(Collectors.joining(" AND "));
        return new ConversionResult(new LanceSubstraitSerializer(schema).serialize(combined), debugPredicate,
                pushedConjuncts, notPushedIndices);
    }

    private Optional<Expression> convert(ConnectorExpression expr) {
        if (expr instanceof ConnectorComparison) {
            return convertComparison((ConnectorComparison) expr);
        }
        if (expr instanceof ConnectorAnd) {
            return convertAll("and:bool", ((ConnectorAnd) expr).getConjuncts());
        }
        if (expr instanceof ConnectorOr) {
            return convertAll("or:bool", ((ConnectorOr) expr).getDisjuncts());
        }
        if (expr instanceof ConnectorNot) {
            return convert(((ConnectorNot) expr).getOperand())
                    .map(operand -> booleanFunction("not:bool", Collections.singletonList(operand)));
        }
        if (expr instanceof ConnectorIn) {
            return convertIn((ConnectorIn) expr);
        }
        if (expr instanceof ConnectorIsNull) {
            return convertIsNull((ConnectorIsNull) expr);
        }
        if (expr instanceof ConnectorLike) {
            return convertLike((ConnectorLike) expr);
        }
        if (expr instanceof ConnectorFunctionCall) {
            return convertFunction((ConnectorFunctionCall) expr);
        }
        if (expr instanceof ConnectorColumnRef) {
            return convertBooleanColumn((ConnectorColumnRef) expr);
        }
        return Optional.empty();
    }

    /** An AND or an OR is converted only when every operand is; the engine flattens nested ones. */
    private Optional<Expression> convertAll(String function, List<ConnectorExpression> operands) {
        List<Expression> converted = new ArrayList<>(operands.size());
        for (ConnectorExpression operand : operands) {
            Optional<Expression> expression = convert(operand);
            if (!expression.isPresent()) {
                return Optional.empty();
            }
            converted.add(expression.get());
        }
        return Optional.of(booleanFunction(function, converted));
    }

    private Optional<Expression> convertComparison(ConnectorComparison predicate) {
        ResolvedField field = findField(predicate.getLeft());
        if (field == null || !LanceSubstraitSerializer.supportsType(field.field.getType())
                || !(predicate.getRight() instanceof ConnectorLiteral)) {
            return Optional.empty();
        }
        ConnectorLiteral literal = (ConnectorLiteral) predicate.getRight();
        ConnectorComparison.Operator operator = predicate.getOperator();
        Expression fieldReference = fieldReference(field);
        if (literal.isNull()) {
            // Doris EQ_FOR_NULL is the null-safe equality operator (<=>). For a direct column,
            // `column <=> NULL` has exactly the same two-valued semantics as `column IS NULL`.
            if (operator == ConnectorComparison.Operator.EQ_FOR_NULL) {
                return Optional.of(comparisonFunction("is_null:any", fieldReference));
            }
            // Ordinary comparisons with NULL evaluate to UNKNOWN. Keep them in Doris so NOT,
            // AND, and OR continue to observe Doris SQL three-valued logic.
            return Optional.empty();
        }
        Optional<Expression> value = convertLiteral(field.field.getType(), literal);
        if (!value.isPresent()) {
            return Optional.empty();
        }
        String function;
        switch (operator) {
            case EQ:
            case EQ_FOR_NULL:
                function = "equal:any_any";
                break;
            case NE:
                function = "not_equal:any_any";
                break;
            case LT:
                function = "lt:any_any";
                break;
            case LE:
                function = "lte:any_any";
                break;
            case GT:
                function = "gt:any_any";
                break;
            case GE:
                function = "gte:any_any";
                break;
            default:
                return Optional.empty();
        }
        Expression comparison = comparisonFunction(function, fieldReference, value.get());
        if (operator == ConnectorComparison.Operator.EQ_FOR_NULL && field.field.isNullable()) {
            // Null-safe equality returns FALSE, rather than NULL, for a NULL field. Preserve that
            // two-valued result when this expression is nested under NOT, AND, or OR.
            // column <=> 10 -----> column is not null and column = 10
            return Optional.of(booleanFunction("and:bool", Arrays.asList(
                    comparisonFunction("is_not_null:any", fieldReference), comparison)));
        }
        return Optional.of(comparison);
    }

    private Optional<Expression> convertIn(ConnectorIn predicate) {
        ResolvedField field = findField(predicate.getValue());
        if (field == null || !LanceSubstraitSerializer.supportsType(field.field.getType())
                || predicate.getInList().isEmpty()) {
            return Optional.empty();
        }
        List<Expression> values = new ArrayList<>(predicate.getInList().size());
        for (ConnectorExpression element : predicate.getInList()) {
            // Keep NULL in an IN list in Doris until its three-valued semantics have
            // differential coverage for every supported Lance/DataFusion version.
            if (!(element instanceof ConnectorLiteral) || ((ConnectorLiteral) element).isNull()) {
                return Optional.empty();
            }
            Optional<Expression> value = convertLiteral(field.field.getType(), (ConnectorLiteral) element);
            if (!value.isPresent()) {
                return Optional.empty();
            }
            values.add(value.get());
        }
        Expression in = Expression.SingleOrList.builder()
                .condition(fieldReference(field))
                .options(values)
                .build();
        return Optional.of(predicate.isNegated()
                ? booleanFunction("not:bool", Collections.singletonList(in)) : in);
    }

    private Optional<Expression> convertIsNull(ConnectorIsNull predicate) {
        ResolvedField field = findField(predicate.getOperand());
        if (field == null || !LanceSubstraitSerializer.supportsType(field.field.getType())) {
            return Optional.empty();
        }
        String function = predicate.isNegated() ? "is_not_null:any" : "is_null:any";
        return Optional.of(comparisonFunction(function, fieldReference(field)));
    }

    private Optional<Expression> convertLike(ConnectorLike predicate) {
        if (predicate.getOperator() != ConnectorLike.Operator.LIKE) {
            return Optional.empty();
        }
        return convertStringPredicate("like:str_str", predicate.getValue(), predicate.getPattern(), true);
    }

    /**
     * Only two-argument builtin calls of these names. The engine hands the plugin a call by name and arguments; an
     * expression it cannot model arrives as an argument-less call named by its SQL text, which never matches. A
     * Java or Python UDF may carry one of these names but computes something else, so it stays in Doris.
     */
    private Optional<Expression> convertFunction(ConnectorFunctionCall function) {
        if (!function.isBuiltin() || function.getArguments().size() != 2) {
            return Optional.empty();
        }
        ConnectorExpression first = function.getArguments().get(0);
        ConnectorExpression second = function.getArguments().get(1);
        switch (function.getFunctionName().toLowerCase(Locale.ROOT)) {
            case "arrays_overlap":
                return convertArraysOverlap(first, second);
            case "array_contains":
                return convertArrayContains(first, second);
            case "starts_with":
                return convertStringPredicate("starts_with:str_str", first, second, false);
            case "ends_with":
                return convertStringPredicate("ends_with:str_str", first, second, false);
            default:
                return Optional.empty();
        }
    }

    private Optional<Expression> convertArraysOverlap(ConnectorExpression first, ConnectorExpression second) {
        ConnectorExpression input = first;
        ConnectorExpression values = second;
        if (arrayElements(first).isPresent()) {
            input = second;
            values = first;
        }
        ResolvedField field = findField(input);
        Optional<List<?>> needles = arrayElements(values);
        if (field == null || !LanceSubstraitSerializer.supportsStringList(field.field)
                || !needles.isPresent() || needles.get().isEmpty()) {
            return Optional.empty();
        }
        // Nereids folds three or more membership disjuncts into arrays_overlap. Expand
        // only non-NULL string needles: Doris also matches NULL elements to NULL needles.
        List<Expression> level = new ArrayList<>();
        for (Object needle : needles.get()) {
            if (!(needle instanceof String)) {
                return Optional.empty();
            }
            level.add(arrayMembership(field, (String) needle));
        }
        // Keep expression depth logarithmic for large constant label lists.
        while (level.size() > 1) {
            List<Expression> next = new ArrayList<>();
            for (int i = 0; i < level.size(); i += 2) {
                next.add(i + 1 == level.size() ? level.get(i)
                        : booleanFunction("or:bool", Arrays.asList(level.get(i), level.get(i + 1))));
            }
            level = next;
        }
        return Optional.of(level.get(0));
    }

    private Optional<Expression> convertArrayContains(ConnectorExpression input, ConnectorExpression needle) {
        ResolvedField field = findField(input);
        String value = stringLiteral(needle);
        if (field == null || !LanceSubstraitSerializer.supportsStringList(field.field) || value == null) {
            return Optional.empty();
        }
        // Doris matches a NULL needle against NULL elements; DataFusion does not. Also,
        // array_contains_all is a contiguous subsequence test, not DataFusion's set containment.
        // Only non-NULL scalar membership is interchangeable across the two engines.
        return Optional.of(arrayMembership(field, value));
    }

    private Expression arrayMembership(ResolvedField field, String needle) {
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(EXTENSIONS.getScalarFunction(
                        SimpleExtension.FunctionAnchor.of(LANCE_FUNCTIONS, "array_has:list_str")))
                .outputType(TypeCreator.of(field.field.isNullable()).BOOLEAN)
                .arguments(Arrays.asList(fieldReference(field),
                        ExpressionCreator.string(false, needle)))
                .build();
    }

    private Optional<Expression> convertStringPredicate(
            String function, ConnectorExpression input, ConnectorExpression pattern, boolean rejectEscapedPattern) {
        ResolvedField field = findField(input);
        String patternValue = stringLiteral(pattern);
        if (field == null || !isStringType(field.field.getType()) || patternValue == null) {
            return Optional.empty();
        }
        // Doris uses backslash as LIKE's default escape character, while the Substrait function
        // has no escape argument. Keep escaped LIKE patterns in Doris rather than changing meaning.
        if (patternValue.indexOf('\0') >= 0
                || (rejectEscapedPattern && patternValue.indexOf('\\') >= 0)) {
            return Optional.empty();
        }
        return Optional.of(stringFunction(function, fieldReference(field),
                ExpressionCreator.string(false, patternValue)));
    }

    private Optional<Expression> convertBooleanColumn(ConnectorColumnRef column) {
        ResolvedField field = findField(column);
        if (field == null || !(field.field.getType() instanceof ArrowType.Bool)) {
            return Optional.empty();
        }
        return Optional.of(comparisonFunction("equal:any_any",
                fieldReference(field), ExpressionCreator.bool(false, true)));
    }

    /**
     * Converts a non-NULL Doris literal to a Substrait literal of the field's Arrow type. The literal's Doris type
     * decides which values qualify, as the 4.x converter's literal classes did; see the engine's value domain in
     * the {@code pushdown} package javadoc.
     */
    private Optional<Expression> convertLiteral(ArrowType type, ConnectorLiteral literal) {
        String literalType = literal.getType().getTypeName();
        Object value = literal.getValue();
        if (type instanceof ArrowType.Bool && "BOOLEAN".equals(literalType)) {
            return Optional.of(ExpressionCreator.bool(false, (Boolean) value));
        } else if (type instanceof ArrowType.Int && isIntegerLiteral(literalType)) {
            ArrowType.Int integer = (ArrowType.Int) type;
            BigInteger number = "LARGEINT".equals(literalType)
                    ? new BigInteger((String) value) : BigInteger.valueOf((Long) value);
            int bitWidth = integer.getBitWidth();
            BigInteger minimum = integer.getIsSigned()
                    ? BigInteger.ONE.shiftLeft(bitWidth - 1).negate() : BigInteger.ZERO;
            BigInteger maximum = BigInteger.ONE.shiftLeft(
                    integer.getIsSigned() ? bitWidth - 1 : bitWidth).subtract(BigInteger.ONE);
            if (number.compareTo(minimum) < 0 || number.compareTo(maximum) > 0) {
                return Optional.empty();
            }
            switch (integer.getBitWidth()) {
                case 8:
                    return Optional.of(ExpressionCreator.i8(false, number.intValueExact()));
                case 16:
                    return Optional.of(ExpressionCreator.i16(false, number.intValueExact()));
                case 32:
                    return Optional.of(ExpressionCreator.i32(
                            false, integer.getIsSigned() ? number.intValueExact() : number.intValue()));
                case 64:
                    return Optional.of(ExpressionCreator.i64(
                            false, integer.getIsSigned() ? number.longValueExact() : number.longValue()));
                default:
                    return Optional.empty();
            }
        } else if (isStringType(type) && isStringLiteral(literalType)) {
            return Optional.of(ExpressionCreator.string(false, (String) value));
        } else if (type instanceof ArrowType.Timestamp && "DATETIMEV2".equals(literalType)) {
            return convertTimestampLiteral((ArrowType.Timestamp) type, (LocalDateTime) value);
        } else if (type instanceof ArrowType.Date && "DATEV2".equals(literalType)) {
            // Arrow Date(DAY) and Substrait Date both use signed days from the Unix epoch.
            // Accept only a DATEV2 literal; do not truncate DATETIME or reinterpret legacy
            // DATE semantics in this direct pushdown path.
            return Optional.of(ExpressionCreator.date(false,
                    Math.toIntExact(((LocalDate) value).toEpochDay())));
        } else if (type instanceof ArrowType.Decimal
                && (literalType.startsWith("DECIMAL") || isIntegerLiteral(literalType))) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            BigDecimal number;
            if (value instanceof BigDecimal) {
                number = (BigDecimal) value;
            } else {
                number = "LARGEINT".equals(literalType)
                        ? new BigDecimal((String) value) : BigDecimal.valueOf((Long) value);
            }
            // ExpressionCreator.decimal truncates to the field's scale without complaint, which would turn
            // `column = 12.505` into `column = 12.50`; push only a value the field's type holds exactly.
            BigDecimal exact;
            try {
                exact = number.setScale(decimal.getScale(), RoundingMode.UNNECESSARY);
            } catch (ArithmeticException e) {
                return Optional.empty();
            }
            if (exact.precision() > decimal.getPrecision()) {
                return Optional.empty();
            }
            return Optional.of(ExpressionCreator.decimal(false, exact, decimal.getPrecision(), decimal.getScale()));
        } else if (type instanceof ArrowType.FloatingPoint
                && ("FLOAT".equals(literalType) || "DOUBLE".equals(literalType)
                        || (isIntegerLiteral(literalType) && !"LARGEINT".equals(literalType)))) {
            double number = ((Number) value).doubleValue();
            if (!Double.isFinite(number)) {
                return Optional.empty();
            }
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                float floatValue = (float) number;
                return Float.isFinite(floatValue)
                        ? Optional.of(ExpressionCreator.fp32(false, floatValue))
                        : Optional.empty();
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return Optional.of(ExpressionCreator.fp64(false, number));
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Expression> convertTimestampLiteral(ArrowType.Timestamp timestamp,
            LocalDateTime dateTime) {
        int precision = LanceSubstraitSerializer.timestampPrecision(timestamp);
        if (precision < 0) {
            return Optional.empty();
        }
        long micros = dateTime.getNano() / 1000;
        if ((timestamp.getUnit() == TimeUnit.SECOND && micros != 0)
                || (timestamp.getUnit() == TimeUnit.MILLISECOND && micros % 1000 != 0)) {
            return Optional.empty();
        }
        try {
            // UTC is only the coordinate system for encoding this timezone-naive wall-clock
            // value; no session-timezone conversion is intended.
            long epochSeconds = dateTime.toEpochSecond(ZoneOffset.UTC);
            long value;
            switch (timestamp.getUnit()) {
                case SECOND:
                    value = epochSeconds;
                    break;
                case MILLISECOND:
                    value = Math.addExact(Math.multiplyExact(epochSeconds, 1000), micros / 1000);
                    break;
                case MICROSECOND:
                    value = Math.addExact(Math.multiplyExact(epochSeconds, 1000000), micros);
                    break;
                default:
                    return Optional.empty();
            }
            return Optional.of(ExpressionCreator.precisionTimestamp(false, value, precision));
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
    }

    private static boolean isIntegerLiteral(String literalType) {
        return "TINYINT".equals(literalType) || "SMALLINT".equals(literalType) || "INT".equals(literalType)
                || "BIGINT".equals(literalType) || "LARGEINT".equals(literalType);
    }

    private static boolean isStringLiteral(String literalType) {
        return "CHAR".equals(literalType) || "VARCHAR".equals(literalType) || "STRING".equals(literalType);
    }

    private static boolean isStringType(ArrowType type) {
        return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
    }

    /** The value of a non-NULL string literal, or null for anything else. */
    private static String stringLiteral(ConnectorExpression expr) {
        if (!(expr instanceof ConnectorLiteral) || ((ConnectorLiteral) expr).isNull()
                || !isStringLiteral(((ConnectorLiteral) expr).getType().getTypeName())) {
            return null;
        }
        return (String) ((ConnectorLiteral) expr).getValue();
    }

    /** The element values of an ARRAY literal of a string type: a list, NULL elements included as null. */
    static Optional<List<?>> arrayElements(ConnectorExpression expr) {
        if (!(expr instanceof ConnectorLiteral)
                || !"ARRAY".equals(((ConnectorLiteral) expr).getType().getTypeName())
                || !(((ConnectorLiteral) expr).getValue() instanceof List)) {
            return Optional.empty();
        }
        // LARGEINT, IPv4 and JSON elements are Strings too; only a string element type compares as text.
        ConnectorType elementType = ((ConnectorLiteral) expr).getType().getChildren().get(0);
        if (!isStringLiteral(elementType.getTypeName())) {
            return Optional.empty();
        }
        return Optional.of((List<?>) ((ConnectorLiteral) expr).getValue());
    }

    // A column reference with the field's ordinal and Substrait type.
    private Expression fieldReference(ResolvedField field) {
        return FieldReference.newRootStructReference(field.ordinal, LanceSubstraitSerializer.fieldType(field.field));
    }

    private static Expression comparisonFunction(String key, Expression... arguments) {
        return scalarFunction(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, key, Arrays.asList(arguments));
    }

    private static Expression booleanFunction(String key, List<Expression> arguments) {
        return scalarFunction(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, key, arguments);
    }

    private static Expression stringFunction(String key, Expression... arguments) {
        return scalarFunction(DefaultExtensionCatalog.FUNCTIONS_STRING, key, Arrays.asList(arguments));
    }

    private static Expression scalarFunction(String uri, String key, List<Expression> arguments) {
        SimpleExtension.ScalarFunctionVariant declaration = EXTENSIONS.getScalarFunction(
                SimpleExtension.FunctionAnchor.of(uri, key));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(REQUIRED.BOOLEAN)
                .arguments(arguments)
                .build();
    }

    /** The Lance field a direct column reference names, matched without case; null for anything else. */
    private ResolvedField findField(ConnectorExpression expr) {
        if (!(expr instanceof ConnectorColumnRef)) {
            return null;
        }
        return fieldsByLowerCaseName.get(((ConnectorColumnRef) expr).getColumnName().toLowerCase(Locale.ROOT));
    }

    /** The Lance field name a direct column reference resolves to, matched as {@link #convert} matches it. */
    Optional<String> resolveFieldName(ConnectorExpression expr) {
        ResolvedField field = findField(expr);
        return field == null ? Optional.empty() : Optional.of(field.field.getName());
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        try (InputStream definitions =
                LancePredicateConverter.class.getResourceAsStream("/substrait/lance_functions.yaml")) {
            return SimpleExtension.loadDefaults().merge(SimpleExtension.load(LANCE_FUNCTIONS, definitions));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Substrait extension definitions", e);
        }
    }

    /**
     * SQL-like text of a pushed conjunct for EXPLAIN. Only the shapes {@link #convert} accepts reach it, and
     * string literals are quoted so an empty or spaced value stays visible.
     */
    static String render(ConnectorExpression expr) {
        if (expr instanceof ConnectorComparison) {
            ConnectorComparison comparison = (ConnectorComparison) expr;
            return render(comparison.getLeft()) + " " + comparison.getOperator().getSymbol() + " "
                    + render(comparison.getRight());
        } else if (expr instanceof ConnectorAnd) {
            return renderAll(((ConnectorAnd) expr).getConjuncts(), " AND ");
        } else if (expr instanceof ConnectorOr) {
            return renderAll(((ConnectorOr) expr).getDisjuncts(), " OR ");
        } else if (expr instanceof ConnectorNot) {
            return "NOT (" + render(((ConnectorNot) expr).getOperand()) + ")";
        } else if (expr instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expr;
            return render(in.getValue()) + (in.isNegated() ? " NOT IN " : " IN ")
                    + in.getInList().stream().map(LancePredicateConverter::render)
                            .collect(Collectors.joining(", ", "(", ")"));
        } else if (expr instanceof ConnectorIsNull) {
            ConnectorIsNull isNull = (ConnectorIsNull) expr;
            return render(isNull.getOperand()) + (isNull.isNegated() ? " IS NOT NULL" : " IS NULL");
        } else if (expr instanceof ConnectorLike) {
            ConnectorLike like = (ConnectorLike) expr;
            return render(like.getValue()) + " " + like.getOperator() + " " + render(like.getPattern());
        } else if (expr instanceof ConnectorFunctionCall) {
            ConnectorFunctionCall call = (ConnectorFunctionCall) expr;
            return call.getFunctionName() + call.getArguments().stream().map(LancePredicateConverter::render)
                    .collect(Collectors.joining(", ", "(", ")"));
        } else if (expr instanceof ConnectorLiteral) {
            return renderValue(((ConnectorLiteral) expr).getValue());
        }
        return expr.toString();
    }

    private static String renderAll(List<ConnectorExpression> operands, String separator) {
        return operands.stream().map(operand -> "(" + render(operand) + ")")
                .collect(Collectors.joining(separator));
    }

    private static String renderValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        } else if (value instanceof List) {
            return ((List<?>) value).stream().map(LancePredicateConverter::renderValue)
                    .collect(Collectors.joining(", ", "[", "]"));
        } else if (value instanceof LocalDate || value instanceof LocalDateTime) {
            return "'" + value.toString().replace('T', ' ') + "'";
        }
        return value.toString();
    }

    private static class ResolvedField {
        private final Field field;
        private final int ordinal;

        private ResolvedField(Field field, int ordinal) {
            this.field = field;
            this.ordinal = ordinal;
        }
    }

    /** The Substrait filter of the pushed conjuncts, which conjuncts those are, and which stay in Doris. */
    static final class ConversionResult {
        private final byte[] substraitFilter;
        private final String debugPredicate;
        private final List<ConnectorExpression> pushedConjuncts;
        private final Set<Integer> notPushedIndices;

        private ConversionResult(byte[] substraitFilter, String debugPredicate,
                List<ConnectorExpression> pushedConjuncts, Set<Integer> notPushedIndices) {
            this.substraitFilter = substraitFilter;
            this.debugPredicate = debugPredicate;
            this.pushedConjuncts = Collections.unmodifiableList(new ArrayList<>(pushedConjuncts));
            this.notPushedIndices = Collections.unmodifiableSet(new TreeSet<>(notPushedIndices));
        }

        /** The serialized ExtendedExpression; empty when no conjunct was converted. */
        byte[] getSubstraitFilter() {
            return substraitFilter;
        }

        String getDebugPredicate() {
            return debugPredicate;
        }

        List<ConnectorExpression> getPushedConjuncts() {
            return pushedConjuncts;
        }

        /** The positions, in the conjunct list, of the conjuncts Doris must still evaluate. */
        Set<Integer> getNotPushedIndices() {
            return notPushedIndices;
        }

        boolean allPushed() {
            return notPushedIndices.isEmpty();
        }
    }
}
