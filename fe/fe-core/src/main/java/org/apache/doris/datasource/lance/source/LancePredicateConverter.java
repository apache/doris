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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type.Nullability;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
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
import java.util.stream.Collectors;

/** Converts predicates with identical Doris and Lance semantics to a Substrait ExtendedExpression. */
public class LancePredicateConverter {
    private static final int MAX_SUBSTRAIT_DECIMAL_PRECISION = 38;
    // DataFusion represents Arrow unsigned integers as the corresponding Substrait signed
    // integer type class with type variation 1.
    private static final int UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE = 1;
    // DataFusion distinguishes Arrow LargeUtf8 from Utf8 with the Substrait
    // large-container type variation.
    private static final int LARGE_CONTAINER_TYPE_VARIATION_REFERENCE = 1;
    private static final String UNSUPPORTED_FIELD_PREFIX = "__unlikely_name_placeholder_doris_";
    private static final TypeCreator REQUIRED = TypeCreator.of(false);
    private static final SimpleExtension.ExtensionCollection EXTENSIONS = loadExtensions();

    private final Schema schema;
    private final Map<String, ResolvedField> fieldsByLowerCaseName = new HashMap<>();

    public LancePredicateConverter(Schema schema) {
        this.schema = schema;
        List<Field> fields = schema.getFields();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            Field field = fields.get(ordinal);
            fieldsByLowerCaseName.putIfAbsent(
                    field.getName().toLowerCase(Locale.ROOT), new ResolvedField(field, ordinal));
        }
    }

    public ConversionResult convert(List<Expr> conjuncts) {
        List<Expression> filters = new ArrayList<>();
        List<Expr> pushedConjuncts = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            Optional<Expression> filter = convert(conjunct);
            if (filter.isPresent()) {
                filters.add(filter.get());
                pushedConjuncts.add(conjunct);
            }
        }
        if (filters.isEmpty()) {
            return new ConversionResult(new byte[0], "", pushedConjuncts);
        }

        Expression combined = filters.size() == 1 ? filters.get(0) : booleanFunction("and:bool", filters);
        String debugPredicate = pushedConjuncts.stream()
                .map(Expr::toSql)
                .map(LancePredicateConverter::parenthesize)
                .collect(Collectors.joining(" AND "));
        return new ConversionResult(serialize(combined), debugPredicate, pushedConjuncts);
    }

    private Optional<Expression> convert(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            return convertBinary((BinaryPredicate) expr);
        }
        if (expr instanceof CompoundPredicate) {
            return convertCompound((CompoundPredicate) expr);
        }
        if (expr instanceof InPredicate) {
            return convertIn((InPredicate) expr);
        }
        if (expr instanceof IsNullPredicate) {
            return convertIsNull((IsNullPredicate) expr);
        }
        if (expr instanceof LikePredicate) {
            return convertLike((LikePredicate) expr);
        }
        if (expr instanceof FunctionCallExpr) {
            return convertStringFunction((FunctionCallExpr) expr);
        }
        if (expr instanceof SlotRef) {
            return convertBooleanSlot((SlotRef) expr);
        }
        return Optional.empty();
    }

    private Optional<Expression> convertBinary(BinaryPredicate predicate) {
        SlotRef slot = directSlot(predicate.getChild(0));
        LiteralExpr literal = directLiteral(predicate.getChild(1));
        BinaryPredicate.Operator operator = predicate.getOp();
        if (slot == null || literal == null) {
            return Optional.empty();
        }

        ResolvedField field = findField(slot);
        if (field == null || !isPushdownType(field.field.getType())) {
            return Optional.empty();
        }
        Expression fieldReference = fieldReference(field);
        if (literal instanceof NullLiteral) {
            // Doris EQ_FOR_NULL is the null-safe equality operator (<=>). For a direct column,
            // `column <=> NULL` has exactly the same two-valued semantics as `column IS NULL`.
            if (operator == BinaryPredicate.Operator.EQ_FOR_NULL) {
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
        if (operator == BinaryPredicate.Operator.EQ_FOR_NULL && field.field.isNullable()) {
            // Null-safe equality returns FALSE, rather than NULL, for a NULL field. Preserve that
            // two-valued result when this expression is nested under NOT, AND, or OR.
            // column <=> 10 -----> column is not null and column = 10
            return Optional.of(booleanFunction("and:bool", Arrays.asList(
                    comparisonFunction("is_not_null:any", fieldReference), comparison)));
        }
        return Optional.of(comparison);
    }

    private Optional<Expression> convertCompound(CompoundPredicate predicate) {
        Optional<Expression> left = convert(predicate.getChild(0));
        if (!left.isPresent()) {
            return Optional.empty();
        }
        if (predicate.getOp() == CompoundPredicate.Operator.NOT) {
            return Optional.of(booleanFunction("not:bool", Collections.singletonList(left.get())));
        }
        Optional<Expression> right = convert(predicate.getChild(1));
        if (!right.isPresent()) {
            return Optional.empty();
        }
        switch (predicate.getOp()) {
            case AND:
                return Optional.of(booleanFunction("and:bool", Arrays.asList(left.get(), right.get())));
            case OR:
                return Optional.of(booleanFunction("or:bool", Arrays.asList(left.get(), right.get())));
            default:
                return Optional.empty();
        }
    }

    private Optional<Expression> convertIn(InPredicate predicate) {
        SlotRef slot = directSlot(predicate.getChild(0));
        ResolvedField field = slot == null ? null : findField(slot);
        if (field == null || !isPushdownType(field.field.getType()) || predicate.getInElementNum() == 0) {
            return Optional.empty();
        }
        List<Expression> values = new ArrayList<>(predicate.getInElementNum());
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            LiteralExpr literal = directLiteral(predicate.getChild(i));
            // Keep NULL in an IN list in Doris until its three-valued semantics have
            // differential coverage for every supported Lance/DataFusion version.
            if (literal == null || literal instanceof NullLiteral) {
                return Optional.empty();
            }
            Optional<Expression> value = convertLiteral(field.field.getType(), literal);
            if (!value.isPresent()) {
                return Optional.empty();
            }
            values.add(value.get());
        }
        Expression in = Expression.SingleOrList.builder()
                .condition(fieldReference(field))
                .options(values)
                .build();
        return Optional.of(predicate.isNotIn()
                ? booleanFunction("not:bool", Collections.singletonList(in)) : in);
    }

    private Optional<Expression> convertIsNull(IsNullPredicate predicate) {
        SlotRef slot = directSlot(predicate.getChild(0));
        ResolvedField field = slot == null ? null : findField(slot);
        if (field == null || !isPushdownType(field.field.getType())) {
            return Optional.empty();
        }
        String function = predicate.isNotNull() ? "is_not_null:any" : "is_null:any";
        return Optional.of(comparisonFunction(function, fieldReference(field)));
    }

    private Optional<Expression> convertLike(LikePredicate predicate) {
        if (predicate.getOp() != LikePredicate.Operator.LIKE) {
            return Optional.empty();
        }
        return convertStringPredicate("like:str_str", predicate.getChild(0), predicate.getChild(1), true);
    }

    private Optional<Expression> convertStringFunction(FunctionCallExpr function) {
        if (function.getFnName() == null || function.getChildren().size() != 2) {
            return Optional.empty();
        }
        String functionName = function.getFnName().getFunction().toLowerCase(Locale.ROOT);
        switch (functionName) {
            case "like":
                return convertStringPredicate(
                        "like:str_str", function.getChild(0), function.getChild(1), true);
            case "starts_with":
                return convertStringPredicate(
                        "starts_with:str_str", function.getChild(0), function.getChild(1), false);
            case "ends_with":
                return convertStringPredicate(
                        "ends_with:str_str", function.getChild(0), function.getChild(1), false);
            default:
                return Optional.empty();
        }
    }

    private Optional<Expression> convertStringPredicate(
            String function, Expr input, Expr pattern, boolean rejectEscapedPattern) {
        SlotRef slot = directSlot(input);
        LiteralExpr literal = directLiteral(pattern);
        ResolvedField field = slot == null ? null : findField(slot);
        if (field == null || !isStringType(field.field.getType()) || !(literal instanceof StringLiteral)) {
            return Optional.empty();
        }
        String patternValue = literal.getStringValue();
        // Doris uses backslash as LIKE's default escape character, while the Substrait function
        // has no escape argument. Keep escaped LIKE patterns in Doris rather than changing meaning.
        if (rejectEscapedPattern && patternValue.indexOf('\\') >= 0) {
            return Optional.empty();
        }
        return Optional.of(stringFunction(function, fieldReference(field),
                ExpressionCreator.string(false, patternValue)));
    }

    private Optional<Expression> convertBooleanSlot(SlotRef slot) {
        ResolvedField field = findField(slot);
        if (field == null || !(field.field.getType() instanceof ArrowType.Bool)) {
            return Optional.empty();
        }
        return Optional.of(comparisonFunction("equal:any_any",
                fieldReference(field), ExpressionCreator.bool(false, true)));
    }

    // convert doris literal to Substrait literal with arrow type
    private Optional<Expression> convertLiteral(ArrowType type, LiteralExpr literal) {
        if (type instanceof ArrowType.Bool && literal instanceof BoolLiteral) {
            return Optional.of(ExpressionCreator.bool(false, ((BoolLiteral) literal).getValue()));
        } else if (type instanceof ArrowType.Int
                && (literal instanceof IntLiteral || literal instanceof LargeIntLiteral)) {
            ArrowType.Int integer = (ArrowType.Int) type;
            BigInteger value = literal instanceof LargeIntLiteral
                    ? ((LargeIntLiteral) literal).getRealValue()
                    : BigInteger.valueOf(literal.getLongValue());
            int bitWidth = integer.getBitWidth();
            BigInteger minimum = integer.getIsSigned()
                    ? BigInteger.ONE.shiftLeft(bitWidth - 1).negate() : BigInteger.ZERO;
            BigInteger maximum = BigInteger.ONE.shiftLeft(
                    integer.getIsSigned() ? bitWidth - 1 : bitWidth).subtract(BigInteger.ONE);
            if (value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
                return Optional.empty();
            }
            switch (integer.getBitWidth()) {
                case 8:
                    return Optional.of(ExpressionCreator.i8(false, value.intValueExact()));
                case 16:
                    return Optional.of(ExpressionCreator.i16(false, value.intValueExact()));
                case 32:
                    return Optional.of(ExpressionCreator.i32(
                            false, integer.getIsSigned() ? value.intValueExact() : value.intValue()));
                case 64:
                    return Optional.of(ExpressionCreator.i64(
                            false, integer.getIsSigned() ? value.longValueExact() : value.longValue()));
                default:
                    return Optional.empty();
            }
        } else if ((type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8)
                && literal instanceof StringLiteral) {
            return Optional.of(ExpressionCreator.string(false, literal.getStringValue()));
        } else if (type instanceof ArrowType.Timestamp && literal instanceof DateLiteral) {
            return convertTimestampLiteral((ArrowType.Timestamp) type, (DateLiteral) literal);
        } else if (type instanceof ArrowType.Date && literal instanceof DateLiteral) {
            DateLiteral date = (DateLiteral) literal;
            // Arrow Date(DAY) and Substrait Date both use signed days from the Unix epoch.
            // Accept only a bound DATEV2 literal; do not truncate DATETIME or reinterpret legacy
            // DATE semantics in this direct pushdown path.
            if (!date.getType().isDateV2()) {
                return Optional.empty();
            }
            try {
                long epochDay = LocalDate.of(
                        Math.toIntExact(date.getYear()),
                        Math.toIntExact(date.getMonth()),
                        Math.toIntExact(date.getDay())).toEpochDay();
                return Optional.of(ExpressionCreator.date(false, Math.toIntExact(epochDay)));
            } catch (ArithmeticException | DateTimeException e) {
                return Optional.empty();
            }
        } else if (type instanceof ArrowType.Decimal
                && (literal instanceof DecimalLiteral || literal instanceof IntLiteral
                        || literal instanceof LargeIntLiteral)) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            BigDecimal value = literal instanceof DecimalLiteral
                    ? ((DecimalLiteral) literal).getValue()
                    : new BigDecimal(literal.getStringValue());
            try {
                return Optional.of(ExpressionCreator.decimal(
                        false, value, decimal.getPrecision(), decimal.getScale()));
            } catch (ArithmeticException | IllegalArgumentException e) {
                return Optional.empty();
            }
        } else if (type instanceof ArrowType.FloatingPoint
                && (literal instanceof FloatLiteral || literal instanceof IntLiteral)) {
            double value = literal.getDoubleValue();
            if (!Double.isFinite(value)) {
                return Optional.empty();
            }
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                float floatValue = (float) value;
                return Float.isFinite(floatValue)
                        ? Optional.of(ExpressionCreator.fp32(false, floatValue))
                        : Optional.empty();
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return Optional.of(ExpressionCreator.fp64(false, value));
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Expression> convertTimestampLiteral(
            ArrowType.Timestamp timestamp, DateLiteral literal) {
        if (!literal.getType().isDatetimeV2()) {
            return Optional.empty();
        }
        int precision = timestampPrecision(timestamp);
        if (precision < 0) {
            return Optional.empty();
        }

        long micros = literal.getMicrosecond();
        if ((timestamp.getUnit() == TimeUnit.SECOND && micros != 0)
                || (timestamp.getUnit() == TimeUnit.MILLISECOND && micros % 1000 != 0)) {
            return Optional.empty();
        }
        try {
            LocalDateTime dateTime = LocalDateTime.of(
                    Math.toIntExact(literal.getYear()),
                    Math.toIntExact(literal.getMonth()),
                    Math.toIntExact(literal.getDay()),
                    Math.toIntExact(literal.getHour()),
                    Math.toIntExact(literal.getMinute()),
                    Math.toIntExact(literal.getSecond()),
                    Math.toIntExact(micros * 1000));
            // UTC is only the coordinate system for encoding this timezone-naive wall-clock
            // value; no session-timezone conversion is intended.
            long epochSeconds = dateTime.toEpochSecond(ZoneOffset.UTC);
            long value;
            switch (timestamp.getUnit()) {
                case SECOND:
                    value = epochSeconds;
                    break;
                case MILLISECOND:
                    value = Math.addExact(
                            Math.multiplyExact(epochSeconds, 1000), micros / 1000);
                    break;
                case MICROSECOND:
                    value = Math.addExact(
                            Math.multiplyExact(epochSeconds, 1000000), micros);
                    break;
                default:
                    return Optional.empty();
            }
            return Optional.of(ExpressionCreator.precisionTimestamp(false, value, precision));
        } catch (ArithmeticException | DateTimeException e) {
            return Optional.empty();
        }
    }

    private static boolean isPushdownType(ArrowType type) {
        /*
         * Keep this allow-list aligned with convertLiteral(), toSubstraitType(), and
         * toSubstraitProtoType(). A type is pushed only when Doris can encode both its field and
         * literals without changing comparison or null semantics across FE -> lance-c -> DataFusion.
         *
         * Supported scalar types:
         * - Bool; signed/unsigned Int8/16/32/64; Float32/64; Utf8/LargeUtf8.
         * - Decimal128 with a valid non-negative scale and Substrait precision up to 38.
         * - Date32 (DAY) and timezone-free Timestamp in SECOND/MILLISECOND/MICROSECOND units.
         *
         * Types intentionally left as Doris residual predicates:
         * - Float16, because this converter has no lossless Substrait FP16 field/literal mapping.
         * - Decimal256, because Substrait decimal precision is limited to 38 and the 256-bit
         *   physical mapping has no end-to-end differential coverage.
         * - Date64 (MILLISECOND), because Substrait Date is expressed as days and conversion would
         *   truncate values rather than preserve Arrow semantics.
         * - Timestamp(NANOSECOND), because Doris timestamp predicates preserve at most
         *   microseconds; timezone-aware Timestamp, because Arrow instant semantics must be
         *   reconciled with Doris session-timezone semantics first.
         * - Binary variants, Time, Duration, Interval, and nested/container types, because their
         *   field/literal or nested-reference mappings have not yet been implemented and covered
         *   by differential tests.
         */
        if (type instanceof ArrowType.Int) {
            ArrowType.Int integer = (ArrowType.Int) type;
            return integer.getBitWidth() == 8 || integer.getBitWidth() == 16
                    || integer.getBitWidth() == 32 || integer.getBitWidth() == 64;
        }
        if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            return precision == FloatingPointPrecision.SINGLE || precision == FloatingPointPrecision.DOUBLE;
        }
        if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return decimal.getBitWidth() == 128
                    && decimal.getPrecision() > 0
                    && decimal.getPrecision() <= MAX_SUBSTRAIT_DECIMAL_PRECISION
                    && decimal.getScale() >= 0
                    && decimal.getScale() <= decimal.getPrecision();
        }
        if (type instanceof ArrowType.Date) {
            return ((ArrowType.Date) type).getUnit() == DateUnit.DAY;
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            String timezone = timestamp.getTimezone();
            return (timezone == null || timezone.isEmpty())
                    && timestampPrecision(timestamp) >= 0;
        }
        return type instanceof ArrowType.Bool
                || type instanceof ArrowType.Utf8
                || type instanceof ArrowType.LargeUtf8;
    }

    private static boolean isStringType(ArrowType type) {
        return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
    }

    // slotref with ordinal index with Substrait Type
    private Expression fieldReference(ResolvedField field) {
        return FieldReference.newRootStructReference(field.ordinal, toSubstraitType(field.field));
    }

    private static Type toSubstraitType(Field field) {
        TypeCreator creator = TypeCreator.of(field.isNullable());
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Bool) {
            return creator.BOOLEAN;
        } else if (type instanceof ArrowType.Int) {
            switch (((ArrowType.Int) type).getBitWidth()) {
                case 8:
                    return creator.I8;
                case 16:
                    return creator.I16;
                case 32:
                    return creator.I32;
                case 64:
                    return creator.I64;
                default:
                    break;
            }
        } else if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                return creator.FP32;
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return creator.FP64;
            }
        } else if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return creator.decimal(decimal.getPrecision(), decimal.getScale());
        } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return creator.STRING;
        } else if (type instanceof ArrowType.Date) {
            return creator.DATE;
        } else if (type instanceof ArrowType.Timestamp) {
            return creator.precisionTimestamp(timestampPrecision((ArrowType.Timestamp) type));
        }
        throw new IllegalArgumentException("Unsupported Lance Substrait field type: " + type);
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

    private byte[] serialize(Expression expression) {
        ExtensionCollector extensionCollector = new ExtensionCollector();
        RelProtoConverter relConverter = new RelProtoConverter(extensionCollector);
        ExpressionProtoConverter expressionConverter =
                new ExpressionProtoConverter(extensionCollector, relConverter);
        io.substrait.proto.Expression protoExpression = expression.accept(expressionConverter);

        NamedStruct.Builder schemaBuilder = NamedStruct.newBuilder();
        io.substrait.proto.Type.Struct.Builder structBuilder = io.substrait.proto.Type.Struct.newBuilder()
                .setNullability(Nullability.NULLABILITY_REQUIRED);
        List<Field> fields = schema.getFields();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            Field field = fields.get(ordinal);
            Optional<io.substrait.proto.Type> type = toSubstraitProtoType(field);
            if (type.isPresent()) {
                schemaBuilder.addNames(field.getName());
                structBuilder.addTypes(type.get());
            } else {
                // Lance 4.x removes user-defined top-level fields before handing the
                // ExtendedExpression to DataFusion and remaps field ordinals. This keeps the
                // envelope aligned with the full dataset schema when unrelated complex or
                // otherwise unsupported columns are present.
                schemaBuilder.addNames(UNSUPPORTED_FIELD_PREFIX + ordinal);
                structBuilder.addTypes(io.substrait.proto.Type.newBuilder()
                        .setUserDefined(io.substrait.proto.Type.UserDefined.newBuilder()
                                .setTypeReference(0)
                                .setNullability(nullability(field)))
                        .build());
            }
        }
        schemaBuilder.setStruct(structBuilder);

        protoExpression = applyTypeVariations(protoExpression);
        ExpressionReference expressionReference = ExpressionReference.newBuilder()
                .setExpression(protoExpression)
                .addOutputNames("filter_mask")
                .build();
        ExtendedExpression.Builder extendedExpression = ExtendedExpression.newBuilder()
                .setVersion(io.substrait.proto.Version.newBuilder()
                        .setMajorNumber(0)
                        .setMinorNumber(70)
                        .setPatchNumber(0)
                        .setProducer("doris-lance")
                        .build())
                .setBaseSchema(schemaBuilder)
                .addReferredExpr(expressionReference);
        extensionCollector.addExtensionsToExtendedExpression(extendedExpression);
        return extendedExpression.build().toByteArray();
    }

    private static Optional<io.substrait.proto.Type> toSubstraitProtoType(Field field) {
        if (!isPushdownType(field.getType())) {
            return Optional.empty();
        }
        ArrowType type = field.getType();
        io.substrait.proto.Type.Builder builder = io.substrait.proto.Type.newBuilder();
        if (type instanceof ArrowType.Bool) {
            return Optional.of(builder.setBool(io.substrait.proto.Type.Boolean.newBuilder()
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Int) {
            ArrowType.Int integer = (ArrowType.Int) type;
            int typeVariationReference = integer.getIsSigned()
                    ? 0 : UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE;
            switch (integer.getBitWidth()) {
                case 8:
                    return Optional.of(builder.setI8(io.substrait.proto.Type.I8.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 16:
                    return Optional.of(builder.setI16(io.substrait.proto.Type.I16.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 32:
                    return Optional.of(builder.setI32(io.substrait.proto.Type.I32.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 64:
                    return Optional.of(builder.setI64(io.substrait.proto.Type.I64.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                default:
                    return Optional.empty();
            }
        } else if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                return Optional.of(builder.setFp32(io.substrait.proto.Type.FP32.newBuilder()
                        .setNullability(nullability(field))).build());
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return Optional.of(builder.setFp64(io.substrait.proto.Type.FP64.newBuilder()
                        .setNullability(nullability(field))).build());
            }
            return Optional.empty();
        } else if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return Optional.of(builder.setDecimal(io.substrait.proto.Type.Decimal.newBuilder()
                    .setPrecision(decimal.getPrecision())
                    .setScale(decimal.getScale())
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            int typeVariationReference = type instanceof ArrowType.LargeUtf8
                    ? LARGE_CONTAINER_TYPE_VARIATION_REFERENCE : 0;
            return Optional.of(builder.setString(io.substrait.proto.Type.String.newBuilder()
                    .setTypeVariationReference(typeVariationReference)
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Date) {
            return Optional.of(builder.setDate(io.substrait.proto.Type.Date.newBuilder()
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Timestamp) {
            return Optional.of(builder.setPrecisionTimestamp(
                    io.substrait.proto.Type.PrecisionTimestamp.newBuilder()
                            .setPrecision(timestampPrecision((ArrowType.Timestamp) type))
                            .setNullability(nullability(field))).build());
        } else {
            return Optional.empty();
        }
    }

    private static int timestampPrecision(ArrowType.Timestamp timestamp) {
        switch (timestamp.getUnit()) {
            case SECOND:
                return 0;
            case MILLISECOND:
                return 3;
            case MICROSECOND:
                return 6;
            default:
                return -1;
        }
    }

    private io.substrait.proto.Expression applyTypeVariations(
            io.substrait.proto.Expression expression) {
        io.substrait.proto.Expression.Builder expressionBuilder = expression.toBuilder();
        if (expression.hasScalarFunction()) {
            io.substrait.proto.Expression.ScalarFunction.Builder functionBuilder =
                    expression.getScalarFunction().toBuilder();
            for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                if (argument.hasValue()) {
                    functionBuilder.getArgumentsBuilder(i).setValue(
                            applyTypeVariations(argument.getValue()));
                }
            }

            ArrowType argumentFieldType = null;
            for (int i = 0; i < functionBuilder.getArgumentsCount() && argumentFieldType == null; i++) {
                io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                if (argument.hasValue()) {
                    argumentFieldType = fieldType(argument.getValue());
                }
            }
            if (argumentFieldType instanceof ArrowType.Int
                    && !((ArrowType.Int) argumentFieldType).getIsSigned()) {
                int bitWidth = ((ArrowType.Int) argumentFieldType).getBitWidth();
                for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                    io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                    if (argument.hasValue()) {
                        functionBuilder.getArgumentsBuilder(i).setValue(
                                applyUnsignedIntegerLiteralVariation(
                                        argument.getValue(), bitWidth));
                    }
                }
            }
            if (argumentFieldType instanceof ArrowType.LargeUtf8) {
                for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                    io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                    if (argument.hasValue()) {
                        functionBuilder.getArgumentsBuilder(i).setValue(
                                applyLargeUtf8LiteralVariation(argument.getValue()));
                    }
                }
            }
            expressionBuilder.setScalarFunction(functionBuilder);
        } else if (expression.hasSingularOrList()) {
            io.substrait.proto.Expression.SingularOrList.Builder inBuilder =
                    expression.getSingularOrList().toBuilder();
            inBuilder.setValue(applyTypeVariations(inBuilder.getValue()));
            ArrowType valueType = fieldType(inBuilder.getValue());
            for (int i = 0; i < inBuilder.getOptionsCount(); i++) {
                io.substrait.proto.Expression option =
                        applyTypeVariations(inBuilder.getOptions(i));
                if (valueType instanceof ArrowType.Int
                        && !((ArrowType.Int) valueType).getIsSigned()) {
                    option = applyUnsignedIntegerLiteralVariation(
                            option, ((ArrowType.Int) valueType).getBitWidth());
                }
                if (valueType instanceof ArrowType.LargeUtf8) {
                    option = applyLargeUtf8LiteralVariation(option);
                }
                inBuilder.setOptions(i, option);
            }
            expressionBuilder.setSingularOrList(inBuilder);
        }
        return expressionBuilder.build();
    }

    private ArrowType fieldType(io.substrait.proto.Expression expression) {
        if (!expression.hasSelection()
                || !expression.getSelection().hasDirectReference()
                || !expression.getSelection().getDirectReference().hasStructField()) {
            return null;
        }
        int ordinal = expression.getSelection().getDirectReference().getStructField().getField();
        if (ordinal < 0 || ordinal >= schema.getFields().size()) {
            return null;
        }
        return schema.getFields().get(ordinal).getType();
    }

    private static io.substrait.proto.Expression applyUnsignedIntegerLiteralVariation(
            io.substrait.proto.Expression expression, int bitWidth) {
        if (!expression.hasLiteral()) {
            return expression;
        }
        io.substrait.proto.Expression.Literal literal = expression.getLiteral();
        boolean matchingWidth = (bitWidth == 8 && literal.hasI8())
                || (bitWidth == 16 && literal.hasI16())
                || (bitWidth == 32 && literal.hasI32())
                || (bitWidth == 64 && literal.hasI64());
        if (!matchingWidth) {
            return expression;
        }
        return expression.toBuilder()
                .setLiteral(literal.toBuilder().setTypeVariationReference(
                        UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE))
                .build();
    }

    private static io.substrait.proto.Expression applyLargeUtf8LiteralVariation(
            io.substrait.proto.Expression expression) {
        if (!expression.hasLiteral() || !expression.getLiteral().hasString()) {
            return expression;
        }
        return expression.toBuilder()
                .setLiteral(expression.getLiteral().toBuilder().setTypeVariationReference(
                        LARGE_CONTAINER_TYPE_VARIATION_REFERENCE))
                .build();
    }

    private static Nullability nullability(Field field) {
        return field.isNullable() ? Nullability.NULLABILITY_NULLABLE : Nullability.NULLABILITY_REQUIRED;
    }

    private ResolvedField findField(SlotRef slot) {
        String columnName = slot.getColumnName();
        return columnName == null ? null : fieldsByLowerCaseName.get(columnName.toLowerCase(Locale.ROOT));
    }

    private static SlotRef directSlot(Expr expr) {
        return expr instanceof SlotRef ? (SlotRef) expr : null;
    }

    private static LiteralExpr directLiteral(Expr expr) {
        return expr instanceof LiteralExpr ? (LiteralExpr) expr : null;
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        try {
            return SimpleExtension.loadDefaults();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Substrait extension definitions", e);
        }
    }

    private static String parenthesize(String value) {
        return "(" + value + ")";
    }

    private static class ResolvedField {
        private final Field field;
        private final int ordinal;

        private ResolvedField(Field field, int ordinal) {
            this.field = field;
            this.ordinal = ordinal;
        }
    }

    public static class ConversionResult {
        private final byte[] substraitFilter;
        private final String debugPredicate;
        private final List<Expr> pushedConjuncts;

        private ConversionResult(byte[] substraitFilter, String debugPredicate, List<Expr> pushedConjuncts) {
            this.substraitFilter = substraitFilter;
            this.debugPredicate = debugPredicate;
            this.pushedConjuncts = pushedConjuncts;
        }

        public byte[] getSubstraitFilter() {
            return substraitFilter;
        }

        public String getDebugPredicate() {
            return debugPredicate;
        }

        public List<Expr> getPushedConjuncts() {
            return pushedConjuncts;
        }
    }
}
