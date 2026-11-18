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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.In;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.TypeChange;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** File pruning only: every original predicate remains a BE residual. */
final class DeltaLakePredicateConverter {
    private record SignedZeros(Literal negative, Literal positive) {
    }

    private static final SignedZeros FLOAT_ZEROS = new SignedZeros(Literal.ofFloat(-0.0f), Literal.ofFloat(0.0f));
    private static final SignedZeros DOUBLE_ZEROS = new SignedZeros(Literal.ofDouble(-0.0), Literal.ofDouble(0.0));
    private static final TypeChange FLOAT_TO_DOUBLE = new TypeChange(FloatType.FLOAT, DoubleType.DOUBLE);
    private final StructType schema;
    private final ZoneId zone;
    private final Collection<String> partitionColumns;
    private final Collection<String> possibleEmptyPartitionColumns;

    DeltaLakePredicateConverter(StructType schema, ZoneId zone, Collection<String> partitionColumns,
            Collection<String> possibleEmptyPartitionColumns) {
        this.schema = schema;
        this.zone = zone;
        this.partitionColumns = partitionColumns;
        this.possibleEmptyPartitionColumns = possibleEmptyPartitionColumns;
    }

    Optional<Predicate> convert(ConnectorExpression expression) {
        if (expression instanceof ConnectorAnd) {
            Predicate result = null;
            for (ConnectorExpression child : expression.getChildren()) {
                Optional<Predicate> converted = convert(child);
                if (converted.isPresent()) {
                    result = result == null ? converted.get() : new Predicate("AND", result, converted.get());
                }
            }
            return Optional.ofNullable(result);
        }
        return Optional.ofNullable(exact(expression));
    }

    Optional<java.util.function.Predicate<Map<String, String>>> normalizedPartitionFilter(
            ConnectorExpression expression, Engine engine) {
        if (possibleEmptyPartitionColumns.isEmpty()) {
            return Optional.empty();
        }
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        collectColumnNames(expression, names);
        List<StructField> fields = partitionColumns.stream().filter(names::contains).map(schema::get).toList();
        if (fields.stream().noneMatch(this::requiresNormalizedPartitionValue)) {
            return Optional.empty();
        }
        // Reuse the same AND/OR/NOT conversion on just the referenced partition fields. A disjunction
        // with a data column stays residual; a disjunction with another partition key can be evaluated here.
        StructType partitions = new StructType(fields);
        return new DeltaLakePredicateConverter(partitions, zone, partitionColumns, List.of()).convert(expression)
                .map(predicate -> DeltaKernelCompat.normalizedPartitionFilter(engine, partitions, predicate));
    }

    private static void collectColumnNames(ConnectorExpression expression, Set<String> names) {
        if (expression instanceof ConnectorColumnRef) {
            names.add(((ConnectorColumnRef) expression).getColumnName());
        }
        expression.getChildren().forEach(child -> collectColumnNames(child, names));
    }

    private boolean requiresNormalizedPartitionValue(StructField field) {
        return DeltaLakePartitionValues.emptyStringIsNull(field.getDataType())
                && possibleEmptyPartitionColumns.contains(field.getName());
    }

    private Predicate exact(ConnectorExpression expression) {
        if (expression instanceof ConnectorAnd || expression instanceof ConnectorOr) {
            Predicate result = null;
            for (ConnectorExpression child : expression.getChildren()) {
                Predicate converted = exact(child);
                if (converted == null) {
                    // Dropping a child beneath OR or NOT can exclude matching rows. Only top-level
                    // ANDs in convert() may retain a subset of their conjuncts.
                    return null;
                }
                result = result == null ? converted : new Predicate(
                        expression instanceof ConnectorAnd ? "AND" : "OR", result, converted);
            }
            return result;
        }
        if (expression instanceof ConnectorNot) {
            Predicate child = exact(((ConnectorNot) expression).getOperand());
            return child == null ? null : new Predicate("NOT", child);
        }
        if (expression instanceof ConnectorIsNull) {
            ConnectorIsNull isNull = (ConnectorIsNull) expression;
            StructField field = field(isNull.getOperand());
            return field == null ? null : new Predicate(isNull.isNegated() ? "IS_NOT_NULL" : "IS_NULL",
                    new Column(field.getName()));
        }
        if (expression instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expression;
            StructField field = field(in.getValue());
            if (field == null || in.getInList().isEmpty() || hasRoundedFloatBounds(field)) {
                return null;
            }
            List<Expression> values = new ArrayList<>();
            for (ConnectorExpression item : in.getInList()) {
                Literal value = literal(field.getDataType(), item);
                if (value == null) {
                    return null;
                }
                SignedZeros zeros = signedZeros(value);
                if (zeros != null) {
                    values.add(zeros.negative());
                    values.add(zeros.positive());
                } else {
                    values.add(value);
                }
            }
            Predicate predicate = new In(new Column(field.getName()), values);
            return in.isNegated() ? new Predicate("NOT", predicate) : predicate;
        }
        if (expression instanceof ConnectorComparison) {
            ConnectorComparison comparison = (ConnectorComparison) expression;
            boolean reversed = comparison.getLeft() instanceof ConnectorLiteral;
            StructField field = field(reversed ? comparison.getRight() : comparison.getLeft());
            if (field == null) {
                return null;
            }
            Literal value = literal(field.getDataType(), reversed ? comparison.getLeft() : comparison.getRight());
            if (value == null) {
                return null;
            }
            ConnectorComparison.Operator op = comparison.getOperator();
            // Kernel rewrites null-safe equality to a NULL literal as IS_NULL, which uses nullCount.
            boolean usesMinMax = op != ConnectorComparison.Operator.EQ_FOR_NULL || value.getValue() != null;
            if (usesMinMax && hasRoundedFloatBounds(field)) {
                return null;
            }
            Column column = new Column(field.getName());
            String operator = op.getSymbol();
            if (op == ConnectorComparison.Operator.EQ_FOR_NULL) {
                // Keep null-safe semantics even beneath NOT; '=' would turn null into UNKNOWN.
                operator = "IS NOT DISTINCT FROM";
            }
            SignedZeros zeros = signedZeros(value);
            if (zeros != null) {
                // Kernel orders -0.0 below +0.0. Doris treats them as one value, including inside NOT.
                if (op == ConnectorComparison.Operator.EQ || op == ConnectorComparison.Operator.NE
                        || op == ConnectorComparison.Operator.EQ_FOR_NULL) {
                    String equality = op == ConnectorComparison.Operator.NE ? "=" : operator;
                    Predicate either = new Predicate("OR", new Predicate(equality, column, zeros.negative()),
                            new Predicate(equality, column, zeros.positive()));
                    return op == ConnectorComparison.Operator.NE ? new Predicate("NOT", either) : either;
                }
                // For column < 0 and column >= 0, the boundary is the first zero in Kernel's order.
                boolean useNegative = op == ConnectorComparison.Operator.LT || op == ConnectorComparison.Operator.GE;
                value = useNegative != reversed ? zeros.negative() : zeros.positive();
            }
            if (op == ConnectorComparison.Operator.NE) {
                return new Predicate("NOT", new Predicate("=", column, value));
            }
            return reversed ? new Predicate(operator, value, column) : new Predicate(operator, column, value);
        }
        return null;
    }

    private static SignedZeros signedZeros(Literal literal) {
        Object value = literal.getValue();
        if (value instanceof Double && (double) value == 0.0) {
            return DOUBLE_ZEROS;
        }
        if (value instanceof Float && (float) value == 0.0f) {
            return FLOAT_ZEROS;
        }
        return null;
    }

    private boolean hasRoundedFloatBounds(StructField field) {
        // An old FLOAT bound written as JSON 0.1 can represent 0.10000000149011612 after widening.
        // Kernel parses bounds as the current DOUBLE type. Partition filters use AddFile partition values.
        return field.getTypeChanges().contains(FLOAT_TO_DOUBLE)
                && partitionColumns.stream().noneMatch(field.getName()::equalsIgnoreCase);
    }

    private StructField field(ConnectorExpression expression) {
        if (!(expression instanceof ConnectorColumnRef)) {
            return null;
        }
        String name = ((ConnectorColumnRef) expression).getColumnName();
        for (StructField field : schema.fields()) {
            if (field.getName().equalsIgnoreCase(name)) {
                DataType type = field.getDataType();
                // Kernel tests the raw null bit. It also parses an empty BOOLEAN as false.
                if (requiresNormalizedPartitionValue(field)) {
                    return null;
                }
                // Kernel 4.4.1 parses min/max using the current type. Old DATE statistics
                // contain only yyyy-MM-dd and fail its timestamp_ntz parser after widening.
                // Keep predicates on these columns as BE residuals until Kernel handles them.
                if (field.getTypeChanges().contains(DeltaLakeFeatureCheck.DATE_TO_TIMESTAMP_NTZ)) {
                    return null;
                }
                // The SPI currently carries root-column references, not nested field paths.
                return (type instanceof BasePrimitiveType || type instanceof DecimalType) ? field : null;
            }
        }
        return null;
    }

    Literal literal(DataType type, ConnectorExpression expression) {
        if (!(expression instanceof ConnectorLiteral)
                || !(type instanceof BasePrimitiveType || type instanceof DecimalType)) {
            return null;
        }
        ConnectorLiteral literal = (ConnectorLiteral) expression;
        Object value = literal.getValue();
        if (literal.isNull()) {
            return Literal.ofNull(type);
        }
        try {
            if (type instanceof BooleanType && value instanceof Boolean) {
                return Literal.ofBoolean((Boolean) value);
            }
            if (type instanceof StringType && ((StringType) type).isUTF8BinaryCollated()
                    && value instanceof String) {
                return Literal.ofString((String) value);
            }
            if (type instanceof BinaryType && value instanceof byte[]) {
                return Literal.ofBinary((byte[]) value);
            }
            if (type instanceof DateType && value instanceof LocalDate) {
                return Literal.ofDate(Math.toIntExact(((LocalDate) value).toEpochDay()));
            }
            if ((type instanceof TimestampType || type instanceof TimestampNTZType)
                    && value instanceof LocalDateTime) {
                LocalDateTime dateTime = (LocalDateTime) value;
                boolean adjusted = type instanceof TimestampType;
                boolean utcCarrier = "TIMESTAMPTZ".equalsIgnoreCase(literal.getType().getTypeName());
                Instant instant = dateTime.atZone(adjusted && !utcCarrier ? zone : ZoneOffset.UTC).toInstant();
                long micros = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                        instant.getNano() / 1000L);
                return adjusted ? Literal.ofTimestamp(micros) : Literal.ofTimestampNtz(micros);
            }
            if (value instanceof Number) {
                if (type instanceof FloatType && "FLOAT".equalsIgnoreCase(literal.getType().getTypeName())) {
                    float number = ((Number) value).floatValue();
                    return Float.isFinite(number) ? Literal.ofFloat(number) : null;
                }
                if (type instanceof DoubleType && (value instanceof Double || value instanceof Float)) {
                    double number = ((Number) value).doubleValue();
                    return Double.isFinite(number) ? Literal.ofDouble(number) : null;
                }
                if (value instanceof Double || value instanceof Float) {
                    // Comparing an exact integer/decimal column in floating-point space can match
                    // multiple adjacent exact values. An integral-looking float is still lossy.
                    return null;
                }
                BigDecimal number = new BigDecimal(value.toString());
                if (type instanceof DecimalType) {
                    DecimalType decimal = (DecimalType) type;
                    BigDecimal scaled = number.setScale(decimal.getScale(), RoundingMode.UNNECESSARY);
                    return scaled.precision() <= decimal.getPrecision()
                            ? Literal.ofDecimal(scaled, decimal.getPrecision(), decimal.getScale()) : null;
                }
                if (type instanceof ByteType) {
                    return Literal.ofByte(number.byteValueExact());
                }
                if (type instanceof ShortType) {
                    return Literal.ofShort(number.shortValueExact());
                }
                if (type instanceof IntegerType) {
                    return Literal.ofInt(number.intValueExact());
                }
                if (type instanceof LongType) {
                    return Literal.ofLong(number.longValueExact());
                }
            }
        } catch (ArithmeticException | NumberFormatException e) {
            // Out-of-range or inexact coercions remain residuals, never rounded pruning boundaries.
            return null;
        }
        return null;
    }
}
