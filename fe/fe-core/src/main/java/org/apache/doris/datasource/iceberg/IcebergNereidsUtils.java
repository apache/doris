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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.TimestampType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Utility class for converting Nereids expressions to Iceberg expressions.
 */
public class IcebergNereidsUtils {

    /**
     * Convert Nereids Expression to Iceberg Expression
     */
    public static org.apache.iceberg.expressions.Expression convertNereidsToIcebergExpression(
            Expression nereidsExpr, Schema schema) throws UserException {
        if (nereidsExpr == null) {
            throw new UserException("Nereids expression is null");
        }

        // Handle logical operators
        if (nereidsExpr instanceof And) {
            And andExpr = (And) nereidsExpr;
            org.apache.iceberg.expressions.Expression left = convertNereidsToIcebergExpression(andExpr.child(0),
                    schema);
            org.apache.iceberg.expressions.Expression right = convertNereidsToIcebergExpression(andExpr.child(1),
                    schema);
            if (left != null && right != null) {
                return Expressions.and(left, right);
            }
            throw new UserException("Failed to convert AND expression: one or both children are unsupported");
        }

        if (nereidsExpr instanceof Or) {
            Or orExpr = (Or) nereidsExpr;
            org.apache.iceberg.expressions.Expression left = convertNereidsToIcebergExpression(orExpr.child(0),
                    schema);
            org.apache.iceberg.expressions.Expression right = convertNereidsToIcebergExpression(orExpr.child(1),
                    schema);
            if (left != null && right != null) {
                return Expressions.or(left, right);
            }
            throw new UserException("Failed to convert OR expression: one or both children are unsupported");
        }

        if (nereidsExpr instanceof Not) {
            Not notExpr = (Not) nereidsExpr;
            org.apache.iceberg.expressions.Expression child = convertNereidsToIcebergExpression(notExpr.child(),
                    schema);
            if (child != null) {
                return Expressions.not(child);
            }
            throw new UserException("Failed to convert NOT expression: child is unsupported");
        }

        // Handle comparison operators
        if (nereidsExpr instanceof EqualTo) {
            return convertNereidsBinaryPredicate((EqualTo) nereidsExpr,
                    schema, Expressions::equal);
        }

        if (nereidsExpr instanceof GreaterThan) {
            return convertNereidsBinaryPredicate(
                    (GreaterThan) nereidsExpr, schema,
                    Expressions::greaterThan);
        }

        if (nereidsExpr instanceof GreaterThanEqual) {
            return convertNereidsBinaryPredicate(
                    (GreaterThanEqual) nereidsExpr, schema,
                    Expressions::greaterThanOrEqual);
        }

        if (nereidsExpr instanceof LessThan) {
            return convertNereidsBinaryPredicate((LessThan) nereidsExpr,
                    schema, Expressions::lessThan);
        }

        if (nereidsExpr instanceof LessThanEqual) {
            return convertNereidsBinaryPredicate(
                    (LessThanEqual) nereidsExpr, schema,
                    Expressions::lessThanOrEqual);
        }

        // Handle IN predicates
        if (nereidsExpr instanceof InPredicate) {
            return convertNereidsInPredicate((InPredicate) nereidsExpr,
                    schema);
        }

        // Handle IS NULL
        if (nereidsExpr instanceof IsNull) {
            Expression child = ((IsNull) nereidsExpr).child();
            if (child instanceof Slot) {
                String colName = extractColumnName((Slot) child);
                NestedField nestedField = schema.caseInsensitiveFindField(colName);
                if (nestedField == null) {
                    throw new UserException("Column not found in Iceberg schema: " + colName);
                }
                return Expressions.isNull(nestedField.name());
            }
            throw new UserException("IS NULL requires a column reference");
        }

        // Handle BETWEEN predicates
        if (nereidsExpr instanceof Between) {
            return convertNereidsBetween((Between) nereidsExpr,
                    schema);
        }

        throw new UserException("Unsupported expression type: " + nereidsExpr.getClass().getName());
    }

    /**
     * Convert Nereids binary predicate (comparison operators)
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsBinaryPredicate(
            Expression nereidsExpr, Schema schema,
            BiFunction<String, Object, org.apache.iceberg.expressions.Expression> converter) throws UserException {

        // Extract slot and literal from the binary predicate
        Slot slot = null;
        Literal literal = null;

        if (nereidsExpr.children().size() == 2) {
            Expression left = nereidsExpr.child(0);
            Expression right = nereidsExpr.child(1);

            if (left instanceof Slot && right instanceof Literal) {
                slot = (Slot) left;
                literal = (Literal) right;
            } else if (left instanceof Literal && right instanceof Slot) {
                slot = (Slot) right;
                literal = (Literal) left;
            }
        }

        if (slot == null || literal == null) {
            throw new UserException("Binary predicate must be between a column and a literal");
        }

        String colName = extractColumnName(slot);
        NestedField nestedField = schema.caseInsensitiveFindField(colName);
        if (nestedField == null) {
            throw new UserException("Column not found in Iceberg schema: " + colName);
        }

        colName = nestedField.name();
        Object value = extractNereidsLiteralValue(literal, nestedField.type());

        if (value == null) {
            if (literal instanceof NullLiteral) {
                return Expressions.isNull(colName);
            }
            throw new UserException("Unsupported or null literal value for column: " + colName);
        }

        return converter.apply(colName, value);
    }

    /**
     * Convert Nereids IN predicate
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsInPredicate(
            InPredicate inPredicate, Schema schema) throws UserException {
        if (inPredicate.children().size() < 2) {
            throw new UserException("IN predicate requires at least one value");
        }

        org.apache.doris.nereids.trees.expressions.Expression left = inPredicate.child(0);
        if (!(left instanceof Slot)) {
            throw new UserException("Left side of IN predicate must be a slot");
        }

        Slot slot = (Slot) left;
        String colName = extractColumnName(slot);
        NestedField nestedField = schema.caseInsensitiveFindField(colName);
        if (nestedField == null) {
            throw new UserException("Column not found in Iceberg schema: " + colName);
        }

        colName = nestedField.name();
        List<Object> values = new ArrayList<>();

        for (int i = 1; i < inPredicate.children().size(); i++) {
            Expression child = inPredicate.child(i);
            if (!(child instanceof Literal)) {
                throw new UserException("IN predicate values must be literals");
            }

            Object value = extractNereidsLiteralValue(
                    (Literal) child, nestedField.type());
            if (value == null) {
                throw new UserException("Null or unsupported value in IN predicate for column: " + colName);
            }
            values.add(value);
        }

        return Expressions.in(colName, values);
    }

    /**
     * Convert Nereids BETWEEN predicate
     * BETWEEN a AND b is equivalent to: a <= col <= b
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsBetween(
            Between between, Schema schema) throws UserException {
        if (between.children().size() != 3) {
            throw new UserException("BETWEEN predicate must have exactly 3 children");
        }

        Expression compareExpr = between.getCompareExpr();
        Expression lowerBound = between.getLowerBound();
        Expression upperBound = between.getUpperBound();

        // Validate that compareExpr is a slot
        if (!(compareExpr instanceof Slot)) {
            throw new UserException("Left side of BETWEEN predicate must be a slot");
        }

        // Validate that lowerBound and upperBound are literals
        if (!(lowerBound instanceof Literal)) {
            throw new UserException("Lower bound of BETWEEN predicate must be a literal");
        }
        if (!(upperBound instanceof Literal)) {
            throw new UserException("Upper bound of BETWEEN predicate must be a literal");
        }

        Slot slot = (Slot) compareExpr;
        String colName = extractColumnName(slot);
        NestedField nestedField = schema.caseInsensitiveFindField(colName);
        if (nestedField == null) {
            throw new UserException("Column not found in Iceberg schema: " + colName);
        }

        colName = nestedField.name();

        // Extract values
        Object lowerValue = extractNereidsLiteralValue((Literal) lowerBound, nestedField.type());
        Object upperValue = extractNereidsLiteralValue((Literal) upperBound, nestedField.type());

        if (lowerValue == null || upperValue == null) {
            throw new UserException("BETWEEN predicate bounds cannot be null for column: " + colName);
        }

        // BETWEEN a AND b is equivalent to: a <= col AND col <= b
        org.apache.iceberg.expressions.Expression lowerBoundExpr = Expressions.greaterThanOrEqual(colName, lowerValue);
        org.apache.iceberg.expressions.Expression upperBoundExpr = Expressions.lessThanOrEqual(colName, upperValue);

        return Expressions.and(lowerBoundExpr, upperBoundExpr);
    }

    /**
     * Extract column name from Slot (SlotReference or UnboundSlot).
     * For UnboundSlot, validates that nameParts is a singleton list (single column
     * name).
     *
     * @param slot the slot to extract column name from
     * @return the column name
     * @throws UserException if UnboundSlot has multiple nameParts or if slot type
     *                       is unsupported
     */
    private static String extractColumnName(Slot slot) throws UserException {
        if (slot instanceof SlotReference) {
            return ((SlotReference) slot).getName();
        } else if (slot instanceof UnboundSlot) {
            UnboundSlot unboundSlot = (UnboundSlot) slot;
            // Validate that nameParts is a singleton list (simple column name)
            if (unboundSlot.getNameParts().size() != 1) {
                throw new UserException(
                        "UnboundSlot must have a single name part, but got: " + unboundSlot.getNameParts());
            }
            return unboundSlot.getNameParts().get(0);
        } else {
            throw new UserException("Unsupported slot type: " + slot.getClass().getName());
        }
    }

    /**
     * Extract literal value from Nereids Literal expression
     */
    private static Object extractNereidsLiteralValue(
            Literal literal,
            Type icebergType) throws UserException {
        try {
            Object raw = literal.getValue();
            if (raw == null) {
                if (literal instanceof NullLiteral) {
                    return null;
                }
                throw new UserException("Literal value is null: " + literal);
            }

            switch (icebergType.typeId()) {
                case BOOLEAN:
                    if (literal instanceof BooleanLiteral) {
                        return ((BooleanLiteral) literal).getValue();
                    }
                    // try to convert to boolean
                    return Boolean.valueOf(raw.toString());
                case STRING:
                    return literal.getStringValue();
                case INTEGER:
                    if (raw instanceof Number) {
                        return ((Number) raw).intValue();
                    }
                    // try to convert to integer
                    return Integer.parseInt(literal.getStringValue());

                case LONG:
                case TIME:
                    if (raw instanceof Number) {
                        return ((Number) raw).longValue();
                    }
                    // try to convert to long
                    return Long.parseLong(literal.getStringValue());
                case FLOAT:
                    if (raw instanceof Number) {
                        return ((Number) raw).floatValue();
                    }
                    // try to convert to float
                    return Float.parseFloat(literal.getStringValue());
                case DOUBLE:
                    if (raw instanceof Number) {
                        return ((Number) raw).doubleValue();
                    }
                    // try to convert to double
                    return Double.parseDouble(literal.getStringValue());
                case DECIMAL:
                    if (literal instanceof DecimalV3Literal) {
                        return ((DecimalV3Literal) literal)
                                .getValue();
                    }
                    if (literal instanceof DecimalLiteral) {
                        return ((DecimalLiteral) literal).getValue();
                    }
                    // try parse from string/number
                    return new BigDecimal(literal.getStringValue());
                case DATE:
                    if (literal instanceof DateLiteral) {
                        return ((DateLiteral) literal)
                                .getStringValue();
                    }
                    // accept string value for date
                    return literal.getStringValue();
                case TIMESTAMP:
                case TIMESTAMP_NANO: {
                    // Iceberg expects microseconds since epoch. Honor with/without zone semantics.
                    if (literal instanceof DateTimeLiteral
                            || literal instanceof DateLiteral) {
                        LocalDateTime ldt;
                        long microSecond = 0L;
                        if (literal instanceof DateTimeLiteral) {
                            DateTimeLiteral dt = (DateTimeLiteral) literal;
                            ldt = dt.toJavaDateType();
                            microSecond = dt.getMicroSecond();
                        } else {
                            DateLiteral d = (DateLiteral) literal;
                            ldt = d.toJavaDateType();
                            microSecond = 0L;
                        }
                        TimestampType ts = (TimestampType) icebergType;
                        ZoneId zone = ts.shouldAdjustToUTC()
                                ? DateUtils.getTimeZone()
                                : ZoneId.of("UTC");
                        long epochMicros = ldt.atZone(zone).toInstant().toEpochMilli() * 1000L + microSecond;
                        return epochMicros;
                    }
                    // String literal: try to parse using Doris's built-in datetime parser
                    // which supports multiple formats including 'yyyy-MM-dd HH:mm:ss'
                    if (raw instanceof String) {
                        String value = (String) raw;
                        // 1) If numeric, treat as epoch micros directly
                        try {
                            return Long.parseLong(value);
                        } catch (NumberFormatException ignored) {
                            // not a pure number, fall through to datetime parsing
                        }

                        // 2) Try to parse using Doris's DateLiteral.parseDateTime() which supports
                        // various formats: 'yyyy-MM-dd', 'yyyy-MM-dd HH:mm:ss', ISO formats, etc.
                        try {
                            java.time.temporal.TemporalAccessor temporal = DateLiteral.parseDateTime(value).get();
                            TimestampType ts = (TimestampType) icebergType;
                            ZoneId zone = ts.shouldAdjustToUTC()
                                    ? DateUtils.getTimeZone()
                                    : ZoneId.of("UTC");

                            // Build LocalDateTime from TemporalAccessor using DateUtils helper methods
                            LocalDateTime ldt = LocalDateTime.of(
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.YEAR),
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.MONTH_OF_YEAR),
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.DAY_OF_MONTH),
                                    DateUtils.getHourOrDefault(temporal),
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.MINUTE_OF_HOUR),
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.SECOND_OF_MINUTE),
                                    DateUtils.getOrDefault(temporal, java.time.temporal.ChronoField.NANO_OF_SECOND));

                            long microSecond = DateUtils.getOrDefault(temporal,
                                    java.time.temporal.ChronoField.NANO_OF_SECOND) / 1000L;
                            return ldt.atZone(zone).toInstant().toEpochMilli() * 1000L + microSecond;
                        } catch (Exception ignored) {
                            // If Doris parser fails, fall back to passing as string for Iceberg to try
                        }

                        return literal.getStringValue();
                    }
                    if (raw instanceof Number) {
                        return ((Number) raw).longValue();
                    }
                    throw new UserException("Failed to convert timestamp literal to long: " + raw);
                }
                case UUID:
                case FIXED:
                case BINARY:
                case GEOMETRY:
                case GEOGRAPHY:
                    // Pass through as bytes/strings where possible
                    return raw;
                default:
                    throw new UserException("Unsupported literal type: " + icebergType.typeId());
            }
        } catch (Exception e) {
            throw new UserException("Failed to extract literal value: " + e.getMessage());
        }
    }
}
