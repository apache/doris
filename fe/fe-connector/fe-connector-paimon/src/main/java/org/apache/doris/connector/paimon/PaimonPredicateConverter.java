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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts {@link ConnectorExpression} trees to Paimon {@link Predicate} objects.
 *
 * <p>This is the plugin-side equivalent of the original
 * {@code PaimonPredicateConverter} in fe-core, but operates on
 * {@code ConnectorExpression} rather than Doris {@code Expr}.</p>
 */
public class PaimonPredicateConverter {

    private static final Logger LOG = LogManager.getLogger(PaimonPredicateConverter.class);

    private final PredicateBuilder builder;
    private final List<String> fieldNames;
    private final List<DataType> fieldTypes;

    public PaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.fieldNames = rowType.getFields().stream()
                .map(f -> f.name().toLowerCase())
                .collect(Collectors.toList());
        this.fieldTypes = rowType.getFields().stream()
                .map(DataField::type)
                .collect(Collectors.toList());
    }

    /**
     * Convert a ConnectorExpression tree into a list of Paimon predicates.
     * Top-level AND nodes are flattened into the list; unconvertible
     * expressions are silently dropped.
     */
    public List<Predicate> convert(ConnectorExpression expr) {
        List<Predicate> results = new ArrayList<>();
        if (expr == null) {
            return results;
        }
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                Predicate p = convertSingle(child);
                if (p != null) {
                    results.add(p);
                }
            }
        } else {
            Predicate p = convertSingle(expr);
            if (p != null) {
                results.add(p);
            }
        }
        return results;
    }

    private Predicate convertSingle(ConnectorExpression expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof ConnectorAnd) {
            return convertAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return convertOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorComparison) {
            return convertComparison((ConnectorComparison) expr);
        } else if (expr instanceof ConnectorIn) {
            return convertIn((ConnectorIn) expr);
        } else if (expr instanceof ConnectorIsNull) {
            return convertIsNull((ConnectorIsNull) expr);
        } else if (expr instanceof ConnectorLike) {
            return convertLike((ConnectorLike) expr);
        }
        return null;
    }

    private Predicate convertAnd(ConnectorAnd and) {
        Predicate result = null;
        for (ConnectorExpression child : and.getConjuncts()) {
            Predicate p = convertSingle(child);
            if (p != null) {
                result = (result == null) ? p : PredicateBuilder.and(result, p);
            }
        }
        return result;
    }

    private Predicate convertOr(ConnectorOr or) {
        Predicate result = null;
        for (ConnectorExpression child : or.getDisjuncts()) {
            Predicate p = convertSingle(child);
            if (p == null) {
                return null;
            }
            result = (result == null) ? p : PredicateBuilder.or(result, p);
        }
        return result;
    }

    private Predicate convertComparison(ConnectorComparison cmp) {
        ConnectorExpression left = cmp.getLeft();
        ConnectorExpression right = cmp.getRight();
        if (!(left instanceof ConnectorColumnRef) || !(right instanceof ConnectorLiteral)) {
            return null;
        }
        ConnectorColumnRef colRef = (ConnectorColumnRef) left;
        ConnectorLiteral literal = (ConnectorLiteral) right;
        int idx = fieldNames.indexOf(colRef.getColumnName().toLowerCase());
        if (idx < 0) {
            return null;
        }
        Object value = convertLiteralValue(literal, fieldTypes.get(idx));
        if (value == null) {
            // A null value here means one of two unrelated things, and conflating them is what caused
            // `col <=> 5` to be pushed as IS NULL: either the literal really IS null, or this Paimon
            // type is deliberately not pushed down (FLOAT / CHAR / timestamp with local time zone).
            // Only the first case has a translation - and only for the null-safe operator. Checking
            // the operator alone would resurrect the same bug on a FLOAT column.
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL && literal.isNull()) {
                return builder.isNull(idx);
            }
            return null;
        }
        switch (cmp.getOperator()) {
            case EQ:
            case EQ_FOR_NULL:
                // Against a NON-null literal, `col <=> v` and `col = v` have identical result sets:
                // <=> yields false (never unknown) when col is null, and Paimon's Equal likewise never
                // matches nulls. Translating this to IS NULL - as the port from fe-core did - is not a
                // narrowing but an inversion: Paimon prunes away every file that holds col = v, and the
                // BE-side residual filter can only remove rows, never bring pruned files back.
                return builder.equal(idx, value);
            case NE:
                return builder.notEqual(idx, value);
            case LT:
                return builder.lessThan(idx, value);
            case LE:
                return builder.lessOrEqual(idx, value);
            case GT:
                return builder.greaterThan(idx, value);
            case GE:
                return builder.greaterOrEqual(idx, value);
            default:
                return null;
        }
    }

    private Predicate convertIn(ConnectorIn in) {
        ConnectorExpression valueExpr = in.getValue();
        if (!(valueExpr instanceof ConnectorColumnRef)) {
            return null;
        }
        ConnectorColumnRef colRef = (ConnectorColumnRef) valueExpr;
        int idx = fieldNames.indexOf(colRef.getColumnName().toLowerCase());
        if (idx < 0) {
            return null;
        }
        DataType dataType = fieldTypes.get(idx);
        List<Object> values = new ArrayList<>();
        for (ConnectorExpression item : in.getInList()) {
            if (!(item instanceof ConnectorLiteral)) {
                return null;
            }
            Object v = convertLiteralValue((ConnectorLiteral) item, dataType);
            if (v == null) {
                return null;
            }
            values.add(v);
        }
        return in.isNegated() ? builder.notIn(idx, values) : builder.in(idx, values);
    }

    private Predicate convertIsNull(ConnectorIsNull isNull) {
        ConnectorExpression operand = isNull.getOperand();
        if (!(operand instanceof ConnectorColumnRef)) {
            return null;
        }
        int idx = fieldNames.indexOf(
                ((ConnectorColumnRef) operand).getColumnName().toLowerCase());
        if (idx < 0) {
            return null;
        }
        return isNull.isNegated() ? builder.isNotNull(idx) : builder.isNull(idx);
    }

    private Predicate convertLike(ConnectorLike like) {
        if (like.getOperator() != ConnectorLike.Operator.LIKE) {
            return null;
        }
        ConnectorExpression valueExpr = like.getValue();
        ConnectorExpression patternExpr = like.getPattern();
        if (!(valueExpr instanceof ConnectorColumnRef)
                || !(patternExpr instanceof ConnectorLiteral)) {
            return null;
        }
        int idx = fieldNames.indexOf(
                ((ConnectorColumnRef) valueExpr).getColumnName().toLowerCase());
        if (idx < 0) {
            return null;
        }
        String pattern = ((ConnectorLiteral) patternExpr).getValue().toString();
        String prefix = literalPrefixOrNull(pattern);
        if (prefix == null) {
            return null;
        }
        return builder.startsWith(idx, BinaryString.fromString(prefix));
    }

    /**
     * The literal prefix a Doris LIKE pattern is exactly equivalent to, or {@code null} when no such
     * proof exists.
     *
     * <p>Declining is always safe - the predicate is simply not pushed and BE filters every row with
     * the original LIKE. Narrowing is not: the predicate returned here drives Paimon's partition and
     * data-file pruning at planning time and the BE-side JNI row filter, so a file skipped because the
     * pushed prefix was stricter than the user's pattern can never be read back.
     *
     * <p>Doris LIKE uses backslash as its default escape character, {@code %} matches any run of
     * characters and {@code _} matches exactly one. Only {@code literal%} is provably a prefix match:
     * <ul>
     *   <li>{@code _} anywhere is a wildcard, so {@code 'a_c%'} must also match {@code abc...};</li>
     *   <li>a backslash escapes the next character, so the raw text is not the literal to match
     *       ({@code 'a\%%'} means "starts with a%", not "starts with a\%"). Rejecting the whole
     *       pattern on any backslash also guarantees the {@code %} we strip below is a real wildcard
     *       and not an escaped literal one;</li>
     *   <li>a {@code %} left anywhere but the tail means the rest is not a literal prefix.</li>
     * </ul>
     */
    private static String literalPrefixOrNull(String pattern) {
        if (pattern.indexOf('_') >= 0 || pattern.indexOf('\\') >= 0) {
            return null;
        }
        int end = pattern.length();
        while (end > 0 && pattern.charAt(end - 1) == '%') {
            end--;
        }
        if (end == pattern.length()) {
            // No trailing '%': the pattern is anchored at both ends (or starts with '%'), not a prefix.
            return null;
        }
        String body = pattern.substring(0, end);
        if (body.isEmpty() || body.indexOf('%') >= 0) {
            return null;
        }
        return body;
    }

    /**
     * Convert a ConnectorLiteral's value to the appropriate Paimon-typed object.
     */
    private Object convertLiteralValue(ConnectorLiteral literal, DataType paimonType) {
        if (literal.isNull()) {
            return null;
        }
        Object value = literal.getValue();
        DataTypeRoot root = paimonType.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return value instanceof Boolean ? value : null;
            case TINYINT:
                return value instanceof Number ? ((Number) value).byteValue() : null;
            case SMALLINT:
                return value instanceof Number ? ((Number) value).shortValue() : null;
            case INTEGER:
                return value instanceof Number ? ((Number) value).intValue() : null;
            case BIGINT:
                return value instanceof Number ? ((Number) value).longValue() : null;
            case FLOAT:
                return null;
            case DOUBLE:
                return value instanceof Number ? ((Number) value).doubleValue() : null;
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    BigDecimal bd = (BigDecimal) value;
                    return Decimal.fromBigDecimal(bd, bd.precision(), bd.scale());
                }
                return null;
            case VARCHAR:
                return BinaryString.fromString(value.toString());
            case CHAR:
                return null;
            case DATE:
                if (value instanceof LocalDate) {
                    return (int) ((LocalDate) value).toEpochDay();
                }
                return null;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // Zone-free type: interpret the literal's wall-clock in UTC to match paimon's
                // stored min/max file/partition stats (computed by reading the wall clock as UTC).
                // Mirrors legacy PaimonValueConverter#visit(TimestampType), which uses a fixed
                // GMT Calendar. Using the session zone here would shift the epoch-millis vs the
                // stored stats and risk false file/partition pruning = silent data loss.
                if (value instanceof LocalDateTime) {
                    LocalDateTime dt = (LocalDateTime) value;
                    long millis = dt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    return Timestamp.fromEpochMillis(millis);
                }
                return null;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Do NOT push: legacy never pushed LTZ predicates (PaimonValueConverter has no
                // visit(LocalZonedTimestampType), so it fell to defaultMethod -> null). Pushing
                // via a fixed zone is an instant mismatch under non-UTC sessions; leave LTZ
                // conjuncts to BE-side filtering (this conjunct is cleanly dropped).
                return null;
            default:
                return null;
        }
    }
}
