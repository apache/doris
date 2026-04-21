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

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

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
            return null;
        }
        switch (cmp.getOperator()) {
            case EQ:
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
            case EQ_FOR_NULL:
                return builder.isNull(idx);
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
        if (!pattern.startsWith("%") && pattern.endsWith("%")) {
            String prefix = pattern.substring(0, pattern.length() - 1);
            return builder.startsWith(idx, BinaryString.fromString(prefix));
        }
        return null;
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
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof LocalDateTime) {
                    LocalDateTime dt = (LocalDateTime) value;
                    long millis = dt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    return Timestamp.fromEpochMillis(millis);
                }
                return null;
            default:
                return null;
        }
    }
}
