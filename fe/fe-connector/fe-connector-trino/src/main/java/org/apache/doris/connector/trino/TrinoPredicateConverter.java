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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;

/**
 * Converts Doris {@link ConnectorExpression} trees to Trino {@link TupleDomain}.
 *
 * <p>Ported from {@code TrinoConnectorPredicateConverter} in fe-core, adapted
 * to work with {@link ConnectorExpression} instead of Doris {@code Expr} types.</p>
 */
public class TrinoPredicateConverter {

    private static final Logger LOG = LogManager.getLogger(TrinoPredicateConverter.class);

    private final Map<String, ColumnHandle> columnHandleMap;
    private final Map<String, ColumnMetadata> columnMetadataMap;

    public TrinoPredicateConverter(Map<String, ColumnHandle> columnHandleMap,
            Map<String, ColumnMetadata> columnMetadataMap) {
        this.columnHandleMap = columnHandleMap;
        this.columnMetadataMap = columnMetadataMap;
    }

    /**
     * Convert a ConnectorExpression to a Trino TupleDomain.
     * Returns TupleDomain.all() if conversion fails (graceful degradation).
     */
    public TupleDomain<ColumnHandle> convert(ConnectorExpression expr) {
        if (expr == null) {
            return TupleDomain.all();
        }
        try {
            return doConvert(expr);
        } catch (Exception e) {
            LOG.warn("Failed to convert expression to Trino TupleDomain: {}", e.getMessage());
            return TupleDomain.all();
        }
    }

    private TupleDomain<ColumnHandle> doConvert(ConnectorExpression expr) {
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
        }
        throw new UnsupportedOperationException(
                "Cannot convert expression type: " + expr.getClass().getSimpleName());
    }

    private TupleDomain<ColumnHandle> convertAnd(ConnectorAnd and) {
        TupleDomain<ColumnHandle> result = TupleDomain.all();
        for (ConnectorExpression child : and.getConjuncts()) {
            try {
                result = result.intersect(doConvert(child));
            } catch (Exception e) {
                LOG.warn("Failed to convert AND child: {}", e.getMessage());
            }
        }
        return result;
    }

    private TupleDomain<ColumnHandle> convertOr(ConnectorOr or) {
        TupleDomain<ColumnHandle> left = doConvert(or.getDisjuncts().get(0));
        TupleDomain<ColumnHandle> right = doConvert(or.getDisjuncts().get(1));
        return TupleDomain.columnWiseUnion(left, right);
    }

    private TupleDomain<ColumnHandle> convertComparison(ConnectorComparison cmp) {
        String colName = extractColumnName(cmp.getLeft());
        ColumnHandle handle = columnHandleMap.get(colName);
        Type type = columnMetadataMap.get(colName).getType();
        Object value = convertLiteralValue(type, cmp.getRight());

        Domain domain;
        switch (cmp.getOperator()) {
            case EQ:
                domain = Domain.create(
                        ValueSet.ofRanges(Range.equal(type, value)), false);
                break;
            case EQ_FOR_NULL:
                if (cmp.getRight() instanceof ConnectorLiteral
                        && ((ConnectorLiteral) cmp.getRight()).isNull()) {
                    domain = Domain.onlyNull(type);
                } else {
                    domain = Domain.create(
                            ValueSet.ofRanges(Range.equal(type, value)), false);
                }
                break;
            case NE:
                domain = Domain.create(
                        ValueSet.all(type).subtract(
                                ValueSet.ofRanges(Range.equal(type, value))), false);
                break;
            case LT:
                domain = Domain.create(
                        ValueSet.ofRanges(Range.lessThan(type, value)), false);
                break;
            case LE:
                domain = Domain.create(
                        ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false);
                break;
            case GT:
                domain = Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(type, value)), false);
                break;
            case GE:
                domain = Domain.create(
                        ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator: " + cmp.getOperator());
        }
        return TupleDomain.withColumnDomains(ImmutableMap.of(handle, domain));
    }

    private TupleDomain<ColumnHandle> convertIn(ConnectorIn in) {
        String colName = extractColumnName(in.getValue());
        ColumnHandle handle = columnHandleMap.get(colName);
        Type type = columnMetadataMap.get(colName).getType();

        List<Range> ranges = Lists.newArrayList();
        for (ConnectorExpression child : in.getInList()) {
            ranges.add(Range.equal(type, convertLiteralValue(type, child)));
        }

        Domain domain;
        if (in.isNegated()) {
            domain = Domain.create(
                    ValueSet.all(type).subtract(ValueSet.ofRanges(ranges)), false);
        } else {
            domain = Domain.create(ValueSet.ofRanges(ranges), false);
        }
        return TupleDomain.withColumnDomains(ImmutableMap.of(handle, domain));
    }

    private TupleDomain<ColumnHandle> convertIsNull(ConnectorIsNull isNull) {
        String colName = extractColumnName(isNull.getOperand());
        ColumnHandle handle = columnHandleMap.get(colName);
        Type type = columnMetadataMap.get(colName).getType();

        Domain domain;
        if (isNull.isNegated()) {
            domain = Domain.notNull(type);
        } else {
            domain = Domain.onlyNull(type);
        }
        return TupleDomain.withColumnDomains(ImmutableMap.of(handle, domain));
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof ConnectorColumnRef) {
            return ((ConnectorColumnRef) expr).getColumnName();
        }
        throw new UnsupportedOperationException(
                "Expected column reference, got: " + expr.getClass().getSimpleName());
    }

    /**
     * Convert a ConnectorLiteral value to the Java type expected by Trino's Domain/Range.
     *
     * <p>The mapping between Trino types and Java types in Range:</p>
     * <ul>
     *   <li>BooleanType → boolean</li>
     *   <li>TinyintType/SmallintType/IntegerType/BigintType → long</li>
     *   <li>RealType → long (Float.floatToIntBits)</li>
     *   <li>DoubleType → double</li>
     *   <li>ShortDecimalType → long (scaled)</li>
     *   <li>LongDecimalType → Int128 (scaled)</li>
     *   <li>CharType/VarcharType/VarbinaryType → Slice</li>
     *   <li>DateType → long (days from epoch)</li>
     *   <li>ShortTimestampType → long (epoch micros)</li>
     * </ul>
     */
    private Object convertLiteralValue(Type type, ConnectorExpression expr) {
        if (!(expr instanceof ConnectorLiteral)) {
            throw new UnsupportedOperationException(
                    "Expected literal, got: " + expr.getClass().getSimpleName());
        }
        ConnectorLiteral literal = (ConnectorLiteral) expr;
        Object value = literal.getValue();
        String typeName = type.getClass().getSimpleName();

        switch (typeName) {
            case "BooleanType":
                return value;
            case "TinyintType":
            case "SmallintType":
            case "IntegerType":
            case "BigintType":
                return ((Number) value).longValue();
            case "RealType":
                return (long) Float.floatToIntBits(((Number) value).floatValue());
            case "DoubleType":
                return ((Number) value).doubleValue();
            case "ShortDecimalType": {
                BigDecimal bd = toBigDecimal(value);
                int scale = type.getTypeParameters().isEmpty()
                        ? bd.scale()
                        : ((io.trino.spi.type.DecimalType) type).getScale();
                return bd.movePointRight(scale).longValueExact();
            }
            case "LongDecimalType": {
                BigDecimal bd = toBigDecimal(value);
                int scale = ((io.trino.spi.type.DecimalType) type).getScale();
                return Int128.valueOf(bd.movePointRight(scale).toBigIntegerExact());
            }
            case "CharType":
            case "VarbinaryType":
            case "VarcharType":
                return Slices.utf8Slice(String.valueOf(value));
            case "DateType": {
                if (value instanceof LocalDate) {
                    return ((LocalDate) value).toEpochDay();
                }
                return Long.parseLong(String.valueOf(value));
            }
            case "ShortTimestampType": {
                if (value instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) value;
                    long epochSecond = ldt.toEpochSecond(ZoneOffset.UTC);
                    long micros = ldt.get(ChronoField.MICRO_OF_SECOND);
                    return epochSecond * 1_000_000L + micros;
                }
                return Long.parseLong(String.valueOf(value));
            }
            default:
                throw new UnsupportedOperationException(
                        "Cannot convert literal for Trino type: " + typeName);
        }
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return new BigDecimal(String.valueOf(value));
    }
}
