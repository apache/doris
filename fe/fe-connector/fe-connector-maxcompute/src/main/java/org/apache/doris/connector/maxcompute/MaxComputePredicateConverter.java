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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.table.optimizer.predicate.Attribute;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.RawPredicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts {@link ConnectorExpression} trees to ODPS SDK {@link Predicate} objects.
 *
 * <p>Ported from {@code MaxComputeScanNode.convertExprToOdpsPredicate} in fe-core.</p>
 */
public class MaxComputePredicateConverter {
    private static final Logger LOG = LogManager.getLogger(MaxComputePredicateConverter.class);

    static final DateTimeFormatter DATETIME_3_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static final DateTimeFormatter DATETIME_6_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final Map<String, OdpsType> columnTypeMap;
    private final boolean dateTimePushDown;
    private final ZoneId sourceTimeZone;

    /**
     * @param columnTypeMap mapping from column name to ODPS type
     * @param dateTimePushDown whether DATETIME/TIMESTAMP predicate push down is enabled
     * @param sourceTimeZone the session time zone for datetime conversion
     */
    public MaxComputePredicateConverter(Map<String, OdpsType> columnTypeMap,
            boolean dateTimePushDown, ZoneId sourceTimeZone) {
        this.columnTypeMap = columnTypeMap;
        this.dateTimePushDown = dateTimePushDown;
        this.sourceTimeZone = sourceTimeZone;
    }

    /**
     * Convert a ConnectorExpression to an ODPS Predicate.
     * Returns {@link Predicate#NO_PREDICATE} if the expression cannot be converted.
     */
    public Predicate convert(ConnectorExpression expr) {
        if (expr == null) {
            return Predicate.NO_PREDICATE;
        }
        try {
            return doConvert(expr);
        } catch (Exception e) {
            LOG.warn("Failed to convert expression to ODPS predicate: {}", e.getMessage());
            return Predicate.NO_PREDICATE;
        }
    }

    private Predicate doConvert(ConnectorExpression expr) {
        if (expr instanceof ConnectorAnd) {
            return convertAnd((ConnectorAnd) expr);
        } else if (expr instanceof ConnectorOr) {
            return convertOr((ConnectorOr) expr);
        } else if (expr instanceof ConnectorNot) {
            return convertNot((ConnectorNot) expr);
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

    private Predicate convertAnd(ConnectorAnd and) {
        List<Predicate> children = new ArrayList<>();
        for (ConnectorExpression child : and.getConjuncts()) {
            children.add(doConvert(child));
        }
        return new CompoundPredicate(CompoundPredicate.Operator.AND, children);
    }

    private Predicate convertOr(ConnectorOr or) {
        List<Predicate> children = new ArrayList<>();
        for (ConnectorExpression child : or.getDisjuncts()) {
            children.add(doConvert(child));
        }
        return new CompoundPredicate(CompoundPredicate.Operator.OR, children);
    }

    private Predicate convertNot(ConnectorNot not) {
        List<Predicate> children = new ArrayList<>();
        children.add(doConvert(not.getOperand()));
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, children);
    }

    private Predicate convertComparison(ConnectorComparison cmp) {
        String columnName = extractColumnName(cmp.getLeft());
        String value = formatLiteralValue(columnName, cmp.getRight());

        String opDesc;
        switch (cmp.getOperator()) {
            case EQ:
                opDesc = "==";
                break;
            case NE:
                opDesc = "!=";
                break;
            case LT:
                opDesc = "<";
                break;
            case LE:
                opDesc = "<=";
                break;
            case GT:
                opDesc = ">";
                break;
            case GE:
                opDesc = ">=";
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + cmp.getOperator());
        }
        return new RawPredicate(columnName + " " + opDesc + " " + value);
    }

    private Predicate convertIn(ConnectorIn in) {
        String columnName = extractColumnName(in.getValue());
        String opDesc = in.isNegated() ? "NOT IN" : "IN";

        StringBuilder sb = new StringBuilder();
        sb.append(columnName).append(" ").append(opDesc).append(" (");
        List<ConnectorExpression> values = in.getInList();
        for (int i = 0; i < values.size(); i++) {
            sb.append(formatLiteralValue(columnName, values.get(i)));
            if (i < values.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(" )");
        return new RawPredicate(sb.toString());
    }

    private Predicate convertIsNull(ConnectorIsNull isNull) {
        String columnName = extractColumnName(isNull.getOperand());
        UnaryPredicate.Operator op = isNull.isNegated()
                ? UnaryPredicate.Operator.NOT_NULL
                : UnaryPredicate.Operator.IS_NULL;
        return new UnaryPredicate(op, new Attribute(columnName));
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof ConnectorColumnRef) {
            return ((ConnectorColumnRef) expr).getColumnName();
        }
        throw new UnsupportedOperationException(
                "Expected column reference, got: " + expr.getClass().getSimpleName());
    }

    private String formatLiteralValue(String columnName, ConnectorExpression expr) {
        if (!(expr instanceof ConnectorLiteral)) {
            throw new UnsupportedOperationException(
                    "Expected literal, got: " + expr.getClass().getSimpleName());
        }
        ConnectorLiteral literal = (ConnectorLiteral) expr;
        String rawValue = String.valueOf(literal.getValue());

        OdpsType odpsType = columnTypeMap.get(columnName);
        if (odpsType == null) {
            return " \"" + rawValue + "\" ";
        }

        switch (odpsType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return " " + rawValue + " ";

            case STRING:
            case CHAR:
            case VARCHAR:
                return " \"" + rawValue + "\" ";

            case DATE:
                return " \"" + rawValue + "\" ";

            case DATETIME:
                if (dateTimePushDown) {
                    return " \"" + convertDateTimezone(
                            rawValue, DATETIME_3_FORMATTER, ZoneId.of("UTC")) + "\" ";
                }
                break;

            case TIMESTAMP:
                if (dateTimePushDown) {
                    return " \"" + convertDateTimezone(
                            rawValue, DATETIME_6_FORMATTER, ZoneId.of("UTC")) + "\" ";
                }
                break;

            case TIMESTAMP_NTZ:
                if (dateTimePushDown) {
                    return " \"" + rawValue + "\" ";
                }
                break;

            default:
                break;
        }
        throw new UnsupportedOperationException(
                "Cannot push down ODPS type: " + odpsType + " for column " + columnName);
    }

    private String convertDateTimezone(String dateTimeStr,
            DateTimeFormatter formatter, ZoneId toZone) {
        if (sourceTimeZone.equals(toZone)) {
            return dateTimeStr;
        }
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeStr, formatter);
        ZonedDateTime sourceZoned = localDateTime.atZone(sourceTimeZone);
        ZonedDateTime targetZoned = sourceZoned.withZoneSameInstant(toZone);
        return targetZoned.format(formatter);
    }
}
