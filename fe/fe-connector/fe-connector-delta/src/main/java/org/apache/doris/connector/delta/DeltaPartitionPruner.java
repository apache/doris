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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * Conservative pruning for Delta partition values.
 *
 * <p>This class only returns {@code false} when the expression is proven not to
 * match a file. Unknown expressions and values return {@code true}, so the
 * original predicate remains available to the Doris scan for row filtering.</p>
 */
final class DeltaPartitionPruner {

    private DeltaPartitionPruner() {
    }

    static boolean mayMatch(Map<String, String> partitionValues, ConnectorExpression expression) {
        if (expression instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expression).getConjuncts()) {
                if (!mayMatch(partitionValues, child)) {
                    return false;
                }
            }
            return true;
        }
        if (expression instanceof ConnectorOr) {
            for (ConnectorExpression child : ((ConnectorOr) expression).getDisjuncts()) {
                if (mayMatch(partitionValues, child)) {
                    return true;
                }
            }
            return false;
        }
        if (expression instanceof ConnectorComparison) {
            return mayMatchComparison(partitionValues, (ConnectorComparison) expression);
        }
        if (expression instanceof ConnectorIn) {
            return mayMatchIn(partitionValues, (ConnectorIn) expression);
        }
        if (expression instanceof ConnectorIsNull) {
            return mayMatchIsNull(partitionValues, (ConnectorIsNull) expression);
        }
        return true;
    }

    private static boolean mayMatchComparison(Map<String, String> partitionValues,
            ConnectorComparison comparison) {
        if (!(comparison.getLeft() instanceof ConnectorColumnRef)
                || !(comparison.getRight() instanceof ConnectorLiteral)) {
            return true;
        }
        ConnectorColumnRef column = (ConnectorColumnRef) comparison.getLeft();
        if (!hasPartitionValue(partitionValues, column)) {
            return true;
        }
        String partitionValue = partitionValues.get(column.getColumnName());
        ConnectorLiteral literal = (ConnectorLiteral) comparison.getRight();
        if (comparison.getOperator() != ConnectorComparison.Operator.EQ
                && comparison.getOperator() != ConnectorComparison.Operator.EQ_FOR_NULL) {
            return true;
        }
        if (literal.isNull()) {
            return comparison.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL
                    && partitionValue == null;
        }
        return partitionValue != null && valuesEqual(partitionValue, literal, column);
    }

    private static boolean mayMatchIn(Map<String, String> partitionValues, ConnectorIn in) {
        if (in.isNegated() || !(in.getValue() instanceof ConnectorColumnRef)) {
            return true;
        }
        ConnectorColumnRef column = (ConnectorColumnRef) in.getValue();
        if (!hasPartitionValue(partitionValues, column)) {
            return true;
        }
        String partitionValue = partitionValues.get(column.getColumnName());
        if (partitionValue == null) {
            return true;
        }
        List<ConnectorExpression> values = in.getInList();
        for (ConnectorExpression expression : values) {
            if (expression instanceof ConnectorLiteral
                    && !((ConnectorLiteral) expression).isNull()
                    && valuesEqual(partitionValue, (ConnectorLiteral) expression, column)) {
                return true;
            }
        }
        return false;
    }

    private static boolean mayMatchIsNull(Map<String, String> partitionValues, ConnectorIsNull isNull) {
        if (!(isNull.getOperand() instanceof ConnectorColumnRef)) {
            return true;
        }
        ConnectorColumnRef column = (ConnectorColumnRef) isNull.getOperand();
        if (!hasPartitionValue(partitionValues, column)) {
            return true;
        }
        boolean isNullValue = partitionValues.get(column.getColumnName()) == null;
        return isNull.isNegated() ? !isNullValue : isNullValue;
    }

    private static boolean hasPartitionValue(Map<String, String> partitionValues,
            ConnectorColumnRef column) {
        return partitionValues.containsKey(column.getColumnName());
    }

    private static boolean valuesEqual(String partitionValue, ConnectorLiteral literal,
            ConnectorColumnRef column) {
        String typeName = column.getType().getTypeName();
        Object literalValue = literal.getValue();
        if (literalValue instanceof Boolean) {
            if (!"true".equalsIgnoreCase(partitionValue)
                    && !"false".equalsIgnoreCase(partitionValue)) {
                return true;
            }
            return Boolean.toString((Boolean) literalValue).equalsIgnoreCase(partitionValue);
        }
        if (isNumeric(typeName) && literalValue instanceof Number) {
            try {
                return new BigDecimal(partitionValue).compareTo(
                        new BigDecimal(literalValue.toString())) == 0;
            } catch (NumberFormatException e) {
                return true;
            }
        }
        if (literalValue instanceof LocalDate) {
            try {
                return LocalDate.parse(partitionValue).equals(literalValue);
            } catch (DateTimeParseException e) {
                return true;
            }
        }
        if (literalValue instanceof LocalDateTime) {
            try {
                String normalized = partitionValue.indexOf(' ') >= 0
                        ? partitionValue.replace(' ', 'T') : partitionValue;
                return LocalDateTime.parse(normalized).equals(literalValue);
            } catch (DateTimeParseException e) {
                return true;
            }
        }
        if (literalValue instanceof String) {
            return literalValue.equals(partitionValue);
        }
        return true;
    }

    private static boolean isNumeric(String typeName) {
        return "TINYINT".equals(typeName) || "SMALLINT".equals(typeName)
                || "INT".equals(typeName) || "BIGINT".equals(typeName)
                || "FLOAT".equals(typeName) || "DOUBLE".equals(typeName)
                || typeName.startsWith("DECIMAL");
    }
}
