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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Converts the safe subset of Nereids partition predicates to the HMS filter grammar. */
public final class HivePartitionFilterBuilder {

    private HivePartitionFilterBuilder() {
    }

    /**
     * Builds an HMS filter from direct equality and IN predicates. Returns null when any predicate
     * or column type is outside the supported grammar, so callers can retain local pruning.
     */
    public static String build(Expression predicate, List<Column> partitionColumns) {
        Map<String, Column> columnsByName = partitionColumns.stream()
                .collect(Collectors.toMap(column -> column.getName().toLowerCase(Locale.ROOT),
                        Function.identity()));
        Map<String, List<String>> valuesByName = new HashMap<>();
        for (Expression conjunct : ExpressionUtils.extractConjunction(predicate)) {
            if (!collectValues(conjunct, columnsByName, valuesByName)) {
                return null;
            }
        }
        return buildFilter(partitionColumns, valuesByName);
    }

    private static boolean collectValues(Expression expression, Map<String, Column> columnsByName,
            Map<String, List<String>> valuesByName) {
        if (expression instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) expression;
            return collectEqualValue(equalTo.left(), equalTo.right(), columnsByName, valuesByName)
                    || collectEqualValue(equalTo.right(), equalTo.left(), columnsByName, valuesByName);
        }
        if (expression instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expression;
            String columnName = slotName(inPredicate.getCompareExpr());
            if (columnName == null || !columnsByName.containsKey(columnName)) {
                return false;
            }
            List<String> values = Lists.newArrayList();
            for (Expression option : inPredicate.getOptions()) {
                String value = literalValue(option);
                if (value == null
                        || !literalIsSupported((Literal) option, columnsByName.get(columnName))) {
                    return false;
                }
                values.add(value);
            }
            valuesByName.computeIfAbsent(columnName, ignored -> Lists.newArrayList()).addAll(values);
            return true;
        }
        return false;
    }

    private static boolean collectEqualValue(Expression slotExpression, Expression literalExpression,
            Map<String, Column> columnsByName, Map<String, List<String>> valuesByName) {
        String columnName = slotName(slotExpression);
        if (columnName == null || !columnsByName.containsKey(columnName)) {
            return false;
        }
        String value = literalValue(literalExpression);
        if (value == null
                || !literalIsSupported((Literal) literalExpression, columnsByName.get(columnName))) {
            return false;
        }
        valuesByName.computeIfAbsent(columnName, ignored -> Lists.newArrayList()).add(value);
        return true;
    }

    private static String slotName(Expression expression) {
        if (expression instanceof SlotReference) {
            return ((SlotReference) expression).getName().toLowerCase(Locale.ROOT);
        }
        return null;
    }

    private static String literalValue(Expression expression) {
        if (!(expression instanceof Literal)) {
            return null;
        }
        Object value = ((Literal) expression).getValue();
        return value == null ? null : value.toString();
    }

    private static boolean literalIsSupported(Literal literal, Column column) {
        DataType literalType = literal.getDataType();
        String value = literal.getValue().toString();
        if (column.getType().isIntegerType()) {
            return literalType.isIntegerType() && isIntegralLiteral(literal.getValue().toString());
        }
        return column.getType().isStringType() && literalType.isStringType()
                && value.indexOf('\\') < 0 && value.indexOf('\'') < 0;
    }

    private static String buildFilter(List<Column> partitionColumns, Map<String, List<String>> valuesByName) {
        List<String> filters = Lists.newArrayList();
        for (Column partitionColumn : partitionColumns) {
            List<String> values = valuesByName.get(partitionColumn.getName().toLowerCase(Locale.ROOT));
            if (values == null || values.isEmpty()) {
                continue;
            }
            if (!isHmsFilterIdentifier(partitionColumn.getName())) {
                return null;
            }
            List<String> valueFilters = values.stream()
                    .map(value -> partitionColumn.getName() + " = " + toHmsLiteral(value, partitionColumn))
                    .collect(Collectors.toList());
            filters.add("(" + String.join(" OR ", valueFilters) + ")");
        }
        return filters.isEmpty() ? null : String.join(" AND ", filters);
    }

    private static String toHmsLiteral(String value, Column column) {
        return column.getType().isIntegerType() ? value : "'" + value + "'";
    }

    private static boolean isHmsFilterIdentifier(String value) {
        if (value.isEmpty() || !isHmsFilterLetterOrDigit(value.charAt(0))) {
            return false;
        }
        String lowerCaseValue = value.toLowerCase(Locale.ROOT);
        if (lowerCaseValue.equals("not") || lowerCaseValue.equals("and") || lowerCaseValue.equals("or")
                || lowerCaseValue.equals("like") || lowerCaseValue.equals("date")
                || lowerCaseValue.equals("const") || lowerCaseValue.equals("struct")
                || isAllDigits(value)) {
            return false;
        }
        for (int index = 1; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!isHmsFilterLetterOrDigit(character) && character != '_') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllDigits(String value) {
        return value.chars().allMatch(character -> character >= '0' && character <= '9');
    }

    private static boolean isHmsFilterLetterOrDigit(char value) {
        return value >= 'a' && value <= 'z'
                || value >= 'A' && value <= 'Z'
                || value >= '0' && value <= '9';
    }

    private static boolean isIntegralLiteral(String value) {
        int start = value.startsWith("-") ? 1 : 0;
        if (start == value.length()) {
            return false;
        }
        return value.chars().skip(start).allMatch(character -> character >= '0' && character <= '9');
    }
}
