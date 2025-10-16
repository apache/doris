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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Utility class for converting Nereids expressions to Iceberg expressions.
 * This class provides methods to convert Doris Nereids expressions to Iceberg
 * expressions
 * for use in Iceberg table operations like filtering and predicate pushdown.
 */
public class IcebergNereidsUtils {

    /**
     * Convert Nereids Expression to Iceberg Expression
     * This handles basic predicates like AND, OR, NOT, comparison operators, and IN
     * predicates
     */
    public static org.apache.iceberg.expressions.Expression convertNereidsToIcebergExpression(
            Expression nereidsExpr, Schema schema) {
        try {
            if (nereidsExpr == null) {
                return null;
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
                } else if (left != null) {
                    return left;
                } else if (right != null) {
                    return right;
                }
                return null;
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
                return null;
            }

            if (nereidsExpr instanceof Not) {
                Not notExpr = (Not) nereidsExpr;
                org.apache.iceberg.expressions.Expression child = convertNereidsToIcebergExpression(notExpr.child(),
                        schema);
                if (child != null) {
                    return Expressions.not(child);
                }
                return null;
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

            return null;

        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Convert Nereids binary predicate (comparison operators)
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsBinaryPredicate(
            Expression nereidsExpr, Schema schema,
            BiFunction<String, Object, org.apache.iceberg.expressions.Expression> converter) {

        // Extract slot reference and literal from the binary predicate
        SlotReference slotRef = null;
        Literal literal = null;

        if (nereidsExpr.children().size() == 2) {
            Expression left = nereidsExpr.child(0);
            Expression right = nereidsExpr.child(1);

            if (left instanceof SlotReference
                    && right instanceof Literal) {
                slotRef = (SlotReference) left;
                literal = (Literal) right;
            } else if (left instanceof Literal
                    && right instanceof SlotReference) {
                slotRef = (SlotReference) right;
                literal = (Literal) left;
            }
        }

        if (slotRef == null || literal == null) {
            return null;
        }

        String colName = slotRef.getName();
        org.apache.iceberg.types.Types.NestedField nestedField = schema.caseInsensitiveFindField(colName);
        if (nestedField == null) {
            return null;
        }

        colName = nestedField.name();
        Object value = extractNereidsLiteralValue(literal, nestedField.type());

        if (value == null) {
            if (literal instanceof NullLiteral) {
                return Expressions.isNull(colName);
            }
            return null;
        }

        return converter.apply(colName, value);
    }

    /**
     * Convert Nereids IN predicate
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsInPredicate(
            InPredicate inPredicate, org.apache.iceberg.Schema schema) {
        if (inPredicate.children().size() < 2) {
            return null;
        }

        org.apache.doris.nereids.trees.expressions.Expression left = inPredicate.child(0);
        if (!(left instanceof SlotReference)) {
            return null;
        }

        SlotReference slotRef = (SlotReference) left;
        String colName = slotRef.getName();
        NestedField nestedField = schema.caseInsensitiveFindField(colName);
        if (nestedField == null) {
            return null;
        }

        colName = nestedField.name();
        List<Object> values = new ArrayList<>();

        for (int i = 1; i < inPredicate.children().size(); i++) {
            Expression child = inPredicate.child(i);
            if (!(child instanceof Literal)) {
                return null;
            }

            Object value = extractNereidsLiteralValue(
                    (Literal) child, nestedField.type());
            if (value == null) {
                return null;
            }
            values.add(value);
        }

        return Expressions.in(colName, values);
    }

    /**
     * Extract literal value from Nereids Literal expression
     */
    private static Object extractNereidsLiteralValue(
            Literal literal,
            Type icebergType) {
        try {
            if (literal instanceof NullLiteral) {
                return null;
            }
            // TODO: handle all the different literal types and convert them to the appropriate Iceberg types
            return literal.getValue();

        } catch (Exception e) {
            return null;
        }
    }
}
