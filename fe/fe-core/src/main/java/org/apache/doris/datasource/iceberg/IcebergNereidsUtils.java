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

        throw new UserException("Unsupported expression type: " + nereidsExpr.getClass().getName());
    }

    /**
     * Convert Nereids binary predicate (comparison operators)
     */
    private static org.apache.iceberg.expressions.Expression convertNereidsBinaryPredicate(
            Expression nereidsExpr, Schema schema,
            BiFunction<String, Object, org.apache.iceberg.expressions.Expression> converter) throws UserException {

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
            throw new UserException("Binary predicate must be between a column and a literal");
        }

        String colName = slotRef.getName();
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
        if (!(left instanceof SlotReference)) {
            throw new UserException("Left side of IN predicate must be a column");
        }

        SlotReference slotRef = (SlotReference) left;
        String colName = slotRef.getName();
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
     * Extract literal value from Nereids Literal expression
     */
    private static Object extractNereidsLiteralValue(
            Literal literal,
            Type icebergType) {
        return literal.getValue();
    }
}
