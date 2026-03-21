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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Utilities for building Iceberg RowDelta conflict detection filters from Nereids plans.
 */
public final class IcebergConflictDetectionFilterUtils {

    private IcebergConflictDetectionFilterUtils() {
    }

    public static Optional<org.apache.iceberg.expressions.Expression> buildConflictDetectionFilter(
            Plan analyzedPlan, IcebergExternalTable targetTable) {
        if (analyzedPlan == null || targetTable == null) {
            return Optional.empty();
        }
        List<Expression> targetConjuncts = new ArrayList<>();
        collectTargetConjuncts(analyzedPlan, targetTable, targetConjuncts);
        if (targetConjuncts.isEmpty()) {
            return Optional.empty();
        }
        Schema schema = targetTable.getIcebergTable().schema();
        org.apache.iceberg.expressions.Expression combined = null;
        for (Expression predicate : targetConjuncts) {
            Optional<org.apache.iceberg.expressions.Expression> icebergExpr =
                    convertPredicateToIcebergExpression(predicate, schema);
            if (!icebergExpr.isPresent()) {
                continue;
            }
            combined = combined == null ? icebergExpr.get() : Expressions.and(combined, icebergExpr.get());
        }
        return combined == null ? Optional.empty() : Optional.of(combined);
    }

    private static void collectTargetConjuncts(Plan plan, IcebergExternalTable targetTable,
            List<Expression> output) {
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                if (isTargetOnlyPredicate(conjunct, targetTable)) {
                    output.add(conjunct);
                }
            }
        }
        for (Plan child : plan.children()) {
            collectTargetConjuncts(child, targetTable, output);
        }
    }

    private static boolean isTargetOnlyPredicate(Expression predicate, IcebergExternalTable targetTable) {
        if (predicate == null) {
            return false;
        }
        Set<Slot> slots = predicate.getInputSlots();
        if (slots.isEmpty()) {
            return false;
        }
        for (Slot slot : slots) {
            if (!(slot instanceof SlotReference)) {
                return false;
            }
            SlotReference slotReference = (SlotReference) slot;
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slotReference.getName())) {
                return false;
            }
            if (IcebergMetadataColumn.isMetadataColumn(slotReference.getName())) {
                return false;
            }
            Optional<TableIf> table = slotReference.getOriginalTable();
            if (!table.isPresent() || table.get().getId() != targetTable.getId()) {
                return false;
            }
        }
        return true;
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertPredicateToIcebergExpression(
            Expression predicate, Schema schema) {
        if (predicate == null) {
            return Optional.empty();
        }
        if (predicate instanceof And) {
            And andExpr = (And) predicate;
            Optional<org.apache.iceberg.expressions.Expression> left =
                    convertPredicateToIcebergExpression(andExpr.child(0), schema);
            Optional<org.apache.iceberg.expressions.Expression> right =
                    convertPredicateToIcebergExpression(andExpr.child(1), schema);
            return combineAnd(left, right);
        }
        if (predicate instanceof Or) {
            Or orExpr = (Or) predicate;
            Optional<org.apache.iceberg.expressions.Expression> left =
                    convertPredicateToIcebergExpression(orExpr.child(0), schema);
            Optional<org.apache.iceberg.expressions.Expression> right =
                    convertPredicateToIcebergExpression(orExpr.child(1), schema);
            if (!left.isPresent() || !right.isPresent()) {
                return Optional.empty();
            }
            if (!isSameColumnPredicate(orExpr.child(0), orExpr.child(1), schema)) {
                return Optional.empty();
            }
            return Optional.of(Expressions.or(left.get(), right.get()));
        }
        if (predicate instanceof Not) {
            Not notExpr = (Not) predicate;
            if (!(notExpr.child() instanceof IsNull)) {
                return Optional.empty();
            }
            return convertIsNullPredicate((IsNull) notExpr.child(), schema, true);
        }
        if (predicate instanceof IsNull) {
            return convertIsNullPredicate((IsNull) predicate, schema, false);
        }
        if (predicate instanceof InPredicate) {
            return convertInPredicate((InPredicate) predicate, schema);
        }
        if (predicate instanceof Between) {
            return convertComparablePredicate((Between) predicate, schema);
        }
        if (predicate instanceof EqualTo
                || predicate instanceof GreaterThan
                || predicate instanceof GreaterThanEqual
                || predicate instanceof LessThan
                || predicate instanceof LessThanEqual) {
            return convertComparablePredicate(predicate, schema);
        }
        return Optional.empty();
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertIsNullPredicate(
            IsNull predicate, Schema schema, boolean negated) {
        Optional<NestedField> nestedField = resolveSingleField(predicate, schema);
        if (!nestedField.isPresent()) {
            return Optional.empty();
        }
        if (isStructuralType(nestedField.get().type())) {
            return Optional.empty();
        }
        org.apache.iceberg.expressions.Expression isNullExpr = Expressions.isNull(nestedField.get().name());
        return Optional.of(negated ? Expressions.not(isNullExpr) : isNullExpr);
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertInPredicate(
            InPredicate predicate, Schema schema) {
        if (!(predicate.child(0) instanceof Slot)) {
            return Optional.empty();
        }
        Optional<NestedField> nestedField = resolveSingleField(predicate, schema);
        if (!nestedField.isPresent()) {
            return Optional.empty();
        }
        Type type = nestedField.get().type();
        if (isStructuralType(type)) {
            return Optional.empty();
        }

        boolean hasNull = false;
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < predicate.children().size(); i++) {
            Expression child = predicate.child(i);
            if (!(child instanceof Literal)) {
                return Optional.empty();
            }
            Literal literal = (Literal) child;
            if (literal instanceof NullLiteral) {
                hasNull = true;
                continue;
            }
            try {
                Object value = IcebergNereidsUtils.extractNereidsLiteralValue(literal, type);
                if (value == null) {
                    return Optional.empty();
                }
                values.add(value);
            } catch (UserException ignored) {
                return Optional.empty();
            }
        }

        if (isUuidType(type) && !values.isEmpty()) {
            return Optional.empty();
        }

        org.apache.iceberg.expressions.Expression valuesExpr = values.isEmpty()
                ? null
                : Expressions.in(nestedField.get().name(), values);
        org.apache.iceberg.expressions.Expression nullExpr = hasNull
                ? Expressions.isNull(nestedField.get().name())
                : null;
        return combineOr(nullExpr, valuesExpr);
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertComparablePredicate(
            Expression predicate, Schema schema) {
        Optional<NestedField> nestedField = resolveSingleField(predicate, schema);
        if (!nestedField.isPresent()) {
            return Optional.empty();
        }
        Type type = nestedField.get().type();
        if (isStructuralType(type)) {
            return Optional.empty();
        }
        if (isUuidType(type)) {
            return isNullComparison(predicate)
                    ? Optional.of(Expressions.isNull(nestedField.get().name()))
                    : Optional.empty();
        }
        try {
            return Optional.of(IcebergNereidsUtils.convertNereidsToIcebergExpression(predicate, schema));
        } catch (UserException ignored) {
            return Optional.empty();
        }
    }

    private static boolean isNullComparison(Expression predicate) {
        if (!(predicate instanceof EqualTo)) {
            return false;
        }
        Expression left = predicate.child(0);
        Expression right = predicate.child(1);
        return (left instanceof Slot && right instanceof NullLiteral)
                || (right instanceof Slot && left instanceof NullLiteral);
    }

    private static Optional<NestedField> resolveSingleField(Expression predicate, Schema schema) {
        Set<Slot> slots = predicate.getInputSlots();
        if (slots.size() != 1) {
            return Optional.empty();
        }
        Slot slot = slots.iterator().next();
        if (!(slot instanceof SlotReference)) {
            return Optional.empty();
        }
        String columnName = ((SlotReference) slot).getName();
        if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(columnName)) {
            return Optional.empty();
        }
        if (IcebergMetadataColumn.isMetadataColumn(columnName)) {
            return Optional.empty();
        }
        NestedField nestedField = schema.caseInsensitiveFindField(columnName);
        return Optional.ofNullable(nestedField);
    }

    private static boolean isSameColumnPredicate(Expression left, Expression right, Schema schema) {
        Optional<NestedField> leftField = resolveSingleField(left, schema);
        if (!leftField.isPresent()) {
            return false;
        }
        Optional<NestedField> rightField = resolveSingleField(right, schema);
        if (!rightField.isPresent()) {
            return false;
        }
        return leftField.get().fieldId() == rightField.get().fieldId();
    }

    private static Optional<org.apache.iceberg.expressions.Expression> combineAnd(
            Optional<org.apache.iceberg.expressions.Expression> left,
            Optional<org.apache.iceberg.expressions.Expression> right) {
        if (!left.isPresent()) {
            return right;
        }
        if (!right.isPresent()) {
            return left;
        }
        return Optional.of(Expressions.and(left.get(), right.get()));
    }

    private static Optional<org.apache.iceberg.expressions.Expression> combineOr(
            org.apache.iceberg.expressions.Expression left,
            org.apache.iceberg.expressions.Expression right) {
        if (left == null) {
            return Optional.ofNullable(right);
        }
        if (right == null) {
            return Optional.of(left);
        }
        return Optional.of(Expressions.or(left, right));
    }

    private static boolean isStructuralType(Type type) {
        return type.isStructType() || type.isListType() || type.isMapType();
    }

    private static boolean isUuidType(Type type) {
        return type.typeId() == Type.TypeID.UUID;
    }
}
