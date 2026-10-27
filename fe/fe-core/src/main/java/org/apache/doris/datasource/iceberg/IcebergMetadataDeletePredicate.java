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
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the complete statement predicate for an Iceberg whole-file metadata delete.
 *
 * <p>Unlike the conflict-detection helper, this path may be destructive: every conjunct must be
 * representable exactly. A missing conjunct would widen the delete and can drop a file that still
 * contains live rows, so any uncertainty rejects the whole fast path.
 */
public final class IcebergMetadataDeletePredicate {
    private IcebergMetadataDeletePredicate() {
    }

    /**
     * Returns the exact Iceberg predicate for {@code plan}, or empty when the plan is not provably a
     * single-table DELETE whose complete predicate is representable.
     */
    public static Optional<org.apache.iceberg.expressions.Expression> build(
            Table icebergTable, Plan plan, long targetTableId) {
        List<Expression> conjuncts = new ArrayList<>();
        if (!collectCompletePredicate(plan, targetTableId, conjuncts)) {
            return Optional.empty();
        }
        if (conjuncts.isEmpty()) {
            return Optional.of(Expressions.alwaysTrue());
        }

        Schema schema = icebergTable.schema();
        Set<Integer> unsafeNanosFields = IcebergTimestampUtils.unsafeNanosFields(icebergTable, schema);
        org.apache.iceberg.expressions.Expression result = null;
        for (Expression conjunct : conjuncts) {
            if (IcebergTimestampUtils.referencesUnsafe(conjunct, schema, unsafeNanosFields)) {
                return Optional.empty();
            }
            Optional<org.apache.iceberg.expressions.Expression> converted =
                    convertExact(conjunct, schema);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            result = result == null ? converted.get() : Expressions.and(result, converted.get());
        }
        return Optional.of(result);
    }

    /**
     * Walks the single-input analyzed plan and returns false as soon as a node can change which
     * target rows are produced without exposing that restriction as a LogicalFilter.
     */
    private static boolean collectCompletePredicate(
            Plan plan, long targetTableId, List<Expression> output) {
        if (plan instanceof LogicalFilter) {
            for (Expression conjunct : ((LogicalFilter<?>) plan).getConjuncts()) {
                if (!isExactPhysicalTargetPredicate(conjunct, targetTableId)) {
                    return false;
                }
                output.add(conjunct);
            }
        } else if (!(plan instanceof LogicalSink) && !(plan instanceof LogicalProject)
                && !(plan instanceof LogicalCheckPolicy) && !(plan instanceof LogicalSubQueryAlias)
                && !(plan instanceof LogicalCatalogRelation)) {
            return false;
        }

        List<Plan> children = plan.children();
        if (plan instanceof LogicalCatalogRelation) {
            return children.isEmpty() && ((LogicalCatalogRelation) plan).getTable().getId() == targetTableId;
        }
        if (children.size() != 1) {
            return false;
        }
        return collectCompletePredicate(children.get(0), targetTableId, output);
    }

    /**
     * A conjunct must target only the DELETE table, use physical column names, and contain no
     * synthetic Iceberg metadata columns.
     */
    private static boolean isExactPhysicalTargetPredicate(Expression predicate, long targetTableId) {
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
            SlotReference reference = (SlotReference) slot;
            Optional<TableIf> originalTable = reference.getOriginalTable();
            if (!originalTable.isPresent() || originalTable.get().getId() != targetTableId) {
                return false;
            }
            Optional<Column> originalColumn = reference.getOriginalColumn();
            if (!originalColumn.isPresent() || !reference.getName().equals(originalColumn.get().getName())) {
                return false;
            }
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(reference.getName())
                    || IcebergMetadataColumn.isMetadataColumn(reference.getName())
                    || !reference.getSubPath().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertExact(
            Expression expression, Schema schema) {
        if (expression instanceof And) {
            org.apache.iceberg.expressions.Expression combined = null;
            for (Expression child : expression.children()) {
                Optional<org.apache.iceberg.expressions.Expression> converted =
                        convertExact(child, schema);
                if (!converted.isPresent()) {
                    return Optional.empty();
                }
                combined = combined == null ? converted.get()
                        : Expressions.and(combined, converted.get());
            }
            return combined == null ? Optional.empty() : Optional.of(combined);
        }
        if (expression instanceof Or) {
            org.apache.iceberg.expressions.Expression combined = null;
            for (Expression child : expression.children()) {
                Optional<org.apache.iceberg.expressions.Expression> converted =
                        convertExact(child, schema);
                if (!converted.isPresent()) {
                    return Optional.empty();
                }
                combined = combined == null ? converted.get()
                        : Expressions.or(combined, converted.get());
            }
            return combined == null ? Optional.empty() : Optional.of(combined);
        }
        if (expression instanceof Not) {
            Expression child = ((Not) expression).child();
            // Iceberg's notEqual/notIn match NULL while SQL NOT(UNKNOWN) does not. IS NOT NULL is
            // the only negation whose SQL semantics are preserved by this conversion.
            if (!(child instanceof IsNull)) {
                return Optional.empty();
            }
            Optional<org.apache.iceberg.expressions.Expression> converted =
                    convertIsNull((IsNull) child, schema);
            return converted.map(Expressions::not);
        }
        if (expression instanceof EqualTo
                || expression instanceof GreaterThan
                || expression instanceof GreaterThanEqual
                || expression instanceof LessThan
                || expression instanceof LessThanEqual) {
            return convertComparison(expression, schema);
        }
        if (expression instanceof InPredicate) {
            return convertIn((InPredicate) expression, schema);
        }
        if (expression instanceof IsNull) {
            return convertIsNull((IsNull) expression, schema);
        }
        if (expression instanceof Between) {
            return convertBetween((Between) expression, schema);
        }
        return Optional.empty();
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertComparison(
            Expression expression, Schema schema) {
        Optional<NestedField> field = resolveSingleField(expression, schema);
        if (!field.isPresent() || !isSupportedType(field.get().type())) {
            return Optional.empty();
        }

        Expression left = expression.child(0);
        Expression right = expression.child(1);
        Literal literal;
        boolean literalOnLeft;
        if (left instanceof Slot && right instanceof Literal) {
            literal = (Literal) right;
            literalOnLeft = false;
        } else if (left instanceof Literal && right instanceof Slot) {
            literal = (Literal) left;
            literalOnLeft = true;
        } else {
            return Optional.empty();
        }
        if (literal instanceof NullLiteral) {
            // SQL `col = NULL` does not match rows; do not silently lower it to IS NULL.
            return Optional.empty();
        }

        Object value;
        try {
            value = IcebergNereidsUtils.extractNereidsLiteralValue(literal, field.get().type());
        } catch (UserException e) {
            return Optional.empty();
        }
        if (value == null) {
            return Optional.empty();
        }

        String columnName = field.get().name();
        if (expression instanceof EqualTo) {
            return Optional.of(Expressions.equal(columnName, value));
        }
        if (expression instanceof GreaterThan) {
            return Optional.of(literalOnLeft
                    ? Expressions.lessThan(columnName, value)
                    : Expressions.greaterThan(columnName, value));
        }
        if (expression instanceof GreaterThanEqual) {
            return Optional.of(literalOnLeft
                    ? Expressions.lessThanOrEqual(columnName, value)
                    : Expressions.greaterThanOrEqual(columnName, value));
        }
        if (expression instanceof LessThan) {
            return Optional.of(literalOnLeft
                    ? Expressions.greaterThan(columnName, value)
                    : Expressions.lessThan(columnName, value));
        }
        if (expression instanceof LessThanEqual) {
            return Optional.of(literalOnLeft
                    ? Expressions.greaterThanOrEqual(columnName, value)
                    : Expressions.lessThanOrEqual(columnName, value));
        }
        return Optional.empty();
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertIn(
            InPredicate predicate, Schema schema) {
        Optional<NestedField> field = resolveSingleField(predicate, schema);
        if (!field.isPresent() || !isSupportedType(field.get().type())) {
            return Optional.empty();
        }
        if (!(predicate.child(0) instanceof Slot)
                || predicate.optionsContainsNullLiteral()) {
            return Optional.empty();
        }

        List<Object> values = new ArrayList<>();
        try {
            for (int i = 1; i < predicate.children().size(); i++) {
                Expression child = predicate.child(i);
                if (!(child instanceof Literal)) {
                    return Optional.empty();
                }
                Object value = IcebergNereidsUtils.extractNereidsLiteralValue(
                        (Literal) child, field.get().type());
                if (value == null) {
                    return Optional.empty();
                }
                values.add(value);
            }
        } catch (UserException e) {
            return Optional.empty();
        }
        return values.isEmpty() ? Optional.empty()
                : Optional.of(Expressions.in(field.get().name(), values));
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertIsNull(
            IsNull predicate, Schema schema) {
        Optional<NestedField> field = resolveSingleField(predicate, schema);
        if (!field.isPresent() || !isSupportedType(field.get().type())) {
            return Optional.empty();
        }
        return Optional.of(Expressions.isNull(field.get().name()));
    }

    private static Optional<org.apache.iceberg.expressions.Expression> convertBetween(
            Between predicate, Schema schema) {
        Optional<NestedField> field = resolveSingleField(predicate, schema);
        if (!field.isPresent() || !isSupportedType(field.get().type())
                || !(predicate.getCompareExpr() instanceof Slot)
                || !(predicate.getLowerBound() instanceof Literal)
                || !(predicate.getUpperBound() instanceof Literal)) {
            return Optional.empty();
        }
        Object lower;
        Object upper;
        try {
            lower = IcebergNereidsUtils.extractNereidsLiteralValue(
                    (Literal) predicate.getLowerBound(), field.get().type());
            upper = IcebergNereidsUtils.extractNereidsLiteralValue(
                    (Literal) predicate.getUpperBound(), field.get().type());
        } catch (UserException e) {
            return Optional.empty();
        }
        if (lower == null || upper == null) {
            return Optional.empty();
        }
        return Optional.of(Expressions.and(
                Expressions.greaterThanOrEqual(field.get().name(), lower),
                Expressions.lessThanOrEqual(field.get().name(), upper)));
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
        SlotReference reference = (SlotReference) slot;
        if (!reference.getSubPath().isEmpty()
                || Column.ICEBERG_ROWID_COL.equalsIgnoreCase(reference.getName())
                || IcebergMetadataColumn.isMetadataColumn(reference.getName())) {
            return Optional.empty();
        }
        return Optional.ofNullable(schema.caseInsensitiveFindField(reference.getName()));
    }

    private static boolean isSupportedType(Type type) {
        return type != null && !type.isStructType() && !type.isListType() && !type.isMapType()
                && type.typeId() != Type.TypeID.UUID;
    }
}
