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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Engine-side (fe-core) production half of O5-2 (P6.3-T07b): extracts, from an analyzed DELETE/UPDATE/MERGE
 * plan, the conjuncts that reference <i>only the target table's own columns</i> and hands the connector a
 * neutral {@link ConnectorPredicate} for write-time optimistic conflict detection (via
 * {@code ConnectorTransaction.applyWriteConstraint}). It is the connector-agnostic generalization of legacy
 * {@code IcebergConflictDetectionFilterUtils}'s collection half: the {@code IcebergExternalTable} parameter
 * becomes a plain {@code long targetTableId}, the inline {@code $row_id}/metadata-column exclusion becomes an
 * injected {@link Predicate} (the row-level DML transform supplies the connector-specific predicate in T07c),
 * and the per-conjunct iceberg lowering moves to the connector — here each surviving conjunct is converted to
 * a neutral {@link ConnectorExpression} by {@link NereidsToConnectorExpressionConverter}.
 *
 * <p>Conjuncts the converter cannot represent are dropped: dropping a conjunct only ever <i>widens</i> the
 * resulting conflict-detection filter (more conservative, never missing a real concurrent-write conflict).
 * The wiring of the extracted predicate into {@code applyWriteConstraint} is done by the T07c command shell;
 * this class is independently unit-testable.</p>
 */
public final class WriteConstraintExtractor {

    private WriteConstraintExtractor() {
    }

    /**
     * Extracts the target-only write constraint from an analyzed plan.
     *
     * @param analyzedPlan the analyzed DELETE/UPDATE/MERGE plan (may be {@code null})
     * @param targetTableId the id of the target table whose own-column conjuncts are kept
     * @param exclusion a predicate marking slots to exclude (synthetic {@code $row_id} / metadata columns);
     *                  a conjunct referencing any excluded slot is dropped. May be {@code null} (no exclusion).
     * @return the neutral predicate over the target table's own columns, or empty when none survive
     */
    public static Optional<ConnectorPredicate> extract(Plan analyzedPlan, long targetTableId,
            Predicate<SlotReference> exclusion) {
        if (analyzedPlan == null) {
            return Optional.empty();
        }
        List<Expression> targetConjuncts = new ArrayList<>();
        collectTargetConjuncts(analyzedPlan, targetTableId, exclusion, targetConjuncts);
        if (targetConjuncts.isEmpty()) {
            return Optional.empty();
        }
        List<ConnectorExpression> converted = new ArrayList<>();
        for (Expression conjunct : targetConjuncts) {
            ConnectorExpression neutral = NereidsToConnectorExpressionConverter.convert(conjunct);
            if (neutral != null) {
                converted.add(neutral);
            }
        }
        if (converted.isEmpty()) {
            return Optional.empty();
        }
        ConnectorExpression combined = converted.size() == 1 ? converted.get(0) : new ConnectorAnd(converted);
        return Optional.of(new ConnectorPredicate(combined));
    }

    private static void collectTargetConjuncts(Plan plan, long targetTableId,
            Predicate<SlotReference> exclusion, List<Expression> output) {
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                if (isTargetOnlyPredicate(conjunct, targetTableId, exclusion)) {
                    output.add(conjunct);
                }
            }
        }
        for (Plan child : plan.children()) {
            collectTargetConjuncts(child, targetTableId, exclusion, output);
        }
    }

    /**
     * Strict, whole-plan variant of {@link #extract} used by the connector's whole-file "metadata delete" fast
     * path (Spark/StarRocks-style {@code DELETE} that drops data files instead of writing delete files): it
     * returns the statement's complete predicate, or empty when completeness cannot be proven.
     *
     * <p>The two extractors deliberately differ. {@link #extract} drops anything it cannot represent, which only
     * ever <i>widens</i> a conflict-detection filter and is therefore safe there. Here a dropped conjunct would
     * widen the <i>delete</i> and drop whole data files that still hold live rows, so this variant bails out
     * instead of guessing whenever a conjunct is not provably part of the statement predicate — anything that
     * references another table (join / subquery), any conjunct whose Nereids form is outside the neutral matrix,
     * or any plan node that could restrict/reshape rows without appearing as a {@link LogicalFilter}.</p>
     *
     * <p>Conjuncts over the excluded synthetic columns (the connector's {@code $row_id} metadata column) are
     * ignored rather than rejected: they are injected by the engine, not authored by the statement.</p>
     *
     * @param analyzedPlan  the analyzed DELETE plan
     * @param targetTableId the id of the DELETE target table
     * @param exclusion     a predicate marking synthetic (non-statement) slots, may be {@code null}
     * @return the complete predicate — a {@code true} literal when the statement has no {@code WHERE} clause at
     *         all — or empty when it cannot be extracted exactly
     */
    public static Optional<ConnectorExpression> extractCompletePredicate(Plan analyzedPlan, long targetTableId,
            Predicate<SlotReference> exclusion) {
        if (analyzedPlan == null) {
            return Optional.empty();
        }
        List<Expression> targetConjuncts = new ArrayList<>();
        if (!collectCompletePredicate(analyzedPlan, targetTableId, exclusion, targetConjuncts)) {
            return Optional.empty();
        }
        if (targetConjuncts.isEmpty()) {
            // No WHERE clause (or only engine-injected conjuncts): the statement deletes every row.
            return Optional.of(ConnectorLiteral.ofBoolean(true));
        }
        List<ConnectorExpression> converted = new ArrayList<>(targetConjuncts.size());
        for (Expression conjunct : targetConjuncts) {
            ConnectorExpression neutral = NereidsToConnectorExpressionConverter.convertStrict(conjunct);
            if (neutral == null) {
                return Optional.empty();
            }
            converted.add(neutral);
        }
        return Optional.of(converted.size() == 1 ? converted.get(0) : new ConnectorAnd(converted));
    }

    /**
     * Walks the analyzed plan along its single-input chain, collecting every statement conjunct, and returns
     * {@code false} as soon as the plan proves to be more than "target table → filter(s) → projection(s) → sink".
     * Only nodes that cannot change which rows of the target are produced are allowed; any branching node (a
     * join, set operation, ...) fails the walk, because its predicate would not be visible here.
     */
    private static boolean collectCompletePredicate(Plan plan, long targetTableId,
            Predicate<SlotReference> exclusion, List<Expression> output) {
        if (plan instanceof LogicalFilter) {
            for (Expression conjunct : ((LogicalFilter<?>) plan).getConjuncts()) {
                if (isTargetOnlyPredicate(conjunct, targetTableId, exclusion)) {
                    output.add(conjunct);
                } else if (!isSyntheticOnlyPredicate(conjunct, exclusion)) {
                    return false;
                }
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
        return collectCompletePredicate(children.get(0), targetTableId, exclusion, output);
    }

    /** Whether every slot of {@code predicate} is a synthetic (engine-injected) slot, so it can be ignored. */
    private static boolean isSyntheticOnlyPredicate(Expression predicate, Predicate<SlotReference> exclusion) {
        if (exclusion == null) {
            return false;
        }
        Set<Slot> slots = predicate.getInputSlots();
        if (slots.isEmpty()) {
            return false;
        }
        for (Slot slot : slots) {
            if (!(slot instanceof SlotReference) || !exclusion.test((SlotReference) slot)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTargetOnlyPredicate(Expression predicate, long targetTableId,
            Predicate<SlotReference> exclusion) {
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
            if (exclusion != null && exclusion.test(slotReference)) {
                return false;
            }
            Optional<TableIf> table = slotReference.getOriginalTable();
            if (!table.isPresent() || table.get().getId() != targetTableId) {
                return false;
            }
        }
        return true;
    }
}
