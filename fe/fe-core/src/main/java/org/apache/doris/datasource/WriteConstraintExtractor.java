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

package org.apache.doris.datasource;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

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
