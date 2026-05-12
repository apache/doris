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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Removes partition-prunable conjuncts that were registered by {@link
 * org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition} but kept in
 * the logical plan during cascades. Doing the removal here, after
 * materialized-view rewrite has finished, ensures MV matching observes the
 * original predicates; otherwise the MV view-predicate may incorrectly cover
 * the dropped partition predicate and produce extra rows.
 */
public class PrunePartitionPredicate extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter = (PhysicalFilter<? extends Plan>) super.visit(filter, context);
        Plan child = filter.child();
        if (!(child instanceof PhysicalOlapScan)) {
            return filter;
        }
        PhysicalOlapScan scan = (PhysicalOlapScan) child;
        Optional<PartitionPrunablePredicate> entryOpt = scan.getPartitionPrunablePredicates();
        if (!entryOpt.isPresent()) {
            return filter;
        }
        boolean skipPrunePredicate = context.getConnectContext().getSessionVariable().skipPrunePredicate
                || context.getStatementContext().isDelete();
        if (skipPrunePredicate) {
            return filter;
        }
        Set<Long> scanPartitions = new HashSet<>(scan.getSelectedPartitionIds());
        Map<String, Slot> nameToOutputSlot = buildNameToSlotMap(scan);

        Set<Expression> remaining = new LinkedHashSet<>(filter.getConjuncts());
        boolean changed = false;
        PartitionPrunablePredicate entry = entryOpt.get();
        if (entry.getSelectedPartitionIds().containsAll(scanPartitions)) {
            Map<Expression, Expression> slotReplaceMap =
                    buildSlotReplaceMap(entry.getSnapshotPartitionSlots(), nameToOutputSlot);
            if (slotReplaceMap != null) {
                for (Expression conjunct : entry.getPrunableConjuncts()) {
                    Expression rewritten = slotReplaceMap.isEmpty()
                            ? conjunct : ExpressionUtils.replace(conjunct, slotReplaceMap);
                    if (remaining.remove(rewritten)) {
                        changed = true;
                    }
                }
            }
        }
        if (!changed) {
            return filter;
        }
        if (remaining.isEmpty()) {
            return scan;
        }
        return filter.withConjunctsAndChild(remaining, scan)
                .copyStatsAndGroupIdFrom((AbstractPhysicalPlan) filter);
    }

    private static Map<String, Slot> buildNameToSlotMap(PhysicalOlapScan scan) {
        OlapTable table = scan.getTable();
        List<Slot> slots = scan.getOutput();
        Map<String, Slot> map = new HashMap<>(slots.size());
        if (scan.getSelectedIndexId() == table.getBaseIndexId()) {
            for (Slot slot : slots) {
                map.put(slot.getName().toLowerCase(), slot);
            }
        } else {
            for (Slot slot : slots) {
                if (!(slot instanceof SlotReference)) {
                    continue;
                }
                SlotReference slotReference = (SlotReference) slot;
                Optional<Column> columnOptional = slotReference.getOriginalColumn();
                if (!columnOptional.isPresent()) {
                    continue;
                }
                Expr expr = columnOptional.get().getDefineExpr();
                if (!(expr instanceof SlotRef)) {
                    continue;
                }
                map.put(((SlotRef) expr).getColumnName().toLowerCase(), slot);
            }
        }
        return map;
    }

    /**
     * Map each recorded snapshot slot to the scan's current output slot of the
     * same column name. Returns null when any snapshot slot cannot be located,
     * so the caller can skip the entry.
     */
    private static Map<Expression, Expression> buildSlotReplaceMap(
            List<Slot> snapshotSlots, Map<String, Slot> nameToOutputSlot) {
        Map<Expression, Expression> replaceMap = new HashMap<>(snapshotSlots.size());
        for (Slot snapshot : snapshotSlots) {
            Slot current = nameToOutputSlot.get(snapshot.getName().toLowerCase());
            if (current == null) {
                return null;
            }
            if (!snapshot.equals(current)) {
                replaceMap.put(snapshot, current);
            }
        }
        return replaceMap;
    }
}
