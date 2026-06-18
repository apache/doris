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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Partition selection state for OLAP scans.
 */
public class OlapPartitionSelection extends PartitionSelection<Long> {
    private final List<Long> selectedPartitionIds;
    private final List<Long> manuallySpecifiedPartitions;

    public OlapPartitionSelection(List<Long> selectedPartitionIds, boolean partitionPruned,
            boolean hasPartitionConstraint, List<Long> manuallySpecifiedPartitions) {
        super(partitionPruned, hasPartitionConstraint);
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        this.manuallySpecifiedPartitions = ImmutableList.copyOf(manuallySpecifiedPartitions);
    }

    private OlapPartitionSelection(List<Long> selectedPartitionIds, boolean partitionPruned,
            boolean hasPartitionConstraint, List<Long> manuallySpecifiedPartitions,
            List<Slot> partitionSlots, Set<Expression> conjuncts) {
        super(partitionPruned, hasPartitionConstraint, partitionSlots, conjuncts);
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        this.manuallySpecifiedPartitions = ImmutableList.copyOf(manuallySpecifiedPartitions);
    }

    public List<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public List<Long> getManuallySpecifiedPartitions() {
        return manuallySpecifiedPartitions;
    }

    /**
     * Narrow current selected partitions. The recorded partition conjuncts remain valid because
     * the new partition ids must be a subset of the current selected partition ids.
     */
    public OlapPartitionSelection withNarrowedSelectedPartitionIds(List<Long> selectedPartitionIds) {
        Preconditions.checkArgument(ImmutableSet.copyOf(this.selectedPartitionIds).containsAll(selectedPartitionIds),
                "selectedPartitionIds must be a subset of current selectedPartitionIds");
        return new OlapPartitionSelection(selectedPartitionIds, partitionPruned, hasPartitionConstraint,
                manuallySpecifiedPartitions, appliedPartitionSlots, appliedPartitionConjuncts);
    }

    /**
     * Start a fresh partition prune result. Old recorded partition conjuncts are dropped; the prune
     * rule should record fresh conjuncts through withAppliedPartitionConjuncts.
     */
    public OlapPartitionSelection withPartitionPruneResult(List<Long> selectedPartitionIds,
            boolean hasPartitionConstraint) {
        return new OlapPartitionSelection(selectedPartitionIds, true, hasPartitionConstraint,
                manuallySpecifiedPartitions);
    }

    /**
     * Drop partition prune state and recorded applied partition conjuncts.
     */
    public OlapPartitionSelection resetPartitionPruneState() {
        return new OlapPartitionSelection(selectedPartitionIds, false, false, manuallySpecifiedPartitions);
    }

    public OlapPartitionSelection withAppliedPartitionConjuncts(List<Slot> partitionSlots,
            Set<Expression> conjuncts) {
        return new OlapPartitionSelection(selectedPartitionIds, partitionPruned, hasPartitionConstraint,
                manuallySpecifiedPartitions, partitionSlots, conjuncts);
    }

    /**
     * Returns the recorded prunable conjuncts rewritten to the scan's current output slots.
     */
    public Set<Expression> getAppliedPartitionConjuncts(OlapScan scan, List<Slot> output) {
        return rewriteAppliedPartitionConjuncts(buildNameToSlotMap(scan, output));
    }

    private static Map<String, Slot> buildNameToSlotMap(OlapScan scan, List<Slot> output) {
        OlapTable table = scan.getTable();
        Map<String, Slot> map = new HashMap<>(output.size());
        if (scan.getSelectedIndexId() == table.getBaseIndexId()) {
            for (Slot slot : output) {
                map.put(slot.getName().toLowerCase(), slot);
            }
        } else {
            for (Slot slot : output) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OlapPartitionSelection that = (OlapPartitionSelection) o;
        return selectedPartitionIds.equals(that.selectedPartitionIds)
                && manuallySpecifiedPartitions.equals(that.manuallySpecifiedPartitions)
                && partitionSelectionBaseEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectedPartitionIds, manuallySpecifiedPartitions, partitionSelectionBaseHashCode());
    }
}
