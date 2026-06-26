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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Partition selection state for external file scans.
 */
public class ExternalPartitionSelection extends PartitionSelection<String> {
    // NOT_PRUNED means the Nereids planner does not handle the partition pruning.
    // This can be treated as the initial value of ExternalPartitionSelection.
    // Or used to indicate that the partition pruning is not processed.
    public static final ExternalPartitionSelection NOT_PRUNED =
            new ExternalPartitionSelection(0, ImmutableMap.of(), false, false);

    /**
     * total partition number before pruning.
     */
    public final long totalPartitionNum;

    /**
     * partition name -> partition item
     */
    public final Map<String, PartitionItem> selectedPartitionItems;

    /**
     * Constructor for ExternalPartitionSelection.
     */
    public ExternalPartitionSelection(long totalPartitionNum, Map<String, PartitionItem> selectedPartitionItems,
            boolean partitionPruned, boolean hasPartitionConstraint) {
        super(partitionPruned, hasPartitionConstraint);
        this.totalPartitionNum = totalPartitionNum;
        this.selectedPartitionItems = ImmutableMap.copyOf(Objects.requireNonNull(selectedPartitionItems,
                "selectedPartitionItems is null"));
    }

    /**
     * Constructor for ExternalPartitionSelection.
     */
    public ExternalPartitionSelection(long totalPartitionNum, Map<String, PartitionItem> selectedPartitionItems,
            boolean partitionPruned, boolean hasPartitionConstraint, List<Slot> partitionSlots,
            Set<Expression> conjuncts) {
        super(partitionPruned, hasPartitionConstraint, partitionSlots, conjuncts);
        this.totalPartitionNum = totalPartitionNum;
        this.selectedPartitionItems = ImmutableMap.copyOf(Objects.requireNonNull(selectedPartitionItems,
                "selectedPartitionItems is null"));
    }

    public boolean isPruned() {
        return partitionPruned && selectedPartitionItems.size() < totalPartitionNum;
    }

    /**
     * Returns partition conjuncts that have already been applied to the selected partition row count.
     */
    public Set<Expression> getAppliedPartitionConjuncts(List<Slot> output) {
        return rewriteAppliedPartitionConjuncts(buildNameToSlotMap(output));
    }

    private static Map<String, Slot> buildNameToSlotMap(List<Slot> output) {
        Map<String, Slot> map = new HashMap<>(output.size());
        for (Slot slot : output) {
            map.put(slot.getName().toLowerCase(), slot);
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
        ExternalPartitionSelection that = (ExternalPartitionSelection) o;
        return totalPartitionNum == that.totalPartitionNum
                && Objects.equals(selectedPartitionItems.keySet(), that.selectedPartitionItems.keySet())
                && partitionSelectionBaseEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalPartitionNum, selectedPartitionItems.keySet(), partitionSelectionBaseHashCode());
    }
}
