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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Partition selection carried by scan plans.
 */
public abstract class PartitionSelection<P> {
    /**
     * true means the partition prune rule has processed this scan.
     */
    public final boolean partitionPruned;

    /**
     * true means this scan's selected partitions are constrained by a partition predicate
     * or explicit partition/tablet selection.
     */
    public final boolean hasPartitionConstraint;

    protected final List<Slot> appliedPartitionSlots;
    protected final Set<Expression> appliedPartitionConjuncts;

    protected PartitionSelection(boolean partitionPruned, boolean hasPartitionConstraint) {
        this.partitionPruned = partitionPruned;
        this.hasPartitionConstraint = hasPartitionConstraint;
        this.appliedPartitionSlots = ImmutableList.of();
        this.appliedPartitionConjuncts = ImmutableSet.of();
    }

    protected PartitionSelection(boolean partitionPruned, boolean hasPartitionConstraint, List<Slot> partitionSlots,
            Set<Expression> conjuncts) {
        Objects.requireNonNull(partitionSlots, "partitionSlots is null");
        Objects.requireNonNull(conjuncts, "conjuncts is null");
        Preconditions.checkArgument(partitionPruned || (partitionSlots.isEmpty() && conjuncts.isEmpty()),
                "recorded applied partition conjuncts require partition prune state");
        this.partitionPruned = partitionPruned;
        this.hasPartitionConstraint = hasPartitionConstraint;
        this.appliedPartitionSlots = ImmutableList.copyOf(partitionSlots);
        this.appliedPartitionConjuncts = ImmutableSet.copyOf(conjuncts);
    }

    /**
     * Returns partition conjuncts that have already been applied, rewritten by the given slot map.
     */
    protected Set<Expression> rewriteAppliedPartitionConjuncts(Map<String, Slot> nameToOutputSlot) {
        Map<Expression, Expression> slotReplaceMap = buildSlotReplaceMap(nameToOutputSlot);
        ImmutableSet.Builder<Expression> rewrittenConjuncts =
                ImmutableSet.builderWithExpectedSize(appliedPartitionConjuncts.size());
        for (Expression conjunct : appliedPartitionConjuncts) {
            rewrittenConjuncts.add(slotReplaceMap.isEmpty()
                    ? conjunct : ExpressionUtils.replace(conjunct, slotReplaceMap));
        }
        return rewrittenConjuncts.build();
    }

    /**
     * Map each recorded partition slot to the current output slot of the same column name.
     */
    private Map<Expression, Expression> buildSlotReplaceMap(Map<String, Slot> nameToOutputSlot) {
        Map<Expression, Expression> replaceMap = new HashMap<>(appliedPartitionSlots.size());
        for (Slot snapshot : appliedPartitionSlots) {
            Slot current = nameToOutputSlot.get(snapshot.getName().toLowerCase());
            Preconditions.checkState(current != null,
                    "Can not find output slot for applied partition slot: %s", snapshot.getName());
            if (!snapshot.equals(current)) {
                replaceMap.put(snapshot, current);
            }
        }
        return replaceMap;
    }

    protected boolean partitionSelectionBaseEquals(PartitionSelection<?> that) {
        return partitionPruned == that.partitionPruned
                && hasPartitionConstraint == that.hasPartitionConstraint
                && Objects.equals(appliedPartitionSlots, that.appliedPartitionSlots)
                && Objects.equals(appliedPartitionConjuncts, that.appliedPartitionConjuncts);
    }

    protected int partitionSelectionBaseHashCode() {
        return Objects.hash(partitionPruned, hasPartitionConstraint, appliedPartitionSlots,
                appliedPartitionConjuncts);
    }
}
