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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Records that, on the scan whose partition list equals {@link
 * #selectedPartitionIds}, the {@link #prunableConjuncts} are guaranteed to
 * evaluate to TRUE for every surviving row.
 *
 * <p>The predicate is registered by {@link
 * org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition} but kept in
 * the logical filter during cascades. The actual removal happens later in
 * {@link org.apache.doris.nereids.processor.post.PrunePartitionPredicate} so
 * that materialized-view rewrite still sees the original predicates. Keeping
 * the predicate in the plan avoids the wrong-result problem in which the MV
 * view-predicate happens to cover the remaining conjuncts after the partition
 * predicate has been silently dropped.
 *
 * <p>The predicate lives on the scan itself (see {@code LogicalOlapScan} and
 * {@code PhysicalOlapScan}) so we no longer need to match it back to its scan
 * via a table identifier. {@link #partitionSlots} always belong to the scan
 * state carrying this proof. A scan transformation that changes its output
 * slots must explicitly call {@link #rebindSlots(Map)} or discard the
 * proof, so statistics derivation and physical post-processing never need to
 * infer slot lineage on the read path.
 */
public class PartitionPrunablePredicate {
    private final Set<Long> selectedPartitionIds;
    private final List<Slot> partitionSlots;
    private final Set<Expression> prunableConjuncts;

    public PartitionPrunablePredicate(Set<Long> selectedPartitionIds,
            List<Slot> partitionSlots,
            Set<Expression> prunableConjuncts) {
        this.selectedPartitionIds = ImmutableSet.copyOf(selectedPartitionIds);
        this.partitionSlots = ImmutableList.copyOf(partitionSlots);
        this.prunableConjuncts = ImmutableSet.copyOf(prunableConjuncts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionPrunablePredicate that = (PartitionPrunablePredicate) o;
        return selectedPartitionIds.equals(that.selectedPartitionIds)
                && partitionSlots.equals(that.partitionSlots)
                && prunableConjuncts.equals(that.prunableConjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectedPartitionIds, partitionSlots, prunableConjuncts);
    }

    public List<Slot> getPartitionSlots() {
        return partitionSlots;
    }

    public boolean covers(List<Long> currentSelectedPartitionIds) {
        return selectedPartitionIds.containsAll(currentSelectedPartitionIds);
    }

    public Set<Expression> getPrunableConjuncts() {
        return prunableConjuncts;
    }

    /**
     * Rebind this proof to slots from another scan state of the same table. The caller owns the scan-specific
     * lineage rules and must provide a mapping for every recorded partition slot.
     */
    public PartitionPrunablePredicate rebindSlots(Map<Slot, Slot> slotMapping) {
        Map<Expression, Expression> replacements = new HashMap<>(partitionSlots.size());
        ImmutableList.Builder<Slot> reboundSlots =
                ImmutableList.builderWithExpectedSize(partitionSlots.size());
        for (Slot partitionSlot : partitionSlots) {
            Slot reboundSlot = Objects.requireNonNull(slotMapping.get(partitionSlot),
                    "missing rebound slot for partition slot: " + partitionSlot);
            reboundSlots.add(reboundSlot);
            if (!partitionSlot.equals(reboundSlot)) {
                replacements.put(partitionSlot, reboundSlot);
            }
        }
        if (replacements.isEmpty()) {
            return this;
        }
        ImmutableSet.Builder<Expression> reboundConjuncts =
                ImmutableSet.builderWithExpectedSize(prunableConjuncts.size());
        for (Expression conjunct : prunableConjuncts) {
            reboundConjuncts.add(ExpressionUtils.replace(conjunct, replacements));
        }
        return new PartitionPrunablePredicate(selectedPartitionIds, reboundSlots.build(), reboundConjuncts.build());
    }
}
