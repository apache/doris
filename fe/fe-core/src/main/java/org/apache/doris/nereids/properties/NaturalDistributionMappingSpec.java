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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Describes storage bucket locality that remains valid after distribution-key slots are projected out.
 *
 * <p>This property is only a proof artifact for mapping-based colocate join. It must never be used to
 * build an Exchange or a bucket-shuffle requirement because some distribution positions may have no
 * materialized output slot.
 */
public class NaturalDistributionMappingSpec {
    private final long tableId;
    private final long selectedIndexId;
    private final Set<Long> partitionIds;
    private final int distributionKeyCount;
    private final Map<ExprId, Integer> visibleDistributionExprToIndex;
    private final List<DistributionMapping> distributionMappings;

    /** Constructor. */
    public NaturalDistributionMappingSpec(long tableId, long selectedIndexId, Set<Long> partitionIds,
            int distributionKeyCount, Map<ExprId, Integer> visibleDistributionExprToIndex,
            List<DistributionMapping> distributionMappings) {
        Preconditions.checkArgument(distributionKeyCount > 0, "distributionKeyCount must be positive");
        this.tableId = tableId;
        this.selectedIndexId = selectedIndexId;
        this.partitionIds = ImmutableSet.copyOf(partitionIds);
        this.distributionKeyCount = distributionKeyCount;
        this.visibleDistributionExprToIndex = ImmutableMap.copyOf(visibleDistributionExprToIndex);
        this.distributionMappings = ImmutableList.copyOf(distributionMappings);
    }

    public long getTableId() {
        return tableId;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public Set<Long> getPartitionIds() {
        return partitionIds;
    }

    public int getDistributionKeyCount() {
        return distributionKeyCount;
    }

    public Map<ExprId, Integer> getVisibleDistributionExprToIndex() {
        return visibleDistributionExprToIndex;
    }

    public List<DistributionMapping> getDistributionMappings() {
        return distributionMappings;
    }

    /** Return whether direct slots and complete mapping determinants cover every bucket position. */
    public boolean distributionKeysCoveredByDirectOrMapping(Set<ExprId> exprIds) {
        BitSet coveredIndices = new BitSet(distributionKeyCount);
        for (ExprId exprId : exprIds) {
            Integer index = visibleDistributionExprToIndex.get(exprId);
            if (index != null) {
                coveredIndices.set(index);
            }
        }
        for (DistributionMapping mapping : distributionMappings) {
            if (exprIds.containsAll(mapping.getDeterminantExprIds())) {
                mapping.getTargetDistributionIndices().forEach(coveredIndices::set);
            }
        }
        return coveredIndices.nextClearBit(0) >= distributionKeyCount;
    }

    /** Return whether direct slots and mapping determinants cover every underlying bucket position. */
    public boolean satisfy(List<ExprId> requiredExprIds) {
        return distributionKeysCoveredByDirectOrMapping(ImmutableSet.copyOf(requiredExprIds));
    }

    /**
     * Remap visible distribution slots and determinants through a projection.
     * Missing slots are intentionally omitted while the underlying bucket positions remain unchanged.
     */
    public Optional<NaturalDistributionMappingSpec> project(Map<ExprId, ExprId> projections) {
        ImmutableMap.Builder<ExprId, Integer> visibleDistributionExprs = ImmutableMap.builder();
        for (Map.Entry<ExprId, Integer> entry : visibleDistributionExprToIndex.entrySet()) {
            ExprId projected = projections.get(entry.getKey());
            if (projected != null) {
                visibleDistributionExprs.put(projected, entry.getValue());
            }
        }

        ImmutableList.Builder<DistributionMapping> projectedMappings = ImmutableList.builder();
        for (DistributionMapping mapping : distributionMappings) {
            mapping.project(projections).ifPresent(projectedMappings::add);
        }
        List<DistributionMapping> mappings = projectedMappings.build();
        if (mappings.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new NaturalDistributionMappingSpec(tableId, selectedIndexId, partitionIds,
                distributionKeyCount, visibleDistributionExprs.buildKeepingLast(), mappings));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NaturalDistributionMappingSpec)) {
            return false;
        }
        NaturalDistributionMappingSpec that = (NaturalDistributionMappingSpec) o;
        return tableId == that.tableId
                && selectedIndexId == that.selectedIndexId
                && distributionKeyCount == that.distributionKeyCount
                && partitionIds.equals(that.partitionIds)
                && visibleDistributionExprToIndex.equals(that.visibleDistributionExprToIndex)
                && distributionMappings.equals(that.distributionMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, selectedIndexId, partitionIds, distributionKeyCount,
                visibleDistributionExprToIndex, distributionMappings);
    }

    @Override
    public String toString() {
        return "NaturalDistributionMappingSpec{"
                + "tableId=" + tableId
                + ", selectedIndexId=" + selectedIndexId
                + ", partitionIds=" + partitionIds
                + ", distributionKeyCount=" + distributionKeyCount
                + ", visibleDistributionExprToIndex=" + visibleDistributionExprToIndex
                + ", distributionMappings=" + distributionMappings
                + '}';
    }
}
