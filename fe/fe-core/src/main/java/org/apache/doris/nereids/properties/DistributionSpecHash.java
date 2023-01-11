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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Describe hash distribution.
 */
@Developing
public class DistributionSpecHash extends DistributionSpec {

    private final List<ExprId> orderedShuffledColumns;

    private final ShuffleType shuffleType;

    // below two attributes use for colocate join
    private final long tableId;

    private final Set<Long> partitionIds;

    // use for satisfied judge
    private final List<Set<ExprId>> equivalenceExprIds;

    private final Map<ExprId, Integer> exprIdToEquivalenceSet;

    /**
     * Use for no need set table related attributes.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType) {
        this(orderedShuffledColumns, shuffleType, -1L, Collections.emptySet());
    }

    /**
     * Use for merge two shuffle columns.
     */
    public DistributionSpecHash(List<ExprId> leftColumns, List<ExprId> rightColumns, ShuffleType shuffleType) {
        this(leftColumns, shuffleType, -1L, Collections.emptySet());
        Objects.requireNonNull(rightColumns);
        Preconditions.checkArgument(leftColumns.size() == rightColumns.size());
        int i = 0;
        Iterator<Set<ExprId>> iter = equivalenceExprIds.iterator();
        for (ExprId id : rightColumns) {
            exprIdToEquivalenceSet.put(id, i++);
            iter.next().add(id);
        }
    }

    /**
     * Normal constructor.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType,
            long tableId, Set<Long> partitionIds) {
        this.orderedShuffledColumns = Objects.requireNonNull(orderedShuffledColumns);
        this.shuffleType = Objects.requireNonNull(shuffleType);
        this.partitionIds = Objects.requireNonNull(partitionIds);
        this.tableId = tableId;
        equivalenceExprIds = Lists.newArrayListWithCapacity(orderedShuffledColumns.size());
        exprIdToEquivalenceSet = Maps.newHashMapWithExpectedSize(orderedShuffledColumns.size());
        int i = 0;
        for (ExprId id : orderedShuffledColumns) {
            exprIdToEquivalenceSet.put(id, i++);
            equivalenceExprIds.add(Sets.newHashSet(id));
        }
    }

    /**
     * Used in merge outside and put result into it.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType, long tableId,
            Set<Long> partitionIds, List<Set<ExprId>> equivalenceExprIds,
            Map<ExprId, Integer> exprIdToEquivalenceSet) {
        this.orderedShuffledColumns = Objects.requireNonNull(orderedShuffledColumns);
        this.shuffleType = Objects.requireNonNull(shuffleType);
        this.tableId = tableId;
        this.partitionIds = Objects.requireNonNull(partitionIds);
        this.equivalenceExprIds = Objects.requireNonNull(equivalenceExprIds);
        this.exprIdToEquivalenceSet = Objects.requireNonNull(exprIdToEquivalenceSet);
    }

    static DistributionSpecHash merge(DistributionSpecHash left, DistributionSpecHash right, ShuffleType shuffleType) {
        List<ExprId> orderedShuffledColumns = left.getOrderedShuffledColumns();
        List<Set<ExprId>> equivalenceExprIds = Lists.newArrayListWithCapacity(orderedShuffledColumns.size());
        for (int i = 0; i < orderedShuffledColumns.size(); i++) {
            Set<ExprId> equivalenceExprId = Sets.newHashSet();
            equivalenceExprId.addAll(left.getEquivalenceExprIds().get(i));
            equivalenceExprId.addAll(right.getEquivalenceExprIds().get(i));
            equivalenceExprIds.add(equivalenceExprId);
        }
        Map<ExprId, Integer> exprIdToEquivalenceSet = Maps.newHashMapWithExpectedSize(
                left.getExprIdToEquivalenceSet().size() + right.getExprIdToEquivalenceSet().size());
        exprIdToEquivalenceSet.putAll(left.getExprIdToEquivalenceSet());
        exprIdToEquivalenceSet.putAll(right.getExprIdToEquivalenceSet());
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType,
                left.getTableId(), left.getPartitionIds(), equivalenceExprIds, exprIdToEquivalenceSet);
    }

    static DistributionSpecHash merge(DistributionSpecHash left, DistributionSpecHash right) {
        return merge(left, right, left.getShuffleType());
    }

    public List<ExprId> getOrderedShuffledColumns() {
        return orderedShuffledColumns;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public long getTableId() {
        return tableId;
    }

    public Set<Long> getPartitionIds() {
        return partitionIds;
    }

    public List<Set<ExprId>> getEquivalenceExprIds() {
        return equivalenceExprIds;
    }

    public Map<ExprId, Integer> getExprIdToEquivalenceSet() {
        return exprIdToEquivalenceSet;
    }

    public Set<ExprId> getEquivalenceExprIdsOf(ExprId exprId) {
        if (exprIdToEquivalenceSet.containsKey(exprId)) {
            return equivalenceExprIds.get(exprIdToEquivalenceSet.get(exprId));
        }
        return new HashSet<>();
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        if (required instanceof DistributionSpecAny) {
            return true;
        }

        if (!(required instanceof DistributionSpecHash)) {
            return false;
        }

        DistributionSpecHash requiredHash = (DistributionSpecHash) required;

        if (this.orderedShuffledColumns.size() > requiredHash.orderedShuffledColumns.size()) {
            return false;
        }

        if (requiredHash.shuffleType == ShuffleType.NATURAL && this.shuffleType != ShuffleType.NATURAL) {
            // this shuffle type is not natural but require natural
            return false;
        }

        if (requiredHash.shuffleType == ShuffleType.AGGREGATE) {
            return containsSatisfy(requiredHash.getOrderedShuffledColumns());
        }

        // If the required property is from join and this property is not enforced, we only need to check to contain
        // And more checking is in ChildrenPropertiesRegulator
        if (requiredHash.shuffleType == shuffleType.JOIN && this.shuffleType != shuffleType.ENFORCED) {
            return containsSatisfy(requiredHash.getOrderedShuffledColumns());
        }

        return equalsSatisfy(requiredHash.getOrderedShuffledColumns());
    }

    private boolean containsSatisfy(List<ExprId> required) {
        BitSet containsBit = new BitSet(orderedShuffledColumns.size());
        required.forEach(e -> {
            if (exprIdToEquivalenceSet.containsKey(e)) {
                containsBit.set(exprIdToEquivalenceSet.get(e));
            }
        });
        return containsBit.nextClearBit(0) >= orderedShuffledColumns.size();
    }

    private boolean equalsSatisfy(List<ExprId> required) {
        if (equivalenceExprIds.size() != required.size()) {
            return false;
        }
        for (int i = 0; i < required.size(); i++) {
            if (!equivalenceExprIds.get(i).contains(required.get(i))) {
                return false;
            }
        }
        return true;
    }

    public DistributionSpecHash withShuffleType(ShuffleType shuffleType) {
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType, tableId, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    /**
     * generate a new DistributionSpec after projection.
     */
    public DistributionSpec project(Map<ExprId, ExprId> projections, Set<ExprId> obstructions) {
        List<ExprId> orderedShuffledColumns = Lists.newArrayList();
        List<Set<ExprId>> equivalenceExprIds = Lists.newArrayList();
        Map<ExprId, Integer> exprIdToEquivalenceSet = Maps.newHashMap();
        for (ExprId shuffledColumn : this.orderedShuffledColumns) {
            if (obstructions.contains(shuffledColumn)) {
                return DistributionSpecAny.INSTANCE;
            }
            orderedShuffledColumns.add(projections.getOrDefault(shuffledColumn, shuffledColumn));
        }
        for (Set<ExprId> equivalenceSet : this.equivalenceExprIds) {
            Set<ExprId> projectionEquivalenceSet = Sets.newHashSet();
            for (ExprId equivalence : equivalenceSet) {
                if (obstructions.contains(equivalence)) {
                    return DistributionSpecAny.INSTANCE;
                }
                projectionEquivalenceSet.add(projections.getOrDefault(equivalence, equivalence));
            }
            equivalenceExprIds.add(projectionEquivalenceSet);
        }
        for (Map.Entry<ExprId, Integer> exprIdSetKV : this.exprIdToEquivalenceSet.entrySet()) {
            if (obstructions.contains(exprIdSetKV.getKey())) {
                return DistributionSpecAny.INSTANCE;
            }
            if (projections.containsKey(exprIdSetKV.getKey())) {
                exprIdToEquivalenceSet.put(projections.get(exprIdSetKV.getKey()), exprIdSetKV.getValue());
            } else {
                exprIdToEquivalenceSet.put(exprIdSetKV.getKey(), exprIdSetKV.getValue());
            }
        }
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType, tableId, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        DistributionSpecHash that = (DistributionSpecHash) o;
        return shuffleType == that.shuffleType && orderedShuffledColumns.equals(that.orderedShuffledColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffleType, orderedShuffledColumns);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("DistributionSpecHash",
                "orderedShuffledColumns", orderedShuffledColumns,
                "shuffleType", shuffleType,
                "tableId", tableId,
                "partitionIds", partitionIds,
                "equivalenceExprIds", equivalenceExprIds,
                "exprIdToEquivalenceSet", exprIdToEquivalenceSet);
    }

    /**
     * Enums for concrete shuffle type.
     */
    public enum ShuffleType {
        // require, need to satisfy the distribution spec by aggregation way.
        AGGREGATE,
        // require, need to satisfy the distribution spec by join way.
        JOIN,
        // output, for olap scan node and colocate join
        NATURAL,
        // output, for all join except colocate join
        BUCKETED,
        // output, all distribute enforce
        ENFORCED,
    }

}
