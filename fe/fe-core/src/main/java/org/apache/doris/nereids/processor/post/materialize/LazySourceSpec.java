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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.Relation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable lazy-fetch specification for one relation. */
public final class LazySourceSpec {
    private final Relation relation;
    private final SlotReference rowIdSlot;
    private final List<DeferredColumnSpec> deferredColumns;

    /** Create and validate an ordered lazy source specification. */
    public LazySourceSpec(Relation relation, SlotReference rowIdSlot,
            List<DeferredColumnSpec> deferredColumns) {
        this.relation = Objects.requireNonNull(relation, "relation must not be null");
        this.rowIdSlot = Objects.requireNonNull(rowIdSlot, "rowIdSlot must not be null");
        Preconditions.checkArgument(!deferredColumns.isEmpty(), "deferredColumns must not be empty");
        this.deferredColumns = ImmutableList.sortedCopyOf(
                (left, right) -> left.getSourceColumnKey().compareTo(right.getSourceColumnKey()), deferredColumns);
        Set<SourceColumnKey> keys = new HashSet<>();
        for (DeferredColumnSpec deferredColumn : this.deferredColumns) {
            Preconditions.checkArgument(deferredColumn.getSourceColumnKey().getRelationId()
                            .equals(relation.getRelationId()),
                    "deferred column does not belong to relation: %s", deferredColumn.getSourceColumnKey());
            Preconditions.checkArgument(keys.add(deferredColumn.getSourceColumnKey()),
                    "duplicate deferred source column: %s", deferredColumn.getSourceColumnKey());
        }
    }

    public Relation getRelation() {
        return relation;
    }

    public SlotReference getRowIdSlot() {
        return rowIdSlot;
    }

    /**
     * Return the non-null row-id produced by the source scan.
     *
     * <p>The materialization input may use the nullable variant of this slot after
     * passing through the null-supplying side of an outer join. The scan itself
     * must always produce the non-null physical row-id column.</p>
     */
    public SlotReference getScanRowIdSlot() {
        return rowIdSlot.nullable() ? (SlotReference) rowIdSlot.withNullable(false) : rowIdSlot;
    }

    public List<DeferredColumnSpec> getDeferredColumns() {
        return deferredColumns;
    }

    public List<Slot> getLazyOutputSlots() {
        ImmutableList.Builder<Slot> outputs = ImmutableList.builder();
        for (DeferredColumnSpec deferredColumn : deferredColumns) {
            outputs.addAll(deferredColumn.getOutputSlots());
        }
        return outputs.build();
    }
}
