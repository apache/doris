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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** One deferred physical source column and all final outputs backed by it. */
public final class DeferredColumnSpec {
    private final SourceColumnKey sourceColumnKey;
    private final SlotReference baseSlot;
    private final Column originalColumn;
    private final int baseColumnIndex;
    private final List<Slot> outputSlots;

    /** Create and validate one deferred source-column specification. */
    public DeferredColumnSpec(SourceColumnKey sourceColumnKey, SlotReference baseSlot,
            int baseColumnIndex, List<Slot> outputSlots) {
        this.sourceColumnKey = Objects.requireNonNull(sourceColumnKey, "sourceColumnKey must not be null");
        this.baseSlot = Objects.requireNonNull(baseSlot, "baseSlot must not be null");
        Preconditions.checkArgument(baseSlot.getOriginalColumn().isPresent(),
                "baseSlot must have an original column: %s", baseSlot);
        Preconditions.checkArgument(baseColumnIndex >= 0, "baseColumnIndex must not be negative");
        Preconditions.checkArgument(!outputSlots.isEmpty(), "outputSlots must not be empty");
        this.originalColumn = baseSlot.getOriginalColumn().get();
        Preconditions.checkArgument(sourceColumnKey.getColumnUniqueId() == originalColumn.getUniqueId()
                        && sourceColumnKey.getColumnName().equals(originalColumn.getName()),
                "sourceColumnKey does not match baseSlot original column: %s", sourceColumnKey);
        for (Slot outputSlot : outputSlots) {
            Preconditions.checkArgument(outputSlot instanceof SlotReference,
                    "deferred output must be a SlotReference: %s", outputSlot);
        }
        this.baseColumnIndex = baseColumnIndex;
        this.outputSlots = ImmutableList.copyOf(outputSlots);
    }

    /** Resolve the physical base-column index from original source metadata, never from an output alias. */
    public static DeferredColumnSpec from(Relation relation, SlotReference baseSlot, List<Slot> outputSlots) {
        Objects.requireNonNull(relation, "relation must not be null");
        Preconditions.checkArgument(baseSlot.getOriginalColumn().isPresent(),
                "baseSlot must have an original column: %s", baseSlot);
        TableIf table;
        if (relation instanceof CatalogRelation) {
            table = ((CatalogRelation) relation).getTable();
        } else {
            Preconditions.checkArgument(relation instanceof PhysicalTVFRelation,
                    "unsupported lazy source relation: %s", relation);
            table = ((PhysicalTVFRelation) relation).getFunction().getTable();
        }
        Column originalColumn = baseSlot.getOriginalColumn().get();
        int baseColumnIndex = table.getBaseColumnIdxByName(originalColumn.getName());
        return new DeferredColumnSpec(SourceColumnKey.from(relation, baseSlot), baseSlot,
                baseColumnIndex, outputSlots);
    }

    public SourceColumnKey getSourceColumnKey() {
        return sourceColumnKey;
    }

    public SlotReference getBaseSlot() {
        return baseSlot;
    }

    public Column getOriginalColumn() {
        return originalColumn;
    }

    public int getBaseColumnIndex() {
        return baseColumnIndex;
    }

    public List<Slot> getOutputSlots() {
        return outputSlots;
    }
}
