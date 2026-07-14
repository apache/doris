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

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Single immutable source of truth for one TopN lazy-materialization rewrite. */
public final class TopNLazyMaterializationSpec {
    private final List<Slot> materializedSlots;
    private final List<Slot> materializeInput;
    private final List<LazySourceSpec> sources;
    private final Map<RelationId, LazySourceSpec> sourceByRelationId;
    private final List<Slot> materializeOutput;

    /** Create and validate the complete immutable specification for one materialization node. */
    public TopNLazyMaterializationSpec(List<Slot> userVisibleOutput, List<Slot> materializedSlots,
            List<Slot> materializeInput, List<LazySourceSpec> sources) {
        Preconditions.checkArgument(!sources.isEmpty(), "sources must not be empty");
        this.materializedSlots = ImmutableList.copyOf(materializedSlots);
        this.materializeInput = ImmutableList.copyOf(materializeInput);
        this.sources = ImmutableList.sortedCopyOf((left, right) -> Integer.compare(
                left.getRelation().getRelationId().asInt(), right.getRelation().getRelationId().asInt()), sources);

        Map<RelationId, LazySourceSpec> sourcesById = new LinkedHashMap<>();
        Set<SourceColumnKey> sourceColumnKeys = new HashSet<>();
        ImmutableList.Builder<Slot> outputBuilder = ImmutableList.builder();
        outputBuilder.addAll(materializedSlots);
        for (LazySourceSpec source : this.sources) {
            RelationId relationId = source.getRelation().getRelationId();
            Preconditions.checkArgument(sourcesById.put(relationId, source) == null,
                    "duplicate lazy source relation: %s", relationId);
            Preconditions.checkArgument(materializeInput.contains(source.getRowIdSlot()),
                    "materializeInput is missing row-id: %s", source.getRowIdSlot());
            for (DeferredColumnSpec deferredColumn : source.getDeferredColumns()) {
                Preconditions.checkArgument(sourceColumnKeys.add(deferredColumn.getSourceColumnKey()),
                        "duplicate source column across sources: %s", deferredColumn.getSourceColumnKey());
                for (Slot outputSlot : deferredColumn.getOutputSlots()) {
                    outputBuilder.add(restoreSourceMetadata(outputSlot, deferredColumn.getBaseSlot()));
                }
            }
        }
        this.sourceByRelationId = ImmutableMap.copyOf(sourcesById);
        this.materializeOutput = outputBuilder.build();
        Preconditions.checkArgument(materializeInput.size() == materializedSlots.size() + sources.size(),
                "materializeInput must contain materialized slots followed by one row-id per source");
        Preconditions.checkArgument(materializeInput.subList(0, materializedSlots.size()).equals(materializedSlots),
                "materializeInput must start with materializedSlots in the same order");
        for (int i = 0; i < this.sources.size(); i++) {
            Preconditions.checkArgument(materializeInput.get(materializedSlots.size() + i)
                            .equals(this.sources.get(i).getRowIdSlot()),
                    "materializeInput row-id order must match source order");
        }
        Preconditions.checkArgument(materializeOutput.size() == userVisibleOutput.size(),
                "materialize output size must match user-visible output size");
        Preconditions.checkArgument(new HashSet<>(materializeOutput).equals(new HashSet<>(userVisibleOutput)),
                "materialize output slots must match user-visible output slots");
    }

    /** Build the complete rewrite spec from analyzed lazy outputs and deterministic row-id assignments. */
    public static TopNLazyMaterializationSpec create(List<Slot> userVisibleOutput,
            List<Slot> materializedSlots, Map<RelationId, SlotReference> rowIdsByRelationId,
            Map<Slot, MaterializeSource> materializeSources) {
        Map<RelationId, Relation> relationsById = new TreeMap<>();
        Map<RelationId, Map<SourceColumnKey, List<Slot>>> outputsByRelation = new TreeMap<>();
        Map<SourceColumnKey, SlotReference> baseSlotsBySource = new TreeMap<>();
        for (Map.Entry<Slot, MaterializeSource> entry : materializeSources.entrySet()) {
            MaterializeSource source = entry.getValue();
            RelationId relationId = source.getRelation().getRelationId();
            relationsById.putIfAbsent(relationId, source.getRelation());
            outputsByRelation.computeIfAbsent(relationId, ignored -> new TreeMap<>())
                    .computeIfAbsent(source.getSourceColumnKey(), ignored -> new ArrayList<>())
                    .add(entry.getKey());
            baseSlotsBySource.putIfAbsent(source.getSourceColumnKey(), source.getBaseSlot());
        }

        ImmutableList.Builder<LazySourceSpec> sources = ImmutableList.builder();
        ImmutableList.Builder<Slot> materializeInput = ImmutableList.builder();
        materializeInput.addAll(materializedSlots);
        for (Map.Entry<RelationId, Relation> relationEntry : relationsById.entrySet()) {
            RelationId relationId = relationEntry.getKey();
            SlotReference rowId = Preconditions.checkNotNull(rowIdsByRelationId.get(relationId),
                    "missing row-id for relation: %s", relationId);
            materializeInput.add(rowId);
            ImmutableList.Builder<DeferredColumnSpec> deferredColumns = ImmutableList.builder();
            for (Map.Entry<SourceColumnKey, List<Slot>> sourceEntry
                    : outputsByRelation.get(relationId).entrySet()) {
                deferredColumns.add(DeferredColumnSpec.from(relationEntry.getValue(),
                        baseSlotsBySource.get(sourceEntry.getKey()), sourceEntry.getValue()));
            }
            sources.add(new LazySourceSpec(relationEntry.getValue(), rowId, deferredColumns.build()));
        }
        return new TopNLazyMaterializationSpec(userVisibleOutput, materializedSlots,
                materializeInput.build(), sources.build());
    }

    private Slot restoreSourceMetadata(Slot outputSlot, SlotReference baseSlot) {
        SlotReference restored = ((SlotReference) outputSlot).withColumn(baseSlot.getOriginalColumn().get());
        if (baseSlot.getAllAccessPaths().isPresent()) {
            List<ColumnAccessPath> all = baseSlot.getAllAccessPaths().get();
            restored = restored.withAccessPaths(all,
                    baseSlot.getPredicateAccessPaths().orElse(ImmutableList.of()),
                    baseSlot.getDisplayAllAccessPaths().orElse(all),
                    baseSlot.getDisplayPredicateAccessPaths().orElse(ImmutableList.of()));
        }
        return restored;
    }

    public List<Slot> getMaterializedSlots() {
        return materializedSlots;
    }

    public List<Slot> getMaterializeInput() {
        return materializeInput;
    }

    public List<LazySourceSpec> getSources() {
        return sources;
    }

    public Map<RelationId, LazySourceSpec> getSourceByRelationId() {
        return sourceByRelationId;
    }

    public List<Slot> getMaterializeOutput() {
        return materializeOutput;
    }
}
