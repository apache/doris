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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.processor.post.materialize.MaterializationAnalysisResult.NotApplicableReason;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Orchestrates one lineage pass and one demand pass for a candidate physical TopN. */
public final class TopNLazyMaterializationAnalyzer {
    private final SlotLineageAnalyzer lineageAnalyzer = new SlotLineageAnalyzer();
    private final SlotDemandAnalyzer demandAnalyzer = new SlotDemandAnalyzer();

    /** Analyze one physical TopN and return either an immutable decision or a structured skip reason. */
    public MaterializationAnalysisResult<TopNLazyAnalysis> analyze(
            PhysicalTopN<? extends Plan> topN, List<Slot> effectiveOutput) {
        Map<Slot, MaterializeSource> candidates = new LinkedHashMap<>();
        List<Slot> materializedSlots = new ArrayList<>();
        Set<Slot> slotsToResolve = new LinkedHashSet<>(effectiveOutput);
        collectInputSlots(topN, slotsToResolve);
        Map<Slot, MaterializationAnalysisResult<OutputLineage>> lineages =
                lineageAnalyzer.analyze(topN.child(), ImmutableSet.copyOf(slotsToResolve).asList());

        for (Slot outputSlot : effectiveOutput) {
            if (!(outputSlot instanceof SlotReference)) {
                materializedSlots.add(outputSlot);
                continue;
            }
            MaterializationAnalysisResult<OutputLineage> lineageResult = lineages.get(outputSlot);
            if (!lineageResult.isApplicable() || !lineageResult.getValue().getSource().isPresent()) {
                materializedSlots.add(outputSlot);
                continue;
            }
            candidates.put(outputSlot, lineageResult.getValue().getSource().get());
        }

        Set<Slot> requiredEagerSlots = new LinkedHashSet<>(
                demandAnalyzer.analyzeRequiredEagerSlots(topN));
        collectRequiredRelationSlots(topN, requiredEagerSlots);
        Set<SourceColumnKey> eagerSourceColumns = new LinkedHashSet<>();
        Set<Slot> eagerSlots = new LinkedHashSet<>(requiredEagerSlots);
        eagerSlots.addAll(materializedSlots);
        for (Slot eagerSlot : eagerSlots) {
            MaterializationAnalysisResult<OutputLineage> lineage = lineages.get(eagerSlot);
            if (lineage != null && lineage.isApplicable() && lineage.getValue().getSource().isPresent()) {
                eagerSourceColumns.add(lineage.getValue().getSource().get().getSourceColumnKey());
            }
        }
        removeConflictingCandidates(candidates, requiredEagerSlots,
                ImmutableSet.copyOf(materializedSlots), eagerSourceColumns);
        for (Slot outputSlot : effectiveOutput) {
            if (!candidates.containsKey(outputSlot) && !materializedSlots.contains(outputSlot)) {
                materializedSlots.add(outputSlot);
            }
        }
        if (candidates.isEmpty()) {
            return MaterializationAnalysisResult.notApplicable(
                    NotApplicableReason.NO_DEFERRED_OUTPUT, topN.shapeInfo());
        }

        return MaterializationAnalysisResult.applicable(
                new TopNLazyAnalysis(materializedSlots, candidates));
    }

    static void removeConflictingCandidates(Map<Slot, MaterializeSource> candidates,
            Set<Slot> requiredEagerSlots, Set<Slot> materializedSlots) {
        removeConflictingCandidates(candidates, requiredEagerSlots, materializedSlots, new LinkedHashSet<>());
    }

    private static void removeConflictingCandidates(Map<Slot, MaterializeSource> candidates,
            Set<Slot> requiredEagerSlots, Set<Slot> materializedSlots,
            Set<SourceColumnKey> eagerSourceColumns) {
        // Fetch is scheduled per physical source column. If any alias of a column is needed eagerly,
        // every output backed by the same column must remain eager as well.
        for (Map.Entry<Slot, MaterializeSource> entry : candidates.entrySet()) {
            if (requiredEagerSlots.contains(entry.getKey())
                    || requiredEagerSlots.contains(entry.getValue().getBaseSlot())
                    || materializedSlots.contains(entry.getKey())
                    || materializedSlots.contains(entry.getValue().getBaseSlot())) {
                eagerSourceColumns.add(entry.getValue().getSourceColumnKey());
            }
        }
        candidates.entrySet().removeIf(
                entry -> eagerSourceColumns.contains(entry.getValue().getSourceColumnKey()));
    }

    private void collectInputSlots(Plan plan, Set<Slot> slots) {
        slots.addAll(plan.getInputSlots());
        for (Plan child : plan.children()) {
            collectInputSlots(child, slots);
        }
    }

    private void collectRequiredRelationSlots(Plan plan, Set<Slot> requiredSlots) {
        if (plan instanceof PhysicalCatalogRelation) {
            collectRequiredRelationSlots((PhysicalCatalogRelation) plan, requiredSlots);
        }
        for (Plan child : plan.children()) {
            collectRequiredRelationSlots(child, requiredSlots);
        }
    }

    private void collectRequiredRelationSlots(PhysicalCatalogRelation relation, Set<Slot> requiredSlots) {
        if (relation.getTable() instanceof OlapTable) {
            OlapTable table = (OlapTable) relation.getTable();
            if (KeysType.UNIQUE_KEYS.equals(table.getKeysType())
                    && !table.getTableProperty().getEnableUniqueKeyMergeOnWrite()
                    || KeysType.AGG_KEYS.equals(table.getKeysType())
                    || KeysType.PRIMARY_KEYS.equals(table.getKeysType())) {
                for (Slot slot : relation.getOutput()) {
                    SlotReference slotReference = (SlotReference) slot;
                    if (slotReference.getOriginalColumn().isPresent()
                            && slotReference.getOriginalColumn().get().isKey()) {
                        requiredSlots.add(slotReference);
                    }
                }
            }
        }
        for (Slot slot : relation.getOutput()) {
            if (slot instanceof SlotReference && !((SlotReference) slot).getOriginalColumn().isPresent()) {
                requiredSlots.addAll(relation.getOutputSet());
                return;
            }
        }
    }
}
