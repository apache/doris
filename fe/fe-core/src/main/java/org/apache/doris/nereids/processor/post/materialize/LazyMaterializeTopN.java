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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * post rule to do lazy materialize
 */
public class LazyMaterializeTopN extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalTopN(PhysicalTopN topN, CascadesContext ctx) {
        /*
         topn(output=[x] orderkey=[b])
             ->project(a as x)
                ->T(a, b)
         'x' can be lazy materialized.
         materializeMap: x->(T, a)
         */
        Map<Slot, MaterializeSource> materializeMap = new HashMap<>();
        List<Slot> materializedSlots = new ArrayList<>();
        // find the slots which can be lazy materialized
        for (Slot slot : topN.getOutput()) {
            Optional<MaterializeSource> source = computeMaterializeSource(topN, (SlotReference) slot);
            if (source.isPresent()) {
                materializeMap.put(slot, source.get());
            } else {
                materializedSlots.add(slot);
            }
        }
        // find out the slots which are worth doing lazy materialization
        List<Slot> lazyMaterializeSlots = filterSlotsForLazyMaterialization(materializeMap);
        if (lazyMaterializeSlots.isEmpty()) {
            return topN;
        }

        Map<CatalogRelation, List<Slot>> relationToLazySlotMap = new HashMap<>();
        for (Slot slot : lazyMaterializeSlots) {
            MaterializeSource source = materializeMap.get(slot);
            relationToLazySlotMap.computeIfAbsent(source.relation, relation -> new ArrayList<>()).add(slot);
        }

        Plan result = topN;
        List<Slot> originOutput = topN.getOutput();
        BiMap<CatalogRelation, SlotReference> relationToRowId = HashBiMap.create(relationToLazySlotMap.size());
        HashSet<SlotReference> rowIdSet = new HashSet<>();
        for (CatalogRelation relation : relationToLazySlotMap.keySet()) {
            Column rowIdCol = new Column(Column.GLOBAL_ROWID_COL + relation.getTable().getName(),
                    Type.STRING, false, null, false,
                    "", relation.getTable().getName() + ".global_row_id");
            SlotReference rowIdSlot = SlotReference.fromColumn(relation.getTable(), rowIdCol,
                    relation.getQualifier());
            result = result.accept(new LazySlotPruning(),
                    new LazySlotPruning.Context((PhysicalCatalogRelation) relation,
                            rowIdSlot, relationToLazySlotMap.get(relation)));
            relationToRowId.put(relation, rowIdSlot);
            rowIdSet.add(rowIdSlot);
        }

        // materialize.child.output requires
        // rowId only appears once.
        // that is [a, rowId1, b rowId1] is not acceptable
        List<SlotReference> materializeInput = moveRowIdsToTail(result.getOutput(), rowIdSet);

        if (materializeInput == null) {
            /*
            topn
              -->any
            =>
            project
               -->materialize
                   -->topn
                     -->any
             */
            result = new PhysicalLazyMaterialize(result, result.getOutput(),
                    materializedSlots, relationToLazySlotMap, relationToRowId, materializeMap,
                    null, ((AbstractPlan) result).getStats());
        } else {
            /*
            topn
              -->any
            =>
            project
              -->materialize
                -->project
                  -->topn
                     -->any
             */
            List<Slot> reOrderedMaterializedSlots = new ArrayList<>();
            for (Slot slot : materializeInput) {
                if (rowIdSet.contains(slot)) {
                    break;
                }
                reOrderedMaterializedSlots.add(slot);
            }
            result = new PhysicalProject(materializeInput, null, result);
            result = new PhysicalLazyMaterialize(result, materializeInput,
                    reOrderedMaterializedSlots, relationToLazySlotMap, relationToRowId, materializeMap,
                    null, ((AbstractPlan) result).getStats());
        }
        result = new PhysicalProject(originOutput, null, result);
        return result;
    }

    /*
        [a, r1, r2, b, r2] => [a, b, r1, r2]
        move all rowIds to tail, and remove duplicated rowIds
     */
    private List<SlotReference> moveRowIdsToTail(List<Slot> slots, Set<SlotReference> rowIds) {
        List<SlotReference> reArrangedSlots = new ArrayList<>();
        List<SlotReference> reArrangedRowIds = new ArrayList<>();
        boolean moved = false;
        boolean meetRowId = false;
        for (Slot slot : slots) {
            if (rowIds.contains(slot)) {
                if (!reArrangedRowIds.contains(slot)) {
                    reArrangedRowIds.add((SlotReference) slot);
                }
                meetRowId = true;
            } else {
                if (meetRowId) {
                    moved = true;
                }
                reArrangedSlots.add((SlotReference) slot);
            }
        }
        if (!moved) {
            return null;
        }
        reArrangedSlots.addAll(reArrangedRowIds);
        return reArrangedSlots;
    }

    private List<Slot> filterSlotsForLazyMaterialization(Map<Slot, MaterializeSource> materializeMap) {
        return new ArrayList<>(materializeMap.keySet());
    }

    private Optional<MaterializeSource> computeMaterializeSource(PhysicalTopN topN, SlotReference slot) {
        MaterializeProbeVisitor probe = new MaterializeProbeVisitor();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(slot);
        return probe.visit(topN, context);
    }

}
