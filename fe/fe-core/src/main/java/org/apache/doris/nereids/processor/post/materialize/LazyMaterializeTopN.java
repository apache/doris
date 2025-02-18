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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        // find the slots which can be lazy materialized
        for (Slot slot : topN.getOutput()) {
            Optional<MaterializeSource> source = computeMaterializeSource(topN, (SlotReference) slot);
            if (source.isPresent()) {
                materializeMap.put(slot, source.get());
            }
        }
        // find out the slots which are worth doing lazy materialization
        List<Slot> lazyMaterializeSlots = filterSlotsForLazyMaterialization(materializeMap);
        if (lazyMaterializeSlots.isEmpty()) {
            return topN;
        }

        Map<CatalogRelation, List<Slot>> relationToSlotMap = new HashMap<>();
        for (Slot slot : lazyMaterializeSlots) {
            MaterializeSource source = materializeMap.get(slot);
            relationToSlotMap.computeIfAbsent(source.relation, relation -> new ArrayList<>()).add(slot);
        }
        Plan result = topN;
        for (CatalogRelation relation : relationToSlotMap.keySet()) {
            result = result.accept(new LazySlotPruning(),
                    new LazySlotPruning.Context((PhysicalOlapScan) relation, relationToSlotMap.get(relation)));
        }
        PhysicalLazyMaterialize<? extends Plan> materialize = new PhysicalLazyMaterialize(result,
                topN.getOutput(), materializeMap, lazyMaterializeSlots);
        return materialize;
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
