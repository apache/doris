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

package org.apache.doris.nereids.processor.post.runtimefilterv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * RuntimeFilterV2Generator
 */
public class RuntimeFilterV2Generator extends PlanPostProcessor {

    public RuntimeFilterV2Generator() {
    }

    @Override
    public Plan visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        for (int slotIdx : chooseSourceSlots(intersect)) {
            for (int childId = 1; childId < intersect.children().size(); childId++) {
                Plan child = intersect.children().get(childId);
                Statistics stats = ((AbstractPlan) intersect.child(0)).getStats();
                long buildNdvOrRowCount = -1;
                if (stats != null) {
                    buildNdvOrRowCount = (long) stats.getRowCount();
                    ColumnStatistic colStats = stats.findColumnStatistics(
                            intersect.child(0).getOutput().get(slotIdx));
                    if (colStats != null && !colStats.isUnKnown) {
                        buildNdvOrRowCount = Math.max(1, (long) colStats.ndv);
                    }
                }
                PushDownContext pushDownContext = new PushDownContext(
                        context.getRuntimeFilterV2Context(),
                        intersect,
                        intersect.child(0).getOutput().get(slotIdx),
                        buildNdvOrRowCount,
                        slotIdx,
                        intersect.child(childId).getOutput().get(slotIdx));
                child.accept(PushDownVisitor.INSTANCE, pushDownContext);
            }
        }
        return visitPhysicalSetOperation(intersect, context);
    }

    private List<Integer> chooseSourceSlots(PhysicalIntersect intersect) {
        // TODO: choose best slots by ndv
        return ImmutableList.of(0);
    }
}
