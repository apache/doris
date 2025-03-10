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

package org.apache.doris.nereids.processor.post.setRuntimeFilter;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.planner.RuntimeFilterId;

import java.util.List;
import java.util.stream.Collectors;

public class SetRuntimeFilterGenerator extends PlanPostProcessor {
    private SRFContext srfContext = new SRFContext();
    @Override
    public Plan visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        Plan src = intersect.child(1);
        int slotIdx = chooseSourceSlots(intersect);
        for (int childId = 1; childId < intersect.children().size(); childId++) {
            Plan child = intersect.children().get(childId);
            PushDownContext pushDownContext = new PushDownContext(
                    srfContext,
                    intersect,
                    intersect.child(0).getOutput().get(slotIdx),
                    intersect.child(childId).getOutput().get(slotIdx));
            SRFPushDownVisitor visitor = new SRFPushDownVisitor();
            child.accept(visitor, pushDownContext);
        }
        return visitPhysicalSetOperation(intersect, context);
    }

    private int chooseSourceSlots(PhysicalIntersect intersect) {
        // TODO: choose best slots by ndv
        return 0;
    }
}
