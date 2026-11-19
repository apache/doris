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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.statistics.model.Statistics;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/** Copies a physical plan for profile display without retaining Memo or planner-owned mutable state. */
public class PhysicalPlanShallowCopier {
    private final Map<Plan, AbstractPhysicalPlan> copies = new IdentityHashMap<>();
    private final Map<RuntimeFilter, RuntimeFilter> filters = new IdentityHashMap<>();

    private PhysicalPlanShallowCopier() {
    }

    /** Copy a complete plan, then reconnect runtime filters exclusively to copied nodes. */
    public static PhysicalPlan copy(PhysicalPlan plan) {
        PhysicalPlanShallowCopier copier = new PhysicalPlanShallowCopier();
        return copier.copyPlan(plan);
    }

    private PhysicalPlan copyPlan(PhysicalPlan plan) {
        AbstractPhysicalPlan result = copyNode(plan);
        // RFs refer back to builder/target nodes. Rewire only after all plan nodes have been copied.
        for (Plan original : copies.keySet()) {
            AbstractPhysicalPlan source = (AbstractPhysicalPlan) original;
            source.getRuntimeFilters().forEach(this::copyFilter);
            source.getAppliedRuntimeFilters().forEach(this::copyFilter);
        }
        // The RF constructor registers itself on its builder. Restore source order independently
        // of the order in which scans/builders were visited in the identity map.
        for (Map.Entry<Plan, AbstractPhysicalPlan> entry : copies.entrySet()) {
            AbstractPhysicalPlan source = (AbstractPhysicalPlan) entry.getKey();
            AbstractPhysicalPlan copy = entry.getValue();
            copy.getRuntimeFilters().clear();
            source.getRuntimeFilters().forEach(filter -> copy.addRuntimeFilter(filters.get(filter)));
            source.getAppliedRuntimeFilters().forEach(filter -> copy.addAppliedRuntimeFilter(filters.get(filter)));
        }
        return result;
    }

    private AbstractPhysicalPlan copyNode(Plan plan) {
        AbstractPhysicalPlan existing = copies.get(plan);
        if (existing != null) {
            return existing;
        }
        AbstractPhysicalPlan source = (AbstractPhysicalPlan) plan;
        AbstractPhysicalPlan copy = source.copyForProfile(this::copyNode);
        copy.setMutableState(MutableState.KEY_GROUP, source.getGroupIdAsString());
        copy.setStatistics(source.getStats() == null ? null : new Statistics(source.getStats()));
        if (source instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate.TopnPushInfo topn = ((PhysicalHashAggregate<?>) source).getTopnPushInfo();
            if (topn != null) {
                ((PhysicalHashAggregate<?>) copy).setTopnPushInfo(
                        new PhysicalHashAggregate.TopnPushInfo(topn.orderkeys, topn.limit));
            }
        }
        if (source instanceof PhysicalProject) {
            ((PhysicalProject<?>) copy).setMultiLayerProjects(((PhysicalProject<?>) source).getMultiLayerProjects());
        }
        copies.put(plan, copy);
        return copy;
    }

    private RuntimeFilter copyFilter(RuntimeFilter filter) {
        return filters.computeIfAbsent(filter, source -> {
            RuntimeFilter copy = new RuntimeFilter(source.getId(), source.getSrcExpr(), source.getTargetSlot(),
                    source.getTargetExpression(), source.getType(), source.getExprOrder(),
                    Objects.requireNonNull(copies.get(source.getBuilderNode())), source.getBuildSideNdv(),
                    source.isBloomFilterSizeCalculatedByNdv(), source.gettMinMaxType(),
                    (PhysicalRelation) Objects.requireNonNull(copies.get(source.getTargetScan())));
            copy.setNonBlocking(source.isNonBlocking());
            copy.setPruningMetadata(source.canPruneBuckets(), source.getPartitionMonotonicity());
            return copy;
        });
    }
}
