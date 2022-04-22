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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo {
    private final List<Group> groups = Lists.newArrayList();
    private final List<PlanReference> planReferences = Lists.newArrayList();
    private Group rootSet;

    public void initialize(LogicalPlan plan) {
        rootSet = newPlanReference(plan, null).getParent();
    }

    public Group getRootSet() {
        return rootSet;
    }

    /**
     * Add plan to Memo.
     *
     * @param plan {@link Plan} to be added
     * @param target target group to add plan. null to generate new Group
     * @return Reference of plan in Memo
     */
    // TODO: need to merge PlanRefSet if new PlanRef is same with some one already in memo
    public PlanReference newPlanReference(Plan<?> plan, Group target) {
        if (plan.getPlanReference() != null) {
            return plan.getPlanReference();
        }
        List<PlanReference> childReferences = Lists.newArrayList();
        for (Plan<?> childrenPlan : plan.getChildren()) {
            childReferences.add(newPlanReference(childrenPlan, null));
        }
        PlanReference newPlanReference = new PlanReference(plan);
        planReferences.add(newPlanReference);
        for (PlanReference childReference : childReferences) {
            newPlanReference.addChild(childReference.getParent());
        }

        if (target != null) {
            target.addPlanReference(newPlanReference);
        } else {
            Group group = new Group(newPlanReference);
            groups.add(group);
        }
        return newPlanReference;
    }
}
