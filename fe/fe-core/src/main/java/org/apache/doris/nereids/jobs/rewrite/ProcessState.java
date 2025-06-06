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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * ProcessState: this class is used to trace the plan change shape for `explain plan process select ...`
 * */
public class ProcessState {
    private Plan newestPlan;
    private Map<Plan, List<Plan>> originParentToNewestChildren = new IdentityHashMap<>();
    private Map<Plan, Plan> newestPlanMap = new IdentityHashMap<>();

    public ProcessState(Plan newestPlan) {
        this.newestPlan = newestPlan;
    }

    public Plan getNewestPlan() {
        return newestPlan;
    }

    /** updateChildAndGetNewest */
    public Plan updateChildAndGetNewest(Plan originParent, int childIndex, Plan rewrittenChild) {
        if (originParent == null) {
            newestPlanMap.put(newestPlan, rewrittenChild);
            this.newestPlan = rewrittenChild;
            return rewrittenChild;
        }
        Plan newestPlan = mapToNewestPlan(originParent);

        // this condition is used to debug
        // if (!checkContains(newestPlan)) {
        //     String treeString = newestPlan.treeString();
        //     throw new IllegalStateException(treeString);
        // }

        List<Plan> originChildren = newestPlan.children();
        List<Plan> newestChildren = originParentToNewestChildren.computeIfAbsent(
                newestPlan, k -> new ArrayList<>(originChildren)
        );
        newestChildren.set(childIndex, rewrittenChild);
        Plan updateNewestParent = newestPlan.withChildren(newestChildren);
        newestPlanMap.put(originParent, updateNewestParent);
        return rewrite(newestPlan, updateNewestParent);
    }

    public Plan updateChild(Plan originParent, int childIndex, Plan rewrittenChild) {
        updateChildAndGetNewest(originParent, childIndex, rewrittenChild);
        return mapToNewestPlan(originParent);
    }

    private Plan mapToNewestPlan(Plan originPlan) {
        Plan newestPlan = originPlan;
        while (true) {
            Plan mapNewestPlan = newestPlanMap.getOrDefault(newestPlan, newestPlan);
            if (mapNewestPlan == newestPlan) {
                return mapNewestPlan;
            } else {
                newestPlan = mapNewestPlan;
            }
        }
    }

    private boolean checkContains(Plan originParent) {
        boolean contains = newestPlan.anyMatch(p -> p == originParent);
        return contains;
    }

    private Plan rewrite(Plan originPlan, Plan newPlan) {
        this.newestPlan = new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visit(Plan plan, Void context) {
                if (plan == originPlan) {
                    newestPlanMap.put(plan, newPlan);
                    return newPlan;
                }

                Builder<Plan> children = ImmutableList.builderWithExpectedSize(plan.arity());
                boolean changed = false;
                for (Plan child : plan.children()) {
                    Plan newChild = visit(child, null);
                    changed |= newChild != child;
                    children.add(newChild);
                }
                if (changed) {
                    Plan newPlan = plan.withChildren(children.build());
                    newestPlanMap.put(plan, newPlan);
                    return newPlan;
                } else {
                    return plan;
                }
            }
        }.visit(newestPlan, null);
        return this.newestPlan;
    }
}
