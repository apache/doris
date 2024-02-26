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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * prune rf for external db
 */
public class RuntimeFilterPrunerForExternalTable extends PlanPostProcessor {
    /**
     * add parent to plan node and then remove rf if it is an external scan and only used as probe
     */
    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        plan = plan.accept(this, ctx);
        RuntimeFilterContext rfCtx = ctx.getRuntimeFilterContext();
        for (RuntimeFilter rf : rfCtx.getNereidsRuntimeFilter()) {
            AbstractPhysicalJoin join = rf.getBuilderNode();
            if (join instanceof PhysicalHashJoin) {
                List<Plan> joinAncestors = getAncestors(rf.getBuilderNode());
                for (int i = 0; i < rf.getTargetScans().size(); i++) {
                    PhysicalRelation scan = rf.getTargetScans().get(i);
                    if (canPrune(scan, joinAncestors)) {
                        rfCtx.removeFilters(rf.getTargetSlots().get(i).getExprId(), (PhysicalHashJoin) join);
                    }
                }
            }
        }
        return plan;
    }

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        for (Plan child : plan.children()) {
            child.setMutableState(MutableState.KEY_PARENT, plan);
            child.accept(this, context);
        }
        setMaxChildRuntimeFilterJump(plan);
        return plan;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation scan, CascadesContext context) {
        RuntimeFilterContext rfCtx = context.getRuntimeFilterContext();
        List<Slot> slots = rfCtx.getTargetListByScan(scan);
        int maxJump = -1;
        for (Slot slot : slots) {
            if (!rfCtx.getTargetExprIdToFilter().get(slot.getExprId()).isEmpty()) {
                for (RuntimeFilter rf : rfCtx.getTargetExprIdToFilter().get(slot.getExprId())) {
                    Optional<Object> oJump = rf.getBuilderNode().getMutableState(MutableState.KEY_RF_JUMP);
                    if (oJump.isPresent()) {
                        Integer jump = (Integer) (oJump.get());
                        if (jump > maxJump) {
                            maxJump = jump;
                        }
                    }
                }
            }
        }
        scan.setMutableState(MutableState.KEY_RF_JUMP, maxJump + 1);
        return scan;
    }

    @Override
    public PhysicalHashJoin visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
                                                  CascadesContext context) {
        join.right().accept(this, context);
        join.right().setMutableState(MutableState.KEY_PARENT, join);
        join.setMutableState(MutableState.KEY_RF_JUMP,
                join.right().getMutableState(MutableState.KEY_RF_JUMP).get());
        join.left().accept(this, context);
        join.left().setMutableState(MutableState.KEY_PARENT, join);
        return join;
    }

    private List<Plan> getAncestors(Plan plan) {
        List<Plan> ancestors = Lists.newArrayList();
        ancestors.add(plan);
        Optional<Object> parent = plan.getMutableState(MutableState.KEY_PARENT);
        while (parent.isPresent()) {
            ancestors.add((Plan) parent.get());
            parent = ((Plan) parent.get()).getMutableState(MutableState.KEY_PARENT);
        }
        return ancestors;
    }

    private boolean canPrune(PhysicalRelation scan, List<Plan> joinAndAncestors) {
        if (!(scan instanceof PhysicalFileScan)) {
            return false;
        }
        Plan cursor = scan;
        Optional<Plan> parent = cursor.getMutableState(MutableState.KEY_PARENT);
        while (parent.isPresent()) {
            if (parent.get() instanceof Join) {
                if (joinAndAncestors.contains(parent.get())) {
                    Optional oi = parent.get().getMutableState(MutableState.KEY_RF_JUMP);
                    if (oi.isPresent() && ConnectContext.get() != null
                            && (int) (oi.get())
                            > ConnectContext.get().getSessionVariable().runtimeFilterJumpThreshold) {
                        return true;
                    }
                } else {
                    if (isBuildSide(parent.get(), cursor)) {
                        return false;
                    }
                }
            }
            cursor = parent.get();
            parent = cursor.getMutableState(MutableState.KEY_PARENT);
        }
        return false;
    }

    private boolean isBuildSide(Plan parent, Plan child) {
        return parent instanceof Join && child.equals(parent.child(1));
    }

    private void setMaxChildRuntimeFilterJump(Plan plan) {
        int maxJump = 0;
        for (Plan child : plan.children()) {
            Optional oi = child.getMutableState(MutableState.KEY_RF_JUMP);
            if (oi.isPresent()) {
                int jump = (Integer) (oi.get());
                if (child instanceof Join) {
                    jump++;
                }
                if (jump > maxJump) {
                    maxJump = jump;
                }
            }
        }
        plan.setMutableState(MutableState.KEY_RF_JUMP, maxJump);
    }
}
