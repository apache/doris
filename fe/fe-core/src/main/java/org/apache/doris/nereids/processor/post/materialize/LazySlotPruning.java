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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * prune lazy materialized slot
 */
public class LazySlotPruning extends DefaultPlanRewriter<LazySlotPruning.Context> {
    /**
     * Context
     */
    public static class Context {
        private PhysicalCatalogRelation scan;
        private List<Slot> lazySlots;
        private SlotReference rowIdSlot;

        public Context(PhysicalCatalogRelation scan, SlotReference rowIdSlot, List<Slot> lazySlots) {
            this.scan = scan;
            this.lazySlots = lazySlots;
            this.rowIdSlot = rowIdSlot;
        }

        private Context(PhysicalCatalogRelation scan, List<Slot> lazySlots, SlotReference rowIdSlot) {
            this.scan = scan;
            this.lazySlots = lazySlots;
            this.rowIdSlot = rowIdSlot;
        }

        public Context withLazySlots(List<Slot> otherLazySlots) {
            return new Context(this.scan, otherLazySlots, this.rowIdSlot);
        }

        public void forceRowIdNullable() {
            rowIdSlot = rowIdSlot.withNullable(true);
        }

        public void updateRowIdSlot(SlotReference rowIdSlot) {
            this.rowIdSlot = rowIdSlot;
        }
    }

    @Override
    public Plan visit(Plan plan, Context context) {
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            if (child.getOutput().containsAll(context.lazySlots)) {
                Plan newChild = child.accept(this, context);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            } else {
                newChildren.add(child);
            }
        }

        if (hasNewChildren) {
            AbstractPhysicalPlan physicalPlan = (AbstractPhysicalPlan) plan;
            plan = ((AbstractPhysicalPlan) plan.withChildren(newChildren.build()))
                    .copyStatsAndGroupIdFrom(physicalPlan).resetLogicalProperties();
        }
        return plan;
    }

    @Override
    public Plan visitPhysicalOlapScan(PhysicalOlapScan scan, Context context) {
        if (scan.getOutput().containsAll(context.lazySlots)) {
            PhysicalLazyMaterializeOlapScan lazyScan = new PhysicalLazyMaterializeOlapScan(scan,
                    context.rowIdSlot, context.lazySlots);
            return lazyScan;
        } else {
            // should not hit here
            throw new RuntimeException("Lazy materialize fault");
        }
    }

    @Override
    public Plan visitPhysicalFileScan(PhysicalFileScan scan, Context context) {
        if (scan.getOutput().containsAll(context.lazySlots)) {
            PhysicalLazyMaterializeFileScan lazyScan = new PhysicalLazyMaterializeFileScan(scan,
                    context.rowIdSlot, context.lazySlots);
            return lazyScan;
        } else {
            // should not hit here
            throw new RuntimeException("Lazy materialize fault");
        }
    }

    @Override
    public Plan visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan scan, Context context) {
        // should not come here
        return scan;
    }

    // stop pruning when meet OutputPrunable plan node
    @Override
    public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate, Context context) {
        return aggregate;
    }

    @Override
    public Plan visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer, Context context) {
        return cteConsumer;
    }

    @Override
    public Plan visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, Context context) {
        return repeat;
    }

    @Override
    public Plan visitPhysicalSetOperation(PhysicalSetOperation setOperation, Context context) {
        return setOperation;
    }

    @Override
    public Plan visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> producer, Context context) {
        return producer;
    }

    @Override
    public Plan visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, Context context) {
        return oneRowRelation;
    }

    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, Context context) {
        // project A as B
        // singleSlotAliasMap: B->A
        Map<Slot, Slot> singleSlotAliasMap = new HashMap<>();
        for (NamedExpression ne : project.getProjects()) {
            if (ne instanceof Alias && ne.child(0) instanceof Slot) {
                singleSlotAliasMap.put(ne.toSlot(), (Slot) ne.child(0));
            }
        }

        Plan child = project.child();
        if (singleSlotAliasMap.isEmpty()) {
            child = child.accept(this, context);
        } else {
            List<Slot> childLazySlots = new ArrayList<>();
            for (Slot slot : context.lazySlots) {
                if (singleSlotAliasMap.containsKey(slot)) {
                    childLazySlots.add(singleSlotAliasMap.get(slot));
                } else {
                    childLazySlots.add(slot);
                }
            }
            Context childContext = context.withLazySlots(childLazySlots);
            child = child.accept(this, childContext);
        }
        if (child.getOutput().contains(context.rowIdSlot)) {
            List<NamedExpression> newProjections = new ArrayList<>();
            for (NamedExpression ne : project.getProjects()) {
                if (!context.lazySlots.contains(ne.toSlot())) {
                    newProjections.add(ne);
                }
            }
            newProjections.add(context.rowIdSlot);
            project = project.withProjectionsAndChild(newProjections, child).resetLogicalProperties();
        }
        return project;
    }

    @Override
    public Plan visitAbstractPhysicalJoin(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join, Context context) {
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(2);
        boolean hasNewChildren = false;
        for (int i = 0; i < 2; i++) {
            Plan child = join.child(i);
            if (child.getOutputSet().containsAll(context.lazySlots)) {
                Plan newChild = child.accept(this, context);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            } else {
                newChildren.add(child);
            }
        }
        Plan plan = join;
        if (hasNewChildren) {
            AbstractPhysicalPlan physicalPlan = (AbstractPhysicalPlan) plan;
            plan = ((AbstractPhysicalPlan) plan.withChildren(newChildren.build()))
                    .copyStatsAndGroupIdFrom(physicalPlan).resetLogicalProperties();
            // update rowIdSlot.nullable after outer join
            int rowIdPos = plan.getOutput().indexOf(context.rowIdSlot);
            if (rowIdPos != -1 && !context.rowIdSlot.nullable()) {
                SlotReference rowIdSlot = (SlotReference) plan.getOutput().get(rowIdPos);
                if (join.getJoinType().isFullOuterJoin()) {
                    context.updateRowIdSlot(rowIdSlot.withNullable(true));
                } else if (join.getJoinType().isLeftOuterJoin() && plan.child(1).getOutput().contains(rowIdSlot)) {
                    context.updateRowIdSlot(rowIdSlot.withNullable(true));
                } else if (join.getJoinType().isRightOuterJoin() && plan.child(0).getOutput().contains(rowIdSlot)) {
                    context.updateRowIdSlot(rowIdSlot.withNullable(true));
                }
            }
        }
        return plan;
    }
}
