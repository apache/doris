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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.OperativeColumnDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * derive operative columns
 */
public class OperativeColumnDerive extends DefaultPlanRewriter<DeriveContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, new DeriveContext());
    }

    @Override
    public Plan visit(Plan plan, DeriveContext context) {
        context.addOperativeSlots(plan.getInputSlots());
        return visitChildren(this, plan, context);
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> sink, DeriveContext context) {
        return visitChildren(this, sink, context);
    }

    private Plan deriveUnion(LogicalUnion union, DeriveContext context) {
        for (int i = 0; i < union.getOutput().size(); i++) {
            Slot output = union.getOutput().get(i);
            if (context.operativeSlots.contains(output)) {
                for (List<SlotReference> childOutput : union.getRegularChildrenOutputs()) {
                    context.operativeSlots.add(childOutput.get(i));
                }
            }
        }
        return visitChildren(this, union, context);
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, DeriveContext context) {
        Plan round1 = deriveUnion(union, context);
        // check for back propagation
        boolean needBackPropagation = false;
        for (int i = 0; i < union.getOutput().size(); i++) {
            Slot output = union.getOutput().get(i);
            if (!context.operativeSlots.contains(output)) {
                for (List<SlotReference> childOutput : union.getRegularChildrenOutputs()) {
                    if (context.operativeSlots.contains(childOutput.get(i))) {
                        // if any child output is operative, this output is operative
                        context.operativeSlots.add(output);
                        needBackPropagation = true;
                        break;
                    }
                }
            }
        }
        if (needBackPropagation) {
            return deriveUnion(union, context);
        } else {
            return round1;
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, DeriveContext context) {
        for (NamedExpression ne : project.getProjects()) {
            if (!(ne instanceof Slot)) {
                if (ne.child(0) instanceof Slot) {
                    if (context.operativeSlots.contains(ne.toSlot())) {
                        context.operativeSlots.add((Slot) ne.child(0));
                    }
                } else {
                    context.addOperativeSlots(ne.getInputSlots());
                    context.addOperativeSlot(ne.toSlot());
                }
            }
        }
        Plan plan = visitChildren(this, project, context);
        // back propagate
        for (NamedExpression ne : project.getProjects()) {
            if (!(ne instanceof Slot) && ne.child(0) instanceof Slot) {
                if (context.operativeSlots.contains(((Slot) ne.child(0)))) {
                    context.addOperativeSlot(ne.toSlot());
                }
            }
        }
        return plan;
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, DeriveContext context) {
        Set<Slot> intersectSlots = new HashSet<>(olapScan.getOutput());
        intersectSlots.retainAll(context.operativeSlots);
        OlapTable table = olapScan.getTable();
        if (KeysType.UNIQUE_KEYS.equals(table.getKeysType())
                && !table.getTableProperty().getEnableUniqueKeyMergeOnWrite()
                || KeysType.AGG_KEYS.equals(table.getKeysType())
                || KeysType.PRIMARY_KEYS.equals(table.getKeysType())) {
            for (Slot slot : olapScan.getOutput()) {
                SlotReference slotReference = (SlotReference) slot;
                if (slotReference.getColumn().isPresent() && slotReference.getColumn().get().isKey()) {
                    intersectSlots.add(slotReference);
                }
            }
        }
        return (Plan) olapScan.withOperativeSlots(intersectSlots);
    }

    @Override
    public Plan visitLogicalCatalogRelation(LogicalCatalogRelation relation, DeriveContext context) {
        Set<Slot> intersectSlots = new HashSet<>(relation.getOutput());
        intersectSlots.retainAll(context.operativeSlots);
        return (Plan) relation.withOperativeSlots(intersectSlots);
    }

    /**
     * DeriveContext
     */
    public static class DeriveContext {
        public Set<Slot> operativeSlots;

        public DeriveContext() {
            this.operativeSlots = new HashSet<>();
        }

        public void addOperativeSlot(Slot slot) {
            operativeSlots.add(slot);
        }

        public void addOperativeSlots(Set<Slot> slots) {
            operativeSlots.addAll(slots);
        }
    }
}
