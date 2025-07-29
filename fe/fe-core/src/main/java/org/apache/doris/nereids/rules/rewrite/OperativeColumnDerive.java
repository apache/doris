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
import org.apache.doris.nereids.trees.expressions.Expression;
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

import com.google.common.collect.ImmutableSet;
import org.roaringbitmap.RoaringBitmap;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
        for (Expression expression : plan.getExpressions()) {
            context.addOperativeSlots(expression);
        }
        return visitChildren(this, plan, context);
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> sink, DeriveContext context) {
        return visitChildren(this, sink, context);
    }

    private Plan deriveUnion(LogicalUnion union, DeriveContext context) {
        for (int i = 0; i < union.getOutput().size(); i++) {
            Slot output = union.getOutput().get(i);
            if (context.operativeSlotIds.contains(output.getExprId().asInt())) {
                for (List<SlotReference> childOutput : union.getRegularChildrenOutputs()) {
                    context.operativeSlotIds.add(childOutput.get(i).getExprId().asInt());
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
            if (!context.operativeSlotIds.contains(output.getExprId().asInt())) {
                for (List<SlotReference> childOutput : union.getRegularChildrenOutputs()) {
                    if (context.operativeSlotIds.contains(childOutput.get(i).getExprId().asInt())) {
                        // if any child output is operative, this output is operative
                        context.operativeSlotIds.add(output.getExprId().asInt());
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
                    if (context.operativeSlotIds.contains(ne.getExprId().asInt())) {
                        context.operativeSlotIds.add(((Slot) ne.child(0)).getExprId().asInt());
                    }
                } else {
                    context.addOperativeSlots(ne);
                    context.addOperativeSlot(ne);
                }
            }
        }
        Plan plan = visitChildren(this, project, context);
        // back propagate
        for (NamedExpression ne : project.getProjects()) {
            if (!(ne instanceof Slot) && ne.child(0) instanceof Slot) {
                if (context.operativeSlotIds.contains(((Slot) ne.child(0)).getExprId().asInt())) {
                    context.addOperativeSlot(ne);
                }
            }
        }
        return plan;
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, DeriveContext context) {
        // the operativeSlotIds must order by slotId, or else backend will core
        Set<Slot> intersectSlots = new TreeSet<>(Comparator.comparing(Slot::getExprId));
        RoaringBitmap operativeSlotIds = context.operativeSlotIds;
        for (Slot slot : olapScan.getOutput()) {
            if (operativeSlotIds.contains(slot.getExprId().asInt())) {
                intersectSlots.add(slot);
            }
        }

        OlapTable table = olapScan.getTable();
        if (KeysType.UNIQUE_KEYS.equals(table.getKeysType())
                && !table.getTableProperty().getEnableUniqueKeyMergeOnWrite()
                || KeysType.AGG_KEYS.equals(table.getKeysType())
                || KeysType.PRIMARY_KEYS.equals(table.getKeysType())) {
            for (Slot slot : olapScan.getOutput()) {
                SlotReference slotReference = (SlotReference) slot;
                if (slotReference.getOriginalColumn().isPresent() && slotReference.getOriginalColumn().get().isKey()) {
                    intersectSlots.add(slotReference);
                }
            }
        }
        for (NamedExpression virtualColumn : olapScan.getVirtualColumns()) {
            intersectSlots.add(virtualColumn.toSlot());
            intersectSlots.addAll(virtualColumn.getInputSlots());
        }

        return olapScan.withOperativeSlots(intersectSlots);
    }

    @Override
    public Plan visitLogicalCatalogRelation(LogicalCatalogRelation relation, DeriveContext context) {
        ImmutableSet.Builder<Slot> operandSlots = ImmutableSet.builder();
        for (Slot slot : relation.getOutput()) {
            if (context.operativeSlotIds.contains(slot.getExprId().asInt())) {
                operandSlots.add(slot);
            }
        }
        for (NamedExpression virtualColumn : relation.getVirtualColumns()) {
            operandSlots.add(virtualColumn.toSlot());
            operandSlots.addAll(virtualColumn.getInputSlots());
        }
        return relation.withOperativeSlots(operandSlots.build());
    }

    /**
     * DeriveContext
     */
    public static class DeriveContext {
        public RoaringBitmap operativeSlotIds;

        public DeriveContext() {
            this.operativeSlotIds = new RoaringBitmap();
        }

        public void addOperativeSlot(NamedExpression slot) {
            operativeSlotIds.add(slot.getExprId().asInt());
        }

        public void addOperativeSlots(Expression exprTree) {
            exprTree.foreach(child -> {
                if (child instanceof Slot) {
                    operativeSlotIds.add(((Slot) child).getExprId().asInt());
                }
            });
        }
    }
}
