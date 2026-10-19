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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.OperativeColumnDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
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

    @Override
    public Plan visitPhysicalResultSink(PhysicalResultSink<? extends Plan> sink, DeriveContext context) {
        return visitChildren(this, sink, context);
    }

    /**
     * Marks the outputs of a set operation, and the outputs of its children, that it reads.
     *
     * <p>A distinct set operation deduplicates on all of its outputs, so its children have to read
     * them whether or not the parent demands them. The demand of the parent can not stand for it:
     * BuildAggForUnion, which adds the group by that deduplicates, runs after the set operation
     * rules that already consume the statistics, so relying on it would leave the statistics of the
     * deduplication keys unknown while those rules need them. An {@code ALL} set operation does not
     * deduplicate and reads only what is used above it.
     */
    private Plan deriveSetOperation(LogicalSetOperation setOperation, DeriveContext context) {
        boolean distinct = setOperation.getQualifier() == Qualifier.DISTINCT;
        for (int i = 0; i < setOperation.getOutput().size(); i++) {
            Slot output = setOperation.getOutput().get(i);
            if (distinct || context.operativeSlotIds.contains(output.getExprId().asInt())) {
                context.addOperativeSlot(output);
                for (List<SlotReference> childOutput : setOperation.getRegularChildrenOutputs()) {
                    context.addOperativeSlotId(childOutput.get(i).getExprId().asInt());
                }
            }
        }
        return visitChildren(this, setOperation, context);
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, DeriveContext context) {
        return deriveSetOperationWithBackPropagation(union, context);
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, DeriveContext context) {
        return deriveSetOperationWithBackPropagation(intersect, context);
    }

    @Override
    public Plan visitLogicalExcept(LogicalExcept except, DeriveContext context) {
        return deriveSetOperationWithBackPropagation(except, context);
    }

    /**
     * Derives the demand of a set operation and, because the outputs of the set operation and of its
     * children are the same values, propagates a demand of a child output back to the output.
     */
    private Plan deriveSetOperationWithBackPropagation(LogicalSetOperation setOperation,
            DeriveContext context) {
        Plan round1 = deriveSetOperation(setOperation, context);
        boolean needBackPropagation = false;
        for (int i = 0; i < setOperation.getOutput().size(); i++) {
            Slot output = setOperation.getOutput().get(i);
            if (!context.operativeSlotIds.contains(output.getExprId().asInt())) {
                for (List<SlotReference> childOutput : setOperation.getRegularChildrenOutputs()) {
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
            return deriveSetOperation(setOperation, context);
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

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation relation, DeriveContext context) {
        ImmutableSet.Builder<Slot> operandSlots = ImmutableSet.builder();
        for (Slot slot : relation.getOutput()) {
            if (context.operativeSlotIds.contains(slot.getExprId().asInt())) {
                operandSlots.add(slot);
            }
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

        public void addOperativeSlotId(int exprId) {
            operativeSlotIds.add(exprId);
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
