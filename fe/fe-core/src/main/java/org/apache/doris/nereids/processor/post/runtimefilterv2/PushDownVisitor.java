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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PushDownVisitor
 */
public class PushDownVisitor extends PlanVisitor<Boolean, PushDownContext> {
    public static PushDownVisitor INSTANCE = new PushDownVisitor();

    @Override
    public Boolean visit(Plan plan, PushDownContext ctx) {
        boolean pushed = false;
        for (Plan child : plan.children()) {
            if (child.getOutputSet().containsAll(ctx.getTargetExpression().getInputSlots())) {
                pushed |= child.accept(this, ctx);
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalRelation(PhysicalRelation relation, PushDownContext ctx) {
        if (! (relation instanceof PhysicalCatalogRelation) && !(relation instanceof PhysicalCTEConsumer)) {
            return false;
        }
        if (ctx.getTargetExpression().getInputSlots().size() != 1) {
            return false;
        }
        Slot targetSlot = ctx.getTargetExpression().getInputSlots().iterator().next();
        if (relation.getOutputSet().contains(targetSlot)) {
            for (TRuntimeFilterType type : ctx.getRFContext().getTypes()) {
                RuntimeFilterV2 rfV2 = new RuntimeFilterV2(
                        ctx.getRFContext().nextId(),
                        ctx.getSourceNode(),
                        ctx.getSourceExpression(),
                        ctx.getBuildNdvOrRowCount(),
                        ctx.getExprOrder(),
                        relation,
                        ctx.getTargetExpression(),
                        type);
                ctx.getRFContext().addRuntimeFilterV2(rfV2);
                relation.addRuntimeFilterV2(rfV2);
                ctx.getSourceNode().addRuntimeFilterV2(rfV2);
            }
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalProject(PhysicalProject<? extends Plan> project, PushDownContext ctx) {
        if (!project.getOutputSet().containsAll(ctx.getTargetExpression().getInputSlots())) {
            return false;
        }

        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(project.getProjects());
        Expression newTarget = ctx.getTargetExpression().rewriteDownShortCircuit(
                e -> replaceMap.getOrDefault(e, e));
        if (newTarget.getInputSlots().size() == 1) {
            return project.child().accept(this, ctx.withTarget(newTarget));
        } else {
            return false;
        }
    }

    @Override
    public Boolean visitPhysicalSetOperation(PhysicalSetOperation setOp, PushDownContext ctx) {
        if (!setOp.getOutputSet().containsAll(ctx.getTargetExpression().getInputSlots())) {
            return false;
        }

        List<Slot> output = setOp.getOutput();
        List<List<SlotReference>> childrenOutput = setOp.getRegularChildrenOutputs();
        boolean pushed = false;
        for (int i = 1; i < childrenOutput.size(); i++) {
            Map<Slot, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < output.size(); j++) {
                replaceMap.put(output.get(j), childrenOutput.get(i).get(j));
            }
            Expression newTarget = ctx.getTargetExpression().rewriteDownShortCircuit(
                    e -> replaceMap.getOrDefault(e, e));
            if (newTarget.getInputSlots().size() == 1) {
                pushed |= setOp.child(i).accept(this, ctx.withTarget(newTarget));
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join, PushDownContext ctx) {
        if (!join.getOutputSet().containsAll(ctx.getTargetExpression().getInputSlots())) {
            return false;
        }

        if (ctx.isNullSafe() || join.getJoinType().isOuterJoin()) {
            return false;
        }

        boolean pushed = join.left().accept(this, ctx);
        pushed |= join.right().accept(this, ctx);

        List<Expression> hashJoinConditions = join.getHashJoinConjuncts();
        for (Expression hashJoinCondition : hashJoinConditions) {
            if (hashJoinCondition instanceof EqualTo) {
                EqualTo equal = (EqualTo) hashJoinCondition;
                equal = (EqualTo) JoinUtils.swapEqualToForChildrenOrder(equal, join.left().getOutputSet());
                if (ctx.getTargetExpression().equals(equal.left())) {
                    pushed |= join.right().accept(this, ctx.withTarget(equal.right()));
                } else if (ctx.getTargetExpression().equals(equal.right())) {
                    pushed |= join.left().accept(this, ctx.withTarget(equal.left()));
                }
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (!join.getOutputSet().containsAll(ctx.getTargetExpression().getInputSlots())) {
            return false;
        }

        if (ctx.isNullSafe() || join.getJoinType().isOuterJoin()) {
            return false;
        }

        boolean pushed = join.left().accept(this, ctx);
        pushed |= join.right().accept(this, ctx);

        return pushed;
    }

    @Override
    public Boolean visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalWindow(
            PhysicalWindow<? extends Plan> window, PushDownContext ctx) {
        Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
        if (commonPartitionKeys.containsAll(ctx.getTargetExpression().getInputSlots())) {
            return window.child().accept(this, ctx);
        }
        return false;
    }
}
