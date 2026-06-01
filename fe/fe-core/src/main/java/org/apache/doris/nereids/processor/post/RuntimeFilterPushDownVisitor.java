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

import org.apache.doris.nereids.processor.post.RuntimeFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Push down runtime filters using tree-traversal approach.
 * Rewrites probeExpr through Projects and SetOperations, expands through join conditions,
 * and creates RuntimeFilter at scan nodes when probeExpr resolves to a scan slot.
 */
public class RuntimeFilterPushDownVisitor extends PlanVisitor<Boolean, PushDownContext> {

    /**
     * PushDownContext carries the information needed to push a runtime filter down the plan tree.
     * The probeExpr is progressively rewritten as it passes through Projects/SetOps.
     */
    public static class PushDownContext {
        final Expression srcExpr;
        final Expression probeExpr;
        final RuntimeFilterContext rfContext;
        final AbstractPhysicalPlan builderNode;
        final TRuntimeFilterType type;
        final boolean isNot;
        final TMinMaxRuntimeFilterType singleSideMinMax;
        final boolean hasUnknownColStats;
        final long buildSideNdv;
        final int exprOrder;

        private PushDownContext(RuntimeFilterContext rfContext,
                AbstractPhysicalPlan builderNode, Expression srcExpr, Expression probeExpr,
                TRuntimeFilterType type, boolean isNot,
                TMinMaxRuntimeFilterType singleSideMinMax,
                boolean hasUnknownColStats, long buildSideNdv, int exprOrder) {
            this.rfContext = rfContext;
            this.builderNode = builderNode;
            this.srcExpr = srcExpr;
            this.probeExpr = probeExpr;
            this.type = type;
            this.isNot = isNot;
            this.singleSideMinMax = singleSideMinMax;
            this.hasUnknownColStats = hasUnknownColStats;
            this.buildSideNdv = buildSideNdv;
            this.exprOrder = exprOrder;
        }

        public static PushDownContext createPushDownContext(RuntimeFilterContext rfContext,
                AbstractPhysicalPlan builderNode, Expression srcExpr, Expression probeExpr,
                TRuntimeFilterType type, boolean isNot) {
            return createPushDownContext(rfContext, builderNode, srcExpr, probeExpr,
                    type, isNot, false, -1, -1);
        }

        public static PushDownContext createPushDownContext(RuntimeFilterContext rfContext,
                AbstractPhysicalPlan builderNode, Expression srcExpr, Expression probeExpr,
                TRuntimeFilterType type, boolean isNot,
                boolean hasUnknownColStats, long buildSideNdv, int exprOrder) {
            return createPushDownContext(rfContext, builderNode, srcExpr, probeExpr,
                    type, isNot, TMinMaxRuntimeFilterType.MIN_MAX,
                    hasUnknownColStats, buildSideNdv, exprOrder);
        }

        /**
         * singleSideMinMax is only meaningful for MIN_MAX runtime filters.
         * Non-MIN_MAX callers should use the simplified overload above or pass
         * TMinMaxRuntimeFilterType.MIN_MAX as a placeholder.
         */
        public static PushDownContext createPushDownContext(RuntimeFilterContext rfContext,
                AbstractPhysicalPlan builderNode, Expression srcExpr, Expression probeExpr,
                TRuntimeFilterType type, boolean isNot,
                TMinMaxRuntimeFilterType singleSideMinMax,
                boolean hasUnknownColStats, long buildSideNdv, int exprOrder) {
            return new PushDownContext(rfContext, builderNode, srcExpr, probeExpr,
                    type, isNot, singleSideMinMax, hasUnknownColStats, buildSideNdv, exprOrder);
        }

        /**
         * A context is valid if probeExpr references exactly one input slot.
         * Invalid when probeExpr is a constant (e.g., from OneRowRelation) or multi-slot.
         */
        public boolean isValid() {
            return probeExpr.getInputSlots().size() == 1;
        }

        public PushDownContext withNewProbeExpression(Expression newProbe) {
            return new PushDownContext(rfContext, builderNode, srcExpr, newProbe,
                    type, isNot, singleSideMinMax, hasUnknownColStats, buildSideNdv, exprOrder);
        }
    }

    @Override
    public Boolean visit(Plan plan, PushDownContext ctx) {
        boolean pushed = false;
        for (Plan child : plan.children()) {
            if (child.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
                pushed |= child.accept(this, ctx);
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan scan, PushDownContext ctx) {
        return visitPhysicalRelation(scan, ctx);
    }

    @Override
    public Boolean visitPhysicalRelation(PhysicalRelation scan, PushDownContext ctx) {
        if (!scan.canPushDownRuntimeFilter()) {
            return false;
        }

        Set<Slot> inputSlots = ctx.probeExpr.getInputSlots();
        if (inputSlots.size() != 1) {
            return false;
        }
        Slot scanSlot = inputSlots.iterator().next();
        if (!scan.getOutputSet().contains(scanSlot)) {
            return false;
        }

        // For non-numeric types, reject function expressions (e.g. substring()) as RF targets
        // because evaluating such functions at scan time is too expensive.
        // Only bare slots or cast-wrapped slots are allowed for non-numeric types.
        if (!isScanAcceptableProbeExpr(ctx.probeExpr, scanSlot)) {
            return false;
        }

        TRuntimeFilterType type = ctx.type;

        // V2-style: always create a separate RF per target.
        // Dedup: skip if this scan already has an RF from the same (src, type, builder).
        boolean alreadyApplied = scan.getAppliedRuntimeFilters().stream()
                .anyMatch(rf -> rf.getSrcExpr().equals(ctx.srcExpr)
                        && rf.getType() == type
                        && rf.getBuilderNode().equals(ctx.builderNode)
                        && rf.getExprOrder() == ctx.exprOrder
                        && rf.isBitmapFilterNotIn() == ctx.isNot
                        && rf.gettMinMaxType() == ctx.singleSideMinMax);
        if (alreadyApplied) {
            return true;
        }

        RuntimeFilter filter = new RuntimeFilter(ctx.rfContext.getRuntimeFilterIdGen().getNextId(),
                ctx.srcExpr, scanSlot, ctx.probeExpr,
                type, ctx.exprOrder, ctx.builderNode, ctx.isNot, ctx.buildSideNdv,
                !ctx.hasUnknownColStats, ctx.singleSideMinMax, scan);
        scan.addAppliedRuntimeFilter(filter);
        ctx.rfContext.addJoinToTargetMap(ctx.builderNode, scanSlot.getExprId());
        ctx.rfContext.setTargetExprIdToFilter(scanSlot.getExprId(), filter);
        ctx.rfContext.setTargetsOnScanNode(scan, scanSlot);
        return true;
    }

    /**
     * For non-numeric types, only bare slots or cast-wrapped slots are acceptable
     * as RF probe expressions at scan level. Function calls like substring() are too
     * expensive to evaluate per-row at scan time.
     * For numeric types, any single-slot expression is acceptable.
     */
    private boolean isScanAcceptableProbeExpr(Expression probeExpr, Slot scanSlot) {
        if (scanSlot.getDataType() instanceof NumericType) {
            return true;
        }
        Expression unwrapped = probeExpr;
        while (unwrapped instanceof Cast) {
            unwrapped = ((Cast) unwrapped).child();
        }
        return unwrapped instanceof Slot;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (!ctx.builderNode.equals(join)
                && !join.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        if (join.getJoinType().isAsofJoin()) {
            return false;
        }

        // NullSafeEqual cannot be pushed through outer joins
        if (ctx.builderNode instanceof PhysicalHashJoin) {
            /*
             hashJoin( t1.A <=> t2.A )
                +---->left outer Join(t1.B=T3.B)
                            +--->t1
                            +--->t3
                +---->t2
             RF(t1.A <=> t2.A) cannot be pushed down through left outer join
             */
            EqualPredicate equal = (EqualPredicate) ((AbstractPhysicalJoin<?, ?>) ctx.builderNode)
                    .getHashJoinConjuncts().get(ctx.exprOrder);
            if (equal instanceof NullSafeEqual) {
                if (join.getJoinType().isOuterJoin()) {
                    return false;
                }
            }
        }

        boolean pushed = false;
        // Push to children whose output contains the probe slots
        Plan leftNode = join.child(0);
        Plan rightNode = join.child(1);
        if (leftNode.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            pushed |= leftNode.accept(this, ctx);
        }
        if (rightNode.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            pushed |= rightNode.accept(this, ctx);
        }

        // Expand through join conditions at non-builder inner/semi joins
        if (!join.equals(ctx.builderNode)
                && ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().expandRuntimeFilterByInnerJoin
                && (join.getJoinType() == JoinType.INNER_JOIN || join.getJoinType().isSemiJoin())) {
            for (Expression expr : join.getHashJoinConjuncts()) {
                EqualPredicate equalTo = (EqualPredicate) expr;
                Expression newTarget = null;
                if (ctx.probeExpr.equals(equalTo.left())) {
                    newTarget = equalTo.right();
                } else if (ctx.probeExpr.equals(equalTo.right())) {
                    newTarget = equalTo.left();
                }
                if (newTarget != null && newTarget.getInputSlots().size() == 1
                        && !newTarget.equals(ctx.srcExpr)) {
                    PushDownContext expanded = ctx.withNewProbeExpression(newTarget);
                    if (leftNode.getOutputSet().containsAll(newTarget.getInputSlots())) {
                        pushed |= leftNode.accept(this, expanded);
                    }
                    if (rightNode.getOutputSet().containsAll(newTarget.getInputSlots())) {
                        pushed |= rightNode.accept(this, expanded);
                    }
                }
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (!join.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        if (ctx.builderNode instanceof PhysicalHashJoin) {
            /*
             hashJoin( t1.A <=> t2.A )
                +---->left outer Join(t1.B=T3.B)
                            +--->t1
                            +--->t3
                +---->t2
             RF(t1.A <=> t2.A) cannot be pushed down through left outer join
             */
            EqualPredicate equal = (EqualPredicate) ((AbstractPhysicalJoin<?, ?>) ctx.builderNode)
                    .getHashJoinConjuncts().get(ctx.exprOrder);
            if (equal instanceof NullSafeEqual) {
                if (join.getJoinType().isOuterJoin()) {
                    return false;
                }
            }
        }
        boolean pushed = false;
        Plan left = join.left();
        Plan right = join.right();
        if (left.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            pushed |= left.accept(this, ctx);
        }
        if (right.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            pushed |= right.accept(this, ctx);
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalProject(PhysicalProject<? extends Plan> project, PushDownContext ctx) {
        if (!project.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        // Rewrite probeExpr through project aliases using v2-style replaceMap
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(project.getProjects());
        Expression newProbeExpr = ctx.probeExpr.rewriteDownShortCircuit(
                e -> replaceMap.getOrDefault(e, e));
        if (newProbeExpr.getInputSlots().size() == 1) {
            return project.child().accept(this, ctx.withNewProbeExpression(newProbeExpr));
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalSetOperation(PhysicalSetOperation setOperation, PushDownContext ctx) {
        if (!setOperation.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        List<Slot> output = setOperation.getOutput();
        List<List<SlotReference>> childrenOutputs = setOperation.getRegularChildrenOutputs();
        if (childrenOutputs.isEmpty()) {
            return false;
        }
        boolean pushedDown = false;
        for (int i = 0; i < setOperation.children().size(); i++) {
            Map<Slot, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < output.size(); j++) {
                replaceMap.put(output.get(j), childrenOutputs.get(i).get(j));
            }
            Expression newProbeExpr = ctx.probeExpr.rewriteDownShortCircuit(
                    e -> replaceMap.getOrDefault(e, e));
            if (newProbeExpr.getInputSlots().size() == 1) {
                pushedDown |= setOperation.child(i).accept(this, ctx.withNewProbeExpression(newProbeExpr));
            }
        }
        return pushedDown;
    }

    @Override
    public Boolean visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PushDownContext ctx) {
        if (!repeat.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        // Only push through Repeat if the probe slot appears in ALL grouping sets.
        // A slot absent from a grouping set is erased to NULL for rows of that group,
        // so filtering on it would incorrectly discard needed rows.
        Set<Expression> commonGroupingExprs = repeat.getCommonGroupingSetExpressions();
        if (!commonGroupingExprs.containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }
        return repeat.child().accept(this, ctx);
    }

    @Override
    public Boolean visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PushDownContext ctx) {
        if (!window.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return false;
        }

        Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
        if (commonPartitionKeys.containsAll(ctx.probeExpr.getInputSlots())) {
            return window.child().accept(this, ctx);
        }
        return false;
    }

}
