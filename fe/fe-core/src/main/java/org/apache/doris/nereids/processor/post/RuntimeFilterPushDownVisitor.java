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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.processor.post.RuntimeFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * push down rf
 */
public class RuntimeFilterPushDownVisitor extends PlanVisitor<Boolean, PushDownContext> {
    // the context to push down rf by join condition: probeExpr = srcExpr

    /**
     * PushDownContext
     */
    public static class PushDownContext {
        final Expression srcExpr;
        final Expression probeExpr;
        final Slot probeSlot;
        final RuntimeFilterContext rfContext;
        final IdGenerator<RuntimeFilterId> rfIdGen;
        final TRuntimeFilterType type;
        final AbstractPhysicalJoin<? extends Plan, ? extends Plan> builderNode;
        final boolean hasUnknownColStats;
        final long buildSideNdv;
        final int exprOrder;
        final Pair<PhysicalRelation, Slot> finalTarget;
        //bitmap rf used only
        final boolean isNot;
        // only used for Min_Max runtime filter
        final TMinMaxRuntimeFilterType singleSideMinMax;

        /**
         * push down context
         */
        // for hash join runtime filter
        private PushDownContext(Expression srcExpr, Expression probeExpr, RuntimeFilterContext rfContext,
                IdGenerator<RuntimeFilterId> rfIdGen, TRuntimeFilterType type,
                AbstractPhysicalJoin<? extends Plan, ? extends Plan> builderNode,
                boolean hasUnknownColStats, long buildSideNdv, int exprOrder, boolean isNot,
                TMinMaxRuntimeFilterType singleSideMinMax) {
            this.probeExpr = probeExpr;
            this.rfContext = rfContext;
            this.srcExpr = srcExpr;
            this.rfIdGen = rfIdGen;
            this.type = type;
            this.builderNode = builderNode;
            this.hasUnknownColStats = hasUnknownColStats;
            this.buildSideNdv = buildSideNdv;
            this.exprOrder = exprOrder;
            this.isNot = isNot;
            Expression expr = getSingleNumericSlotOrExpressionCoveredByCast(probeExpr);
            /* finalTarget can be null if it is not a column from base table.
            for example:
            select * from T1 join (select 9 as x) T2 on T1.x=T2.x,
            where the final target of T2.x is a constant
            */
            if (expr instanceof Slot) {
                probeSlot = (Slot) expr;
                finalTarget = rfContext.getAliasTransferPair(probeSlot);
            } else {
                finalTarget = null;
                probeSlot = null;
            }
            this.singleSideMinMax = singleSideMinMax;
        }

        // for BitMap runtime filter
        public static PushDownContext createPushDownContextForBitMapFilter(Expression srcExpr, Expression probeExpr,
                RuntimeFilterContext rfContext,
                IdGenerator<RuntimeFilterId> rfIdGen,
                AbstractPhysicalJoin<? extends Plan, ? extends Plan> builderNode,
                long buildSideNdv, int exprOrder, boolean isNot) {
            return new PushDownContext(srcExpr, probeExpr, rfContext, rfIdGen, TRuntimeFilterType.BITMAP, builderNode,
                    false, buildSideNdv,
                    exprOrder, isNot, TMinMaxRuntimeFilterType.MIN_MAX);
        }

        // for NLJ min-max runtime filter
        public static PushDownContext createPushDownContextForNljMinMaxFilter(Expression srcExpr, Expression probeExpr,
                RuntimeFilterContext rfContext,
                IdGenerator<RuntimeFilterId> rfIdGen,
                AbstractPhysicalJoin<? extends Plan, ? extends Plan> builderNode,
                int exprOrder,
                TMinMaxRuntimeFilterType singleSideMinMax) {
            return new PushDownContext(srcExpr, probeExpr, rfContext, rfIdGen, TRuntimeFilterType.MIN_MAX, builderNode,
                    false, -1,
                    exprOrder, false, singleSideMinMax);
        }

        public static PushDownContext createPushDownContextForHashJoin(Expression srcExpr, Expression probeExpr,
                RuntimeFilterContext rfContext,
                IdGenerator<RuntimeFilterId> rfIdGen, TRuntimeFilterType type,
                AbstractPhysicalJoin<? extends Plan, ? extends Plan> builderNode,
                boolean hasUnknownColStats, long buildSideNdv, int exprOrder) {
            return new PushDownContext(srcExpr, probeExpr, rfContext, rfIdGen, type, builderNode,
                    hasUnknownColStats, buildSideNdv,
                    exprOrder, false, TMinMaxRuntimeFilterType.MIN_MAX);
        }

        public boolean isValid() {
            return finalTarget != null && probeSlot != null;
        }

        public PushDownContext withNewProbeExpression(Expression newProbe) {
            return new PushDownContext(srcExpr, newProbe, this.rfContext, rfIdGen, type, builderNode,
                    hasUnknownColStats, buildSideNdv, exprOrder, isNot, singleSideMinMax);
        }

        private Expression getSingleNumericSlotOrExpressionCoveredByCast(Expression expression) {
            if (expression.getInputSlots().size() == 1) {
                Slot slot = expression.getInputSlots().iterator().next();
                if (slot.getDataType() instanceof NumericType) {
                    return expression.getInputSlots().iterator().next();
                }
            }
            // for other datatype, only support cast.
            // example: T1 join T2 on subStr(T1.a, 1,4) = subStr(T2.a, 1,4)
            // the cost of subStr is too high, and hence we do not generate RF subStr(T2.a, 1,4)->subStr(T1.a, 1,4)
            while (expression instanceof Cast) {
                expression = ((Cast) expression).child();
            }
            return expression;
        }
    }

    @Override
    public Boolean visit(Plan plan, PushDownContext ctx) {
        boolean pushed = false;
        for (Plan child : plan.children()) {
            pushed |= child.accept(this, ctx);
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalRelation(PhysicalRelation scan, PushDownContext ctx) {
        if (scan instanceof PhysicalSchemaScan) {
            return false;
        }
        Preconditions.checkArgument(ctx.isValid(),
                "runtime filter pushDownContext is invalid");
        PhysicalRelation relation = ctx.finalTarget.first;
        Slot scanSlot = ctx.finalTarget.second;
        Slot probeSlot = ctx.probeSlot;

        if (!relation.equals(scan)) {
            return Boolean.FALSE;
        }

        TRuntimeFilterType type = ctx.type;
        RuntimeFilter filter = ctx.rfContext.getRuntimeFilterBySrcAndType(ctx.srcExpr, type, ctx.builderNode);
        if (filter != null) {
            if (!filter.hasTargetScan(scan)) {
                // A join B on A.a1=B.b and A.a1 = A.a2
                // RF B.b->(A.a1, A.a2)
                // however, RF(B.b->A.a2) is implied by RF(B.a->A.a1) and A.a1=A.a2
                // we skip RF(B.b->A.a2)
                scan.addAppliedRuntimeFilter(filter);
                filter.addTargetSlot(scanSlot, ctx.probeExpr, scan);
                ctx.rfContext.addJoinToTargetMap(ctx.builderNode, scanSlot.getExprId());
                ctx.rfContext.setTargetExprIdToFilter(scanSlot.getExprId(), filter);
                ctx.rfContext.setTargetsOnScanNode(ctx.rfContext.getAliasTransferPair(probeSlot).first, scanSlot);
            }
        } else {
            filter = new RuntimeFilter(ctx.rfIdGen.getNextId(),
                    ctx.srcExpr, ImmutableList.of(scanSlot), ImmutableList.of(ctx.probeExpr),
                    type, ctx.exprOrder, ctx.builderNode, ctx.isNot, ctx.buildSideNdv,
                    !ctx.hasUnknownColStats, ctx.singleSideMinMax, scan);
            scan.addAppliedRuntimeFilter(filter);
            ctx.rfContext.addJoinToTargetMap(ctx.builderNode, scanSlot.getExprId());
            ctx.rfContext.setTargetExprIdToFilter(scanSlot.getExprId(), filter);
            ctx.rfContext.setTargetsOnScanNode(ctx.rfContext.getAliasTransferPair(probeSlot).first, scanSlot);
            ctx.rfContext.setRuntimeFilterIdentityToFilter(ctx.srcExpr, type, ctx.builderNode, filter);
        }
        return true;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        boolean pushed = false;

        if (ctx.builderNode instanceof PhysicalHashJoin) {
            /*
             hashJoin( t1.A <=> t2.A )
                +---->left outer Join(t1.B=T3.B)
                            +--->t1
                            +--->t3
                +---->t2
             RF(t1.A <=> t2.A) cannot be pushed down through left outer join
             */
            EqualPredicate equal = (EqualPredicate) ctx.builderNode.getHashJoinConjuncts().get(ctx.exprOrder);
            if (equal instanceof NullSafeEqual) {
                if (join.getJoinType().isOuterJoin()) {
                    return false;
                }
            }
        }
        AbstractPhysicalPlan leftNode = (AbstractPhysicalPlan) join.child(0);
        AbstractPhysicalPlan rightNode = (AbstractPhysicalPlan) join.child(1);
        Set<Expression> probExprList = Sets.newLinkedHashSet();
        probExprList.add(ctx.probeExpr);
        Pair<PhysicalRelation, Slot> srcPair = ctx.rfContext.getAliasTransferMap().get(ctx.srcExpr);
        PhysicalRelation srcNode = (srcPair == null) ? null : srcPair.first;
        Pair<PhysicalRelation, Slot> targetPair = ctx.rfContext.getAliasTransferMap().get(ctx.probeExpr);
        if (targetPair == null) {
            /* cases for "targetPair is null"
             when probeExpr is output slot of setOperator, targetPair is null
            */
            return false;
        }
        PhysicalRelation target1 = targetPair.first;
        PhysicalRelation target2 = null;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().expandRuntimeFilterByInnerJoin) {
            if (!join.equals(ctx.builderNode)
                    && (join.getJoinType() == JoinType.INNER_JOIN || join.getJoinType().isSemiJoin())) {
                for (Expression expr : join.getHashJoinConjuncts()) {
                    EqualPredicate equalTo = (EqualPredicate) expr;
                    if (ctx.probeExpr.equals(equalTo.left())) {
                        probExprList.add(equalTo.right());
                        targetPair = ctx.rfContext.getAliasTransferMap().get(equalTo.right());
                        target2 = (targetPair == null) ? null : targetPair.first;
                    } else if (ctx.probeExpr.equals(equalTo.right())) {
                        probExprList.add(equalTo.left());
                        targetPair = ctx.rfContext.getAliasTransferMap().get(equalTo.left());
                        target2 = (targetPair == null) ? null : targetPair.first;
                    }
                    if (target2 != null) {
                        ctx.rfContext.getExpandedRF().add(
                                new RuntimeFilterContext.ExpandRF(join, srcNode, target1, target2, equalTo));
                    }
                }
                probExprList.remove(ctx.srcExpr);

            }
        }
        for (Expression prob : probExprList) {
            PushDownContext ctxForChild = prob.equals(ctx.probeExpr) ? ctx : ctx.withNewProbeExpression(prob);
            if (!ctxForChild.isValid()) {
                continue;
            }
            if (ctx.rfContext.isRelationUseByPlan(leftNode, ctxForChild.finalTarget.first)) {
                pushed |= leftNode.accept(this, ctxForChild);
            }
            if (ctx.rfContext.isRelationUseByPlan(rightNode, ctxForChild.finalTarget.first)) {
                pushed |= rightNode.accept(this, ctxForChild);
            }
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (ctx.builderNode instanceof PhysicalHashJoin) {
            /*
             hashJoin( t1.A <=> t2.A )
                +---->left outer Join(t1.B=T3.B)
                            +--->t1
                            +--->t3
                +---->t2
             RF(t1.A <=> t2.A) cannot be pushed down through left outer join
             */
            EqualPredicate equal = (EqualPredicate) ctx.builderNode.getHashJoinConjuncts().get(ctx.exprOrder);
            if (equal instanceof NullSafeEqual) {
                if (join.getJoinType().isOuterJoin()) {
                    return false;
                }
            }
        }
        boolean pushed = false;
        if (ctx.rfContext.isRelationUseByPlan(join.left(), ctx.finalTarget.first)) {
            pushed |= join.left().accept(this, ctx);
        }
        if (ctx.rfContext.isRelationUseByPlan(join.right(), ctx.finalTarget.first)) {
            pushed |= join.right().accept(this, ctx);
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalProject(PhysicalProject<? extends Plan> project, PushDownContext ctx) {
        // project ( A+1 as x)
        // probeExpr: abs(x) => abs(A+1)
        PushDownContext ctxProjectProbeExpr = ctx;
        if (ctx.probeExpr instanceof SlotReference) {
            for (NamedExpression namedExpression : project.getProjects()) {
                if (namedExpression instanceof Alias
                        && namedExpression.getExprId() == ((SlotReference) ctx.probeExpr).getExprId()) {
                    if (((Alias) namedExpression).child().getInputSlots().size() > 1) {
                        // only support one-slot probeExpr
                        return false;
                    }
                    ctxProjectProbeExpr = ctx.withNewProbeExpression(((Alias) namedExpression).child());
                    break;
                }
            }
        } else {
            for (NamedExpression namedExpression : project.getProjects()) {
                if (namedExpression instanceof Alias
                        && ctx.probeExpr.getInputSlots().contains(namedExpression.toSlot())) {
                    if (((Alias) namedExpression).child().getInputSlots().size() > 1) {
                        // only support one-slot probeExpr
                        return false;
                    }
                    Map<Expression, Expression> map = Maps.newHashMap();
                    // probeExpr only has one input slot
                    map.put(ctx.probeExpr.getInputSlots().iterator().next(),
                            ((Alias) namedExpression).child());
                    Expression newProbeExpr = ctx.probeExpr.accept(ExpressionVisitors.EXPRESSION_MAP_REPLACER, map);
                    ctxProjectProbeExpr = ctx.withNewProbeExpression(newProbeExpr);
                    break;
                }
            }
        }
        if (!ctxProjectProbeExpr.isValid()) {
            return false;
        } else {
            return project.child().accept(this, ctxProjectProbeExpr);
        }
    }

    @Override
    public Boolean visitPhysicalSetOperation(PhysicalSetOperation setOperation, PushDownContext ctx) {
        boolean pushedDown = false;
        int projIndex = -1;
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(ctx.probeExpr);
        if (probeSlot == null) {
            return false;
        }
        List<NamedExpression> output = setOperation.getOutputs();
        for (int j = 0; j < output.size(); j++) {
            NamedExpression expr = output.get(j);
            if (expr.getExprId().equals(probeSlot.getExprId())) {
                projIndex = j;
                break;
            }
        }
        if (projIndex == -1) {
            return false;
        }
        // probeExpr only has one input slot
        for (int i = 0; i < setOperation.children().size(); i++) {
            Map<Expression, Expression> map = Maps.newHashMap();
            map.put(probeSlot,
                    setOperation.getRegularChildrenOutputs().get(i).get(projIndex));
            Expression newProbeExpr = ctx.probeExpr.accept(ExpressionVisitors.EXPRESSION_MAP_REPLACER, map);
            PushDownContext childPushDownContext = ctx.withNewProbeExpression(newProbeExpr);
            if (childPushDownContext.isValid()) {
                /*
                 * childPushDownContext is not valid, for example:
                 * setop
                 *   +--->scan t1(A, B)
                 *   +--->select 0, 0
                 * push down context for "select 0, 0" is invalid
                 */
                pushedDown |= setOperation.child(i).accept(this, childPushDownContext);
            }
        }
        return pushedDown;
    }

    @Override
    public Boolean visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PushDownContext ctx) {
        Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
        if (commonPartitionKeys.containsAll(ctx.probeExpr.getInputSlots())) {
            return window.child().accept(this, ctx);
        }
        return false;
    }

}
