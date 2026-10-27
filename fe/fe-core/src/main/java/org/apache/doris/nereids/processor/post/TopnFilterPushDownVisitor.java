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

import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.DateAddSubMonotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * push down topn filter
 */
public class TopnFilterPushDownVisitor extends PlanVisitor<Boolean, PushDownContext> {

    private final TopnFilterContext topnFilterContext;

    public TopnFilterPushDownVisitor(TopnFilterContext topnFilterContext) {
        this.topnFilterContext = topnFilterContext;
    }

    /**
     * topn filter push-down context
     */
    public static class PushDownContext {
        final Expression probeExpr;
        final TopN topn;
        final boolean nullsFirst;

        public PushDownContext(TopN topn, Expression probeExpr, boolean nullsFirst) {
            this.topn = topn;
            this.probeExpr = probeExpr;
            this.nullsFirst = nullsFirst;
        }

        public PushDownContext withNewProbeExpression(Expression newProbe) {
            return new PushDownContext(topn, newProbe, nullsFirst);
        }

    }

    @Override
    public Boolean visit(Plan plan, PushDownContext ctx) {
        // Unknown operators may enforce constraints absent from getExpressions(), e.g. AssertNumRows.
        // Each traversable operator must explicitly establish that reducing its input is safe.
        return false;
    }

    @Override
    public Boolean visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, PushDownContext ctx) {
        return distribute.child().accept(this, ctx);
    }

    @Override
    public Boolean visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PushDownContext ctx) {
        return !hasEvaluationSensitiveExpressions(filter) && filter.child().accept(this, ctx);
    }

    @Override
    public Boolean visitPhysicalProject(
            PhysicalProject<? extends Plan> project, PushDownContext ctx) {
        // TopN may skip or reorder rows before assert_true or overflowing date arithmetic runs.
        if (hasEvaluationSensitiveExpressions(project)) {
            return false;
        }
        // Expand safe aliases in the probe, e.g. x -> substring(name, 1, 5).
        PushDownContext ctxProjectProbeExpr = ctx;
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        for (NamedExpression ne : project.getProjects()) {
            if (ne instanceof Alias && ctx.probeExpr.getInputSlots().contains(ne.toSlot())) {
                replaceMap.put(ctx.probeExpr.getInputSlots().iterator().next(), ((Alias) ne).child());
            }
        }
        if (! replaceMap.isEmpty()) {
            Expression newProbeExpr = ctx.probeExpr.accept(ExpressionVisitors.EXPRESSION_MAP_REPLACER, replaceMap);
            ctxProjectProbeExpr = ctx.withNewProbeExpression(newProbeExpr);
        }
        return project.child().accept(this, ctxProjectProbeExpr);
    }

    private static boolean hasEvaluationSensitiveExpressions(Plan plan) {
        return plan.getExpressions().stream().anyMatch(expression -> !safeOnReducedInput(expression));
    }

    private static boolean safeOnReducedInput(Expression expression) {
        // Determinism and monotonicity do not imply total evaluation: from_second(-1), for example,
        // throws. Keep a closed set of proven-total expressions instead of enumerating throwing
        // functions. Unknown functions, casts and UDFs must preserve their original evaluation domain.
        if (expression instanceof Slot || expression instanceof Literal) {
            return true;
        }
        boolean safeNode = expression instanceof Alias || expression instanceof Nullable
                || expression instanceof And || expression instanceof Or || expression instanceof Not
                || expression instanceof IsNull;
        if (expression instanceof EqualTo || expression instanceof NullSafeEqual
                || expression instanceof LessThan || expression instanceof LessThanEqual
                || expression instanceof GreaterThan || expression instanceof GreaterThanEqual
                || expression instanceof InPredicate) {
            safeNode = expression.children().stream().allMatch(child -> {
                DataType type = child.getDataType();
                return type.isNumericType() || type.isBooleanType() || type.isStringLikeType()
                        || type.isDateLikeType() || type.isTimeType() || type.isIPType()
                        || type.isUuidType() || type.isNullType();
            });
        } else if (expression instanceof Substring) {
            safeNode = expression.children().subList(1, expression.arity()).stream()
                    .allMatch(Literal.class::isInstance);
        } else if (expression instanceof DateAddSubMonotonic) {
            // Zero intervals are identities on already-typed dates; nonzero arithmetic can overflow.
            safeNode = !ExpressionUtils.containsUnsafeDateArithmetic(expression);
        }
        return safeNode && expression.children().stream().allMatch(TopnFilterPushDownVisitor::safeOnReducedInput);
    }

    @Override
    public Boolean visitPhysicalSetOperation(
            PhysicalSetOperation setOperation, PushDownContext ctx) {
        if (hasEvaluationSensitiveExpressions(setOperation)) {
            return false;
        }
        boolean pushedDown = false;
        if (setOperation.arity() > 0) {
            pushedDown = pushDownFilterToSetOperatorChild(setOperation, ctx, 0);
        }

        if (setOperation instanceof Union) {
            for (int i = 1; i < setOperation.arity(); i++) {
                // push down to the other children
                pushedDown |= pushDownFilterToSetOperatorChild(setOperation, ctx, i);
            }
        }
        return pushedDown;
    }

    private Boolean pushDownFilterToSetOperatorChild(PhysicalSetOperation setOperation,
            PushDownContext ctx, int childIdx) {
        Plan child = setOperation.child(childIdx);
        Map<Expression, Expression> replaceMap = Maps.newHashMap();

        List<NamedExpression> setOutputs = setOperation.getOutputs();
        for (int i = 0; i < setOutputs.size(); i++) {
            replaceMap.put(setOutputs.get(i).toSlot(),
                    setOperation.getRegularChildrenOutputs().get(childIdx).get(i));
        }
        if (!replaceMap.isEmpty()) {
            Expression newProbeExpr = ctx.probeExpr.accept(ExpressionVisitors.EXPRESSION_MAP_REPLACER,
                    replaceMap);
            PushDownContext childPushDownContext = ctx.withNewProbeExpression(newProbeExpr);
            return child.accept(this, childPushDownContext);
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalCTEAnchor(PhysicalCTEAnchor<? extends Plan, ? extends Plan> anchor,
            PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> anchor,
            PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalTopN(PhysicalTopN<? extends Plan> topn, PushDownContext ctx) {
        if (topn.equals(ctx.topn) && !hasEvaluationSensitiveExpressions(topn)) {
            return topn.child().accept(this, ctx);
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PushDownContext ctx) {
        return false;
    }

    private PushDownContext adjustProbeExprNullableThroughOuterJoin(Plan leftChild, PushDownContext ctx) {
        Expression probeExpr = ctx.probeExpr;
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        boolean changed = false;
        for (Slot probSlot : probeExpr.getInputSlots()) {
            for (Slot childSlot : leftChild.getOutput()) {
                if (probSlot.getExprId().asInt() == childSlot.getExprId().asInt()
                        && probSlot.nullable() && !childSlot.nullable()) {
                    replaceMap.put(probSlot, new Nullable(childSlot));
                    changed = true;
                    break;
                }
            }
        }
        if (changed) {
            return ctx.withNewProbeExpression(ExpressionUtils.replace(probeExpr, replaceMap));
        }
        return ctx;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        // Scan conjuncts cannot fence ON expressions retained on the join, including mark predicates.
        if (hasEvaluationSensitiveExpressions(join)) {
            return false;
        }
        if (ctx.nullsFirst && (join.getJoinType().isOuterJoin() || join.getJoinType().isAsofOuterJoin())) {
            // topn-filter can be pushed down to the left child of leftOuterJoin
            // and to the right child of rightOuterJoin,
            // but PushDownTopNThroughJoin rule already pushes topn to the left and right side.
            // the topn-filter will be generated according to the inferred topn node.
            return false;
        }
        if (join.left().getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            PushDownContext childPushDownContext = adjustProbeExprNullableThroughOuterJoin(join.left(), ctx);
            return join.left().accept(this, childPushDownContext);
        }
        if (join.right().getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            // expand expr to the other side of hash join condition:
            // T1 join T2 on T1.x = T2.y order by T2.y limit 10
            // we rewrite probeExpr from T2.y to T1.x and try to push T1.x to left side
            for (Expression conj : join.getHashJoinConjuncts()) {
                if (ctx.probeExpr.equals(conj.child(1))) {
                    // push to left child. right child is blocking operator, do not need topn-filter
                    PushDownContext ctxOnLeftChild = ctx.withNewProbeExpression(conj.child(0));
                    PushDownContext childPushDownContext = adjustProbeExprNullableThroughOuterJoin(join.left(),
                            ctxOnLeftChild);
                    return join.left().accept(this, childPushDownContext);
                }
            }
        }
        // topn key is combination of left and right
        // select * from (select T1.A+T2.B as x from T1 join T2) T3 order by x limit 1;
        return false;
    }

    @Override
    public Boolean visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (hasEvaluationSensitiveExpressions(join)) {
            return false;
        }
        if (ctx.nullsFirst && join.getJoinType().isOuterJoin()) {
            // topn-filter can be pushed down to the left child of leftOuterJoin
            // and to the right child of rightOuterJoin,
            // but PushDownTopNThroughJoin rule already pushes topn to the left and right side.
            // the topn-filter will be generated according to the inferred topn node.
            return false;
        }
        // push to left child. right child is blocking operator, do not need topn-filter
        if (join.left().getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return join.left().accept(this, ctx);
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalRelation(PhysicalRelation relation, PushDownContext ctx) {
        if (supportPhysicalRelations(relation) && safeOnReducedInput(ctx.probeExpr)
                && relation.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            // in ut, relation.getStats() may return null
            if (relation.getStats() == null || ConnectContext.get() == null
                    || ConnectContext.get().getSessionVariable() == null
                    || Math.max(relation.getStats().getRowCount(), 1)
                            * ConnectContext.get().getSessionVariable().topnFilterRatio > ctx.topn.getLimit()
                                    + ctx.topn.getOffset()) {
                topnFilterContext.addTopnFilter(ctx.topn, relation, ctx.probeExpr);
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan, PushDownContext ctx) {
        return visitPhysicalRelation(lazyScan, ctx);
    }

    private boolean supportPhysicalRelations(PhysicalRelation relation) {
        return relation instanceof PhysicalOlapScan
                || relation instanceof PhysicalOdbcScan
                || relation instanceof PhysicalFileScan
                || relation instanceof PhysicalLazyMaterializeOlapScan;
    }
}
