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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

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
        boolean pushed = false;
        for (Plan child : plan.children()) {
            pushed |= child.accept(this, ctx);
        }
        return pushed;
    }

    @Override
    public Boolean visitPhysicalProject(
            PhysicalProject<? extends Plan> project, PushDownContext ctx) {
        // project ( A+1 as x)
        // probeExpr: abs(x) => abs(A+1)
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

    @Override
    public Boolean visitPhysicalSetOperation(
            PhysicalSetOperation setOperation, PushDownContext ctx) {
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
        if (topn.equals(ctx.topn)) {
            return topn.child().accept(this, ctx);
        }
        return false;
    }

    @Override
    public Boolean visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PushDownContext ctx) {
        return false;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            PushDownContext ctx) {
        if (ctx.nullsFirst && join.getJoinType().isOuterJoin()) {
            // topn-filter can be pushed down to the left child of leftOuterJoin
            // and to the right child of rightOuterJoin,
            // but PushDownTopNThroughJoin rule already pushes topn to the left and right side.
            // the topn-filter will be generated according to the inferred topn node.
            return false;
        }
        if (join.left().getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            return join.left().accept(this, ctx);
        }
        if (join.right().getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            // expand expr to the other side of hash join condition:
            // T1 join T2 on T1.x = T2.y order by T2.y limit 10
            // we rewrite probeExpr from T2.y to T1.x and try to push T1.x to left side
            for (Expression conj : join.getHashJoinConjuncts()) {
                if (ctx.probeExpr.equals(conj.child(1))) {
                    // push to left child. right child is blocking operator, do not need topn-filter
                    PushDownContext newCtx = ctx.withNewProbeExpression(conj.child(0));
                    return join.left().accept(this, newCtx);
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
        if (supportPhysicalRelations(relation)
                && relation.getOutputSet().containsAll(ctx.probeExpr.getInputSlots())) {
            // in ut, relation.getStats() may return null
            if (relation.getStats() == null
                    || relation.getStats().getRowCount() > ctx.topn.getLimit() + ctx.topn.getOffset()) {
                topnFilterContext.addTopnFilter(ctx.topn, relation, ctx.probeExpr);
                return true;
            }
        }
        return false;
    }

    private boolean supportPhysicalRelations(PhysicalRelation relation) {
        return relation instanceof PhysicalOlapScan
                || relation instanceof PhysicalOdbcScan
                || relation instanceof PhysicalEsScan
                || relation instanceof PhysicalFileScan
                || relation instanceof PhysicalJdbcScan
                || relation instanceof PhysicalDeferMaterializeOlapScan;
    }
}
