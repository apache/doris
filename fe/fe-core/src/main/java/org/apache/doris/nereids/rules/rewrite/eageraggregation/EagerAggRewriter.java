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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.SessionVariable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * eager aggregation
 * agg[sum(t1.A) group by t1.B]
 *    ->join(t1.C=t2.D)
 *        ->T1(A, B, C)
 *        ->T2(D)
 *
 * =>
 * agg[sum(x) group by t1.B]
 *     ->join(t1.C=t2.D)
 *         ->agg[sum(A) as x, group by B]
 *             ->T1(A, B, C)
 *         ->T2(D)
 */
public class EagerAggRewriter extends DefaultPlanRewriter<PushDownAggContext> {

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context) {
        List<AggregateFunction> pushToLeft = new ArrayList<>();
        List<AggregateFunction> pushToRight = new ArrayList<>();
        boolean toLeft = true;
        boolean toRight = true;
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            if (join.left().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                pushToLeft.add(aggFunc);
                toRight = false;
            } else if (join.right().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                pushToRight.add(aggFunc);
                toLeft = false;
            }
            if (toLeft == toRight) {
                return join;
            }
        }

        List<SlotReference> joinConditionSlots;
        List<NamedExpression> childGroupByKeys = new ArrayList<>();
        if (toLeft) {
            joinConditionSlots = getJoinConditionsInputSlotsFromOneSide(join, join.left());
            for (NamedExpression key : context.getGroupKeys()) {
                if (join.left().getOutputSet().containsAll(key.getInputSlots())) {
                    childGroupByKeys.add(key);
                }
            }
        } else {
            joinConditionSlots = getJoinConditionsInputSlotsFromOneSide(join, join.right());
            for (NamedExpression key : context.getGroupKeys()) {
                if (join.right().getOutputSet().containsAll(key.getInputSlots())) {
                    childGroupByKeys.add(key);
                }
            }
        }

        for (SlotReference slot : joinConditionSlots) {
            if (!childGroupByKeys.contains(slot)) {
                childGroupByKeys.add(slot);
            }
        }

        //TODO: push count() to other side
        PushDownAggContext childContext = context.withGoupKeys(childGroupByKeys);
        if (toLeft) {
            Plan newLeft = join.left().accept(this, childContext);
            if (newLeft != join.left()) {
                context.getFinalGroupKeys().addAll(childContext.getFinalGroupKeys());
                return join.withChildren(newLeft, join.right());
            }
        } else {
            Plan newRight = join.right().accept(this, childContext);
            if (newRight != join.right()) {
                context.getFinalGroupKeys().addAll(childContext.getFinalGroupKeys());
                return join.withChildren(join.left(), newRight);
            }
        }
        return join;
    }

    private List<SlotReference> getJoinConditionsInputSlotsFromOneSide(LogicalJoin<? extends Plan, ? extends Plan> join,
            Plan side) {
        List<SlotReference> oneSideSlots = new ArrayList<>();
        for (Expression condition : join.getHashJoinConjuncts()) {
            for (Slot slot : condition.getInputSlots()) {
                if (side.getOutputSet().contains(slot)) {
                    oneSideSlots.add((SlotReference) slot);
                }
            }
        }
        for (Expression condition : join.getOtherJoinConjuncts()) {
            for (Slot slot : condition.getInputSlots()) {
                if (side.getOutputSet().contains(slot)) {
                    oneSideSlots.add((SlotReference) slot);
                }
            }
        }
        return oneSideSlots;
    }

    private PushDownAggContext createContextFromProject(LogicalProject<? extends Plan> project,
            PushDownAggContext context) {
        HashMap<Expression, Expression> replaceMapAliasBody = new HashMap<>();
        HashMap<Expression, Expression> replaceMapAlias = new HashMap<>();
        for (NamedExpression ne : project.getProjects()) {
            if (ne instanceof Alias) {
                replaceMapAliasBody.put(ne.toSlot(), ((Alias) ne).child());
                replaceMapAlias.put(ne.toSlot(), ne);
            }
        }

        /*
         * context: sum(a) groupBy(y+z as x, l)
         * proj: b+c as a, u+v as y, m+n as l
         * newContext: sum(b+c), groupBy((u+v)+z as x, m+n as l)
         */

        List<NamedExpression> groupKeys = new ArrayList<>();
        for (NamedExpression key : context.getGroupKeys()) {
            NamedExpression newKey;
            if (key instanceof Alias) {
                newKey = (Alias) ExpressionUtils.replace(key, replaceMapAliasBody);
            } else {
                // key is slot
                newKey = (NamedExpression) replaceMapAlias.getOrDefault(key, key);
            }
            groupKeys.add(newKey);
        }

        List<AggregateFunction> aggFunctions = new ArrayList<>();
        Map<AggregateFunction, Alias> aliasMap = new HashMap<>();
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            AggregateFunction newAggFunc = (AggregateFunction) ExpressionUtils.replace(aggFunc, replaceMapAliasBody);
            Alias alias = context.getAliasMap().get(aggFunc);
            aliasMap.put(newAggFunc, (Alias) alias.withChildren(newAggFunc));
            aggFunctions.add(newAggFunc);
        }
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap);
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PushDownAggContext context) {
        if (project.child() instanceof LogicalCatalogRelation
                || (project.child() instanceof LogicalFilter
                && project.child().child(0) instanceof LogicalCatalogRelation)) {
            // project
            //   --> scan
            // =>
            // aggregate
            //   --> project
            //     --> scan
            return genAggregate(project, context);
        }

        // check validation
        // all projections are used in context
        // all slots in context are projected
        List<Slot> slotsInContext = context.getGroupKeys().stream()
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toList());
        slotsInContext.addAll(context.getAggFunctionsInputSlots());
        for (Slot slot : slotsInContext) {
            if (!project.getOutputSet().contains(slot)) {
                if (SessionVariable.isFeDebug()) {
                    throw new RuntimeException("push down failed: " + slot + " is not in project \n"
                            + project.treeString());
                } else {
                    return project;
                }
            }
        }
        for (NamedExpression ne : project.getProjects()) {
            if (!slotsInContext.contains(ne.toSlot())) {
                if (SessionVariable.isFeDebug()) {
                    throw new RuntimeException("push down failed: " + ne + " is not in PushDownAggContext\n"
                            + project);
                } else {
                    return project;
                }
            }
        }

        PushDownAggContext newContext = createContextFromProject(project, context);
        Plan newChild = project.child().accept(this, newContext);
        if (newChild != project.child()) {
            context.getFinalGroupKeys().addAll(newContext.getFinalGroupKeys());
            /*
             * agg[sum(a), groupBy(b)]
             *    -> proj(a, b1+b2 as b)
             *       -> join(c = d)
             *          -> any(a, b1, b2, c,...)
             *          -> any(d, ...)
             *  =>
             *  agg[sum(x), groupBy(b)]
             *    -> proj(x, b)
             *      -> join(c=d)
             *          ->agg[sum(a) as x, groupBy(b, c)]
             *              ->proj(a, b1+b2 as b, c, ...)
             *                  -> any(a, b1, b2, c)
             *          -> any(d, ...)
             */
            Set<Slot> aggFuncInputSlots = context.getAggFunctionsInputSlots();
            List<NamedExpression> newProjections = new ArrayList<>();
            for (NamedExpression ne : project.getProjects()) {
                if (aggFuncInputSlots.contains(ne.toSlot())) {
                    // ne (a) is replaced by alias slot (x)
                    continue;
                } else if (context.getFinalGroupKeys().contains(ne.toSlot())) {
                    newProjections.add(ne.toSlot());
                } else {
                    newProjections.add(ne);
                }
            }
            for (Alias alias : context.getAliasMap().values()) {
                newProjections.add(alias.toSlot());
            }
            for (SlotReference key : context.getFinalGroupKeys()) {
                if (!newProjections.contains(key)) {
                    newProjections.add(key);
                }
            }

            return project.withProjectsAndChild(newProjections, newChild);
        }

        return project;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, PushDownAggContext context) {
        return genAggregate(filter, context);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PushDownAggContext context) {
        return genAggregate(relation, context);
    }

    private Plan genAggregate(Plan child, PushDownAggContext context) {
        if (checkStats(child, context)) {
            List<NamedExpression> aggOutputExpressions = new ArrayList<>();
            aggOutputExpressions.addAll(context.getAliasMap().values());
            aggOutputExpressions.addAll(context.getGroupKeys().stream()
                    .map(e -> (NamedExpression) e).collect(Collectors.toList()));
            for (NamedExpression key : context.getGroupKeys()) {
                context.addFinalGroupKey((SlotReference) key.toSlot());
            }
            return new LogicalAggregate(context.getGroupKeys(), aggOutputExpressions, child);
        } else {
            return child;
        }
    }

    private boolean checkStats(Plan plan, PushDownAggContext context) {
        return true;
    }
}
