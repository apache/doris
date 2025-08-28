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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.expression.ExpressionNormalizationAndOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.ImmutableEqualSet.Builder;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.util.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * constant propagation, like: a = 10 and a + b > 30 => a = 10 and 10 + b > 30,
 * when processing a plan, it will collect all its children's equal sets and constants uniforms,
 * then use them and the plan's expressions to infer more equal sets and constants uniforms,
 * finally use the combine uniforms to replace this plan's expression's slot with literals.
 */
public class ConstantPropagation extends DefaultPlanRewriter<ExpressionRewriteContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        // logical apply uniform maybe not correct.
        if (plan.containsType(LogicalApply.class)) {
            return plan;
        }
        ExpressionRewriteContext context = new ExpressionRewriteContext(jobContext.getCascadesContext());
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, ExpressionRewriteContext context) {
        filter = visitChildren(this, filter, context);
        Expression oldPredicate = filter.getPredicate();
        Expression newPredicate = replaceConstantsAndRewriteExpr(filter, oldPredicate, true, context);
        if (isExprEqualIgnoreOrder(oldPredicate, newPredicate)) {
            return filter;
        } else {
            Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(newPredicate));
            return filter.withConjunctsAndChild(newConjuncts, filter.child());
        }
    }

    @Override
    public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, ExpressionRewriteContext context) {
        having = visitChildren(this, having, context);
        Expression oldPredicate = having.getPredicate();
        Expression newPredicate = replaceConstantsAndRewriteExpr(having, oldPredicate, true, context);
        if (isExprEqualIgnoreOrder(oldPredicate, newPredicate)) {
            return having;
        } else {
            Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(newPredicate));
            return having.withConjunctsAndChild(newConjuncts, having.child());
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, ExpressionRewriteContext context) {
        project = visitChildren(this, project, context);
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait =
                getChildEqualSetAndConstants(project, context);
        ImmutableList.Builder<NamedExpression> newProjectsBuilder
                = ImmutableList.builderWithExpectedSize(project.getProjects().size());
        for (NamedExpression expr : project.getProjects()) {
            newProjectsBuilder.add(
                    replaceNameExpressionConstants(expr, context, childEqualTrait.first, childEqualTrait.second));
        }

        List<NamedExpression> newProjects = newProjectsBuilder.build();
        return newProjects.equals(project.getProjects()) ? project : project.withProjects(newProjects);
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, ExpressionRewriteContext context) {
        sort = visitChildren(this, sort, context);
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait = getChildEqualSetAndConstants(sort, context);
        // for be, order key must be a column, not a literal, so `order by 100#xx` is ok,
        // but `order by 100` will make be core.
        // so after replaced, we need to remove the constant expr.
        ImmutableList.Builder<OrderKey> newOrderKeysBuilder
                = ImmutableList.builderWithExpectedSize(sort.getOrderKeys().size());
        for (OrderKey key : sort.getOrderKeys()) {
            Expression newExpr = replaceConstants(key.getExpr(), false, context,
                    childEqualTrait.first, childEqualTrait.second);
            if (!newExpr.isConstant()) {
                newOrderKeysBuilder.add(key.withExpression(newExpr));
            }
        }
        List<OrderKey> newOrderKeys = newOrderKeysBuilder.build();
        if (newOrderKeys.isEmpty()) {
            return sort.child();
        } else if (!newOrderKeys.equals(sort.getOrderKeys())) {
            return sort.withOrderKeys(newOrderKeys);
        } else {
            return sort;
        }
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, ExpressionRewriteContext context) {
        aggregate = visitChildren(this, aggregate, context);
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait =
                getChildEqualSetAndConstants(aggregate, context);

        List<Expression> oldGroupByExprs = aggregate.getGroupByExpressions();
        List<Expression> newGroupByExprs = Lists.newArrayListWithExpectedSize(oldGroupByExprs.size());
        for (Expression expr : oldGroupByExprs) {
            Expression newExpr = replaceConstants(expr, false, context, childEqualTrait.first, childEqualTrait.second);
            if (!newExpr.isConstant()) {
                newGroupByExprs.add(newExpr);
            }
        }

        // group by with literal and empty group by are different.
        // the former can return 0 row, the latter return at least 1 row.
        // when met all group by expression are constant,
        // 'eliminateGroupByConstant' will put a project(alias constant as slot) below the agg,
        // but this rule cann't put a project below the agg, otherwise this rule may cause a dead loop,
        // so when all replaced group by expression are constant, just let new group by add an origin group by.
        if (newGroupByExprs.isEmpty() && !oldGroupByExprs.isEmpty()) {
            newGroupByExprs.add(oldGroupByExprs.iterator().next());
        }
        Set<Expression> newGroupByExprSet = Sets.newHashSet(newGroupByExprs);

        List<NamedExpression> oldOutputExprs = aggregate.getOutputExpressions();
        List<NamedExpression> newOutputExprs = Lists.newArrayListWithExpectedSize(oldOutputExprs.size());
        ImmutableList.Builder<NamedExpression> projectBuilder
                = ImmutableList.builderWithExpectedSize(oldOutputExprs.size());

        boolean containsConstantOutput = false;

        // after normal agg, group by expressions and output expressions are slots,
        // after this rule, they may rewrite to literal, since literal are not slot,
        // we need eliminate the rewritten literals.
        for (NamedExpression expr : oldOutputExprs) {
            // ColumnPruning will also add all group by expression into output expressions
            // agg output need contains group by expression
            Expression replacedExpr = replaceConstants(expr, false, context,
                    childEqualTrait.first, childEqualTrait.second);
            Expression newOutputExpr = newGroupByExprSet.contains(expr) ? expr : replacedExpr;
            if (newOutputExpr instanceof NamedExpression) {
                newOutputExprs.add((NamedExpression) newOutputExpr);
            }

            if (replacedExpr.isConstant()) {
                projectBuilder.add(new Alias(expr.getExprId(), replacedExpr, expr.getName()));
                containsConstantOutput = true;
            } else {
                Preconditions.checkArgument(newOutputExpr instanceof NamedExpression, newOutputExpr);
                projectBuilder.add(((NamedExpression) newOutputExpr).toSlot());
            }
        }

        if (newGroupByExprs.equals(oldGroupByExprs) && newOutputExprs.equals(oldOutputExprs)) {
            return aggregate;
        }

        aggregate = aggregate.withGroupByAndOutput(newGroupByExprs, newOutputExprs);
        if (containsConstantOutput) {
            return PlanUtils.projectOrSelf(projectBuilder.build(), aggregate);
        } else {
            return aggregate;
        }
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, ExpressionRewriteContext context) {
        // Combine all the join conjuncts together, may infer more constant relations.
        // Then after rewrite the combine conjuncts, we need split the rewritten expression into hash/other/mark
        // join conjuncts. But we can not extract the mark join conjuncts from the rewritten expression.
        // So we only combine the hash conjuncts and other conjuncts.
        join = visitChildren(this, join, context);

        List<Expression> newHashJoinConjuncts = join.getHashJoinConjuncts();
        List<Expression> newOtherJoinConjuncts = join.getOtherJoinConjuncts();
        List<Expression> hashOtherConjuncts = Lists.newArrayListWithExpectedSize(
                join.getHashJoinConjuncts().size() + join.getOtherJoinConjuncts().size());
        hashOtherConjuncts.addAll(join.getHashJoinConjuncts());
        hashOtherConjuncts.addAll(join.getOtherJoinConjuncts());
        if (!hashOtherConjuncts.isEmpty()) {
            // useInnerInfer = true means for a nullable column 'column_a', will extract constant relation
            // (include 'nullable_a = column_b' and 'nullable_a = literal') from the expression itself,
            // then use the extracted constant relation + children's constant relation to rewrite the expression.
            // then its effect will result in: the special NULL (those all its ancestors are AND/OR) will be replaced
            // with FALSE;
            // so useInnerInfer = false will not replace the NULL with FALSE.
            // For null ware left anti join, NULL can not replace with FALSE.
            boolean useInnerInfer = join.getJoinType() != JoinType.NULL_AWARE_LEFT_ANTI_JOIN;
            Expression oldHashOtherPredicate = ExpressionUtils.and(hashOtherConjuncts);
            Expression newHashOtherPredicate
                    = replaceConstantsAndRewriteExpr(join, oldHashOtherPredicate, useInnerInfer, context);
            if (!isExprEqualIgnoreOrder(oldHashOtherPredicate, newHashOtherPredicate)) {
                // TODO: code from FindHashConditionForJoin
                Pair<List<Expression>, List<Expression>> pair
                        = JoinUtils.extractExpressionForHashTable(join.left().getOutput(), join.right().getOutput(),
                        ExpressionUtils.extractConjunction(newHashOtherPredicate));
                newHashJoinConjuncts = pair.first;
                newOtherJoinConjuncts = pair.second;
                if (Sets.newHashSet(newHashJoinConjuncts).equals(Sets.newHashSet(join.getHashJoinConjuncts()))) {
                    newHashJoinConjuncts = join.getHashJoinConjuncts();
                }
                if (Sets.newHashSet(newOtherJoinConjuncts).equals(Sets.newHashSet(join.getOtherJoinConjuncts()))) {
                    newOtherJoinConjuncts = join.getOtherJoinConjuncts();
                }
            }
        }

        List<Expression> newMarkJoinConjuncts = join.getMarkJoinConjuncts();
        if (!join.getMarkJoinConjuncts().isEmpty()) {
            // TODO: we may extract more constant relations from hash conjuncts,
            //       then we may make mark join conjuncts more simplify.
            // mark join conjuncts may rewrite to hash join conjuncts and join convert to null aware anti join.
            // we don't replaced NULL with FALSE in mark join conjuncts, so let useInnerInfer = false.
            Expression oldMarkPredicate = ExpressionUtils.and(join.getMarkJoinConjuncts());
            Expression newMarkPredicate = replaceConstantsAndRewriteExpr(join, oldMarkPredicate, false, context);
            newMarkJoinConjuncts = ExpressionUtils.extractConjunction(newMarkPredicate);
            if (Sets.newHashSet(newMarkJoinConjuncts).equals(Sets.newHashSet(join.getMarkJoinConjuncts()))) {
                newMarkJoinConjuncts = join.getMarkJoinConjuncts();
            }
        }

        if (newHashJoinConjuncts.equals(join.getHashJoinConjuncts())
                && newOtherJoinConjuncts.equals(join.getOtherJoinConjuncts())
                && newMarkJoinConjuncts.equals(join.getMarkJoinConjuncts())) {
            return join;
        }

        JoinType joinType = join.getJoinType();
        if (joinType == JoinType.CROSS_JOIN && !newHashJoinConjuncts.isEmpty()) {
            joinType = JoinType.INNER_JOIN;
        }

        return new LogicalJoin<>(joinType,
                newHashJoinConjuncts,
                newOtherJoinConjuncts,
                newMarkJoinConjuncts,
                join.getDistributeHint(),
                join.getMarkJoinSlotReference(),
                join.children(), join.getJoinReorderContext());
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> sink, ExpressionRewriteContext context) {
        sink = visitChildren(this, sink, context);
        // // for sql: create table t as select cast('1' as varchar(30))
        // // the select will add a parent plan: result sink. the result sink contains a output slot reference, and its
        // // data type is varchar(30),  but if replace the slot reference with a varchar literal '1',
        // // then the data type info varchar(30) will lost, because varchar literal '1' data type is always varchar(1),
        // // so t's column will get a error type. so we don't rewrite logical sink then.
        // Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait
        //         = getChildEqualSetAndConstants(sink, context);
        // List<NamedExpression> newOutputExprs = sink.getOutputExprs().stream()
        //         .map(expr ->
        //                 replaceNameExpressionConstants(expr, context, childEqualTrait.first, childEqualTrait.second))
        //         .collect(ImmutableList.toImmutableList());
        // return newOutputExprs.equals(sink.getOutputExprs()) ? sink : sink.withOutputExprs(newOutputExprs);
        return sink;
    }

    /**
     * replace constants and rewrite expression.
     */
    @VisibleForTesting
    public Expression replaceConstantsAndRewriteExpr(LogicalPlan plan, Expression expression,
            boolean useInnerInfer, ExpressionRewriteContext context) {
        // for expression `a = 1 and a + b = 2 and b + c = 2 and c + d =2 and ...`:
        // propagate constant `a = 1`, then get `1 + b = 2`, after rewrite this expression, will get `b = 1`;
        // then propagate constant `b = 1`, then get `1 + c = 2`, after rewrite this expression, will get `c = 1`,
        // ...
        // so constant propagate and rewrite expression need to do in a loop.
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait = getChildEqualSetAndConstants(plan, context);
        Expression afterExpression = expression;
        for (int i = 0; i < 100; i++) {
            Expression beforeExpression = afterExpression;
            afterExpression = replaceConstants(beforeExpression, useInnerInfer, context,
                    childEqualTrait.first, childEqualTrait.second);
            if (isExprEqualIgnoreOrder(beforeExpression, afterExpression)) {
                break;
            }
            if (afterExpression.isLiteral()) {
                break;
            }
            beforeExpression = afterExpression;
            afterExpression = ExpressionNormalizationAndOptimization.NO_MIN_MAX_RANGE_INSTANCE
                    .rewrite(beforeExpression, context);
        }
        return afterExpression;
    }

    // process NameExpression
    private NamedExpression replaceNameExpressionConstants(NamedExpression expr, ExpressionRewriteContext context,
            ImmutableEqualSet<Slot> equalSet, Map<Slot, Literal> constants) {

        // if a project item is a slot reference, and the slot equals to a constant value, don't rewrite it.
        // because rule `EliminateUnnecessaryProject ` can eliminate a project when the project's output slots equal to
        // its child's output slots.
        // for example, for `sink -> ... -> project(a, b, c) -> filter(a = 10)`
        // if rewrite project to project(alias 10 as a, b, c), later other rule may prune `alias 10 as a`, and project
        // will become project(b, c), so project and filter's output slot will not equal,
        // then the project cannot be eliminated.
        // so we don't replace SlotReference.
        // for safety reason, we only replace Alias
        if (!(expr instanceof Alias)) {
            return expr;
        }

        // PushProjectThroughUnion require projection is a slot reference, or like (cast slot reference as xx);
        // TODO: if PushProjectThroughUnion support projection like  `literal as xx`, then delete this check.
        if (ExpressionUtils.getExpressionCoveredByCast(expr.child(0)) instanceof SlotReference) {
            return expr;
        }

        Expression newExpr = replaceConstants(expr, false, context, equalSet, constants);
        if (newExpr instanceof NamedExpression) {
            return (NamedExpression) newExpr;
        } else {
            return new Alias(expr.getExprId(), newExpr, expr.getName());
        }
    }

    private Expression replaceConstants(Expression expression, boolean useInnerInfer, ExpressionRewriteContext context,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        if (expression instanceof And) {
            return replaceAndConstants((And) expression, useInnerInfer, context, parentEqualSet, parentConstants);
        } else if (expression instanceof Or) {
            return replaceOrConstants((Or) expression, useInnerInfer, context, parentEqualSet, parentConstants);
        } else if (!parentConstants.isEmpty()
                && expression.anyMatch(e -> e instanceof Slot && parentConstants.containsKey(e))) {
            Expression newExpr = ExpressionUtils.replaceIf(expression, parentConstants, this::canReplaceExpression);
            if (!newExpr.equals(expression)) {
                newExpr = FoldConstantRule.evaluate(newExpr, context);
            }
            return newExpr;
        } else {
            return expression;
        }
    }

    // process AND expression
    private Expression replaceAndConstants(And expression, boolean useInnerInfer, ExpressionRewriteContext context,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        List<Expression> conjunctions = ExpressionUtils.extractConjunction(expression);
        Optional<Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>>> equalAndConstantOptions =
                expandEqualSetAndConstants(conjunctions, useInnerInfer, parentEqualSet, parentConstants);
        // infer conflict constants like a = 10 and a = 30, then rewrite this AND to 'FALSE'
        // we have considered a may be null, the rewritten to FALSE is safe.
        // the explanation can see the annotation from function expandEqualSetAndConstants
        if (!equalAndConstantOptions.isPresent()) {
            return BooleanLiteral.FALSE;
        }
        Set<Slot> inputSlots = expression.getInputSlots();
        ImmutableEqualSet<Slot> newEqualSet = equalAndConstantOptions.get().first;
        Map<Slot, Literal> newConstants = equalAndConstantOptions.get().second;
        // myInferConstantSlots : the slots that are inferred by this expression, not inferred by parent
        // myInferConstantSlots[slot] = true means expression had contains conjunct `slot = constant`
        Map<Slot, Boolean> myInferConstantSlots = Maps.newLinkedHashMapWithExpectedSize(
                Math.max(0, newConstants.size() - parentConstants.size()));
        for (Slot slot : newConstants.keySet()) {
            if (!parentConstants.containsKey(slot)) {
                myInferConstantSlots.put(slot, false);
            }
        }
        ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(conjunctions.size());
        for (Expression child : conjunctions) {
            Expression newChild = child;
            // for expression, `a = 10 and a > b` will infer constant relation `a = 10`,
            // need to replace a with 10 to this expression,
            // for the first conjunction `a = 10`, no need to replace because after replace will get `10 = 10`,
            // for the second conjunction `a > b`, need replace and got `10 > b`
            if (needReplaceWithConstant(newChild, newConstants, myInferConstantSlots)) {
                newChild = replaceConstants(newChild, useInnerInfer, context, newEqualSet, newConstants);
            }
            if (newChild.equals(BooleanLiteral.FALSE)) {
                return BooleanLiteral.FALSE;
            }
            if (newChild instanceof And) {
                builder.addAll(ExpressionUtils.extractConjunction(newChild));
            } else {
                builder.add(newChild);
            }
        }
        // if the expression infer `slot = constant`, but not contains conjunct `slot = constant`, need to add it
        for (Map.Entry<Slot, Boolean> entry : myInferConstantSlots.entrySet()) {
            // if this expression don't contain the slot, no add it, to avoid the expression size increase too long
            if (!entry.getValue() && inputSlots.contains(entry.getKey())) {
                Slot slot = entry.getKey();
                EqualTo equal = new EqualTo(slot, newConstants.get(slot), true);
                builder.add(TypeCoercionUtils.processComparisonPredicate(equal));
            }
        }
        return expression.withChildren(builder.build());
    }

    // process OR expression
    private Expression replaceOrConstants(Or expression, boolean useInnerInfer, ExpressionRewriteContext context,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(expression);
        ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(disjunctions.size());
        for (Expression child : disjunctions) {
            Expression newChild = replaceConstants(child, useInnerInfer, context, parentEqualSet, parentConstants);
            if (newChild.equals(BooleanLiteral.TRUE)) {
                return BooleanLiteral.TRUE;
            }
            builder.add(newChild);
        }
        return expression.withChildren(builder.build());
    }

    private boolean needReplaceWithConstant(Expression expression, Map<Slot, Literal> constants,
            Map<Slot, Boolean> myInferConstantSlots) {
        if (expression instanceof EqualTo && expression.child(0) instanceof Slot) {
            Slot slot = (Slot) expression.child(0);
            // my infer constant slots contain this slot and hadn't replaced with slot=constant yet,
            // so let it alone and don't replace it.
            if (myInferConstantSlots.containsKey(slot)
                    && !myInferConstantSlots.get(slot)
                    && expression.child(1).equals(constants.get(slot))) {
                myInferConstantSlots.put(slot, true);
                return false;
            }
        }

        return true;
    }

    private boolean canReplaceExpression(Expression expression) {
        // 'a is not null', EliminateOuterJoin will call TypeUtils.isNotNull
        if (ExpressionUtils.isGeneratedNotNull(expression)) {
            return false;
        }

        // "https://doris.apache.org/docs/sql-manual/basic-element/operators/conditional-operators
        // /full-text-search-operators", the match function require left is a slot, not a literal.
        if (expression instanceof Match) {
            return false;
        }

        // EliminateJoinByFK, join with materialize view need keep `a = b`.
        // But for a common join, need eliminate `a = b`, after eliminate hash join equations,
        // hash join will change to nested loop join.
        // Join with materialize view will no more need `a = b` later.
        // TODO: replace constants with `a = b`
        if (expression instanceof EqualPredicate
                && expression.child(0) instanceof SlotReference
                && expression.child(1) instanceof SlotReference) {
            SlotReference left = (SlotReference) expression.child(0);
            SlotReference right = (SlotReference) expression.child(1);
            return left.getQualifier().equals(right.getQualifier());
        }

        return true;
    }

    private Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> getChildEqualSetAndConstants(
            LogicalPlan plan, ExpressionRewriteContext context) {
        if (plan.children().size() == 1) {
            DataTrait dataTrait = plan.child(0).getLogicalProperties().getTrait();
            return Pair.of(dataTrait.getEqualSet(), getConstantUniforms(dataTrait.getAllUniformValues(), context));
        } else {
            Map<Slot, Literal> uniformConstants = Maps.newHashMap();
            ImmutableEqualSet.Builder<Slot> newEqualSetBuilder = new Builder<>();
            for (Plan child : plan.children()) {
                uniformConstants.putAll(
                        getConstantUniforms(child.getLogicalProperties().getTrait().getAllUniformValues(), context));
                newEqualSetBuilder.addEqualSet(child.getLogicalProperties().getTrait().getEqualSet());
            }
            return Pair.of(newEqualSetBuilder.build(), uniformConstants);
        }
    }

    private Map<Slot, Literal> getConstantUniforms(Map<Slot, Optional<Expression>> uniformValues,
            ExpressionRewriteContext context) {
        Map<Slot, Literal> constantValues = Maps.newHashMap();
        for (Map.Entry<Slot, Optional<Expression>> entry : uniformValues.entrySet()) {
            Expression expr = entry.getValue().isPresent() ? entry.getValue().get() : null;
            if (expr == null || !expr.isConstant()) {
                continue;
            }
            if (!expr.isLiteral()) {
                // uniforms values contains like 'cast(11 as smallint)'
                expr = FoldConstantRule.evaluate(expr, context);
                if (!expr.isLiteral()) {
                    continue;
                }
            }
            constantValues.put(entry.getKey(), (Literal) expr);
        }

        return constantValues;
    }

    /**
     *
     * Extract equal set and constants from conjunctions, then combine them with parentEqualSet and parentConstants.
     * If met conflict constants relation, return optional.empty().
     */
    private Optional<Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>>> expandEqualSetAndConstants(
            List<Expression> conjunctions,
            boolean useInnerInfer,
            ImmutableEqualSet<Slot> parentEqualSet,
            Map<Slot, Literal> parentConstants) {
        // infer conflict constants like a = 10 and a = 30, then rewrite this AND to 'FALSE'
        // we think this is conflict only when:
        // 1) a is not nullable;
        // 2) a is nullable, and it must satisfy the below:
        //    i) the expression is FILTER or HAVING or JOIN condition, so for a PROJECT `a = 10 and a = 30`
        //       will not evaluate to FALSE
        //    ii) this expression is an expression root, or all its ancestors are AND/OR,
        //        and then, this expression can never evaluate to 'TRUE', and can safe replace with 'FALSE'.
        //        for example, for a FILTER expression: '(a = 10 and a = 20) or not(b = 10 and b = 20)',
        //        'a = 10 and a = 20' can evaluate to FALSE  because its ancestors {OR} are all AND/OR,
        //        but 'b=10 and b=20' can not evaluate to FALSE because its ancestors {NOT, OR} contains NOT.
        //
        // so how to achieve this ?
        // use arg useInnerInfer means whether extract constant
        // from the rewritten expression itself, for example, for a=10:
        // 1) if a is not nullable, extract it;
        // 2) if a is nullable, then check useInnerInfer, only useInnerInfer=true, can extract it.
        //    i) only FILTER/HAVING/JOIN,  useInnerInfer may be true, and other plan will use useInnerInfer=false
        //    ii) replace the expression from top to down, and split the expression into AND / OR sub expressions,
        //        for '(a = 10 and a = 20) or not(b = 10 and b = 20)'
        //        first split the OR into two sub expression: (a=10 and a=20),   not(b=10 and b=20)
        //        a = 10 and a = 20 is AND, then recurse call replaceAndConstant to handle it,
        //        not(b = 10 and b = 20) is not AND/OR, then just replace the expression with upper constants,
        //        and will not extract b = 10 and b = 20
        Map<Slot, Literal> newConstants = Maps.newLinkedHashMapWithExpectedSize(parentConstants.size());
        newConstants.putAll(parentConstants);
        ImmutableEqualSet.Builder<Slot> newEqualSetBuilder = new Builder<>(parentEqualSet);
        for (Expression child : conjunctions) {
            Optional<Pair<Slot, Expression>> equalItem = findValidEqualItem(child);
            if (!equalItem.isPresent()) {
                continue;
            }
            Slot slot = equalItem.get().first;
            Expression expr = equalItem.get().second;
            // for expression `a = 1 and a * 3 = 10`
            // if it's in LogicalFilter, then we can infer constant relation `a = 1`, then have:
            // `a = 1 and 1 * 3 = 10` => `a = 1 and FALSE` => `FALSE`
            // but if it's in LogicalProject, then we shouldn't propagate `a=1` to this expression,
            // because a may be null, then `a=1 and a * 3 = 10` should evaluate to `NULL` in the project.
            if (!useInnerInfer && (slot.nullable() || expr.nullable())) {
                continue;
            }
            if (expr instanceof Slot) {
                newEqualSetBuilder.addEqualPair(slot, (Slot) expr);
            } else if (!addConstant(newConstants, slot, (Literal) expr)) {
                return Optional.empty();
            }
        }

        ImmutableEqualSet<Slot> newEqualSet = newEqualSetBuilder.build();
        List<Set<Slot>> multiEqualSlots = newEqualSet.calEqualSetList();
        for (Set<Slot> slots : multiEqualSlots) {
            Slot slot = null;
            for (Slot s : slots) {
                if (newConstants.containsKey(s)) {
                    slot = s;
                    break;
                }
            }
            if (slot == null) {
                continue;
            }
            Literal value = newConstants.get(slot);
            for (Slot s : slots) {
                if (!addConstant(newConstants, s, value)) {
                    return Optional.empty();
                }
            }
        }

        return Optional.of(Pair.of(newEqualSet, newConstants));
    }

    // add a unique constant, if a slot have two different constants value, add fail.
    // for example: a = 10 and a = 20, when add 'a = 10', return true,
    // but later add 'a = 20' will meet a conflict and return false
    private boolean addConstant(Map<Slot, Literal> constants, Slot slot, Literal value) {
        Literal existValue = constants.get(slot);
        if (existValue == null) {
            constants.put(slot, value);
            return true;
        }
        // value equals existsValue, or compare them return 0
        return value.equals(existValue)
                || (value instanceof ComparableLiteral
                    && existValue instanceof ComparableLiteral
                    && ((ComparableLiteral) value).compareTo((ComparableLiteral) existValue) == 0);
    }

    private Optional<Pair<Slot, Expression>> findValidEqualItem(Expression expression) {
        if (!(expression instanceof EqualPredicate)) {
            return Optional.empty();
        }

        Expression left = expression.child(0);
        Expression right = expression.child(1);
        if (!PredicateInferUtils.isSlotOrNotNullLiteral(left) || !PredicateInferUtils.isSlotOrNotNullLiteral(right)) {
            return Optional.empty();
        }

        if (left instanceof Slot) {
            return Optional.of(Pair.of((Slot) left, right));
        } else if (right instanceof Slot) {
            return Optional.of(Pair.of((Slot) right, left));
        } else {
            return Optional.empty();
        }
    }

    private boolean isExprEqualIgnoreOrder(Expression oldExpr, Expression newExpr) {
        if (oldExpr instanceof And) {
            return Sets.newHashSet(ExpressionUtils.extractConjunction(oldExpr))
                    .equals(Sets.newHashSet(ExpressionUtils.extractConjunction(newExpr)));
        } else if (oldExpr instanceof Or) {
            return Sets.newHashSet(ExpressionUtils.extractDisjunction(oldExpr))
                    .equals(Sets.newHashSet(ExpressionUtils.extractDisjunction(newExpr)));
        } else {
            return oldExpr.equals(newExpr);
        }
    }
}
