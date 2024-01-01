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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.AppliedAwareRule.AppliedAwareRuleCondition;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.UsingJoin;
import org.apache.doris.nereids.trees.plans.visitor.InferPlanOutputAlias;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BindSlotReference.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BindExpression implements AnalysisRuleFactory {

    private Scope toScope(CascadesContext cascadesContext, List<Slot> slots) {
        Optional<Scope> outerScope = cascadesContext.getOuterScope();
        if (outerScope.isPresent()) {
            return new Scope(outerScope, slots, outerScope.get().getSubquery());
        } else {
            return new Scope(slots);
        }
    }

    @Override
    public List<Rule> buildRules() {
        /*
         * some rules not only depends on the condition Plan::canBind, for example,
         * BINDING_FILTER_SLOT need transform 'filter(unix_timestamp() > 100)' to
         * 'filter(unix_timestamp() > cast(100 as int))'. there is no any unbound expression
         * in the filter, so the Plan::canBind return false.
         *
         * we need `isAppliedRule` to judge whether a plan is applied to a rule, so need convert
         * the normal rule to `AppliedAwareRule` to read and write the mutable state.
         */
        AppliedAwareRuleCondition ruleCondition = new AppliedAwareRuleCondition() {
            @Override
            protected boolean condition(Rule rule, Plan plan) {
                if (!rule.getPattern().matchRoot(plan)) {
                    return false;
                }
                return plan.canBind() || (plan.bound() && !isAppliedRule(rule, plan));
            }
        };

        return ImmutableList.of(
            RuleType.BINDING_PROJECT_SLOT.build(
                logicalProject().thenApply(ctx -> {
                    LogicalProject<Plan> project = ctx.root;
                    List<NamedExpression> boundProjections =
                            bindSlot(project.getProjects(), project.child(), ctx.cascadesContext);
                    if (boundProjections.stream().anyMatch(BoundStar.class::isInstance)) {
                        List<NamedExpression> boundExceptions = project.getExcepts().isEmpty() ? ImmutableList.of()
                                : bindSlot(project.getExcepts(), project.child(), ctx.cascadesContext);
                        boundProjections = flatBoundStar(boundProjections, boundExceptions);
                    }
                    boundProjections = boundProjections.stream()
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());
                    return project.withProjects(boundProjections);
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;
                    Set<Expression> boundConjuncts = filter.getConjuncts().stream()
                            .map(expr -> bindSlot(expr, filter.child(), ctx.cascadesContext))
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                            .collect(ImmutableSet.toImmutableSet());
                    return new LogicalFilter<>(boundConjuncts, filter.child());
                })
            ),

            RuleType.BINDING_USING_JOIN_SLOT.build(
                usingJoin().thenApply(ctx -> {
                    UsingJoin<Plan, Plan> using = ctx.root;
                    LogicalJoin<Plan, Plan> lj = new LogicalJoin<>(using.getJoinType() == JoinType.CROSS_JOIN
                            ? JoinType.INNER_JOIN : using.getJoinType(),
                            using.getHashJoinConjuncts(),
                            using.getOtherJoinConjuncts(), using.getHint(), using.getMarkJoinSlotReference(),
                            using.children());
                    List<Expression> unboundSlots = lj.getHashJoinConjuncts();
                    Set<String> slotNames = new HashSet<>();
                    List<Slot> leftOutput = new ArrayList<>(lj.left().getOutput());
                    // Suppose A JOIN B USING(name) JOIN C USING(name), [A JOIN B] is the left node, in this case,
                    // C should combine with table B on C.name=B.name. so we reverse the output to make sure that
                    // the most right slot is matched with priority.
                    Collections.reverse(leftOutput);
                    List<Expression> leftSlots = new ArrayList<>();
                    Scope scope = toScope(ctx.cascadesContext, leftOutput.stream()
                            .filter(s -> !slotNames.contains(s.getName()))
                            .peek(s -> slotNames.add(s.getName()))
                            .collect(Collectors.toList()));
                    for (Expression unboundSlot : unboundSlots) {
                        Expression expression = new SlotBinder(scope, ctx.cascadesContext).bind(unboundSlot);
                        leftSlots.add(expression);
                    }
                    slotNames.clear();
                    scope = toScope(ctx.cascadesContext, lj.right().getOutput().stream()
                            .filter(s -> !slotNames.contains(s.getName()))
                            .peek(s -> slotNames.add(s.getName()))
                            .collect(Collectors.toList()));
                    List<Expression> rightSlots = new ArrayList<>();
                    for (Expression unboundSlot : unboundSlots) {
                        Expression expression = new SlotBinder(scope, ctx.cascadesContext).bind(unboundSlot);
                        rightSlots.add(expression);
                    }
                    int size = leftSlots.size();
                    List<Expression> hashEqExpr = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        hashEqExpr.add(new EqualTo(leftSlots.get(i), rightSlots.get(i)));
                    }
                    return lj.withJoinConjuncts(hashEqExpr, lj.getOtherJoinConjuncts());
                })
            ),
            RuleType.BINDING_JOIN_SLOT.build(
                logicalJoin().thenApply(ctx -> {
                    LogicalJoin<Plan, Plan> join = ctx.root;
                    List<Expression> cond = join.getOtherJoinConjuncts().stream()
                            .map(expr -> bindSlot(expr, join.children(), ctx.cascadesContext))
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                            .collect(Collectors.toList());
                    List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                            .map(expr -> bindSlot(expr, join.children(), ctx.cascadesContext))
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                            .collect(Collectors.toList());
                    return new LogicalJoin<>(join.getJoinType(),
                            hashJoinConjuncts, cond, join.getHint(), join.getMarkJoinSlotReference(),
                            join.children());
                })
            ),
            RuleType.BINDING_AGGREGATE_SLOT.build(
                logicalAggregate().thenApply(ctx -> {
                    LogicalAggregate<Plan> agg = ctx.root;
                    List<NamedExpression> output = agg.getOutputExpressions().stream()
                            .map(expr -> bindSlot(expr, agg.child(), ctx.cascadesContext))
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());

                    // The columns referenced in group by are first obtained from the child's output,
                    // and then from the node's output
                    Set<String> duplicatedSlotNames = new HashSet<>();
                    Map<String, Expression> childOutputsToExpr = agg.child().getOutput().stream()
                            .collect(Collectors.toMap(Slot::getName, Slot::toSlot,
                                    (oldExpr, newExpr) -> {
                                        duplicatedSlotNames.add(((Slot) oldExpr).getName());
                                        return oldExpr;
                                }));
                    /*
                    GroupByKey binding priority:
                    1. child.output
                    2. agg.output
                    CASE 1
                     k is not in agg.output
                     plan:
                         agg(group_by: k)
                          +---child(output t1.k, t2.k)

                     group_by_key: k is ambiguous, t1.k and t2.k are candidate.

                    CASE 2
                     k is in agg.output
                     plan:
                         agg(group_by: k, output (k+1 as k)
                          +---child(output t1.k, t2.k)

                     it is failed to bind group_by_key with child.output(ambiguous), but group_by_key can be bound with
                     agg.output

                    CASE 3
                     group by key cannot bind with agg func
                     plan:
                        agg(group_by v, output sum(k) as v)
                     throw AnalysisException

                    CASE 4
                     sql:
                        `select count(1) from t1 join t2 group by a`
                     we cannot bind `group by a`, because it is ambiguous (t1.a and t2.a)

                    CASE 5
                     following case 4, if t1.a is in agg.output, we can bind `group by a` to t1.a
                     sql
                        select t1.a
                        from t1 join t2 on t1.a = t2.a
                        group by a
                     group_by_key is bound on t1.a
                    */
                    duplicatedSlotNames.forEach(childOutputsToExpr::remove);
                    for (int i = 0; i < output.size(); i++) {
                        if (!(output.get(i) instanceof Alias)) {
                            continue;
                        }
                        Alias alias = (Alias) output.get(i);
                        if (alias.child().anyMatch(expr -> expr instanceof AggregateFunction)) {
                            continue;
                        }
                        /*
                            Alias(x) has been bound by binding agg's output
                            we add x to childOutputsToExpr, so when binding group by exprs later, we can use x directly
                            and won't bind it again
                            select
                              p_cycle_time / (select max(p_cycle_time) from log_event_8)  as 'x',
                              count(distinct case_id) as 'y'
                            from
                              log_event_8
                            group by
                              x
                         */
                        childOutputsToExpr.putIfAbsent(alias.getName(), output.get(i).child(0));
                    }

                    Set<Expression> boundedGroupByExpressions = Sets.newHashSet();
                    List<Expression> replacedGroupBy = agg.getGroupByExpressions().stream()
                            .map(groupBy -> {
                                if (groupBy instanceof UnboundSlot) {
                                    UnboundSlot unboundSlot = (UnboundSlot) groupBy;
                                    if (unboundSlot.getNameParts().size() == 1) {
                                        String name = unboundSlot.getNameParts().get(0);
                                        if (childOutputsToExpr.containsKey(name)) {
                                            Expression expression = childOutputsToExpr.get(name);
                                            boundedGroupByExpressions.add(expression);
                                            return expression;
                                        }
                                    }
                                }
                                return groupBy;
                            }).collect(Collectors.toList());
                    /*
                    according to case 4 and case 5, we construct boundSlots
                    */
                    Set<String> outputSlotNames = Sets.newHashSet();
                    Set<Slot> outputSlots = output.stream()
                            .filter(SlotReference.class::isInstance)
                            .peek(slot -> outputSlotNames.add(slot.getName()))
                            .map(NamedExpression::toSlot)
                            .collect(Collectors.toSet());
                    // suppose group by key is a.
                    // if both t1.a and t2.a are in agg.child.output, and t1.a in agg.output,
                    // bind group_by_key a with t1.a
                    // ` .filter(slot -> !outputSlotNames.contains(slot.getName()))`
                    // is used to avoid add t2.a into boundSlots
                    Set<Slot> boundSlots = agg.child().getOutputSet().stream()
                            .filter(slot -> !outputSlotNames.contains(slot.getName()))
                            .collect(Collectors.toSet());

                    boundSlots.addAll(outputSlots);
                    SlotBinder binder = new SlotBinder(
                            toScope(ctx.cascadesContext, ImmutableList.copyOf(boundSlots)), ctx.cascadesContext);
                    SlotBinder childBinder = new SlotBinder(
                            toScope(ctx.cascadesContext, ImmutableList.copyOf(agg.child().getOutputSet())),
                            ctx.cascadesContext);

                    List<Expression> groupBy = replacedGroupBy.stream()
                            .map(expression -> {
                                if (boundedGroupByExpressions.contains(expression)) {
                                    // expr has been bound by binding agg's output
                                    return expression;
                                } else {
                                    // bind slot for unbound exprs
                                    Expression e = binder.bind(expression);
                                    if (e instanceof UnboundSlot) {
                                        return childBinder.bind(e);
                                    }
                                    return e;
                                }
                            })
                            .collect(Collectors.toList());
                    groupBy.forEach(expression -> checkBoundExceptLambda(expression, ctx.root));
                    groupBy = groupBy.stream()
                            // bind function for unbound exprs or return old expr if it's bound by binding agg's output
                            .map(expr -> boundedGroupByExpressions.contains(expr) ? expr
                                    : bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());
                    checkIfOutputAliasNameDuplicatedForGroupBy(groupBy, output);
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            ),
            RuleType.BINDING_REPEAT_SLOT.build(
                logicalRepeat().thenApply(ctx -> {
                    LogicalRepeat<Plan> repeat = ctx.root;
                    List<NamedExpression> output = repeat.getOutputExpressions().stream()
                            .map(expr -> bindSlot(expr, repeat.child(), ctx.cascadesContext))
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());

                    // The columns referenced in group by are first obtained from the child's output,
                    // and then from the node's output
                    Map<String, Expression> childOutputsToExpr = repeat.child().getOutput().stream()
                            .collect(Collectors.toMap(Slot::getName, Slot::toSlot, (oldExpr, newExpr) -> oldExpr));
                    Map<String, Expression> aliasNameToExpr = output.stream()
                            .filter(ne -> ne instanceof Alias)
                            .map(Alias.class::cast)
                            .collect(Collectors.toMap(Alias::getName, UnaryNode::child, (oldExpr, newExpr) -> oldExpr));
                    aliasNameToExpr.forEach(childOutputsToExpr::putIfAbsent);

                    List<List<Expression>> replacedGroupingSets = repeat.getGroupingSets().stream()
                            .map(groupBy ->
                                groupBy.stream().map(expr -> {
                                    if (expr instanceof UnboundSlot) {
                                        UnboundSlot unboundSlot = (UnboundSlot) expr;
                                        if (unboundSlot.getNameParts().size() == 1) {
                                            String name = unboundSlot.getNameParts().get(0);
                                            if (childOutputsToExpr.containsKey(name)) {
                                                return childOutputsToExpr.get(name);
                                            }
                                        }
                                    }
                                    return expr;
                                }).collect(Collectors.toList())
                            ).collect(Collectors.toList());

                    List<List<Expression>> groupingSets = replacedGroupingSets
                            .stream()
                            .map(groupingSet -> groupingSet.stream()
                                    .map(expr -> bindSlot(expr, repeat.child(), ctx.cascadesContext))
                                    .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                                    .collect(ImmutableList.toImmutableList()))
                            .collect(ImmutableList.toImmutableList());
                    List<NamedExpression> newOutput = adjustNullableForRepeat(groupingSets, output);
                    groupingSets.forEach(list -> checkIfOutputAliasNameDuplicatedForGroupBy(list, newOutput));

                    // check all GroupingScalarFunction inputSlots must be from groupingExprs
                    Set<Slot> groupingExprs = groupingSets.stream()
                            .flatMap(Collection::stream).map(expr -> expr.getInputSlots())
                            .flatMap(Collection::stream).collect(Collectors.toSet());
                    Set<GroupingScalarFunction> groupingScalarFunctions = ExpressionUtils
                            .collect(newOutput, GroupingScalarFunction.class::isInstance);
                    for (GroupingScalarFunction function : groupingScalarFunctions) {
                        if (!groupingExprs.containsAll(function.getInputSlots())) {
                            throw new AnalysisException("Column in " + function.getName()
                                    + " does not exist in GROUP BY clause.");
                        }
                    }
                    return repeat.withGroupSetsAndOutput(groupingSets, newOutput);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(aggregate()).thenApply(ctx -> {
                    LogicalSort<Aggregate<Plan>> sort = ctx.root;
                    Aggregate<Plan> aggregate = sort.child();
                    return bindSort(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(aggregate())).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<Aggregate<Plan>>> sort = ctx.root;
                    Aggregate<Plan> aggregate = sort.child().child();
                    return bindSort(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(logicalProject())).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<LogicalProject<Plan>>> sort = ctx.root;
                    LogicalProject<Plan> project = sort.child().child();
                    return bindSort(sort, project, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalProject()).thenApply(ctx -> {
                    LogicalSort<LogicalProject<Plan>> sort = ctx.root;
                    LogicalProject<Plan> project = sort.child();
                    return bindSort(sort, project, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalCTEConsumer()).thenApply(ctx -> {
                    LogicalSort<LogicalCTEConsumer> sort = ctx.root;
                    LogicalCTEConsumer cteConsumer = sort.child();
                    return bindSort(sort, cteConsumer, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalCTEAnchor()).thenApply(ctx -> {
                    LogicalSort<LogicalCTEAnchor<Plan, Plan>> sort = ctx.root;
                    LogicalCTEAnchor<Plan, Plan> cteAnchor = sort.child();
                    return bindSort(sort, cteAnchor, ctx.cascadesContext);
                })
            ), RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalOneRowRelation()).thenApply(ctx -> {
                    LogicalSort<LogicalOneRowRelation> sort = ctx.root;
                    LogicalOneRowRelation oneRowRelation = sort.child();
                    return bindSort(sort, oneRowRelation, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SET_OPERATION_SLOT.build(
                logicalSort(logicalSetOperation()).thenApply(ctx -> {
                    LogicalSort<LogicalSetOperation> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bindSlot(orderKey.getExpr(), sort.child(), ctx.cascadesContext);
                                item = bindFunction(item, ctx.root, ctx.cascadesContext);
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());
                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(aggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalHaving<Aggregate<Plan>> having = ctx.root;
                    Aggregate<Plan> childPlan = having.child();
                    Set<Expression> boundConjuncts = having.getConjuncts().stream()
                            .map(expr -> {
                                expr = bindSlot(expr, childPlan.child(), ctx.cascadesContext, false);
                                return bindSlot(expr, childPlan, ctx.cascadesContext, false);
                            })
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                            .collect(Collectors.toSet());
                    checkIfOutputAliasNameDuplicatedForGroupBy(ImmutableList.copyOf(boundConjuncts),
                            childPlan.getOutputExpressions());
                    return new LogicalHaving<>(boundConjuncts, having.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(any()).thenApply(ctx -> {
                    LogicalHaving<Plan> having = ctx.root;
                    Plan childPlan = having.child();
                    Set<Expression> boundConjuncts = having.getConjuncts().stream()
                            .map(expr -> {
                                expr = bindSlot(expr, childPlan, ctx.cascadesContext, false);
                                return bindSlot(expr, childPlan.children(), ctx.cascadesContext, false);
                            })
                            .map(expr -> bindFunction(expr, ctx.root, ctx.cascadesContext))
                            .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                            .collect(Collectors.toSet());
                    checkIfOutputAliasNameDuplicatedForGroupBy(ImmutableList.copyOf(boundConjuncts),
                            childPlan.getOutput().stream().map(NamedExpression.class::cast)
                                    .collect(Collectors.toList()));
                    return new LogicalHaving<>(boundConjuncts, having.child());
                })
            ),
            RuleType.BINDING_INLINE_TABLE_SLOT.build(
                logicalInlineTable().thenApply(ctx -> {
                    LogicalInlineTable logicalInlineTable = ctx.root;
                    // ensure all expressions are valid.
                    List<LogicalPlan> relations
                            = Lists.newArrayListWithCapacity(logicalInlineTable.getConstantExprsList().size());
                    for (int i = 0; i < logicalInlineTable.getConstantExprsList().size(); i++) {
                        if (logicalInlineTable.getConstantExprsList().get(i).stream()
                                .anyMatch(DefaultValueSlot.class::isInstance)) {
                            throw new AnalysisException("Default expression"
                                    + " can't exist in SELECT statement at row " + (i + 1));
                        }
                        relations.add(new UnboundOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                                logicalInlineTable.getConstantExprsList().get(i)));
                    }
                    // construct union all tree
                    return LogicalPlanBuilder.reduceToLogicalPlanTree(0, relations.size() - 1,
                            relations, Qualifier.ALL);
                })
            ),
            RuleType.BINDING_ONE_ROW_RELATION_SLOT.build(
                // we should bind UnboundAlias in the UnboundOneRowRelation
                unboundOneRowRelation().thenApply(ctx -> {
                    UnboundOneRowRelation oneRowRelation = ctx.root;
                    List<NamedExpression> projects = oneRowRelation.getProjects()
                            .stream()
                            .map(project -> bindSlot(project, ImmutableList.of(), ctx.cascadesContext))
                            .map(project -> bindFunction(project, ctx.root, ctx.cascadesContext))
                            .collect(Collectors.toList());
                    return new LogicalOneRowRelation(oneRowRelation.getRelationId(), projects);
                })
            ),
            RuleType.BINDING_SET_OPERATION_SLOT.build(
                // LogicalSetOperation don't bind again if LogicalSetOperation.outputs is not empty, this is special
                // we should not remove LogicalSetOperation::canBind, because in default case, the plan can run into
                // bind callback if not bound or **not run into bind callback yet**.
                logicalSetOperation().when(LogicalSetOperation::canBind).then(setOperation -> {
                    // check whether the left and right child output columns are the same
                    if (setOperation.child(0).getOutput().size() != setOperation.child(1).getOutput().size()) {
                        throw new AnalysisException("Operands have unequal number of columns:\n"
                                + "'" + setOperation.child(0).getOutput() + "' has "
                                + setOperation.child(0).getOutput().size() + " column(s)\n"
                                + "'" + setOperation.child(1).getOutput() + "' has "
                                + setOperation.child(1).getOutput().size() + " column(s)");
                    }

                    // INTERSECT and EXCEPT does not support ALL qualified
                    if (setOperation.getQualifier() == Qualifier.ALL
                            && (setOperation instanceof LogicalExcept || setOperation instanceof LogicalIntersect)) {
                        throw new AnalysisException("INTERSECT and EXCEPT does not support ALL qualified");
                    }
                    // we need to do cast before set operation, because we maybe use these slot to do shuffle
                    // so, we must cast it before shuffle to get correct hash code.
                    List<List<NamedExpression>> childrenProjections = setOperation.collectChildrenProjections();
                    ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
                    Builder<Plan> newChildren = ImmutableList.builder();
                    for (int i = 0; i < childrenProjections.size(); i++) {
                        Plan newChild;
                        if (childrenProjections.stream().allMatch(SlotReference.class::isInstance)) {
                            newChild = setOperation.child(i);
                        } else {
                            newChild = new LogicalProject<>(childrenProjections.get(i), setOperation.child(i));
                        }
                        newChildren.add(newChild);
                        childrenOutputs.add(newChild.getOutput().stream()
                                .map(SlotReference.class::cast)
                                .collect(ImmutableList.toImmutableList()));
                    }
                    setOperation = setOperation.withChildrenAndTheirOutputs(
                            newChildren.build(), childrenOutputs.build());
                    List<NamedExpression> newOutputs = setOperation.buildNewOutputs();
                    return setOperation.withNewOutputs(newOutputs);
                })
            ),
            RuleType.BINDING_GENERATE_SLOT.build(
                logicalGenerate().thenApply(ctx -> {
                    LogicalGenerate<Plan> generate = ctx.root;
                    List<Function> boundSlotGenerators
                            = bindSlot(generate.getGenerators(), generate.child(), ctx.cascadesContext);
                    List<Function> boundFunctionGenerators = boundSlotGenerators.stream()
                            .map(f -> bindTableGeneratingFunction((UnboundFunction) f, ctx.root, ctx.cascadesContext))
                            .collect(Collectors.toList());
                    Builder<Slot> slotBuilder = ImmutableList.builder();
                    for (int i = 0; i < generate.getGeneratorOutput().size(); i++) {
                        Function generator = boundFunctionGenerators.get(i);
                        UnboundSlot slot = (UnboundSlot) generate.getGeneratorOutput().get(i);
                        Preconditions.checkState(slot.getNameParts().size() == 2,
                                "the size of nameParts of UnboundSlot in LogicalGenerate must be 2.");
                        Slot boundSlot = new SlotReference(slot.getNameParts().get(1), generator.getDataType(),
                                generator.nullable(), ImmutableList.of(slot.getNameParts().get(0)));
                        slotBuilder.add(boundSlot);
                    }
                    return new LogicalGenerate<>(boundFunctionGenerators, slotBuilder.build(), generate.child());
                })
            ),
            RuleType.BINDING_UNBOUND_TVF_RELATION_FUNCTION.build(
                unboundTVFRelation().thenApply(ctx -> {
                    UnboundTVFRelation relation = ctx.root;
                    return bindTableValuedFunction(relation, ctx.statementContext);
                })
            ),
            RuleType.BINDING_SUBQUERY_ALIAS_SLOT.build(
                logicalSubQueryAlias().thenApply(ctx -> {
                    LogicalSubQueryAlias<Plan> subQueryAlias = ctx.root;
                    checkSameNameSlot(subQueryAlias.child(0).getOutput(), subQueryAlias.getAlias());
                    return subQueryAlias;
                })
            ),
            RuleType.BINDING_RESULT_SINK.build(
                unboundResultSink().thenApply(ctx -> {
                    LogicalSink<Plan> sink = ctx.root;
                    if (ctx.connectContext.getState().isQuery()) {
                        List<NamedExpression> outputExprs = sink.child().getOutput().stream()
                                .map(NamedExpression.class::cast)
                                .collect(ImmutableList.toImmutableList());
                        return new LogicalResultSink<>(outputExprs, sink.child());
                    }
                    // Should infer column name for expression when query command
                    final ImmutableListMultimap.Builder<ExprId, Integer> exprIdToIndexMapBuilder =
                            ImmutableListMultimap.builder();
                    List<Slot> childOutput = sink.child().getOutput();
                    for (int index = 0; index < childOutput.size(); index++) {
                        exprIdToIndexMapBuilder.put(childOutput.get(index).getExprId(), index);
                    }
                    InferPlanOutputAlias aliasInfer = new InferPlanOutputAlias(childOutput);
                    sink.child().accept(aliasInfer, exprIdToIndexMapBuilder.build());
                    return new LogicalResultSink<>(aliasInfer.getOutputs(), sink.child());
                })
            )
        ).stream().map(ruleCondition).collect(ImmutableList.toImmutableList());
    }

    private Plan bindSort(LogicalSort<? extends Plan> sort, Plan plan, CascadesContext ctx) {
        // 1. We should deduplicate the slots, otherwise the binding process will fail due to the
        //    ambiguous slots exist.
        // 2. try to bound order-key with agg output, if failed, try to bound with output of agg.child
        //    binding priority example:
        //        select
        //        col1 * -1 as col1    # inner_col1 * -1 as alias_col1
        //        from
        //                (
        //                        select 1 as col1
        //                        union
        //                        select -2 as col1
        //                ) t
        //        group by col1
        //        order by col1;     # order by order_col1
        //    bind order_col1 with alias_col1, then, bind it with inner_col1
        List<OrderKey> sortItemList = sort.getOrderKeys()
                .stream()
                .map(orderKey -> {
                    Expression item = bindSlot(orderKey.getExpr(), plan, ctx, true, false);
                    item = bindSlot(item, plan.children(), ctx, true, false);
                    item = bindFunction(item, sort, ctx);
                    return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                }).collect(Collectors.toList());
        return new LogicalSort<>(sortItemList, sort.child());
    }

    private List<NamedExpression> flatBoundStar(
            List<NamedExpression> boundSlots,
            List<NamedExpression> boundExceptions) {
        return boundSlots
            .stream()
            .flatMap(slot -> {
                if (slot instanceof BoundStar) {
                    return ((BoundStar) slot).getSlots().stream();
                } else {
                    return Stream.of(slot);
                }
            })
            .filter(s -> !boundExceptions.contains(s))
            .collect(ImmutableList.toImmutableList());
    }

    private <E extends Expression> List<E> bindSlot(
            List<E> exprList, Plan input, CascadesContext cascadesContext) {
        List<E> slots = new ArrayList<>(exprList.size());
        for (E expr : exprList) {
            E result = bindSlot(expr, input, cascadesContext);
            slots.add(result);
        }
        return slots;
    }

    private <E extends Expression> E bindSlot(E expr, Plan input, CascadesContext cascadesContext) {
        return bindSlot(expr, input, cascadesContext, true, true);
    }

    private <E extends Expression> E bindSlot(E expr, Plan input, CascadesContext cascadesContext,
            boolean enableExactMatch) {
        return bindSlot(expr, input, cascadesContext, enableExactMatch, true);
    }

    private <E extends Expression> E bindSlot(E expr, Plan input, CascadesContext cascadesContext,
            boolean enableExactMatch, boolean bindSlotInOuterScope) {
        return (E) new SlotBinder(toScope(cascadesContext, input.getOutput()), cascadesContext,
                enableExactMatch, bindSlotInOuterScope).bind(expr);
    }

    @SuppressWarnings("unchecked")
    private <E extends Expression> E bindSlot(E expr, List<Plan> inputs, CascadesContext cascadesContext,
            boolean enableExactMatch) {
        return bindSlot(expr, inputs, cascadesContext, enableExactMatch, true);
    }

    private <E extends Expression> E bindSlot(E expr, List<Plan> inputs, CascadesContext cascadesContext,
            boolean enableExactMatch, boolean bindSlotInOuterScope) {
        List<Slot> boundedSlots = new ArrayList<>();
        for (Plan input : inputs) {
            boundedSlots.addAll(input.getOutput());
        }
        return (E) new SlotBinder(toScope(cascadesContext, boundedSlots), cascadesContext,
                enableExactMatch, bindSlotInOuterScope).bind(expr);
    }

    @SuppressWarnings("unchecked")
    private <E extends Expression> E bindSlot(E expr, List<Plan> inputs, CascadesContext cascadesContext) {
        return bindSlot(expr, inputs, cascadesContext, true);
    }

    @SuppressWarnings("unchecked")
    private <E extends Expression> E bindFunction(E expr, Plan plan, CascadesContext cascadesContext) {
        return (E) FunctionBinder.INSTANCE.rewrite(checkBoundExceptLambda(expr, plan),
                new ExpressionRewriteContext(cascadesContext));
    }

    /**
     * For the columns whose output exists in grouping sets, they need to be assigned as nullable.
     */
    private List<NamedExpression> adjustNullableForRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> output) {
        Set<Slot> groupingSetsSlots = groupingSets.stream()
                .flatMap(Collection::stream)
                .map(Expression::getInputSlots)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        return output.stream()
                .map(e -> e.accept(RewriteNullableToTrue.INSTANCE, groupingSetsSlots))
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    private static class RewriteNullableToTrue extends DefaultExpressionRewriter<Set<Slot>> {
        public static RewriteNullableToTrue INSTANCE = new RewriteNullableToTrue();

        @Override
        public Expression visitSlotReference(SlotReference slotReference, Set<Slot> childrenOutput) {
            if (childrenOutput.contains(slotReference)) {
                return slotReference.withNullable(true);
            }
            return slotReference;
        }
    }

    private LogicalTVFRelation bindTableValuedFunction(UnboundTVFRelation unboundTVFRelation,
            StatementContext statementContext) {
        Env env = statementContext.getConnectContext().getEnv();
        FunctionRegistry functionRegistry = env.getFunctionRegistry();

        String functionName = unboundTVFRelation.getFunctionName();
        Properties arguments = unboundTVFRelation.getProperties();
        FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(functionName, arguments);
        Expression function = functionBuilder.build(functionName, arguments);
        if (!(function instanceof TableValuedFunction)) {
            throw new AnalysisException(function.toSql() + " is not a TableValuedFunction");
        }
        return new LogicalTVFRelation(unboundTVFRelation.getRelationId(), (TableValuedFunction) function);
    }

    private void checkSameNameSlot(List<Slot> childOutputs, String subQueryAlias) {
        Set<String> nameSlots = new HashSet<>();
        for (Slot s : childOutputs) {
            if (nameSlots.contains(s.getInternalName())) {
                throw new AnalysisException("Duplicated inline view column alias: '" + s.getName()
                        + "'" + " in inline view: '" + subQueryAlias + "'");
            } else {
                nameSlots.add(s.getInternalName());
            }
        }
    }

    private BoundFunction bindTableGeneratingFunction(UnboundFunction unboundFunction, Plan plan,
            CascadesContext cascadesContext) {
        List<Expression> boundArguments = unboundFunction.getArguments().stream()
                .map(e -> bindFunction(e, plan, cascadesContext))
                .collect(Collectors.toList());
        FunctionRegistry functionRegistry = cascadesContext.getConnectContext().getEnv().getFunctionRegistry();

        String functionName = unboundFunction.getName();
        FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(functionName, boundArguments);
        Expression function = functionBuilder.build(functionName, boundArguments);
        if (!(function instanceof TableGeneratingFunction)) {
            throw new AnalysisException(function.toSql() + " is not a TableGeneratingFunction");
        }
        return (BoundFunction) TypeCoercionUtils.processBoundFunction((BoundFunction) function);
    }

    private void checkIfOutputAliasNameDuplicatedForGroupBy(List<Expression> expressions,
            List<NamedExpression> output) {
        // if group_by_and_having_use_alias_first=true, we should fall back to original planner until we
        // support the session variable.
        if (output.stream().noneMatch(Alias.class::isInstance)) {
            return;
        }
        List<Alias> aliasList = output.stream().filter(Alias.class::isInstance)
                .map(Alias.class::cast).collect(Collectors.toList());

        List<NamedExpression> exprAliasList = expressions.stream()
                .map(expr -> (Set<NamedExpression>) expr.collect(NamedExpression.class::isInstance))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        boolean isGroupByContainAlias = exprAliasList.stream().anyMatch(ne ->
                aliasList.stream().anyMatch(alias -> !alias.getExprId().equals(ne.getExprId())
                        && alias.getName().equals(ne.getName())));

        if (isGroupByContainAlias
                && ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().isGroupByAndHavingUseAliasFirst()) {
            throw new AnalysisException("group_by_and_having_use_alias=true is unsupported for Nereids");
        }
    }

    private <E extends Expression> E checkBoundExceptLambda(E expression, Plan plan) {
        if (expression instanceof Lambda) {
            return expression;
        }
        if (expression instanceof UnboundSlot) {
            UnboundSlot unboundSlot = (UnboundSlot) expression;
            String tableName = StringUtils.join(unboundSlot.getQualifier(), ".");
            if (tableName.isEmpty()) {
                tableName = "table list";
            }
            throw new AnalysisException("Unknown column '"
                    + unboundSlot.getNameParts().get(unboundSlot.getNameParts().size() - 1)
                    + "' in '" + tableName + "' in "
                    + plan.getType().toString().substring("LOGICAL_".length()) + " clause");
        }
        expression.children().forEach(e -> checkBoundExceptLambda(e, plan));
        return expression;
    }
}
