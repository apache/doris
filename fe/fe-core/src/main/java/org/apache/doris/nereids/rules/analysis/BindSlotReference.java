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

import org.apache.doris.catalog.BuiltinAggregateFunctions;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindFunction.FunctionBinder;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.UsingJoin;
import org.apache.doris.planner.PlannerContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BindSlotReference.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BindSlotReference implements AnalysisRuleFactory {

    private final Optional<Scope> outerScope;

    public BindSlotReference(Optional<Scope> outerScope) {
        this.outerScope = Objects.requireNonNull(outerScope, "outerScope cannot be null");
    }

    private Scope toScope(List<Slot> slots) {
        if (outerScope.isPresent()) {
            return new Scope(outerScope, slots, outerScope.get().getSubquery());
        } else {
            return new Scope(slots);
        }
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_SLOT.build(
                logicalProject().when(Plan::canBind).thenApply(ctx -> {
                    LogicalProject<GroupPlan> project = ctx.root;
                    List<NamedExpression> boundSlots =
                            bind(project.getProjects(), project.children(), project, ctx.cascadesContext);
                    List<NamedExpression> exceptSlots = bind(project.getExcepts(), project.children(), project,
                            ctx.cascadesContext);
                    List<NamedExpression> newOutput = flatBoundStar(adjustNullableForProjects(project, boundSlots));
                    newOutput.removeAll(exceptSlots);
                    return new LogicalProject<>(newOutput, project.child(), project.isDistinct());
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().when(Plan::canBind).thenApply(ctx -> {
                    LogicalFilter<GroupPlan> filter = ctx.root;
                    Set<Expression> boundConjuncts
                            = bind(filter.getConjuncts(), filter.children(), filter, ctx.cascadesContext);
                    return new LogicalFilter<>(boundConjuncts, filter.child());
                })
            ),

            RuleType.BINDING_USING_JOIN_SLOT.build(
                    usingJoin().thenApply(ctx -> {
                        UsingJoin<GroupPlan, GroupPlan> using = ctx.root;
                        LogicalJoin lj = new LogicalJoin(using.getJoinType() == JoinType.CROSS_JOIN
                                ? JoinType.INNER_JOIN : using.getJoinType(),
                                using.getHashJoinConjuncts(),
                                using.getOtherJoinConjuncts(), using.getHint(), using.left(),
                                using.right());
                        List<Expression> unboundSlots = lj.getHashJoinConjuncts();
                        Set<String> slotNames = new HashSet<>();
                        List<Slot> leftOutput = new ArrayList<>(lj.left().getOutput());
                        // Suppose A JOIN B USING(name) JOIN C USING(name), [A JOIN B] is the left node, in this case,
                        // C should combine with table B on C.name=B.name. so we reverse the output to make sure that
                        // the most right slot is matched with priority.
                        Collections.reverse(leftOutput);
                        List<Expression> leftSlots = new ArrayList<>();
                        Scope scope = toScope(leftOutput.stream()
                                .filter(s -> !slotNames.contains(s.getName()))
                                .peek(s -> slotNames.add(s.getName())).collect(
                                        Collectors.toList()));
                        for (Expression unboundSlot : unboundSlots) {
                            Expression expression = new SlotBinder(scope, lj, ctx.cascadesContext).bind(unboundSlot);
                            leftSlots.add(expression);
                        }
                        slotNames.clear();
                        scope = toScope(lj.right().getOutput().stream()
                                .filter(s -> !slotNames.contains(s.getName()))
                                .peek(s -> slotNames.add(s.getName())).collect(
                                        Collectors.toList()));
                        List<Expression> rightSlots = new ArrayList<>();
                        for (Expression unboundSlot : unboundSlots) {
                            Expression expression = new SlotBinder(scope, lj, ctx.cascadesContext).bind(unboundSlot);
                            rightSlots.add(expression);
                        }
                        int size = leftSlots.size();
                        List<Expression> hashEqExpr = new ArrayList<>();
                        for (int i = 0; i < size; i++) {
                            hashEqExpr.add(new EqualTo(leftSlots.get(i), rightSlots.get(i)));
                        }
                        return lj.withHashJoinConjuncts(hashEqExpr);
                    })
                ),
                RuleType.BINDING_JOIN_SLOT.build(
                        logicalJoin().when(Plan::canBind)
                                .whenNot(j -> j.getJoinType().equals(JoinType.USING_JOIN)).thenApply(ctx -> {
                                    LogicalJoin<GroupPlan, GroupPlan> join = ctx.root;
                                    List<Expression> cond = join.getOtherJoinConjuncts().stream()
                                            .map(expr -> bind(expr, join.children(), join, ctx.cascadesContext))
                                            .collect(Collectors.toList());
                                    List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                                            .map(expr -> bind(expr, join.children(), join, ctx.cascadesContext))
                                            .collect(Collectors.toList());
                                    return new LogicalJoin<>(join.getJoinType(),
                                            hashJoinConjuncts, cond, join.getHint(), join.left(), join.right());
                                })
                ),
            RuleType.BINDING_AGGREGATE_SLOT.build(
                logicalAggregate().when(Plan::canBind).thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> agg = ctx.root;
                    List<NamedExpression> output =
                            bind(agg.getOutputExpressions(), agg.children(), agg, ctx.cascadesContext);

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
                    */
                    duplicatedSlotNames.stream().forEach(dup -> childOutputsToExpr.remove(dup));
                    Map<String, Expression> aliasNameToExpr = output.stream()
                            .filter(ne -> ne instanceof Alias)
                            .map(Alias.class::cast)
                            //agg function cannot be bound with group_by_key
                            .filter(alias -> ! alias.child().anyMatch(expr -> {
                                        if (expr instanceof UnboundFunction) {
                                            UnboundFunction unboundFunction = (UnboundFunction) expr;
                                            return BuiltinAggregateFunctions.aggFuncNames.contains(
                                                    unboundFunction.getName().toLowerCase());
                                        }
                                        return false;
                                    }
                            ))
                            .collect(Collectors.toMap(Alias::getName, UnaryNode::child, (oldExpr, newExpr) -> oldExpr));
                    aliasNameToExpr.entrySet().stream()
                            .forEach(e -> childOutputsToExpr.putIfAbsent(e.getKey(), e.getValue()));

                    List<Expression> replacedGroupBy = agg.getGroupByExpressions().stream()
                            .map(groupBy -> {
                                if (groupBy instanceof UnboundSlot) {
                                    UnboundSlot unboundSlot = (UnboundSlot) groupBy;
                                    if (unboundSlot.getNameParts().size() == 1) {
                                        String name = unboundSlot.getNameParts().get(0);
                                        if (childOutputsToExpr.containsKey(name)) {
                                            return childOutputsToExpr.get(name);
                                        }
                                    }
                                }
                                return groupBy;
                            }).collect(Collectors.toList());

                    List<Expression> groupBy = bind(replacedGroupBy, agg.children(), agg, ctx.cascadesContext);
                    List<Expression> unboundGroupBys = Lists.newArrayList();
                    boolean hasUnbound = groupBy.stream().anyMatch(
                            expression -> {
                                if (expression.anyMatch(UnboundSlot.class::isInstance)) {
                                    unboundGroupBys.add(expression);
                                    return true;
                                }
                                return false;
                            });
                    if (hasUnbound) {
                        throw new AnalysisException("cannot bind GROUP BY KEY: " + unboundGroupBys.get(0).toSql());
                    }
                    List<NamedExpression> newOutput = adjustNullableForAgg(agg, output);
                    return agg.withGroupByAndOutput(groupBy, newOutput);
                })
            ),
            RuleType.BINDING_REPEAT_SLOT.build(
                logicalRepeat().when(Plan::canBind).thenApply(ctx -> {
                    LogicalRepeat<GroupPlan> repeat = ctx.root;

                    List<NamedExpression> output =
                            bind(repeat.getOutputExpressions(), repeat.children(), repeat, ctx.cascadesContext);

                    // The columns referenced in group by are first obtained from the child's output,
                    // and then from the node's output
                    Map<String, Expression> childOutputsToExpr = repeat.child().getOutput().stream()
                            .collect(Collectors.toMap(Slot::getName, Slot::toSlot, (oldExpr, newExpr) -> oldExpr));
                    Map<String, Expression> aliasNameToExpr = output.stream()
                            .filter(ne -> ne instanceof Alias)
                            .map(Alias.class::cast)
                            .collect(Collectors.toMap(Alias::getName, UnaryNode::child, (oldExpr, newExpr) -> oldExpr));
                    aliasNameToExpr.entrySet().stream()
                            .forEach(e -> childOutputsToExpr.putIfAbsent(e.getKey(), e.getValue()));

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
                            .map(groupingSet -> bind(groupingSet, repeat.children(), repeat, ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());
                    List<NamedExpression> newOutput = adjustNullableForRepeat(groupingSets, output);
                    return repeat.withGroupSetsAndOutput(groupingSets, newOutput);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(aggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<Aggregate<GroupPlan>> sort = ctx.root;
                    Aggregate<GroupPlan> aggregate = sort.child();
                    return bindSortWithAggregateFunction(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(aggregate())).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<Aggregate<GroupPlan>>> sort = ctx.root;
                    Aggregate<GroupPlan> aggregate = sort.child().child();
                    return bindSortWithAggregateFunction(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(logicalProject())).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<LogicalProject<GroupPlan>>> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.children(), sort, ctx.cascadesContext);
                                if (item.containsType(UnboundSlot.class)) {
                                    item = bind(item, sort.child().children(), sort, ctx.cascadesContext);
                                }
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());

                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalProject()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalProject<GroupPlan>> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.children(), sort, ctx.cascadesContext);
                                if (item.containsType(UnboundSlot.class)) {
                                    item = bind(item, sort.child().children(), sort, ctx.cascadesContext);
                                }
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());

                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_SORT_SET_OPERATION_SLOT.build(
                logicalSort(logicalSetOperation()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalSetOperation> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.children(), sort, ctx.cascadesContext);
                                if (item.containsType(UnboundSlot.class)) {
                                    item = bind(item, sort.child().children(), sort, ctx.cascadesContext);
                                }
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());

                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(any()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalHaving<Plan> having = ctx.root;
                    Plan childPlan = having.child();
                    // We should deduplicate the slots, otherwise the binding process will fail due to the
                    // ambiguous slots exist.
                    List<Slot> childChildSlots = childPlan.children().stream()
                            .flatMap(plan -> plan.getOutputSet().stream())
                            .collect(Collectors.toList());
                    SlotBinder childChildBinder = new SlotBinder(toScope(childChildSlots), having,
                            ctx.cascadesContext);
                    List<Slot> childSlots = childPlan.getOutputSet().stream()
                            .collect(Collectors.toList());
                    SlotBinder childBinder = new SlotBinder(toScope(childSlots), having,
                            ctx.cascadesContext);
                    Set<Expression> boundConjuncts = having.getConjuncts().stream().map(
                            expr -> {
                                expr = childChildBinder.bind(expr);
                                return childBinder.bind(expr);
                            })
                            .collect(Collectors.toSet());
                    return new LogicalHaving<>(boundConjuncts, having.child());
                })
            ),
            RuleType.BINDING_ONE_ROW_RELATION_SLOT.build(
                // we should bind UnboundAlias in the UnboundOneRowRelation
                unboundOneRowRelation().thenApply(ctx -> {
                    UnboundOneRowRelation oneRowRelation = ctx.root;
                    List<NamedExpression> projects = oneRowRelation.getProjects()
                            .stream()
                            .map(project -> bind(project, ImmutableList.of(), oneRowRelation, ctx.cascadesContext))
                            .collect(Collectors.toList());
                    return new LogicalOneRowRelation(projects);
                })
            ),
            RuleType.BINDING_SET_OPERATION_SLOT.build(
                logicalSetOperation().when(Plan::canBind).then(setOperation -> {
                    // check whether the left and right child output columns are the same
                    if (setOperation.child(0).getOutput().size() != setOperation.child(1).getOutput().size()) {
                        throw new AnalysisException("Operands have unequal number of columns:\n"
                                + "'" + setOperation.child(0).getOutput() + "' has "
                                + setOperation.child(0).getOutput().size() + " column(s)\n"
                                + "'" + setOperation.child(1).getOutput() + "' has "
                                + setOperation.child(1).getOutput().size() + " column(s)");
                    }

                    // INTERSECT and EXCEPT does not support ALL qualifie
                    if (setOperation.getQualifier() == Qualifier.ALL
                            && (setOperation instanceof LogicalExcept || setOperation instanceof LogicalIntersect)) {
                        throw new AnalysisException("INTERSECT and EXCEPT does not support ALL qualifie");
                    }

                    List<List<Expression>> castExpressions = setOperation.collectCastExpressions();
                    List<NamedExpression> newOutputs = setOperation.buildNewOutputs(castExpressions.get(0));

                    return setOperation.withNewOutputs(newOutputs);
                })
            ),
            RuleType.BINDING_GENERATE_SLOT.build(
              logicalGenerate().when(Plan::canBind).thenApply(ctx -> {
                  LogicalGenerate<GroupPlan> generate = ctx.root;
                  List<Function> boundSlotGenerators = bind(generate.getGenerators(), generate.children(),
                          generate, ctx.cascadesContext);
                  List<Function> boundFunctionGenerators = boundSlotGenerators.stream()
                          .map(f -> FunctionBinder.INSTANCE.bindTableGeneratingFunction(
                                  (UnboundFunction) f, ctx.statementContext))
                          .collect(Collectors.toList());
                  ImmutableList.Builder<Slot> slotBuilder = ImmutableList.builder();
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

            // when child update, we need update current plan's logical properties,
            // since we use cache to avoid compute more than once.
            RuleType.BINDING_NON_LEAF_LOGICAL_PLAN.build(
                logicalPlan()
                    .when(plan -> plan.canBind() && !(plan instanceof LeafPlan))
                    .then(LogicalPlan::recomputeLogicalProperties)
            )
        );
    }

    private Plan bindSortWithAggregateFunction(
            LogicalSort<? extends Plan> sort, Aggregate<? extends Plan> aggregate, CascadesContext ctx) {
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
        SlotBinder outputBinder = new SlotBinder(
                toScope(aggregate.getOutputSet().stream().collect(Collectors.toList())), sort, ctx);
        List<Slot> childOutputSlots = aggregate.child().getOutputSet().stream().collect(Collectors.toList());
        SlotBinder childOutputBinder = new SlotBinder(toScope(childOutputSlots), sort, ctx);
        List<OrderKey> sortItemList = sort.getOrderKeys()
                .stream()
                .map(orderKey -> {
                    Expression item = outputBinder.bind(orderKey.getExpr());
                    item = childOutputBinder.bind(item);
                    return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                }).collect(Collectors.toList());
        return new LogicalSort<>(sortItemList, sort.child());
    }

    private List<NamedExpression> flatBoundStar(List<NamedExpression> boundSlots) {
        return boundSlots
            .stream()
            .flatMap(slot -> {
                if (slot instanceof BoundStar) {
                    return ((BoundStar) slot).getSlots().stream();
                } else {
                    return Stream.of(slot);
                }
            }).collect(Collectors.toList());
    }

    private <E extends Expression> List<E> bind(List<E> exprList, List<Plan> inputs, Plan plan,
            CascadesContext cascadesContext) {
        return exprList.stream()
            .map(expr -> bind(expr, inputs, plan, cascadesContext))
            .collect(Collectors.toList());
    }

    private <E extends Expression> Set<E> bind(Set<E> exprList, List<Plan> inputs, Plan plan,
            CascadesContext cascadesContext) {
        return exprList.stream()
                .map(expr -> bind(expr, inputs, plan, cascadesContext))
                .collect(Collectors.toSet());
    }

    private <E extends Expression> E bind(E expr, List<Plan> inputs, Plan plan, CascadesContext cascadesContext) {
        List<Slot> boundedSlots = inputs.stream()
                .flatMap(input -> input.getOutput().stream())
                .collect(Collectors.toList());
        return (E) new SlotBinder(toScope(boundedSlots), plan, cascadesContext).bind(expr);
    }

    private class SlotBinder extends SubExprAnalyzer {
        private final Plan plan;

        public SlotBinder(Scope scope, Plan plan, CascadesContext cascadesContext) {
            super(scope, cascadesContext);
            this.plan = plan;
        }

        public Expression bind(Expression expression) {
            return expression.accept(this, null);
        }

        @Override
        public Expression visitUnboundAlias(UnboundAlias unboundAlias, PlannerContext context) {
            Expression child = unboundAlias.child().accept(this, context);
            if (unboundAlias.getAlias().isPresent()) {
                return new Alias(child, unboundAlias.getAlias().get());
            }
            if (child instanceof NamedExpression) {
                return new Alias(child, ((NamedExpression) child).getName());
            } else {
                // TODO: resolve aliases
                return new Alias(child, child.toSql());
            }
        }

        @Override
        public Slot visitUnboundSlot(UnboundSlot unboundSlot, PlannerContext context) {
            Optional<List<Slot>> boundedOpt = Optional.of(bindSlot(unboundSlot, getScope().getSlots()));
            boolean foundInThisScope = !boundedOpt.get().isEmpty();
            // Currently only looking for symbols on the previous level.
            if (!foundInThisScope && getScope().getOuterScope().isPresent()) {
                boundedOpt = Optional.of(bindSlot(unboundSlot,
                        getScope()
                        .getOuterScope()
                        .get()
                        .getSlots()));
            }
            List<Slot> bounded = boundedOpt.get();
            switch (bounded.size()) {
                case 0:
                    // just return, give a chance to bind on another slot.
                    // if unbound finally, check will throw exception
                    return unboundSlot;
                case 1:
                    if (!foundInThisScope) {
                        getScope().getOuterScope().get().getCorrelatedSlots().add(bounded.get(0));
                    }
                    return bounded.get(0);
                default:
                    throw new AnalysisException(String.format("%s is ambiguous: %s.",
                            unboundSlot.toSql(),
                            bounded.stream()
                                    .map(Slot::toString)
                                    .collect(Collectors.joining(", "))));
            }
        }

        @Override
        public Expression visitUnboundStar(UnboundStar unboundStar, PlannerContext context) {
            List<String> qualifier = unboundStar.getQualifier();
            switch (qualifier.size()) {
                case 0: // select *
                    return new BoundStar(getScope().getSlots());
                case 1: // select table.*
                case 2: // select db.table.*
                    return bindQualifiedStar(qualifier);
                default:
                    throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
            }
        }

        private BoundStar bindQualifiedStar(List<String> qualifierStar) {
            // FIXME: compatible with previous behavior:
            // https://github.com/apache/doris/pull/10415/files/3fe9cb0c3f805ab3a9678033b281b16ad93ec60a#r910239452
            List<Slot> slots = getScope().getSlots().stream().filter(boundSlot -> {
                switch (qualifierStar.size()) {
                    // table.*
                    case 1:
                        List<String> boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0: return false;
                            case 1: // bound slot is `table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0));
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, "."));
                        }
                    case 2: // db.table.*
                        boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0:
                            case 1: // bound slot is `table`.`column`
                                return false;
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0))
                                        && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                        }
                    default:
                        throw new AnalysisException("Not supported name: "
                            + StringUtils.join(qualifierStar, ".") + ".*");
                }
            }).collect(Collectors.toList());

            return new BoundStar(slots);
        }

        private List<Slot> bindSlot(UnboundSlot unboundSlot, List<Slot> boundSlots) {
            return boundSlots.stream().filter(boundSlot -> {
                List<String> nameParts = unboundSlot.getNameParts();
                if (nameParts.size() == 1) {
                    return nameParts.get(0).equalsIgnoreCase(boundSlot.getName());
                } else if (nameParts.size() <= 3) {
                    int size = nameParts.size();
                    // if nameParts.size() == 3, nameParts.get(0) is cluster name.
                    return handleNamePartsTwoOrThree(boundSlot, nameParts.subList(size - 2, size));
                }
                //TODO: handle name parts more than three.
                throw new AnalysisException("Not supported name: "
                        + StringUtils.join(nameParts, "."));
            }).collect(Collectors.toList());
        }
    }

    private boolean handleNamePartsTwoOrThree(Slot boundSlot, List<String> nameParts) {
        List<String> qualifier = boundSlot.getQualifier();
        String name = boundSlot.getName();
        switch (qualifier.size()) {
            case 2:
                // qualifier is `db`.`table`
                return nameParts.get(0).equalsIgnoreCase(qualifier.get(1))
                        && nameParts.get(1).equalsIgnoreCase(name);
            case 1:
                // qualifier is `table`
                return nameParts.get(0).equalsIgnoreCase(qualifier.get(0))
                        && nameParts.get(1).equalsIgnoreCase(name);
            case 0:
                // has no qualifiers
                return nameParts.get(1).equalsIgnoreCase(name);
            default:
                throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
        }
    }

    /** BoundStar is used to wrap list of slots for temporary. */
    public static class BoundStar extends NamedExpression implements PropagateNullable {
        public BoundStar(List<Slot> children) {
            super(children.toArray(new Slot[0]));
            Preconditions.checkArgument(children.stream().noneMatch(slot -> slot instanceof UnboundSlot),
                    "BoundStar can not wrap UnboundSlot"
            );
        }

        public String toSql() {
            return children.stream().map(Expression::toSql).collect(Collectors.joining(", "));
        }

        public List<Slot> getSlots() {
            return (List) children();
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return visitor.visitBoundStar(this, context);
        }
    }

    /**
     * Use the visitor to iterate sub expression.
     */
    private static class SubExprAnalyzer extends DefaultExpressionRewriter<PlannerContext> {
        private final Scope scope;
        private final CascadesContext cascadesContext;

        public SubExprAnalyzer(Scope scope, CascadesContext cascadesContext) {
            this.scope = scope;
            this.cascadesContext = cascadesContext;
        }

        @Override
        public Expression visitNot(Not not, PlannerContext context) {
            Expression child = not.child();
            if (child instanceof Exists) {
                return visitExistsSubquery(
                        new Exists(((Exists) child).getQueryPlan(), true), context);
            } else if (child instanceof InSubquery) {
                return visitInSubquery(new InSubquery(((InSubquery) child).getCompareExpr(),
                        ((InSubquery) child).getListQuery(), true), context);
            }
            return visit(not, context);
        }

        @Override
        public Expression visitExistsSubquery(Exists exists, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(exists);

            return new Exists(analyzedResult.getLogicalPlan(),
                    analyzedResult.getCorrelatedSlots(), exists.isNot());
        }

        @Override
        public Expression visitInSubquery(InSubquery expr, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(expr);

            checkOutputColumn(analyzedResult.getLogicalPlan());
            checkHasGroupBy(analyzedResult);

            return new InSubquery(
                    expr.getCompareExpr().accept(this, context),
                    new ListQuery(analyzedResult.getLogicalPlan()),
                    analyzedResult.getCorrelatedSlots(), expr.isNot());
        }

        @Override
        public Expression visitScalarSubquery(ScalarSubquery scalar, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(scalar);

            checkOutputColumn(analyzedResult.getLogicalPlan());
            checkRootIsAgg(analyzedResult);
            checkHasGroupBy(analyzedResult);

            return new ScalarSubquery(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots());
        }

        private void checkOutputColumn(LogicalPlan plan) {
            if (plan.getOutput().size() != 1) {
                throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                        + plan.getOutput().size());
            }
        }

        private void checkRootIsAgg(AnalyzedResult analyzedResult) {
            if (!analyzedResult.isCorrelated()) {
                return;
            }
            if (!analyzedResult.rootIsAgg()) {
                throw new AnalysisException("The select item in correlated subquery of binary predicate "
                        + "should only be sum, min, max, avg and count. Current subquery: "
                        + analyzedResult.getLogicalPlan());
            }
        }

        private void checkHasGroupBy(AnalyzedResult analyzedResult) {
            if (!analyzedResult.isCorrelated()) {
                return;
            }
            if (analyzedResult.hasGroupBy()) {
                throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                        + analyzedResult.getLogicalPlan());
            }
        }

        private AnalyzedResult analyzeSubquery(SubqueryExpr expr) {
            CascadesContext subqueryContext = new Memo(expr.getQueryPlan())
                    .newCascadesContext((cascadesContext.getStatementContext()), cascadesContext.getCteContext());
            Scope subqueryScope = genScopeWithSubquery(expr);
            subqueryContext
                    .newAnalyzer(Optional.of(subqueryScope))
                    .analyze();
            return new AnalyzedResult((LogicalPlan) subqueryContext.getMemo().copyOut(false),
                    subqueryScope.getCorrelatedSlots());
        }

        private Scope genScopeWithSubquery(SubqueryExpr expr) {
            return new Scope(getScope().getOuterScope(),
                    getScope().getSlots(),
                    Optional.ofNullable(expr));
        }

        public Scope getScope() {
            return scope;
        }

        public CascadesContext getCascadesContext() {
            return cascadesContext;
        }
    }

    private static class AnalyzedResult {
        private final LogicalPlan logicalPlan;
        private final List<Slot> correlatedSlots;

        public AnalyzedResult(LogicalPlan logicalPlan, List<Slot> correlatedSlots) {
            this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan can not be null");
            this.correlatedSlots = correlatedSlots == null ? new ArrayList<>() : ImmutableList.copyOf(correlatedSlots);
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public List<Slot> getCorrelatedSlots() {
            return correlatedSlots;
        }

        public boolean isCorrelated() {
            return !correlatedSlots.isEmpty();
        }

        public boolean rootIsAgg() {
            return logicalPlan instanceof LogicalAggregate;
        }

        public boolean hasGroupBy() {
            if (rootIsAgg()) {
                return !((LogicalAggregate<? extends Plan>) logicalPlan).getGroupByExpressions().isEmpty();
            }
            return false;
        }
    }

    /**
     * When there is a repeat operator in the query,
     * it will change the nullable information of the output column, which will be changed uniformly here.
     *
     * adjust the project with the children output
     */
    private List<NamedExpression> adjustNullableForProjects(
            LogicalProject<GroupPlan> project, List<NamedExpression> projects) {
        Set<Slot> childrenOutput = project.children().stream()
                .map(Plan::getOutput)
                .flatMap(List::stream)
                .filter(ExpressionTrait::nullable)
                .collect(Collectors.toSet());
        return projects.stream()
                .map(e -> e.accept(new RewriteNullableToTrue(childrenOutput), null))
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * same as project, adjust the agg with the children output.
     */
    private List<NamedExpression> adjustNullableForAgg(
            LogicalAggregate<GroupPlan> aggregate, List<NamedExpression> output) {
        Set<Slot> childrenOutput = aggregate.children().stream()
                .map(Plan::getOutput)
                .flatMap(List::stream)
                .filter(ExpressionTrait::nullable)
                .collect(Collectors.toSet());
        return output.stream()
                .map(e -> e.accept(new RewriteNullableToTrue(childrenOutput), null))
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * For the columns whose output exists in grouping sets, they need to be assigned as nullable.
     */
    private List<NamedExpression> adjustNullableForRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> output) {
        Set<Slot> groupingSetsSlots = groupingSets.stream()
                .flatMap(e -> e.stream())
                .flatMap(e -> e.<Set<SlotReference>>collect(SlotReference.class::isInstance).stream())
                .collect(Collectors.toSet());
        return output.stream()
                .map(e -> e.accept(new RewriteNullableToTrue(groupingSetsSlots), null))
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    private static class RewriteNullableToTrue extends DefaultExpressionRewriter<PlannerContext> {
        private final Set<Slot> childrenOutput;

        public RewriteNullableToTrue(Set<Slot> childrenOutput) {
            this.childrenOutput = ImmutableSet.copyOf(childrenOutput);
        }

        @Override
        public Expression visitSlotReference(SlotReference slotReference, PlannerContext context) {
            if (childrenOutput.contains(slotReference)) {
                return slotReference.withNullable(true);
            }
            return slotReference;
        }
    }
}
