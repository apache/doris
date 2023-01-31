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
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindFunction.FunctionBinder;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
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
                    List<NamedExpression> boundProjections =
                            bind(project.getProjects(), project.children(), ctx.cascadesContext);
                    List<NamedExpression> boundExceptions = bind(project.getExcepts(), project.children(),
                            ctx.cascadesContext);
                    boundProjections = flatBoundStar(boundProjections, boundExceptions);
                    return new LogicalProject<>(boundProjections, project.child(), project.isDistinct());
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().when(Plan::canBind).thenApply(ctx -> {
                    LogicalFilter<GroupPlan> filter = ctx.root;
                    Set<Expression> boundConjuncts
                            = bind(filter.getConjuncts(), filter.children(), ctx.cascadesContext);
                    return new LogicalFilter<>(boundConjuncts, filter.child());
                })
            ),

            RuleType.BINDING_USING_JOIN_SLOT.build(
                usingJoin().thenApply(ctx -> {
                    UsingJoin<GroupPlan, GroupPlan> using = ctx.root;
                    LogicalJoin<Plan, Plan> lj = new LogicalJoin<>(using.getJoinType() == JoinType.CROSS_JOIN
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
                            .peek(s -> slotNames.add(s.getName()))
                            .collect(Collectors.toList()));
                    for (Expression unboundSlot : unboundSlots) {
                        Expression expression = new Binder(scope, ctx.cascadesContext).bind(unboundSlot);
                        leftSlots.add(expression);
                    }
                    slotNames.clear();
                    scope = toScope(lj.right().getOutput().stream()
                            .filter(s -> !slotNames.contains(s.getName()))
                            .peek(s -> slotNames.add(s.getName()))
                            .collect(Collectors.toList()));
                    List<Expression> rightSlots = new ArrayList<>();
                    for (Expression unboundSlot : unboundSlots) {
                        Expression expression = new Binder(scope, ctx.cascadesContext).bind(unboundSlot);
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
                logicalJoin().when(Plan::canBind).thenApply(ctx -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = ctx.root;
                    List<Expression> cond = join.getOtherJoinConjuncts().stream()
                            .map(expr -> bind(expr, join.children(), ctx.cascadesContext))
                            .collect(Collectors.toList());
                    List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                            .map(expr -> bind(expr, join.children(), ctx.cascadesContext))
                            .collect(Collectors.toList());
                    return new LogicalJoin<>(join.getJoinType(),
                            hashJoinConjuncts, cond, join.getHint(), join.left(), join.right());
                })
            ),
            RuleType.BINDING_AGGREGATE_SLOT.build(
                logicalAggregate().when(Plan::canBind).thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> agg = ctx.root;
                    List<NamedExpression> output =
                            bind(agg.getOutputExpressions(), agg.children(), ctx.cascadesContext);

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
                    output.stream()
                            .filter(ne -> ne instanceof Alias)
                            .map(Alias.class::cast)
                            // agg function cannot be bound with group_by_key
                            // TODO(morrySnow): after bind function moved here,
                            //  we could just use instanceof AggregateFunction
                            .filter(alias -> !alias.child().anyMatch(expr -> {
                                        if (expr instanceof UnboundFunction) {
                                            UnboundFunction unboundFunction = (UnboundFunction) expr;
                                            return BuiltinAggregateFunctions.INSTANCE.aggFuncNames.contains(
                                                    unboundFunction.getName().toLowerCase());
                                        }
                                        return false;
                                    }
                            ))
                            .forEach(alias -> childOutputsToExpr.putIfAbsent(alias.getName(), alias.child()));

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
                    Binder binder = new Binder(toScope(Lists.newArrayList(boundSlots)), ctx.cascadesContext);
                    Binder childBinder
                            = new Binder(toScope(new ArrayList<>(agg.child().getOutputSet())), ctx.cascadesContext);

                    List<Expression> groupBy = replacedGroupBy.stream()
                            .map(expression -> {
                                Expression e = binder.bind(expression);
                                if (e instanceof UnboundSlot) {
                                    return childBinder.bind(e);
                                }
                                return e;
                            })
                            .collect(Collectors.toList());
                    List<Expression> unboundGroupBys = Lists.newArrayList();
                    Predicate<List<Expression>> hasUnBound = (exprs) -> exprs.stream().anyMatch(
                            expression -> {
                                if (expression.anyMatch(UnboundSlot.class::isInstance)) {
                                    unboundGroupBys.add(expression);
                                    return true;
                                }
                                return false;
                            });
                    if (hasUnBound.test(groupBy)) {
                        throw new AnalysisException("cannot bind GROUP BY KEY: " + unboundGroupBys.get(0).toSql());
                    }
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            ),
            RuleType.BINDING_REPEAT_SLOT.build(
                logicalRepeat().when(Plan::canBind).thenApply(ctx -> {
                    LogicalRepeat<GroupPlan> repeat = ctx.root;

                    List<NamedExpression> output =
                            bind(repeat.getOutputExpressions(), repeat.children(), ctx.cascadesContext);

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
                            .map(groupingSet -> bind(groupingSet, repeat.children(), ctx.cascadesContext))
                            .collect(ImmutableList.toImmutableList());
                    List<NamedExpression> newOutput = adjustNullableForRepeat(groupingSets, output);
                    return repeat.withGroupSetsAndOutput(groupingSets, newOutput);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(aggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<Aggregate<GroupPlan>> sort = ctx.root;
                    Aggregate<GroupPlan> aggregate = sort.child();
                    return bindSort(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(aggregate())).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<Aggregate<GroupPlan>>> sort = ctx.root;
                    Aggregate<GroupPlan> aggregate = sort.child().child();
                    return bindSort(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(logicalProject())).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<LogicalProject<GroupPlan>>> sort = ctx.root;
                    LogicalProject<GroupPlan> project = sort.child().child();
                    return bindSort(sort, project, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalProject()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalProject<GroupPlan>> sort = ctx.root;
                    LogicalProject<GroupPlan> project = sort.child();
                    return bindSort(sort, project, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SET_OPERATION_SLOT.build(
                logicalSort(logicalSetOperation()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalSetOperation> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.child(), ctx.cascadesContext);
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());
                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(aggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalHaving<Aggregate<GroupPlan>> having = ctx.root;
                    Plan childPlan = having.child();
                    Set<Expression> boundConjuncts = having.getConjuncts().stream().map(
                            expr -> {
                                expr = bind(expr, childPlan.children(), ctx.cascadesContext);
                                return bind(expr, childPlan, ctx.cascadesContext);
                            })
                            .collect(Collectors.toSet());
                    return new LogicalHaving<>(boundConjuncts, having.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(any()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalHaving<Plan> having = ctx.root;
                    Plan childPlan = having.child();
                    Set<Expression> boundConjuncts = having.getConjuncts().stream().map(
                                    expr -> {
                                        expr = bind(expr, childPlan, ctx.cascadesContext);
                                        return bind(expr, childPlan.children(), ctx.cascadesContext);
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
                            .map(project -> bind(project, ImmutableList.of(), ctx.cascadesContext))
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

                    // INTERSECT and EXCEPT does not support ALL qualified
                    if (setOperation.getQualifier() == Qualifier.ALL
                            && (setOperation instanceof LogicalExcept || setOperation instanceof LogicalIntersect)) {
                        throw new AnalysisException("INTERSECT and EXCEPT does not support ALL qualified");
                    }

                    List<List<Expression>> castExpressions = setOperation.collectCastExpressions();
                    List<NamedExpression> newOutputs = setOperation.buildNewOutputs(castExpressions.get(0));

                    return setOperation.withNewOutputs(newOutputs);
                })
            ),
            RuleType.BINDING_GENERATE_SLOT.build(
              logicalGenerate().when(Plan::canBind).thenApply(ctx -> {
                  LogicalGenerate<GroupPlan> generate = ctx.root;
                  List<Function> boundSlotGenerators
                          = bind(generate.getGenerators(), generate.children(), ctx.cascadesContext);
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

    private Plan bindSort(
            LogicalSort<? extends Plan> sort, Plan plan, CascadesContext ctx) {
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
                    Expression item = bind(orderKey.getExpr(), plan, ctx);
                    item = bind(item, plan.children(), ctx);
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

    private <E extends Expression> List<E> bind(List<E> exprList, List<Plan> inputs, CascadesContext cascadesContext) {
        return exprList.stream()
            .map(expr -> bind(expr, inputs, cascadesContext))
            .collect(Collectors.toList());
    }

    private <E extends Expression> Set<E> bind(Set<E> exprList, List<Plan> inputs, CascadesContext cascadesContext) {
        return exprList.stream()
                .map(expr -> bind(expr, inputs, cascadesContext))
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private <E extends Expression> E bind(E expr, Plan input, CascadesContext cascadesContext) {
        return (E) new Binder(toScope(input.getOutput()), cascadesContext).bind(expr);
    }

    @SuppressWarnings("unchecked")
    private <E extends Expression> E bind(E expr, List<Plan> inputs, CascadesContext cascadesContext) {
        List<Slot> boundedSlots = inputs.stream()
                .flatMap(input -> input.getOutput().stream())
                .collect(Collectors.toList());
        return (E) new Binder(toScope(boundedSlots), cascadesContext).bind(expr);
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
}
