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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * try to eliminate sub plan tree which contains EmptyRelation
 */
@DependsRules ({
    BuildAggForUnion.class
})
public class EliminateEmptyRelation implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            // join->empty
            logicalJoin(any(), any())
                .when(join -> hasEmptyRelationChild(join) && canReplaceJoinByEmptyRelation(join)
                        || bothChildrenEmpty(join))
                .then(join -> new LogicalEmptyRelation(
                            ConnectContext.get().getStatementContext().getNextRelationId(),
                            join.getOutput())
                )
                .toRule(RuleType.ELIMINATE_JOIN_ON_EMPTYRELATION),
            logicalFilter(logicalEmptyRelation())
                .then(filter -> new LogicalEmptyRelation(
                    ConnectContext.get().getStatementContext().getNextRelationId(),
                    filter.getOutput())
                ).toRule(RuleType.ELIMINATE_FILTER_ON_EMPTYRELATION),
            logicalAggregate(logicalEmptyRelation())
                .when(agg -> !agg.getGroupByExpressions().isEmpty())
                .then(agg -> new LogicalEmptyRelation(
                    ConnectContext.get().getStatementContext().getNextRelationId(),
                    agg.getOutput())
                ).toRule(RuleType.ELIMINATE_AGG_ON_EMPTYRELATION),
            // proj->empty
            logicalProject(logicalEmptyRelation())
                    .thenApply(ctx -> {
                        LogicalProject<? extends Plan> project = ctx.root;
                        return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                                project.getOutputs());
                    }).toRule(RuleType.ELIMINATE_AGG_ON_EMPTYRELATION),
            // after BuildAggForUnion rule, union may have more than 2 children.
            logicalUnion(multi()).then(union -> {
                boolean needProcess = union.arity() == 1;
                for (int i = 0; i < union.arity(); i++) {
                    if (union.child(i) instanceof EmptyRelation) {
                        needProcess = true;
                        break;
                    }
                }
                if (!needProcess) {
                    return null;
                }
                ImmutableList.Builder<Plan> nonEmptyChildrenBuilder
                        = ImmutableList.builderWithExpectedSize(union.arity());
                ImmutableList.Builder<List<SlotReference>> nonEmptyOutputsBuilder
                        = ImmutableList.builderWithExpectedSize(union.arity());
                for (int i = 0; i < union.arity(); i++) {
                    if (!(union.child(i) instanceof EmptyRelation)) {
                        nonEmptyChildrenBuilder.add(union.child(i));
                        nonEmptyOutputsBuilder.add(union.getRegularChildOutput(i));
                    }
                }
                List<Plan> nonEmptyChildren = nonEmptyChildrenBuilder.build();
                List<List<SlotReference>> nonEmptyOutputs = nonEmptyOutputsBuilder.build();
                if (nonEmptyChildren.isEmpty()) {
                    if (union.getConstantExprsList().isEmpty()) {
                        return new LogicalEmptyRelation(
                                ConnectContext.get().getStatementContext().getNextRelationId(),
                                union.getOutput());
                    } else {
                        return union.withChildrenAndTheirOutputs(ImmutableList.of(), ImmutableList.of());
                    }
                } else if (nonEmptyChildren.size() == 1) {
                    if (union.getConstantExprsList().isEmpty()) {
                        ImmutableList.Builder<NamedExpression> projectsBuilder = ImmutableList.builder();
                        List<Slot> unionOutput = union.getOutput();
                        for (int i = 0; i < unionOutput.size(); i++) {
                            ExprId id = unionOutput.get(i).getExprId();
                            Alias alias = new Alias(id, nonEmptyOutputs.get(0).get(i), unionOutput.get(i).getName());
                            projectsBuilder.add(alias);
                        }
                        return new LogicalProject<>(projectsBuilder.build(), nonEmptyChildren.get(0));
                    }
                }

                if (union.children().size() != nonEmptyChildren.size()) {
                    return union.withChildrenAndTheirOutputs(nonEmptyChildren, nonEmptyOutputs);
                } else {
                    // no empty relation child, do not change union
                    return null;
                }
            }).toRule(RuleType.ELIMINATE_UNION_ON_EMPTYRELATION),
            // topn->empty
            logicalTopN(logicalEmptyRelation())
                    .then(topn -> new LogicalEmptyRelation(
                            ConnectContext.get().getStatementContext().getNextRelationId(),
                            topn.getOutput()))
                            .toRule(RuleType.ELIMINATE_TOPN_ON_EMPTYRELATION),
            // sort->empty
            logicalSort(logicalEmptyRelation())
                    .then(sort -> new LogicalEmptyRelation(
                            ConnectContext.get().getStatementContext().getNextRelationId(),
                            sort.getOutput()))
                    .toRule(RuleType.ELIMINATE_SORT_ON_EMPTYRELATION),
            // set intersect
            logicalIntersect(multi()).then(intersect -> {
                List<Plan> emptyChildren = intersect.children().stream()
                        .filter(EmptyRelation.class::isInstance)
                        .collect(Collectors.toList());
                if (emptyChildren.isEmpty()) {
                    // no empty relation child, plan not changed
                    return null;
                } else {
                    // there is empty relation child, the intersection result is empty.
                    return new LogicalEmptyRelation(
                            ConnectContext.get().getStatementContext().getNextRelationId(),
                            intersect.getOutput());
                }
            }).toRule(RuleType.ELIMINATE_INTERSECTION_ON_EMPTYRELATION),
            // limit -> empty
            logicalLimit(logicalEmptyRelation())
                    .then(UnaryNode::child)
                    .toRule(RuleType.ELIMINATE_LIMIT_ON_EMPTY_RELATION),
            // set except
            logicalExcept(multi()).then(except -> {
                Plan first = except.child(0);
                if (first instanceof EmptyRelation) {
                    // empty except any => empty
                    return new LogicalEmptyRelation(
                            ConnectContext.get().getStatementContext().getNextRelationId(),
                            except.getOutput());
                } else {
                    boolean needProcess = false;
                    for (int i = 1; i < except.arity(); i++) {
                        if (except.child(i) instanceof EmptyRelation) {
                            needProcess = true;
                            break;
                        }
                    }
                    if (!needProcess) {
                        return null;
                    }
                    ImmutableList.Builder<Plan> nonEmptyChildrenBuilder
                            = ImmutableList.builderWithExpectedSize(except.arity());
                    ImmutableList.Builder<List<SlotReference>> nonEmptyOutputsBuilder
                            = ImmutableList.builderWithExpectedSize(except.arity());
                    for (int i = 0; i < except.arity(); i++) {
                        if (!(except.child(i) instanceof EmptyRelation)) {
                            nonEmptyChildrenBuilder.add(except.child(i));
                            nonEmptyOutputsBuilder.add(except.getRegularChildOutput(i));
                        }
                    }
                    List<Plan> nonEmptyChildren = nonEmptyChildrenBuilder.build();
                    List<List<SlotReference>> nonEmptyOutputs = nonEmptyOutputsBuilder.build();
                    if (nonEmptyChildren.size() == 1) {
                        // the first child is not empty, others are all empty
                        // case 1. FIRST except(distinct) empty = > project(AGG(FIRST))
                        // case 2. FIRST except(all) empty = > project(FIRST)
                        Plan projectChild;
                        if (except.getQualifier() == SetOperation.Qualifier.DISTINCT) {
                            projectChild = new LogicalAggregate<>((List) nonEmptyOutputs.get(0),
                                    (List) nonEmptyOutputs.get(0), true, Optional.empty(), first);
                        } else {
                            projectChild = first;
                        }
                        List<Slot> exceptOutput = except.getOutput();
                        List<SlotReference> projectInputSlots = nonEmptyOutputs.get(0);
                        ImmutableList.Builder<NamedExpression> projectsBuilder = ImmutableList.builder();
                        for (int i = 0; i < exceptOutput.size(); i++) {
                            ExprId id = exceptOutput.get(i).getExprId();
                            Alias alias = new Alias(id, projectInputSlots.get(i), exceptOutput.get(i).getName());
                            projectsBuilder.add(alias);
                        }
                        return new LogicalProject<>(projectsBuilder.build(), projectChild);
                    } else {
                        return except.withChildrenAndTheirOutputs(nonEmptyChildren, nonEmptyOutputsBuilder.build());
                    }
                }
            }).toRule(RuleType.ELIMINATE_EXCEPT_ON_EMPTYRELATION)
        );
    }

    private boolean hasEmptyRelationChild(LogicalJoin<?, ?> join) {
        return join.left() instanceof EmptyRelation || join.right() instanceof EmptyRelation;
    }

    private boolean bothChildrenEmpty(LogicalJoin<?, ?> join) {
        return join.left() instanceof EmptyRelation && join.right() instanceof EmptyRelation;
    }

    private boolean canReplaceJoinByEmptyRelation(LogicalJoin<?, ?> join) {
        return !join.isMarkJoin() && ((join.getJoinType() == JoinType.INNER_JOIN
            || join.getJoinType() == JoinType.LEFT_SEMI_JOIN
            || join.getJoinType() == JoinType.RIGHT_SEMI_JOIN
            || join.getJoinType() == JoinType.CROSS_JOIN)
            || (join.getJoinType() == JoinType.LEFT_OUTER_JOIN && join.left() instanceof EmptyRelation)
            || (join.getJoinType() == JoinType.RIGHT_OUTER_JOIN && join.right() instanceof EmptyRelation));
    }

}
