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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Convert the expression in the filter into the output column corresponding to the child node and push it down.
 */
public class PushDownFilterThroughSetOperation extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalSetOperation()
            .when(s -> s.arity() > 0
                    || (s instanceof LogicalUnion && !((LogicalUnion) s).getConstantExprsList().isEmpty())))
            .thenApply(ctx -> {
                LogicalFilter<LogicalSetOperation> filter = ctx.root;
                LogicalSetOperation setOperation = filter.child();
                List<Plan> newChildren = new ArrayList<>();
                List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
                CascadesContext cascadesContext = ctx.cascadesContext;
                if (setOperation instanceof LogicalUnion) {
                    List<List<NamedExpression>> constantExprs = ((LogicalUnion) setOperation).getConstantExprsList();
                    StatementContext statementContext = ctx.statementContext;
                    List<List<NamedExpression>> newConstantExprs = new ArrayList<>();
                    addFiltersToNewChildren(setOperation, filter, constantExprs,
                            setOperation.getRegularChildrenOutputs(), cascadesContext,
                            newChildren, newRegularChildrenOutputs, newConstantExprs,
                            (rowIndex, columnIndex) -> constantExprs.get(rowIndex).get(columnIndex).toSlot(),
                            selectConstants -> new LogicalOneRowRelation(
                                    statementContext.getNextRelationId(), selectConstants)
                    );
                    addFiltersToNewChildren(setOperation, filter, setOperation.children(),
                            setOperation.getRegularChildrenOutputs(), cascadesContext,
                            newChildren, newRegularChildrenOutputs, newConstantExprs,
                            (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                            Function.identity());

                    List<NamedExpression> setOutputs = setOperation.getOutputs();
                    if (newChildren.isEmpty() && newConstantExprs.isEmpty()) {
                        return new LogicalEmptyRelation(
                                statementContext.getNextRelationId(), setOutputs
                        );
                    } else if (newChildren.isEmpty() && newConstantExprs.size() == 1) {
                        ImmutableList.Builder<NamedExpression> newOneRowRelationOutput
                                = ImmutableList.builderWithExpectedSize(newConstantExprs.size());
                        for (int i = 0; i < newConstantExprs.get(0).size(); i++) {
                            NamedExpression setOutput = setOutputs.get(i);
                            NamedExpression constantExpr = newConstantExprs.get(0).get(i);
                            Alias oneRowRelationOutput;
                            if (constantExpr instanceof Alias) {
                                oneRowRelationOutput = new Alias(setOutput.getExprId(),
                                        ((Alias) constantExpr).child(), setOutput.getName());
                            } else {
                                oneRowRelationOutput = new Alias(
                                        setOutput.getExprId(), constantExpr, setOutput.getName());
                            }
                            newOneRowRelationOutput.add(oneRowRelationOutput);
                        }
                        return new LogicalOneRowRelation(
                                ctx.statementContext.getNextRelationId(), newOneRowRelationOutput.build()
                        );
                    }

                    Builder<List<SlotReference>> newChildrenOutput
                            = ImmutableList.builderWithExpectedSize(newChildren.size());
                    for (Plan newChild : newChildren) {
                        newChildrenOutput.add((List) newChild.getOutput());
                    }

                    return ((LogicalUnion) setOperation).withChildrenAndConstExprsList(
                            newChildren, newRegularChildrenOutputs, newConstantExprs);
                }

                addFiltersToNewChildren(setOperation, filter, setOperation.children(),
                        setOperation.getRegularChildrenOutputs(),
                        cascadesContext, newChildren, newRegularChildrenOutputs, null,
                        (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                        Function.identity());
                return setOperation.withChildren(newChildren);
            }).toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_SET_OPERATION);
    }

    @VisibleForTesting
    protected static <T> void addFiltersToNewChildren(
            LogicalSetOperation setOperation, LogicalFilter<?> filter, List<T> children,
            List<List<SlotReference>> regulatorChildrenOutputs,
            CascadesContext cascadesContext, /* output */ List<Plan> newChildren,
            /* output */ List<List<SlotReference>> newRegulatorChildrenOutputs,
            /* output */ List<List<NamedExpression>> newConstantOutput,
            ChildOutputSupplier childOutputSupplier, Function<T, Plan> newChildBuilder) {
        for (int childIdx = 0; childIdx < children.size(); ++childIdx) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int i = 0; i < setOperation.getOutputs().size(); ++i) {
                NamedExpression output = setOperation.getOutputs().get(i);
                replaceMap.put(output, childOutputSupplier.getChildOutput(childIdx, i));
            }

            Set<Expression> newFilterPredicates = filter.getConjuncts()
                    .stream()
                    .map(conjunct -> ExpressionUtils.replace(conjunct, replaceMap))
                    .collect(ImmutableSet.toImmutableSet());
            Plan newChild = newChildBuilder.apply(children.get(childIdx));
            LogicalFilter<Plan> newFilter = new LogicalFilter<>(newFilterPredicates, newChild);
            // only union support constant exprs, so if not union, add to children list directly.
            if (!(setOperation instanceof LogicalUnion) || !(newChild instanceof LogicalOneRowRelation)) {
                newChildren.add(newFilter);
                // copy from original regulator list to avoid output order change.
                newRegulatorChildrenOutputs.add(regulatorChildrenOutputs.get(childIdx));
            } else {
                Plan eliminateFilter = EliminateFilter.eliminateFilterOnOneRowRelation(
                        (LogicalFilter) newFilter, cascadesContext);
                if (!(eliminateFilter instanceof EmptyRelation)) {
                    if (newConstantOutput != null && eliminateFilter instanceof LogicalOneRowRelation) {
                        LogicalOneRowRelation oneRowRelation = (LogicalOneRowRelation) eliminateFilter;
                        if (children.get(childIdx) instanceof LogicalPlan) {
                            // if it is come from children list, we should merge project with regulator outputs
                            Optional<List<NamedExpression>> constantOutput = PlanUtils.tryMergeProjections(
                                    oneRowRelation.getProjects(), regulatorChildrenOutputs.get(childIdx));
                            if (constantOutput.isPresent()) {
                                newConstantOutput.add(constantOutput.get());
                            } else {
                                newChildren.add(eliminateFilter);
                                newRegulatorChildrenOutputs.add(regulatorChildrenOutputs.get(childIdx));
                            }
                        } else {
                            newConstantOutput.add(oneRowRelation.getProjects());
                        }
                    } else {
                        newChildren.add(eliminateFilter);
                        if (children.get(childIdx) instanceof LogicalPlan) {
                            // copy from original regulator list to avoid output order change.
                            newRegulatorChildrenOutputs.add(regulatorChildrenOutputs.get(childIdx));
                        } else {
                            // this child from the original constant list, so need to generate new regulator outputs
                            newRegulatorChildrenOutputs.add((List) eliminateFilter.getOutput());
                        }
                    }
                }
            }
        }
    }

    /**
     * used in addFiltersToNewChildren to construct lambda to get child output from instance of different classes.
     */
    @VisibleForTesting
    protected interface ChildOutputSupplier {
        Expression getChildOutput(int rowIndex, int columnIndex);
    }
}
