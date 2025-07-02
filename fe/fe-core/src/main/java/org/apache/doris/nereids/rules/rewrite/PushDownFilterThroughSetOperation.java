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
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                CascadesContext cascadesContext = ctx.cascadesContext;
                if (setOperation instanceof LogicalUnion) {
                    List<List<NamedExpression>> constantExprs = ((LogicalUnion) setOperation).getConstantExprsList();
                    StatementContext statementContext = ctx.statementContext;
                    List<List<NamedExpression>> newConstantExprs = new ArrayList<>();
                    addFiltersToNewChildren(
                            setOperation, filter, constantExprs, cascadesContext, newChildren, newConstantExprs,
                            (rowIndex, columnIndex) -> constantExprs.get(rowIndex).get(columnIndex).toSlot(),
                            selectConstants -> new LogicalOneRowRelation(
                                    statementContext.getNextRelationId(), selectConstants)
                    );
                    addFiltersToNewChildren(setOperation, filter, setOperation.children(),
                            cascadesContext, newChildren, newConstantExprs,
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

                    return new LogicalUnion(setOperation.getQualifier(), setOutputs,
                            newChildrenOutput.build(), newConstantExprs, false, newChildren);
                }

                addFiltersToNewChildren(setOperation, filter, setOperation.children(),
                        cascadesContext, newChildren, null,
                        (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                        Function.identity());
                return setOperation.withChildren(newChildren);
            }).toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_SET_OPERATION);
    }

    private <T> void addFiltersToNewChildren(
            LogicalSetOperation setOperation, LogicalFilter<?> filter, List<T> children,
            CascadesContext cascadesContext, /* output */ List<Plan> newChildren,
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
            if (newChild instanceof LogicalOneRowRelation) {
                Plan eliminateFilter = EliminateFilter.eliminateFilterOnOneRowRelation(
                        (LogicalFilter) newFilter, cascadesContext);
                if (!(eliminateFilter instanceof EmptyRelation)) {
                    if (newConstantOutput != null && eliminateFilter instanceof LogicalOneRowRelation) {
                        newConstantOutput.add(((LogicalOneRowRelation) eliminateFilter).getProjects());
                    } else {
                        newChildren.add(eliminateFilter);
                    }
                } else if (!(setOperation instanceof LogicalUnion)) {
                    newChildren.add(newFilter);
                }
            } else {
                newChildren.add(newFilter);
            }
        }
    }

    private interface ChildOutputSupplier {
        Expression getChildOutput(int rowIndex, int columnIndex);
    }
}
