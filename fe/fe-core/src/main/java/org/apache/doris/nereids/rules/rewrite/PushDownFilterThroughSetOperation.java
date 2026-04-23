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
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
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
import java.util.LinkedHashSet;
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
                LogicalFilter<LogicalSetOperation> origFilter = ctx.root;
                LogicalSetOperation setOperation = origFilter.child();

                // Pushing a conjunct that contains a volatile expression (rand/uuid/random_bytes/...)
                // into each branch changes semantics for every set-op except UNION ALL.
                // - UNION ALL: each branch row = exactly one output row (1:1), so evaluating
                //   rand() once per branch row still matches the per-output-row semantic.
                // - UNION DISTINCT / INTERSECT / EXCEPT: the set-op semantics depend on the
                //   full branch row sets before dedup/intersect/except. Sampling rows in each
                //   branch independently changes which rows participate (e.g. INTERSECT becomes
                //   "half of A intersect half of B" instead of "half of (A intersect B)").
                boolean canPushVolatileExpr = setOperation instanceof LogicalUnion
                        && setOperation.getQualifier() == Qualifier.ALL;
                Set<Expression> pushableConjuncts;
                Set<Expression> keptAboveConjuncts;
                if (canPushVolatileExpr) {
                    pushableConjuncts = origFilter.getConjuncts();
                    keptAboveConjuncts = ImmutableSet.of();
                } else {
                    pushableConjuncts = new LinkedHashSet<>();
                    Set<Expression> kept = new LinkedHashSet<>();
                    for (Expression c : origFilter.getConjuncts()) {
                        if (c.containsVolatileExpression()) {
                            kept.add(c);
                        } else {
                            pushableConjuncts.add(c);
                        }
                    }
                    keptAboveConjuncts = kept;
                    if (pushableConjuncts.isEmpty()) {
                        return null;
                    }
                }
                LogicalFilter<LogicalSetOperation> filter = pushableConjuncts == origFilter.getConjuncts()
                        ? origFilter
                        : new LogicalFilter<>(ImmutableSet.copyOf(pushableConjuncts), setOperation);

                List<Plan> newChildren = new ArrayList<>();
                List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
                CascadesContext cascadesContext = ctx.cascadesContext;
                Plan rewritten;
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
                        rewritten = new LogicalEmptyRelation(
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
                        rewritten = new LogicalOneRowRelation(
                                ctx.statementContext.getNextRelationId(), newOneRowRelationOutput.build()
                        );
                    } else {
                        Builder<List<SlotReference>> newChildrenOutput
                                = ImmutableList.builderWithExpectedSize(newChildren.size());
                        for (Plan newChild : newChildren) {
                            newChildrenOutput.add((List) newChild.getOutput());
                        }

                        rewritten = ((LogicalUnion) setOperation).withChildrenAndConstExprsList(
                                newChildren, newRegularChildrenOutputs, newConstantExprs);
                    }
                } else {
                    addFiltersToNewChildren(setOperation, filter, setOperation.children(),
                            setOperation.getRegularChildrenOutputs(),
                            cascadesContext, newChildren, newRegularChildrenOutputs, null,
                            (rowIndex, columnIndex) -> setOperation.getRegularChildOutput(rowIndex).get(columnIndex),
                            Function.identity());
                    rewritten = setOperation.withChildren(newChildren);
                }

                if (keptAboveConjuncts.isEmpty()) {
                    return rewritten;
                }
                return new LogicalFilter<>(ImmutableSet.copyOf(keptAboveConjuncts), rewritten);
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
