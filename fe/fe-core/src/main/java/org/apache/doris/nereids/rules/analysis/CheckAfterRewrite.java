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

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * some check need to do after analyze whole plan.
 */
public class CheckAfterRewrite extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return any().then(plan -> {
            checkAllSlotReferenceFromChildren(plan);
            checkUnexpectedExpression(plan);
            checkMetricTypeIsUsedCorrectly(plan);
            checkMatchIsUsedCorrectly(plan);
            return null;
        }).toRule(RuleType.CHECK_ANALYSIS);
    }

    private void checkUnexpectedExpression(Plan plan) {
        if (plan.getExpressions().stream().anyMatch(e -> e.anyMatch(SubqueryExpr.class::isInstance))) {
            throw new AnalysisException("Subquery is not allowed in " + plan.getType());
        }
        if (!(plan instanceof Generate)) {
            if (plan.getExpressions().stream().anyMatch(e -> e.anyMatch(TableGeneratingFunction.class::isInstance))) {
                throw new AnalysisException("table generating function is not allowed in " + plan.getType());
            }
        }
        if (!(plan instanceof LogicalAggregate || plan instanceof LogicalWindow)) {
            if (plan.getExpressions().stream().anyMatch(e -> e.anyMatch(AggregateFunction.class::isInstance))) {
                throw new AnalysisException("aggregate function is not allowed in " + plan.getType());
            }
        }
        if (!(plan instanceof LogicalAggregate)) {
            if (plan.getExpressions().stream().anyMatch(e -> e.anyMatch(GroupingScalarFunction.class::isInstance))) {
                throw new AnalysisException("grouping scalar function is not allowed in " + plan.getType());
            }
        }
        if (!(plan instanceof LogicalWindow)) {
            if (plan.getExpressions().stream().anyMatch(e -> e.anyMatch(WindowExpression.class::isInstance))) {
                throw new AnalysisException("analytic function is not allowed in " + plan.getType());
            }
        }
    }

    private void checkAllSlotReferenceFromChildren(Plan plan) {
        Set<Slot> notFromChildren = plan.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        Set<ExprId> childrenOutput = plan.children().stream()
                .flatMap(child -> child.getOutput().stream())
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());
        notFromChildren = notFromChildren.stream()
                .filter(s -> !childrenOutput.contains(s.getExprId()))
                .collect(Collectors.toSet());
        notFromChildren = removeValidSlotsNotFromChildren(notFromChildren, childrenOutput);
        if (!notFromChildren.isEmpty()) {
            throw new AnalysisException(String.format("Input slot(s) not in child's output: %s in plan: %s,"
                            + " child output is: %s\n" + "plan tree:\n" + plan.treeString(),
                    StringUtils.join(notFromChildren.stream()
                            .map(ExpressionTrait::toString)
                            .collect(Collectors.toSet()), ", "), plan,
                    plan.children().stream()
                            .flatMap(child -> child.getOutput().stream())
                            .collect(Collectors.toSet())));
        }
    }

    private Set<Slot> removeValidSlotsNotFromChildren(Set<Slot> slots, Set<ExprId> childrenOutput) {
        return slots.stream()
                .filter(expr -> {
                    if (expr instanceof VirtualSlotReference) {
                        List<Expression> realExpressions = ((VirtualSlotReference) expr).getRealExpressions();
                        if (realExpressions.isEmpty()) {
                            // valid
                            return false;
                        }
                        return realExpressions.stream()
                                .map(Expression::getInputSlots)
                                .flatMap(Set::stream)
                                .anyMatch(realUsedExpr -> !childrenOutput.contains(realUsedExpr.getExprId()));
                    } else {
                        return !(expr instanceof SlotNotFromChildren);
                    }
                })
                .collect(Collectors.toSet());
    }

    private void checkMetricTypeIsUsedCorrectly(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            if (((LogicalAggregate<?>) plan).getGroupByExpressions().stream()
                    .anyMatch(expression -> expression.getDataType().isOnlyMetricType())) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
        } else if (plan instanceof LogicalSort) {
            if (((LogicalSort<?>) plan).getOrderKeys().stream().anyMatch((
                    orderKey -> orderKey.getExpr().getDataType()
                            .isOnlyMetricType()))) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
        } else if (plan instanceof LogicalTopN) {
            if (((LogicalTopN<?>) plan).getOrderKeys().stream().anyMatch((
                    orderKey -> orderKey.getExpr().getDataType()
                            .isOnlyMetricType()))) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
        } else if (plan instanceof LogicalWindow) {
            ((LogicalWindow<?>) plan).getWindowExpressions().forEach(a -> {
                if (!(a instanceof Alias && ((Alias) a).child() instanceof WindowExpression)) {
                    return;
                }
                WindowExpression windowExpression = (WindowExpression) ((Alias) a).child();
                if (windowExpression.getOrderKeys().stream().anyMatch((
                        orderKey -> orderKey.getDataType().isOnlyMetricType()))) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
                if (windowExpression.getPartitionKeys().stream().anyMatch((
                        partitionKey -> partitionKey.getDataType().isOnlyMetricType()))) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            });
        }
    }

    private void checkMatchIsUsedCorrectly(Plan plan) {
        if (plan.getExpressions().stream().anyMatch(
                expression -> expression instanceof Match)) {
            if (plan instanceof LogicalFilter && plan.child(0) instanceof LogicalOlapScan) {
                return;
            } else {
                throw new AnalysisException(String.format(
                    "Not support match in %s in plan: %s, only support in olapScan filter",
                    plan.child(0), plan));
            }
        }
    }
}
