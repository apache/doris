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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * some check need to do after analyze whole plan.
 */
public class CheckAfterRewrite extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return any().then(plan -> {
            checkAllSlotReferenceFromChildren(plan);
            checkMetricTypeIsUsedCorrectly(plan);
            return null;
        }).toRule(RuleType.CHECK_ANALYSIS);
    }

    private void checkAllSlotReferenceFromChildren(Plan plan) {
        Set<Slot> notFromChildren = plan.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        Set<ExprId> childrenOutput = plan.children().stream()
                .flatMap(child -> Stream.concat(child.getOutput().stream(), child.getNonUserVisibleOutput().stream()))
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());
        notFromChildren = notFromChildren.stream()
                .filter(s -> !childrenOutput.contains(s.getExprId()))
                .collect(Collectors.toSet());
        notFromChildren = removeValidSlotsNotFromChildren(notFromChildren, childrenOutput);
        if (!notFromChildren.isEmpty()) {
            throw new AnalysisException(String.format("Input slot(s) not in child's output: %s in plan: %s",
                    StringUtils.join(notFromChildren.stream()
                            .map(ExpressionTrait::toSql)
                            .collect(Collectors.toSet()), ", "), plan));
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
        }
    }
}
