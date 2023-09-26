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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.Optional;

/**
 * Convert scalarApply to LogicalJoin.
 * <p>
 * UnCorrelated -> CROSS_JOIN
 * Correlated -> LEFT_OUTER_JOIN
 */
public class ScalarApplyToJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply().when(LogicalApply::isScalar).then(apply -> {
            if (apply.isCorrelated()) {
                return correlatedToJoin(apply);
            } else {
                return unCorrelatedToJoin(apply);
            }
        }).toRule(RuleType.SCALAR_APPLY_TO_JOIN);
    }

    private Plan unCorrelatedToJoin(LogicalApply apply) {
        LogicalAssertNumRows assertNumRows = new LogicalAssertNumRows<>(
                new AssertNumRowsElement(
                        1, apply.getSubqueryExpr().toString(),
                        apply.isInProject()
                            ? AssertNumRowsElement.Assertion.EQ : AssertNumRowsElement.Assertion.LE),
                (LogicalPlan) apply.right());
        return new LogicalJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.EMPTY_CONDITION,
                JoinHint.NONE,
                apply.getMarkJoinSlotReference(),
                (LogicalPlan) apply.left(), assertNumRows);
    }

    private Plan correlatedToJoin(LogicalApply apply) {
        Optional<Expression> correlationFilter = apply.getCorrelationFilter();

        if (correlationFilter.isPresent()) {
            ExpressionUtils.extractConjunction(correlationFilter.get()).stream()
                    .filter(e -> !(e instanceof EqualTo))
                    .forEach(e -> {
                        throw new AnalysisException(
                                "scalar subquery's correlatedPredicates's operator must be EQ");
                    });
        } else {
            throw new AnalysisException("correlationFilter can't be null in correlatedToJoin");
        }

        return new LogicalJoin<>(
                apply.isNeedAddSubOutputToProjects() ? JoinType.LEFT_OUTER_JOIN : JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.extractConjunction(correlationFilter.get()),
                JoinHint.NONE,
                apply.getMarkJoinSlotReference(),
                apply.children());
    }
}
