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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Merge the correlated predicate and agg in the filter under apply.
 * And keep the unCorrelated predicate under agg.
 * <p>
 * Use the correlated column as the group by column of agg,
 * the output column is the correlated column and the input column.
 * <pre>
 * before:
 *              apply
 *          /              \
 * Input(output:b)    agg(output:fn; group by:null)
 *                              |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *
 * end:
 *          apply(correlated predicate(Input.e = this.f))
 *         /              \
 * Input(output:b)    agg(output:fn,this.f; group by:this.f)
 *                              |
 *                    Filter(Uncorrelated predicate)
 * </pre>
 */
public class ApplyPullFilterOnAgg extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(group(), logicalAggregate(logicalFilter())).when(LogicalApply::isCorrelated).then(apply -> {
            LogicalAggregate<LogicalFilter<GroupPlan>> agg = apply.right();
            LogicalFilter<GroupPlan> filter = agg.child();
            List<Expression> predicates = ExpressionUtils.extractConjunction(filter.getPredicates());
            Map<Boolean, List<Expression>> split = Utils.splitCorrelatedConjuncts(
                    predicates, apply.getCorrelationSlot());
            List<Expression> correlatedPredicate = split.get(true);
            List<Expression> unCorrelatedPredicate = split.get(false);

            // the representative has experienced the rule and added the correlated predicate to the apply node
            if (correlatedPredicate.isEmpty()) {
                return apply;
            }

            List<NamedExpression> newAggOutput = new ArrayList<>(agg.getOutputExpressions());
            List<Expression> newGroupby = Utils.getCorrelatedSlots(correlatedPredicate,
                    apply.getCorrelationSlot());
            newGroupby.addAll(agg.getGroupByExpressions());
            newAggOutput.addAll(newGroupby.stream().map(NamedExpression.class::cast).collect(Collectors.toList()));
            LogicalAggregate newAgg = new LogicalAggregate<>(
                    newGroupby, newAggOutput,
                    PlanUtils.filterOrSelf(unCorrelatedPredicate, filter.child()));
            return new LogicalApply<>(apply.getCorrelationSlot(),
                    apply.getSubqueryExpr(),
                    ExpressionUtils.optionalAnd(correlatedPredicate),
                    apply.left(), newAgg);
        }).toRule(RuleType.APPLY_PULL_FILTER_ON_AGG);
    }
}
