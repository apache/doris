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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merge the correlated predicate and agg in the filter under apply.
 * And keep the unCorrelated predicate under agg.
 * <p>
 * Use the correlated column as the group by column of agg,
 * the output column is the correlated column and the input column.
 * <pre>
 * before:
 *                 apply
 *             /          \
 *     Input(output:b)   Filter(this node's existence depends on having clause's existence)
 *                              |
 *                         agg(output:fn; group by:null)
 *                              |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *
 * end:
 *          apply(correlated predicate(Input.e = this.f))
 *         /              \
 * Input(output:b)   Filter(this node's existence depends on having clause's existence)
 *                             |
 *                        agg(output:fn,this.f; group by:this.f)
 *                              |
 *                    Filter(Uncorrelated predicate)
 * </pre>
 */
public class UnCorrelatedApplyAggregateFilter implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalApply(any(), logicalAggregate(logicalFilter()))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_AGGREGATE_FILTER),
                logicalApply(any(), logicalFilter(logicalAggregate(logicalFilter())))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_FILTER_AGGREGATE_FILTER));
    }

    private static LogicalApply<?, ?> pullUpCorrelatedFilter(LogicalApply<?, ?> apply) {
        boolean isRightChildAgg = apply.right() instanceof LogicalAggregate;
        // locate agg node
        LogicalAggregate<LogicalFilter<Plan>> agg =
                isRightChildAgg ? (LogicalAggregate<LogicalFilter<Plan>>) (apply.right())
                        : (LogicalAggregate<LogicalFilter<Plan>>) (apply.right().child(0));
        LogicalFilter<Plan> filter = agg.child();
        // split filter conjuncts to correlated and unCorrelated ones
        Map<Boolean, List<Expression>> split =
                Utils.splitCorrelatedConjuncts(filter.getConjuncts(), apply.getCorrelationSlot());
        List<Expression> correlatedPredicate = split.get(true);
        List<Expression> unCorrelatedPredicate = split.get(false);

        // the representative has experienced the rule and added the correlated predicate to the apply node
        if (correlatedPredicate.isEmpty()) {
            return apply;
        }

        // pull up correlated filter into apply node
        List<NamedExpression> newAggOutput = new ArrayList<>(agg.getOutputExpressions());
        List<Expression> newGroupby =
                Utils.getUnCorrelatedExprs(correlatedPredicate, apply.getCorrelationSlot());
        newGroupby.addAll(agg.getGroupByExpressions());
        Map<Expression, Slot> unCorrelatedExprToSlot = Maps.newHashMap();
        for (Expression expression : newGroupby) {
            if (expression instanceof Slot) {
                newAggOutput.add((NamedExpression) expression);
            } else {
                Alias alias = new Alias(expression);
                unCorrelatedExprToSlot.put(expression, alias.toSlot());
                newAggOutput.add(alias);
            }
        }
        correlatedPredicate = ExpressionUtils.replace(correlatedPredicate, unCorrelatedExprToSlot);
        LogicalAggregate newAgg = new LogicalAggregate<>(newGroupby, newAggOutput,
                PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child()));
        return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryExpr(),
                ExpressionUtils.optionalAnd(correlatedPredicate), apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(), apply.isInProject(),
                apply.isMarkJoinSlotNotNull(), apply.left(),
                isRightChildAgg ? newAgg : apply.right().withChildren(newAgg));
    }
}
