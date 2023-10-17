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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the aggregate child.
 * For example:
 * <pre>
 * Logical plan tree:
 *                filter (a>0 and b>0)
 *                   |
 *                group by(a, c)
 * transformed to:
 *              upper filter (b>0)
 *                   |
 *                group by(a, c)
 *                   |
 *              bottom filter (a>0)
 * </pre>
 * Note:
 * 'a>0' could be push down, because 'a' is in group by keys;
 * but 'b>0' could not push down, because 'b' is not in group by keys.
 */

public class PushdownFilterThroughAggregation extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalAggregate()).then(filter -> {
            LogicalAggregate<Plan> aggregate = filter.child();
            Set<Slot> canPushDownSlots = getCanPushDownSlots(aggregate);

            Set<Expression> pushDownPredicates = Sets.newHashSet();
            Set<Expression> filterPredicates = Sets.newHashSet();
            filter.getConjuncts().forEach(conjunct -> {
                Set<Slot> conjunctSlots = conjunct.getInputSlots();
                // NOTICE: filter not contain slot should not be pushed. e.g. 'a' = 'b'
                if (!conjunctSlots.isEmpty() && canPushDownSlots.containsAll(conjunctSlots)) {
                    pushDownPredicates.add(conjunct);
                } else {
                    filterPredicates.add(conjunct);
                }
            });
            if (pushDownPredicates.isEmpty()) {
                return null;
            }
            Plan bottomFilter = new LogicalFilter<>(pushDownPredicates, aggregate.child(0));
            aggregate = aggregate.withChildren(ImmutableList.of(bottomFilter));
            return PlanUtils.filterOrSelf(filterPredicates, aggregate);
        }).toRule(RuleType.PUSHDOWN_PREDICATE_THROUGH_AGGREGATION);
    }

    /**
     * get the slots that can be pushed down
     */
    public static Set<Slot> getCanPushDownSlots(LogicalAggregate<? extends Plan> aggregate) {
        Set<Slot> canPushDownSlots = new HashSet<>();
        if (aggregate.getSourceRepeat().isPresent()) {
            // When there is a repeat, the push-down condition is consistent with the repeat
            aggregate.getSourceRepeat().get().getCommonGroupingSetExpressions().stream()
                    .filter(Slot.class::isInstance)
                    .map(Slot.class::cast)
                    .forEach(canPushDownSlots::add);
        } else {
            for (Expression groupByExpression : aggregate.getGroupByExpressions()) {
                if (groupByExpression instanceof Slot) {
                    canPushDownSlots.add((Slot) groupByExpression);
                }
            }
        }
        return canPushDownSlots;
    }
}
