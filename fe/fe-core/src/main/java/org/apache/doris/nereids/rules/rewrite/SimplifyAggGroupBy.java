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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Remove deterministic grouping expressions whose inputs are already bare grouping slots.
 * <p>
 * GROUP BY ClientIP, ClientIP + 1, ClientIP + 2
 * -->
 * GROUP BY ClientIP
 *
 * <p>Retain existing bare slots so aggregate outputs can still reference them. A cast group key
 * cannot generally provide its original slot to those outputs. Never synthesize a slot from
 * derived keys, since doing so can split groups formed by non-injective expressions.</p>
 */
public class SimplifyAggGroupBy extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() > 1)
                .then(agg -> {
                    List<Expression> simplified = simplifyGroupBy(agg.getGroupByExpressions());
                    if (simplified == null) {
                        return null;
                    }
                    return agg.withGroupByAndOutput(simplified, agg.getOutputExpressions());
                })
                .toRule(RuleType.SIMPLIFY_AGG_GROUP_BY);
    }

    @VisibleForTesting
    protected static List<Expression> simplifyGroupBy(List<Expression> groupByExpressions) {
        Set<Expression> distinctGroupBy = new LinkedHashSet<>(groupByExpressions);
        Set<Expression> determinants = distinctGroupBy.stream()
                .filter(Slot.class::isInstance).collect(ImmutableSet.toImmutableSet());
        // Keep at least one key: removing all constant keys changes the result for empty input.
        if (!determinants.isEmpty()) {
            distinctGroupBy.removeIf(expression -> !(expression instanceof Slot)
                    && !expression.containsVolatileExpression()
                    && determinants.containsAll(expression.getInputSlots()));
        }
        return distinctGroupBy.size() == groupByExpressions.size() ? null : ImmutableList.copyOf(distinctGroupBy);
    }
}
